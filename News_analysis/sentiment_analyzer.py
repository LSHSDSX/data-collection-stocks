#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
深度情感分析模块
使用LLM对新闻进行深度情感评分，输出-1到+1的连续值
"""
import json
import logging
import asyncio
import httpx
import redis
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os
import hashlib

# 设置日志
# 修改点1: 给FileHandler添加 encoding='utf-8'，防止日志文件乱码
# 修改点2: StreamHandler 可能会在Windows下因为特殊字符报错，尽量避免在日志内容中使用特殊Unicode符号
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sentiment_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DeepSentimentAnalyzer:
    """深度情感分析器 - 使用LLM进行情感评分"""

    def __init__(self, config_path=None):
        """初始化情感分析器"""
        # 获取配置文件路径
        if config_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            config_path = os.path.join(project_root, 'config', 'config.json')

        self.config_path = config_path
        self.load_config()

        # 连接Redis
        self.redis_client = redis.Redis(
            host=self.config['redis_config'].get('host', 'localhost'),
            port=self.config['redis_config'].get('port', 6379),
            db=self.config['redis_config'].get('db', 0),
            password=self.config['redis_config'].get('password'),
            decode_responses=True
        )

        # 连接MySQL
        self.mysql_conn = mysql.connector.connect(
            host=self.config['mysql_config']['host'],
            user=self.config['mysql_config']['user'],
            password=self.config['mysql_config']['password'],
            database=self.config['mysql_config']['database']
        )

        # HTTP客户端
        self.http_client = None

        # 创建情感数据表
        self.create_sentiment_table()

    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info("配置文件加载成功")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def create_sentiment_table(self):
        """创建情感分析数据表"""
        try:
            cursor = self.mysql_conn.cursor()

            # 创建新闻情感表
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS news_sentiment (
                id INT AUTO_INCREMENT PRIMARY KEY,
                news_hash VARCHAR(64) UNIQUE NOT NULL COMMENT '新闻哈希值',
                news_content TEXT COMMENT '新闻内容',
                news_datetime DATETIME COMMENT '新闻时间',
                sentiment_score DECIMAL(5,4) COMMENT '情感评分(-1到+1)',
                sentiment_label VARCHAR(20) COMMENT '情感标签(positive/negative/neutral)',
                confidence DECIMAL(5,4) COMMENT '置信度(0到1)',
                emotion_type VARCHAR(50) COMMENT '情绪类型(optimistic/pessimistic/cautious等)',
                key_sentiment_words TEXT COMMENT '关键情感词(JSON数组)',
                related_stocks TEXT COMMENT '相关股票代码(JSON数组)',
                analysis_detail TEXT COMMENT '详细分析(JSON)',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_news_datetime (news_datetime),
                INDEX idx_sentiment_score (sentiment_score),
                INDEX idx_news_hash (news_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新闻深度情感分析表';
            """

            cursor.execute(create_table_sql)
            self.mysql_conn.commit()
            logger.info("情感分析数据表创建成功")

            cursor.close()
        except Exception as e:
            logger.error(f"创建情感分析表失败: {e}")

    async def initialize(self):
        """初始化异步资源"""
        self.http_client = httpx.AsyncClient(timeout=60.0)

    async def close(self):
        """关闭资源"""
        if self.http_client:
            await self.http_client.aclose()
        if self.mysql_conn:
            self.mysql_conn.close()

    def generate_news_hash(self, news: Dict) -> str:
        """生成新闻哈希值"""
        content = news.get('content', '')
        datetime_str = news.get('datetime', '')
        hash_str = f"{content}|{datetime_str}"
        return hashlib.md5(hash_str.encode('utf-8')).hexdigest()

    async def analyze_sentiment_with_llm(self, news: Dict) -> Optional[Dict]:
        """使用LLM进行深度情感分析"""
        try:
            news_content = news.get('content', '')
            news_datetime = news.get('datetime', '')

            # 构建情感分析提示词
            prompt = f"""
请对以下财经新闻进行深度情感分析。作为专业的金融情感分析师，请分析新闻的整体情绪倾向。

新闻内容：
{news_content}
发布时间：{news_datetime}

请从以下维度进行分析，并以JSON格式返回：

1. **sentiment_score**: 情感评分，-1（极度负面）到 +1（极度正面）的连续值
   - -1.0 到 -0.6: 极度负面（恐慌、崩盘预期等）
   - -0.6 到 -0.2: 负面（担忧、风险警告等）
   - -0.2 到 +0.2: 中性（平稳、观望等）
   - +0.2 到 +0.6: 正面（乐观、机会等）
   - +0.6 到 +1.0: 极度正面（狂热、重大利好等）

2. **sentiment_label**: 情感标签 (positive/negative/neutral)

3. **confidence**: 情感判断的置信度 (0-1)

4. **emotion_type**: 主要情绪类型
   - optimistic（乐观）
   - pessimistic（悲观）
   - cautious（谨慎）
   - panic（恐慌）
   - enthusiastic（热情）
   - uncertain（不确定）

5. **key_sentiment_words**: 关键情感词列表（3-5个词）

6. **market_impact**: 对市场的影响评估
   - direction: "bullish"（看涨）/ "bearish"（看跌）/ "neutral"（中性）
   - intensity: "low"（低）/ "medium"（中）/ "high"（高）

7. **reasoning**: 情感判断的简要理由（50字以内）

请以以下JSON格式返回：
{{
    "sentiment_score": 0.75,
    "sentiment_label": "positive",
    "confidence": 0.85,
    "emotion_type": "optimistic",
    "key_sentiment_words": ["增长", "突破", "利好"],
    "market_impact": {{
        "direction": "bullish",
        "intensity": "high"
    }},
    "reasoning": "新闻传达明确利好信息，市场反应积极"
}}
"""

            # 调用LLM API
            ai_config = self.config.get('ai_config', {})
            provider = ai_config.get('provider', 'qwen')

            if provider == 'qwen':
                result = await self._call_qwen_api(prompt)
            elif provider == 'deepseek':
                result = await self._call_deepseek_api(prompt)
            else:
                logger.error(f"不支持的AI提供商: {provider}")
                return None

            return result

        except Exception as e:
            logger.error(f"LLM情感分析失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def _call_qwen_api(self, prompt: str) -> Optional[Dict]:
        """调用通义千问API"""
        try:
            ai_config = self.config['ai_config']
            api_key = ai_config.get('api_key')
            api_base = ai_config.get('api_base', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
            model = ai_config.get('model', 'qwen-plus')

            api_url = f"{api_base}/chat/completions"

            response = await self.http_client.post(
                api_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "你是一位专业的金融情感分析师，擅长从新闻中识别情绪倾向和市场影响。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1  # 降低温度以获得更一致的结果
                }
            )

            if response.status_code == 200:
                result = response.json()
                ai_response = result['choices'][0]['message']['content']

                # 解析JSON
                try:
                    return json.loads(ai_response)
                except json.JSONDecodeError:
                    # 尝试提取JSON部分
                    import re
                    json_pattern = r'```json\s*(.*?)\s*```|(\{.*\})'
                    match = re.search(json_pattern, ai_response, re.DOTALL)
                    if match:
                        json_str = match.group(1) if match.group(1) else match.group(2)
                        return json.loads(json_str)
                    logger.error(f"无法解析LLM响应: {ai_response}")
                    return None
            else:
                logger.error(f"API请求失败 (状态码: {response.status_code}): {response.text}")
                return None

        except Exception as e:
            logger.error(f"调用千问API失败: {e}")
            return None

    async def _call_deepseek_api(self, prompt: str) -> Optional[Dict]:
        """调用DeepSeek API"""
        try:
            ai_config = self.config['ai_config']
            api_key = ai_config.get('api_key')
            api_base = ai_config.get('api_base', 'https://api.deepseek.com')
            api_version = ai_config.get('api_version', 'v1')
            model = ai_config.get('model', 'deepseek-chat')

            api_url = f"{api_base}/{api_version}/chat/completions"

            response = await self.http_client.post(
                api_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "你是一位专业的金融情感分析师，擅长从新闻中识别情绪倾向和市场影响。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"}
                }
            )

            if response.status_code == 200:
                result = response.json()
                ai_response = result['choices'][0]['message']['content']
                return json.loads(ai_response)
            else:
                logger.error(f"API请求失败 (状态码: {response.status_code}): {response.text}")
                return None

        except Exception as e:
            logger.error(f"调用DeepSeek API失败: {e}")
            return None

    def save_sentiment_to_db(self, news: Dict, sentiment_result: Dict, news_hash: str):
        """保存情感分析结果到数据库"""
        try:
            cursor = self.mysql_conn.cursor()

            # 准备数据
            news_content = news.get('content', '')
            news_datetime = news.get('datetime', '')

            # 解析日期时间
            try:
                dt = datetime.strptime(news_datetime, '%Y-%m-%d %H:%M:%S')
            except:
                dt = datetime.now()

            sentiment_score = sentiment_result.get('sentiment_score', 0)
            sentiment_label = sentiment_result.get('sentiment_label', 'neutral')
            confidence = sentiment_result.get('confidence', 0.5)
            emotion_type = sentiment_result.get('emotion_type', 'neutral')
            key_words = json.dumps(sentiment_result.get('key_sentiment_words', []), ensure_ascii=False)
            analysis_detail = json.dumps(sentiment_result, ensure_ascii=False)

            # 插入数据（使用ON DUPLICATE KEY UPDATE避免重复）
            insert_sql = """
            INSERT INTO news_sentiment
            (news_hash, news_content, news_datetime, sentiment_score, sentiment_label,
             confidence, emotion_type, key_sentiment_words, analysis_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            sentiment_score = VALUES(sentiment_score),
            sentiment_label = VALUES(sentiment_label),
            confidence = VALUES(confidence),
            emotion_type = VALUES(emotion_type),
            key_sentiment_words = VALUES(key_sentiment_words),
            analysis_detail = VALUES(analysis_detail)
            """

            cursor.execute(insert_sql, (
                news_hash, news_content, dt, sentiment_score, sentiment_label,
                confidence, emotion_type, key_words, analysis_detail
            ))

            self.mysql_conn.commit()
            logger.info(f"情感分析结果已保存到数据库 (score: {sentiment_score})")
            cursor.close()

        except Exception as e:
            logger.error(f"保存情感数据到数据库失败: {e}")
            import traceback
            traceback.print_exc()

    async def analyze_all_news(self, limit=50):
        """分析所有新闻的情感"""
        try:
            # 从Redis获取新闻
            news_list = self.redis_client.lrange('stock:hot_news', 0, limit - 1)

            if not news_list:
                logger.warning("Redis中没有新闻数据")
                return

            logger.info(f"获取到 {len(news_list)} 条新闻，开始情感分析...")

            analyzed_count = 0
            failed_count = 0

            for news_json in news_list:
                try:
                    news = json.loads(news_json)
                    news_hash = self.generate_news_hash(news)

                    # 检查是否已分析
                    if self._is_analyzed(news_hash):
                        logger.info(f"新闻 {news_hash[:8]} 已分析过，跳过")
                        continue

                    # 进行情感分析
                    logger.info(f"分析新闻: {news.get('content', '')[:50]}...")
                    sentiment_result = await self.analyze_sentiment_with_llm(news)

                    if sentiment_result:
                        # 保存到数据库
                        self.save_sentiment_to_db(news, sentiment_result, news_hash)
                        analyzed_count += 1

                        # 打印结果
                        # 修改点3: 将特殊的打钩符号 '✓' 替换为 '[OK]' 或 '[+]'
                        logger.info(f"[OK] 情感评分: {sentiment_result.get('sentiment_score')}, "
                                    f"标签: {sentiment_result.get('sentiment_label')}, "
                                    f"情绪: {sentiment_result.get('emotion_type')}")
                    else:
                        failed_count += 1
                        # 修改点4: 将特殊的打叉符号 '✗' 替换为 '[FAIL]' 或 '[-]'
                        logger.warning(f"[FAIL] 新闻分析失败")

                    # 短暂延迟，避免API限流
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"处理新闻时出错: {e}")
                    failed_count += 1

            logger.info(f"\n情感分析完成！成功: {analyzed_count}, 失败: {failed_count}")

        except Exception as e:
            logger.error(f"分析新闻情感时出错: {e}")
            import traceback
            traceback.print_exc()

    def _is_analyzed(self, news_hash: str) -> bool:
        """检查新闻是否已分析"""
        try:
            cursor = self.mysql_conn.cursor()
            cursor.execute("SELECT 1 FROM news_sentiment WHERE news_hash = %s", (news_hash,))
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except:
            return False


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='新闻深度情感分析')
    parser.add_argument('--limit', type=int, default=50, help='分析的新闻数量')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("启动深度情感分析系统")
    logger.info("=" * 60)

    analyzer = DeepSentimentAnalyzer()
    await analyzer.initialize()

    try:
        await analyzer.analyze_all_news(limit=args.limit)
    finally:
        await analyzer.close()

    logger.info("情感分析系统结束")


if __name__ == '__main__':
    asyncio.run(main())