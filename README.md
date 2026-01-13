# 股票智能分析系统

一个基于 Django 的实时股票数据采集、分析和预警系统，整合了新闻情感分析、技术指标分析、机器学习预测和多因子预警功能。

## 项目简介

本项目在 [Real-time_stock_analysis](https://github.com/666zyb/Real-time_stock_analysis) 的基础上进行了全面改进和功能扩展，打造了一个功能完整的智能股票分析平台。

### 主要改进和新增功能

#### 1. 数据采集优化
- **多源数据集成**：整合搜狐证券、Tushare等数据源
- **高效爬虫系统**：并发采集新浪财经、同花顺、36氪、财联社等4个新闻源
- **智能去重机制**：基于Redis的新闻去重和存储管理

#### 2. 核心功能新增

**异构关联算法（价格异动触发新闻检索）**
- 实时监控股价异动（涨跌幅超过阈值）
- 自动触发相关新闻检索
- 计算价格变化与新闻事件的关联度
- 实现代码：`News_analysis/price_news_correlator.py`

**LLM 情感分析**
- 使用阿里云通义千问（Qwen）大语言模型
- 深度分析新闻对股票的情感影响
- 输出连续值情感评分（-1 到 +1）
- 实现代码：`News_analysis/sentiment_analyzer.py`

**GPR 建模（高斯过程回归）**
- 基于历史价格、技术指标、情感数据的股价预测
- 预测未来5天的股价走势
- 提供预测置信区间
- 实现代码：`indicator_analysis/gpr_predictor.py`

**多因子预警系统**
- 整合价格、技术指标、情感、GPR预测的综合预警
- 多维度监控：涨跌幅、成交量、RSI超买超卖、情感极值
- 可配置预警阈值
- 实现代码：`indicator_analysis/multi_factor_alert.py`

**数据处理增强**
- 技术指标计算：RSI、MACD、MA、布林带等10+种指标
- 股票决策分析：综合评分系统（0-100分）
- 买卖信号生成器
- 实现代码：`indicator_analysis/indicators_analysis.py`、`stock_analysis_decision.py`

#### 3. 页面优化
- **现代化 UI 设计**：基于 Bootstrap 5 的响应式界面
- **实时数据更新**：WebSocket 推送机制
- **丰富的可视化**：ECharts 动态图表（K线图、MACD、RSI等）
- **智能预警展示**：实时预警列表和历史记录
- **新闻-股票关联展示**：直观的关联度可视化

## 技术架构

### 后端技术栈
- **Web 框架**：Django 4.2+ + Channels（WebSocket支持）
- **数据库**：MySQL（股票数据）+ Redis（缓存和消息队列）
- **数据处理**：Pandas, NumPy, SciPy
- **机器学习**：Scikit-learn（高斯过程回归）
- **技术指标**：TA-lib
- **AI 模型**：阿里云通义千问 Qwen-Plus
- **爬虫**：AIOHTTP, BeautifulSoup

### 前端技术栈
- **UI 框架**：Bootstrap 5 + Bootstrap Icons
- **数据可视化**：ECharts 5.x
- **实时通信**：WebSocket

### 数据流架构

```
┌─────────────────────────────────────────────────────────────┐
│                      数据采集层                                │
├─────────────────────────────────────────────────────────────┤
│  搜狐证券  │  Tushare  │  新闻爬虫（4个源） │                  │
│     ↓           ↓              ↓                              │
│  MySQL      MySQL         Redis                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      分析处理层                                │
├─────────────────────────────────────────────────────────────┤
│  技术指标分析  │  LLM情感分析  │  异构关联算法  │  GPR预测   │
│  RSI/MACD/MA  │  情感评分     │  价格-新闻关联  │  价格预测   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      预警决策层                                │
├─────────────────────────────────────────────────────────────┤
│  多因子预警系统  │  股票决策分析器  │  综合评分（0-100）      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      展示层                                    │
├─────────────────────────────────────────────────────────────┤
│  Django Web  │  ECharts可视化  │  WebSocket实时推送          │
└─────────────────────────────────────────────────────────────┘
```

## 项目结构

```
final/
├── News_analysis/              # 新闻分析模块
│   ├── news_stock_analysis.py      # AI新闻-股票关联分析
│   ├── sentiment_analyzer.py       # 深度情感分析
│   └── price_news_correlator.py    # 异构关联算法
│
├── News_crawler/               # 新闻爬虫模块
│   ├── hot_News_data.py            # 新闻存储基类
│   ├── 新浪财经.py                  # 新浪财经爬虫
│   ├── 同花顺.py                    # 同花顺爬虫
│   ├── kr_36氪.py                   # 36氪爬虫
│   └── 财联社.py                    # 财联社爬虫
│
├── indicator_analysis/         # 技术指标分析模块
│   ├── indicators_analysis.py       # 技术指标计算器
│   ├── gpr_predictor.py             # 高斯过程回归预测
│   ├── multi_factor_alert.py        # 多因子预警系统
│   └── stock_analysis_decision.py   # 股票决策分析器
│
├── data/                       # 数据采集模块
│   ├── 搜狐证券.py                   # 搜狐证券爬虫
│   └── tushare基本面数据.py          # Tushare基本面数据
│
├── web_interface/              # Django Web应用
│   ├── views.py                    # 视图函数
│   ├── urls.py                     # URL路由
│   ├── models.py                   # 数据模型
│   ├── consumers.py                # WebSocket消费者
│   └── services/                   # 服务层
│       ├── chart_service.py        # 图表生成服务
│       └── news_service.py         # 新闻数据服务
│
├── templates/                  # HTML模板
│   ├── index.html                  # 首页
│   ├── stock_list.html             # 股票列表
│   ├── stock_detail.html           # 股票详情
│   ├── news_list.html              # 新闻列表
│   └── settings.html               # 设置页面
│
├── static/                     # 静态资源
│   ├── css/                        # 样式文件
│   └── js/                         # JavaScript文件
│
├── config/                     # 配置文件
│   └── config.json                 # 主配置文件
│
├── run_data_collection.py      # 数据采集启动脚本
├── manage.py                   # Django管理命令
├── requirements                # Python依赖
└── README.md                   # 项目说明文档
```

## 安装和配置

### 环境要求

- Python 3.9+
- MySQL 5.7+
- Redis 6.0+

### 安装步骤

1. **克隆项目**
```bash
git clone https://github.com/LSHSDSX/data-collection-stocks.git
cd final
```

2. **安装依赖**
```bash
pip install -r requirements
```

3. **配置文件设置**

编辑 `config/config.json`：
```json
{
  "mysql_config": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "stock_analysis"
  },
  "redis_config": {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": null
  },
  "ai_config": {
    "provider": "qwen",
    "api_key": "your_qwen_api_key",
    "api_base": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "model": "qwen-plus"
  }
}
```

4. **初始化数据库**
```bash
# 1. 启动所有Docker容器
docker-compose up -d

# 2. 查看容器状态
docker-compose ps

# 3. 等待MySQL容器完全启动（约10-20秒）
docker logs stock_mysql

# 4. 进入Django容器执行迁移
docker exec -it <django容器名> python manage.py migrate
# 或者如果在本地运行Django
python manage.py migrate
```


### 运行系统

 **启动 Web 服务**
```bash
python manage.py runserver 0.0.0.0:8010
```

访问：`http://localhost:8010`


## 使用说明

### 主要功能模块

#### 1. 股票监控
- **路径**：`/stocks/`
- **功能**：查看配置的股票列表，实时价格和涨跌幅
- **特点**：自动刷新，颜色标识涨跌

#### 2. 股票详情
- **路径**：`/stocks/<股票代码>/`
- **功能**：
  - K线图（日线/周线/月线）
  - 技术指标图表（MACD、RSI、布林带等）
  - GPR 股价预测曲线
  - 价格-情感关联分析图
  - 相关新闻列表（带情感评分）
  - 综合决策建议

#### 3. 新闻中心
- **路径**：`/news/`
- **功能**：
  - 热点新闻列表
  - 新闻情感分析结果
  - 新闻与股票的关联度
  - 新闻来源和时间筛选

#### 4. 预警中心
- **路径**：首页预警面板
- **功能**：
  - 实时预警推送（WebSocket）
  - 多维度预警：价格异动、技术指标、情感突变
  - 预警历史记录
  - 预警详情查看

#### 5. 系统设置
- **路径**：`/settings/`
- **功能**：
  - 股票池配置
  - 预警阈值设置
  - 数据更新频率配置
  - 系统参数调整

### API 接口

#### 获取股票列表
```
GET /api/stocks/
```

#### 获取股票详情
```
GET /api/stocks/<code>/
```

#### 获取实时预警
```
GET /api/alerts/
```

#### 获取 GPR 预测
```
GET /api/gpr/<code>/
```

#### 获取情感分析
```
GET /api/sentiment/<code>/
```

## 核心模块说明

### 1. 异构关联算法（Price-News Correlator）

**功能**：监控股价异动，自动触发新闻检索和关联分析

**工作流程**：
1. 实时监控股票价格变化
2. 检测价格异动（涨跌幅超过阈值，默认3%）
3. 触发新闻检索，获取相关时间段的新闻
4. 使用 LLM 分析新闻与价格变化的关联性
5. 计算关联度评分并存储

**关键参数**：
- `price_change_threshold`: 价格异动阈值（默认0.03，即3%）
- `time_window`: 新闻检索时间窗口（默认24小时）
- `correlation_threshold`: 关联度阈值（默认0.5）

### 2. LLM 情感分析（Sentiment Analyzer）

**功能**：使用大语言模型深度分析新闻情感

**特点**：
- 使用阿里云通义千问 Qwen-Plus 模型
- 输出连续值情感评分（-1 到 +1）
- -1：极度负面，0：中性，+1：极度正面
- 支持批量分析和缓存

**分析维度**：
- 新闻标题情感
- 新闻内容情感
- 对特定股票的影响程度
- 短期和长期影响评估

### 3. GPR 建模（Gaussian Process Regression）

**功能**：基于多维数据的股价预测

**输入特征**：
- 历史价格数据（收盘价、最高价、最低价）
- 技术指标（RSI、MACD、MA等）
- 新闻情感评分
- 成交量数据

**输出**：
- 未来5天股价预测值
- 预测置信区间（上界和下界）
- 预测准确度评估

**核心算法**：
```python
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel

# 核函数：常数核 × 径向基函数核
kernel = ConstantKernel(1.0) * RBF(length_scale=1.0)
gpr = GaussianProcessRegressor(kernel=kernel, alpha=1e-5)
```

### 4. 多因子预警系统（Multi-Factor Alert）

**功能**：综合多个维度进行智能预警

**预警因子**：

| 因子 | 阈值 | 预警条件 |
|------|------|---------|
| 价格涨幅 | 3% | 单日涨幅 > 3% |
| 价格跌幅 | 3% | 单日跌幅 > 3% |
| RSI 超买 | 70 | RSI > 70 |
| RSI 超卖 | 30 | RSI < 30 |
| 成交量异动 | 2倍 | 成交量 > 5日均量 × 2 |
| 情感极值 | ±0.7 | 情感评分 > 0.7 或 < -0.7 |
| GPR 预测异常 | 5% | 预测涨跌幅 > 5% |

**预警等级**：
- 🔴 **严重**：多个因子同时触发
- 🟠 **警告**：单个重要因子触发
- 🟡 **提示**：单个一般因子触发

### 5. 技术指标分析

**支持的技术指标**：

- **趋势指标**：MA（移动平均线）、EMA（指数移动平均线）
- **动量指标**：RSI（相对强弱指数）、MACD（指数平滑异同移动平均线）
- **波动指标**：布林带、ATR（真实波动幅度均值）
- **成交量指标**：OBV（能量潮）、成交量变化率

**指标计算示例**：
```python
# RSI 计算
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# MACD 计算
def calculate_macd(prices):
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram
```

## 配置的股票池

系统默认配置了 20 只股票，覆盖多个行业：

### 半导体行业（6只）
- 中芯国际 (688981)
- 兆易创新 (603986)
- 北京君正 (300223)
- 北方华创 (002371)
- 卓胜微 (300782)
- 中微公司 (688012)

### 人工智能行业（6只）
- 科大讯飞 (002230)
- 中科创达 (300496)
- 海康威视 (002415)
- 紫光股份 (000938)
- 中科曙光 (603019)
- 广联达 (002410)

### 新能源行业（4只）
- 宁德时代 (300750)
- 比亚迪 (002594)
- 隆基绿能 (601012)
- 阳光电源 (300274)

### 其他行业（4只）
- 招商银行 (600036)
- 工商银行 (601398)
- 中国联通 (600050)
- 中国石油 (601857)

可在 `config/config.json` 中的 `stocks` 字段添加或修改股票。



## 性能优化建议

1. **数据库索引**：在 `stock_code` 和 `date` 字段上创建索引
2. **Redis 缓存**：合理设置 TTL，避免内存溢出
3. **并发控制**：使用 Celery 处理耗时任务
4. **API 限流**：配置 Django REST framework 限流器
5. **静态文件**：使用 Nginx 或 CDN 托管静态资源

## 开发计划

- [ ] 支持更多技术指标（KDJ、CCI等）
- [ ] 增加回测系统
- [ ] 实现自动交易接口
- [ ] 支持自定义预警策略
- [ ] 移动端适配
- [ ] 多用户支持和权限管理

## 致谢

本项目基于 [Real-time_stock_analysis](https://github.com/666zyb/Real-time_stock_analysis) 进行开发和改进。

感谢以下开源项目：
- Django
- ECharts
- Bootstrap
- TA-Lib
- Scikit-learn
- 阿里云通义千问

## 许可证

MIT License

## 联系方式

如有问题或建议，请提交 Issue 或 Pull Request。

---

**⚠️ 免责声明**：本系统仅供学习和研究使用，不构成任何投资建议。股市有风险，投资需谨慎。使用本系统进行投资决策所产生的任何后果，由使用者自行承担。
