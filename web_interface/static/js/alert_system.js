/**
 * 股票预警系统前端JavaScript
 * 实时获取并显示预警弹窗
 */

class StockAlertSystem {
    constructor() {
        this.alertCheckInterval = 30000; // 30秒检查一次
        this.shownAlerts = new Set(); // 已显示的预警ID
        this.alertContainer = null;
        this.init();
    }

    init() {
        // 创建预警容器
        this.createAlertContainer();

        // 开始轮询预警
        this.startAlertPolling();

        // 加载历史预警
        this.loadStoredAlerts();
    }

    createAlertContainer() {
        // 创建预警显示容器
        const container = document.createElement('div');
        container.id = 'alert-container';
        container.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 10000;
            max-width: 400px;
        `;
        document.body.appendChild(container);
        this.alertContainer = container;

        // 创建预警历史按钮
        this.createAlertHistoryButton();
    }

    createAlertHistoryButton() {
        const button = document.createElement('button');
        button.id = 'alert-history-btn';
        button.innerHTML = '📢 预警历史';
        button.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 9999;
            padding: 10px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 14px;
            font-weight: bold;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            transition: all 0.3s ease;
        `;

        button.addEventListener('click', () => this.showAlertHistory());
        button.addEventListener('mouseenter', () => {
            button.style.transform = 'translateY(-2px)';
            button.style.boxShadow = '0 6px 20px rgba(0,0,0,0.3)';
        });
        button.addEventListener('mouseleave', () => {
            button.style.transform = 'translateY(0)';
            button.style.boxShadow = '0 4px 15px rgba(0,0,0,0.2)';
        });

        document.body.appendChild(button);
    }

    async startAlertPolling() {
        // 立即检查一次
        await this.checkNewAlerts();

        // 定时检查
        setInterval(() => {
            this.checkNewAlerts();
        }, this.alertCheckInterval);
    }

    async checkNewAlerts() {
        try {
            const response = await fetch('/api/alerts/realtime/');
            const data = await response.json();

            if (data.success && data.alerts) {
                for (const alert of data.alerts) {
                    const alertKey = `${alert.stock_code}_${alert.alert_time}_${alert.type}`;

                    // 只显示未显示过的预警
                    if (!this.shownAlerts.has(alertKey)) {
                        this.showAlert(alert);
                        this.shownAlerts.add(alertKey);
                        this.storeAlert(alertKey);
                    }
                }
            }
        } catch (error) {
            console.error('获取预警失败:', error);
        }
    }

    showAlert(alert) {
        // 创建预警卡片
        const card = document.createElement('div');
        card.className = 'alert-card';

        const levelColors = {
            'CRITICAL': {bg: '#FF5252', icon: '🚨'},
            'WARNING': {bg: '#FFA726', icon: '⚠️'},
            'INFO': {bg: '#42A5F5', icon: 'ℹ️'}
        };

        const levelConfig = levelColors[alert.level] || levelColors.INFO;

        card.style.cssText = `
            background: white;
            border-left: 5px solid ${levelConfig.bg};
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            animation: slideIn 0.3s ease-out;
            cursor: pointer;
            transition: all 0.3s ease;
        `;

        card.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: start;">
                <div style="flex: 1;">
                    <div style="font-size: 18px; margin-bottom: 5px;">
                        ${levelConfig.icon} <strong>${alert.stock_name || alert.stock_code}</strong>
                    </div>
                    <div style="color: #666; font-size: 14px; margin-bottom: 8px;">
                        ${alert.message}
                    </div>
                    <div style="color: #999; font-size: 12px;">
                        ${new Date(alert.alert_time).toLocaleString('zh-CN')}
                    </div>
                </div>
                <button class="close-btn" style="
                    background: none;
                    border: none;
                    font-size: 20px;
                    color: #999;
                    cursor: pointer;
                    padding: 0 5px;
                ">×</button>
            </div>
        `;

        // 鼠标悬停效果
        card.addEventListener('mouseenter', () => {
            card.style.transform = 'translateX(-5px)';
            card.style.boxShadow = '0 6px 16px rgba(0,0,0,0.2)';
        });
        card.addEventListener('mouseleave', () => {
            card.style.transform = 'translateX(0)';
            card.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
        });

        // 点击卡片跳转到股票详情
        card.addEventListener('click', (e) => {
            if (!e.target.classList.contains('close-btn')) {
                window.location.href = `/stocks/${alert.stock_code}/`;
            }
        });

        // 关闭按钮
        const closeBtn = card.querySelector('.close-btn');
        closeBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            card.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => card.remove(), 300);
        });

        this.alertContainer.appendChild(card);

        // 5秒后自动淡出
        setTimeout(() => {
            card.style.opacity = '0.8';
        }, 5000);

        // 10秒后自动移除
        setTimeout(() => {
            if (card.parentElement) {
                card.style.animation = 'slideOut 0.3s ease-out';
                setTimeout(() => card.remove(), 300);
            }
        }, 10000);
    }

    storeAlert(alertKey) {
        // 存储到localStorage
        const stored = JSON.parse(localStorage.getItem('shownAlerts') || '[]');
        stored.push(alertKey);

        // 只保留最近100条
        if (stored.length > 100) {
            stored.shift();
        }

        localStorage.setItem('shownAlerts', JSON.stringify(stored));
    }

    loadStoredAlerts() {
        // 从localStorage加载已显示的预警
        const stored = JSON.parse(localStorage.getItem('shownAlerts') || '[]');
        this.shownAlerts = new Set(stored);
    }

    async showAlertHistory() {
        // 创建历史预警弹窗
        const modal = document.createElement('div');
        modal.id = 'alert-history-modal';
        modal.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.5);
            z-index: 10001;
            display: flex;
            align-items: center;
            justify-content: center;
        `;

        const modalContent = document.createElement('div');
        modalContent.style.cssText = `
            background: white;
            width: 90%;
            max-width: 800px;
            max-height: 80vh;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
        `;

        modalContent.innerHTML = `
            <div style="padding: 20px; border-bottom: 1px solid #ddd; display: flex; justify-content: space-between; align-items: center;">
                <h2 style="margin: 0;">预警历史</h2>
                <button id="close-modal" style="background: none; border: none; font-size: 30px; cursor: pointer; color: #666;">×</button>
            </div>
            <div id="alert-history-content" style="padding: 20px; overflow-y: auto; max-height: calc(80vh - 100px);">
                <div style="text-align: center; color: #999;">加载中...</div>
            </div>
        `;

        modal.appendChild(modalContent);
        document.body.appendChild(modal);

        // 关闭弹窗
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });

        document.getElementById('close-modal').addEventListener('click', () => {
            modal.remove();
        });

        // 加载所有预警
        await this.loadAllAlerts();
    }

    async loadAllAlerts() {
        try {
            // 获取所有股票的预警
            const stocks = window.stockList || []; // 假设有全局股票列表
            const allAlerts = [];

            // 并行获取所有股票的预警
            const promises = stocks.map(stock =>
                fetch(`/api/alerts/${stock.code}/`)
                    .then(res => res.json())
                    .then(data => {
                        if (data.success && data.alerts) {
                            allAlerts.push(...data.alerts);
                        }
                    })
                    .catch(err => console.error(`获取${stock.code}预警失败:`, err))
            );

            await Promise.all(promises);

            // 按时间排序
            allAlerts.sort((a, b) => new Date(b.alert_time) - new Date(a.alert_time));

            // 显示预警列表
            this.renderAlertHistory(allAlerts);

        } catch (error) {
            console.error('加载预警历史失败:', error);
        }
    }

    renderAlertHistory(alerts) {
        const container = document.getElementById('alert-history-content');

        if (alerts.length === 0) {
            container.innerHTML = '<div style="text-align: center; color: #999;">暂无预警记录</div>';
            return;
        }

        const levelColors = {
            'CRITICAL': '#FF5252',
            'WARNING': '#FFA726',
            'INFO': '#42A5F5'
        };

        const html = alerts.map(alert => `
            <div style="
                border-left: 4px solid ${levelColors[alert.alert_level] || '#999'};
                padding: 15px;
                margin-bottom: 10px;
                background: #f9f9f9;
                border-radius: 4px;
                cursor: pointer;
                transition: all 0.2s;
            " onmouseover="this.style.background='#f0f0f0'" onmouseout="this.style.background='#f9f9f9'"
               onclick="window.location.href='/stocks/${alert.stock_code}/'">
                <div style="font-weight: bold; margin-bottom: 5px;">
                    ${alert.stock_name || alert.stock_code} - ${alert.alert_type}
                </div>
                <div style="color: #666; font-size: 14px; margin-bottom: 5px;">
                    ${alert.alert_message}
                </div>
                <div style="color: #999; font-size: 12px;">
                    ${new Date(alert.alert_time).toLocaleString('zh-CN')}
                </div>
            </div>
        `).join('');

        container.innerHTML = html;
    }
}

// CSS动画
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }

    .alert-card:hover {
        transform: translateX(-5px) !important;
    }
`;
document.head.appendChild(style);

// 页面加载后初始化
document.addEventListener('DOMContentLoaded', () => {
    window.stockAlertSystem = new StockAlertSystem();
});
