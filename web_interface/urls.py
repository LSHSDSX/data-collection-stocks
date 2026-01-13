from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('stocks/', views.stock_list, name='stock_list'),
    path('stocks/<str:stock_code>/', views.stock_detail, name='stock_detail'),
    path('news/', views.news_list, name='news_list'),
    path('settings/', views.settings_page, name='settings'),
    path('trade_history/', views.trade_history_page, name='trade_history'),
    path('api/stocks/', views.api_stock_data, name='api_stock_data'),
    path('api/stocks/<str:stock_code>/', views.api_stock_data, name='api_stock_detail'),
    path('api/news/', views.api_news_data, name='api_news_data'),
    path('api/stocks/<str:stock_code>/realtime/', views.get_realtime_data, name='get_realtime_data'),
    path('api/search-stock/', views.search_stock, name='search_stock'),
    path('api/add-stock/', views.add_stock, name='add_stock'),
    # 预警API
    path('api/alerts/realtime/', views.api_get_realtime_alerts, name='api_realtime_alerts'),
    path('api/alerts/<str:stock_code>/', views.api_get_stock_alerts, name='api_stock_alerts'),
    # GPR预测API
    path('api/gpr/<str:stock_code>/', views.api_get_gpr_predictions, name='api_gpr_predictions'),
    # 情感分析API
    path('api/sentiment/<str:stock_code>/', views.api_get_stock_sentiment, name='api_stock_sentiment'),
]