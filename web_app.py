#!/usr/bin/env python3
"""ETH EMA 预警系统 - Web 应用服务"""
import json
import os
import sys
import time
import traceback
from datetime import datetime

from flask import Flask, render_template, jsonify, request, redirect, flash

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import monitor as mon

app = Flask(__name__)
app.secret_key = 'eth-ema-alert-secret-key'

PRICE_PUSH_INTERVALS = [
    {'value': 30, 'label': '30秒'},
    {'value': 60, 'label': '1分钟'},
    {'value': 300, 'label': '5分钟'},
    {'value': 600, 'label': '10分钟'},
    {'value': 1800, 'label': '30分钟'},
    {'value': 3600, 'label': '1小时'},
    {'value': 7200, 'label': '2小时'},
    {'value': 14400, 'label': '4小时'},
    {'value': 28800, 'label': '8小时'},
    {'value': 43200, 'label': '12小时'},
]

def format_interval(seconds):
    if seconds < 60:
        return f"{seconds}秒"
    elif seconds < 3600:
        return f"{seconds//60}分钟"
    else:
        hours = seconds / 3600
        if hours == int(hours):
            return f"{int(hours)}小时"
        return f"{hours}小时"

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/state')
def api_state():
    try:
        states = mon.get_all_states()
        last_update = mon.get_last_update_time()
        status = mon.get_connection_status()
        source_health = mon.get_source_health()
        
        cfg = mon.load_config()
        push_interval = cfg.get('feishu', {}).get('price_push_interval_seconds', 14400)
        
        now = time.time()
        if now - last_update > 35:
            mon.update_all_data()
            states = mon.get_all_states()
            last_update = mon.get_last_update_time()
        
        latest_price = None
        for tf in ['5m', '15m', '30m', '1h', '4h']:
            if tf in states and states[tf]:
                latest_price = states[tf].get('price')
                break
        
        data = {
            'states': states,
            'status': status,
            'source_health': source_health,
            'last_update': last_update,
            'update_time_str': datetime.fromtimestamp(last_update).strftime('%Y-%m-%d %H:%M:%S'),
            'price_push_interval': push_interval,
            'price_push_interval_label': format_interval(push_interval),
            'price_push_interval_options': PRICE_PUSH_INTERVALS,
            'latest_price': latest_price,
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/set_push_interval')
def api_set_push_interval():
    try:
        interval = request.args.get('interval', '14400')
        seconds = int(interval)
        
        valid_intervals = [30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200]
        if seconds not in valid_intervals:
            return jsonify({'success': False, 'error': '无效的间隔值，仅支持: 30秒/1分钟/5分钟/10分钟/30分钟/1小时/2小时/4小时/8小时/12小时'})
        
        cfg = mon.load_config()
        cfg['feishu']['price_push_interval_seconds'] = seconds
        mon.save_config(cfg)
        
        global _last_price_push_time
        _last_price_push_time = 0
        
        return jsonify({
            'success': True, 
            'interval': seconds, 
            'interval_label': format_interval(seconds),
            'message': f'推送间隔已设置为 {format_interval(seconds)}'
        })
    except Exception as e:
        mon.logger.error(f"设置推送间隔失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/alerts')
def api_alerts():
    limit = int(request.args.get('limit', 10))
    alerts = mon.get_recent_alerts(limit)
    return jsonify(alerts)

@app.route('/api/delete_alert', methods=['POST'])
def api_delete_alert():
    try:
        data = request.get_json()
        timestamp = data.get('timestamp')
        if mon.delete_alert_by_timestamp(timestamp):
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '未找到记录'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/clear_alerts', methods=['POST'])
def api_clear_alerts():
    try:
        mon.clear_all_alerts()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/add_price_range', methods=['POST'])
def api_add_price_range():
    try:
        data = request.get_json()
        name = data.get('name', '')
        low = float(data.get('low', 0))
        high = float(data.get('high', 0))
        
        if low <= 0 or high <= 0 or low >= high:
            return jsonify({'success': False, 'error': '无效的价格范围'})
        
        cfg = mon.load_config()
        if 'price_ranges' not in cfg or not isinstance(cfg['price_ranges'], list):
            cfg['price_ranges'] = []
        
        cfg['price_ranges'].append({
            'name': name if name else f'区间{len(cfg["price_ranges"])+1}',
            'low': low,
            'high': high,
            'enabled': True,
        })
        mon.save_config(cfg)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/toggle_price_range', methods=['POST'])
def api_toggle_price_range():
    try:
        data = request.get_json()
        index = int(data.get('index', -1))
        
        cfg = mon.load_config()
        if isinstance(cfg.get('price_ranges'), list) and 0 <= index < len(cfg['price_ranges']):
            cfg['price_ranges'][index]['enabled'] = not cfg['price_ranges'][index].get('enabled', True)
            mon.save_config(cfg)
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '无效的索引'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/delete_price_range', methods=['POST'])
def api_delete_price_range():
    try:
        data = request.get_json()
        index = int(data.get('index', -1))
        
        cfg = mon.load_config()
        if isinstance(cfg.get('price_ranges'), list) and 0 <= index < len(cfg['price_ranges']):
            cfg['price_ranges'].pop(index)
            mon.save_config(cfg)
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '无效的索引'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/test_alert', methods=['POST'])
def api_test_alert():
    try:
        cfg = mon.load_config()
        price = mon.get_latest_price()
        if price:
            subject = f"[ETH EMA 测试预警] · ${price:.2f}"
            body_text = f"测试预警消息\n当前价格: ${price:.2f}\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            ok = mon.send_feishu(subject, body_text, cfg)
            if ok:
                return jsonify({'success': True, 'message': '测试预警已发送到飞书'})
            return jsonify({'success': False, 'error': '飞书推送失败'})
        return jsonify({'success': False, 'error': '暂无价格数据'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/refresh_data')
def api_refresh_data():
    try:
        mon.update_all_data()
        return jsonify({'success': True, 'message': '数据刷新完成'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.errorhandler(404)
def page_not_found(e):
    return jsonify({'error': '页面未找到'}), 404

@app.errorhandler(Exception)
def handle_exception(e):
    mon.logger.error(f"未捕获异常: {e}\n{traceback.format_exc()}")
    return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 50)
    print("🚀 启动 ETH EMA 预警系统 Web 服务")
    print("=" * 50)
    
    import threading
    def start_monitor():
        time.sleep(5)
        mon.start_monitor_in_background()
    
    t = threading.Thread(target=start_monitor, daemon=True)
    t.start()
    
    time.sleep(40)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
