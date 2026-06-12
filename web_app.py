#!/usr/bin/env python3
"""ETH EMA 预警系统 - Web 应用服务"""
import json
import os
import sys
import time
import traceback
from datetime import datetime

from flask import Flask, render_template, jsonify, request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import monitor as mon

app = Flask(__name__)
app.secret_key = 'eth-ema-alert-secret-key'

# ========== 页面路由 ==========
@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/history')
def history():
    return render_template('history.html')

# ========== 获取系统状态（前端主要 API）==========
@app.route('/api/state')
def api_state():
    try:
        cfg = mon.load_config()
        push_interval = cfg.get('feishu', {}).get('price_push_interval_seconds', 14400)
        now = time.time()
        last_update = mon.get_last_update_time()

        # 数据过期则后台异步更新，但立即返回当前状态不阻塞
        data_age = now - last_update
        if data_age > 40 and last_update > 0:
            try:
                mon.update_all_data()
            except Exception:
                pass
        elif last_update == 0:
            # 完全没有数据，快速初始化一次
            try:
                mon.update_all_data()
            except Exception:
                pass

        # 再次获取
        states = mon.get_all_states()
        last_update = mon.get_last_update_time()
        status = mon.get_connection_status()
        source_health = mon.get_source_health()

        # 提取最新价格
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
            'update_time_str': datetime.fromtimestamp(last_update).strftime('%Y-%m-%d %H:%M:%S') if last_update > 0 else '--',
            'price_push_interval': push_interval,
            'latest_price': latest_price,
            'price_ranges': cfg.get('price_ranges', []),
            'config': cfg,
            'data_age_sec': int(data_age),
        }
        return jsonify(data)
    except Exception as e:
        mon.logger.error(f"获取状态失败: {e}")
        return jsonify({'error': str(e)}), 500

# ========== 设置推送间隔 ==========
@app.route('/api/set_push_interval')
def api_set_push_interval():
    try:
        interval = request.args.get('interval', '14400')
        seconds = int(interval)

        valid_intervals = [30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200]
        if seconds not in valid_intervals:
            return jsonify({'success': False, 'error': '无效的间隔值'})

        cfg = mon.load_config()
        if 'feishu' not in cfg:
            cfg['feishu'] = {}
        cfg['feishu']['price_push_interval_seconds'] = seconds
        mon.save_config(cfg)

        return jsonify({'success': True, 'interval': seconds})
    except Exception as e:
        mon.logger.error(f"设置推送间隔失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ========== 保存全部系统设置 ==========
@app.route('/api/save_config', methods=['POST'])
def api_save_config():
    try:
        data = request.get_json() or {}

        webhook = str(data.get('feishu_webhook', '')).strip()
        ema_short = int(data.get('ema_short', 180))
        ema_long = int(data.get('ema_long', 250))
        push_interval = int(data.get('push_interval', 14400))
        cooldown_seconds = int(data.get('cooldown_seconds', 600))
        enabled_timeframes = data.get('enabled_timeframes', [])

        if ema_short >= ema_long:
            return jsonify({'success': False, 'error': 'EMA短周期必须小于长周期'})

        cfg = mon.load_config()

        # 确保各部分的配置
        if 'ema_alert' not in cfg:
            cfg['ema_alert'] = {}
        cfg['ema_alert']['ema_short'] = ema_short
        cfg['ema_alert']['ema_long'] = ema_long
        cfg['ema_alert']['enabled_timeframes'] = list(enabled_timeframes) if isinstance(enabled_timeframes, list) else []

        # 飞书配置
        if 'feishu' not in cfg:
            cfg['feishu'] = {}
        cfg['feishu']['webhook'] = webhook
        cfg['feishu']['price_push_interval_seconds'] = push_interval

        # 预警配置
        if 'alert' not in cfg:
            cfg['alert'] = {}
        cfg['alert']['cooldown_seconds'] = cooldown_seconds

        mon.save_config(cfg)

        return jsonify({'success': True, 'message': '设置保存成功', 'config': cfg})
    except Exception as e:
        mon.logger.error(f"保存配置失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ========== 预警历史 ==========
@app.route('/api/alerts')
def api_alerts():
    try:
        limit = int(request.args.get('limit', 20))
        alerts = mon.get_recent_alerts(limit)
        return jsonify(alerts)
    except Exception as e:
        return jsonify([])

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

# ========== 价格区间管理 ==========
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

# ========== 测试预警推送 ==========
@app.route('/api/test_alert', methods=['POST'])
def api_test_alert():
    try:
        cfg = mon.load_config()
        price = mon.get_latest_price()
        if price:
            ok = mon.send_feishu(
                f"[测试预警] ETH 当前价格",
                f"测试预警消息\n当前价格: ${price:.2f}\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                cfg
            )
            if ok:
                return jsonify({'success': True, 'message': '测试预警已发送到飞书'})
            return jsonify({'success': False, 'error': '飞书推送失败，请检查 webhook 配置'})
        return jsonify({'success': False, 'error': '暂无价格数据'})
    except Exception as e:
        mon.logger.error(f"测试预警失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ========== 手动刷新数据 ==========
@app.route('/api/refresh_data')
def api_refresh_data():
    try:
        mon.update_all_data()
        return jsonify({'success': True, 'message': '数据刷新完成'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ========== 健康检查 (用于 UptimeRobot) ==========
@app.route('/health')
def health_check():
    try:
        last_update = mon.get_last_update_time()
        now = time.time()
        if now - last_update > 60:
            try:
                mon.update_all_data()
            except Exception:
                pass
        return jsonify({'ok': True, 'last_update': last_update})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# ========== 错误处理 ==========
@app.errorhandler(404)
def page_not_found(e):
    return jsonify({'error': '页面未找到'}), 404

@app.errorhandler(Exception)
def handle_exception(e):
    mon.logger.error(f"未捕获异常: {e}\n{traceback.format_exc()}")
    return jsonify({'error': str(e)}), 500

# ========== 启动 ==========
if __name__ == '__main__':
    print("=" * 50)
    print("🚀 启动 ETH EMA 预警系统 Web 服务")
    print("=" * 50)

    import threading
    def start_monitor():
        time.sleep(3)
        mon.start_monitor_in_background()

    t = threading.Thread(target=start_monitor, daemon=True)
    t.start()

    time.sleep(30)

    port = int(os.environ.get('PORT', 5000))
    print(f"✅ Web 服务启动，访问 http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
