#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gemini Live API WebSocket 透明代理蓝图

挂载到 xianbao_psql(app.py) 复用其 gunicorn/flask 实例。
提供 Gemini Live API 的 WebSocket 双向转发，让国内用户无需代理即可实时对话。

Key 传递方式（与旧版 gemini_proxy.py 一致）：
    客户端在 WebSocket URL 里带 key，代理透传给 Google，服务端不存 key。
    wss://willai.eu.org/gemini/ws?key=AIza...

访问:
    /gemini/ws   — WebSocket 端点
    /gemini/     — 前端测试页面

架构:
    前端 wss → flask-sock (gthread worker) → websocket-client → Google wss
    两个线程分别转发 c2s / s2c，连接级隔离。
"""
import os
import json
import threading
import logging
from flask import Blueprint, send_from_directory, request
from flask_sock import Sock
from simple_websocket import ConnectionClosed
import websocket  # websocket-client
try:
    from websocket import WebSocketTimeoutException
except ImportError:
    WebSocketTimeoutException = Exception

logger = logging.getLogger("gemini_live")

# 上游 Google Gemini Live WebSocket 基础 URL
GOOGLE_LIVE_BASE = (
    "wss://generativelanguage.googleapis.com/ws/"
    "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
)

# 蓝图 + Sock 实例（app.py 里 init_app）
gemini_bp = Blueprint(
    "gemini_live",
    __name__,
    url_prefix="/gemini",
    static_folder="static",
)
sock = Sock()


def _get_api_key():
    """
    优先从客户端 query 参数取 key（透传模式），
    其次从环境变量取（服务端注入模式，可选）。
    """
    key = request.args.get("key", "").strip()
    if key:
        return key
    return os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY", "")



def _upstream_keepalive(upstream, stop_event):
    """Send WebSocket protocol pings to Google to keep the upstream session alive."""
    while not stop_event.wait(30):
        try:
            upstream.ping("gemini-relay-keepalive")
        except Exception as e:
            logger.warning("[keepalive] upstream.ping error: %s", e)
            break


def _relay_c2s(ws, upstream):
    """前端 → Google 转发线程"""
    try:
        while True:
            try:
                msg = ws.receive(timeout=300)  # 5 分钟无消息则退出
            except ConnectionClosed:
                break
            except Exception:
                break
            if msg is None:
                break
            # 心跳保活
            if isinstance(msg, str) and msg.strip() == "ping":
                try:
                    ws.send("pong")
                except Exception:
                    break
                continue
            try:
                upstream.send(msg)
            except Exception as e:
                logger.warning("[c2s] upstream.send error: %s", e)
                break
    except Exception as e:
        logger.error("[c2s] relay fatal: %s", e)
    finally:
        try:
            upstream.close()
        except Exception:
            pass
        try:
            ws.close()
        except Exception:
            pass


def _relay_s2c(ws, upstream):
    """Google -> client relay thread with idle timeout tolerance."""
    consecutive_timeouts = 0
    try:
        while True:
            try:
                data = upstream.recv()
                consecutive_timeouts = 0
            except WebSocketTimeoutException:
                consecutive_timeouts += 1
                if consecutive_timeouts >= 2:
                    logger.warning("[s2c] upstream idle timeout after keepalive, closing")
                    break
                continue
            except Exception as e:
                logger.warning("[s2c] upstream.recv error: %s", e)
                break
            if data is None or len(data) == 0:
                break
            try:
                ws.send(data)
            except ConnectionClosed:
                break
            except Exception as e:
                logger.warning("[s2c] ws.send error: %s", e)
                break
    except Exception as e:
        logger.error("[s2c] relay fatal: %s", e)
    finally:
        try:
            upstream.close()
        except Exception:
            pass
        try:
            ws.close()
        except Exception:
            pass


def _relay_session(ws):
    """WebSocket transparent relay. Client passes key in URL."""
    api_key = _get_api_key()
    if not api_key:
        ws.send(json.dumps({"error": "No API key. Pass ?key=AIza... in WebSocket URL"}))
        ws.close()
        return

    upstream_url = f"{GOOGLE_LIVE_BASE}?key={api_key}"

    try:
        upstream = websocket.create_connection(
            upstream_url,
            timeout=30,
            enable_multithread=True,
        )
        upstream.settimeout(120)
    except Exception as e:
        ws.send(json.dumps({"error": f"Failed to connect upstream: {e}"}))
        ws.close()
        return

    logger.info("[gemini_ws] upstream connected, starting relay threads")

    stop_event = threading.Event()
    t_keepalive = threading.Thread(target=_upstream_keepalive, args=(upstream, stop_event), daemon=True)
    t_c2s = threading.Thread(target=_relay_c2s, args=(ws, upstream), daemon=True)
    t_s2c = threading.Thread(target=_relay_s2c, args=(ws, upstream), daemon=True)
    t_keepalive.start()
    t_c2s.start()
    t_s2c.start()
    try:
        t_c2s.join()
        t_s2c.join()
    finally:
        stop_event.set()

    logger.info("[gemini_ws] session ended")


@sock.route("/ws", bp=gemini_bp)
def gemini_ws(ws):
    """wss://host/gemini/ws?key=... endpoint"""
    _relay_session(ws)


@sock.route("/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent")
def gemini_ws_standard(ws):
    """Standard Gemini Live path for app default config"""
    _relay_session(ws)

@gemini_bp.route("/")
def gemini_index():
    """前端测试页面"""
    return send_from_directory(gemini_bp.static_folder, "index.html")
