#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
海尼曼点读 Flask 蓝图 — 挂载到线报项目(app.py) 复用其 gunicorn/flask 实例
提供 8 个 API 代理 + 静态前端 (5 海尼曼 + 3 RAZ)
访问:  /hnm/            (点读页面首页)
       /hnm/api/levels  (级别)
       /hnm/api/books/<level>
       /hnm/api/book/<id>
       /hnm/api/quiz/<id>
       /hnm/api/dict/<id>
       /hnm/api/raz_levels   (RAZ 级别)
       /hnm/api/raz_books/<level>
       /hnm/api/raz_book/<id>
"""
import json, time, hashlib, base64, math
import requests
from flask import Blueprint, Response, send_from_directory

# 蓝图自身自带 static_folder，静态文件放在 hnm_blue/static/
# 它不与你现有 Flask 默认 static/ 冲突，绝对隔离
hnm_bp = Blueprint(
    "hnm",
    __name__,
    url_prefix="/hnm",
    static_folder="static",
)

OID = "f033fb75b159cd5"          # 真实设备 oid
VERSION = "5.1"; BUILD = "5010"
HEADERS = {"Referer": "app:dxyyhnm", "User-Agent": "okhttp/4.9.0"}

def md5(s): return hashlib.md5(s.encode()).hexdigest()

def build_cookie():
    now_ms = int(time.time() * 1000)
    m = md5(f"{OID}&{int(now_ms / 1000)}")[:10]
    return " ".join([
        f"version_{VERSION}", f"build_{BUILD}", "pro_0", "lan_zh", f"oid_{OID}",
        "package_com.diiiapp.hnm", "platform_android", "ver_12",
        "model_SM-A5360", "mf_samsung", "t_1700000000000",
        f"t2_{now_ms}", "channel_", f"md5_{m}", "uid_0",
    ])

def decodeData(data, ev):
    """还原 ServerDataDao.decodeData: 分块重排 + 双层 Base64"""
    iv = int(ev[2:4]); iv2 = int(ev[4:5])
    sub = data[iv:]; L = len(sub)
    i = L % iv or iv; i2 = L - i
    chunks = [sub[iv * j: iv * (j + 1)] for j in range((L - i) // iv)]
    size = len(chunks); d = size / iv2
    ce = int(math.ceil(d)); fl = int(math.floor(d)); i5 = size % iv2
    arr2 = [ce if j < i5 else fl for j in range(iv2)]
    arr3 = [0]; acc = 0
    for j in range(iv2 - 1):
        acc += arr2[j]; arr3.append(acc)
    sb = []
    for j in range(ce):
        for k in range(iv2):
            v3 = arr3[k % iv2] + j
            if j < arr2[k % iv2] and v3 < size: sb.append(chunks[v3])
    sb.append(sub[i2:i2 + i])
    return base64.b64decode(base64.b64decode("".join(sb))).decode("utf-8", "ignore")

def post_json(url, payload=None):
    data = {"appCookie": build_cookie()}
    if payload: data.update(payload)
    r = requests.post(url, data=data, headers=HEADERS, timeout=15)
    j = r.json()
    dd = j.get("data")
    if isinstance(dd, dict) and dd.get("data") and dd.get("ev"):
        return _json_loads(decodeData(dd["data"], dd["ev"]))
    return j

def _json_loads(s):
    try:
        import json; return json.loads(s)
    except Exception:
        return s

def _pick(j):
    if isinstance(j, dict) and "data" in j and isinstance(j["data"], (list, dict)):
        return j["data"]
    return j

# ============ 8 个 API (5 海尼曼 + 3 RAZ) ============
@hnm_bp.route("/api/levels")
def api_levels():
    j = post_json("https://api.xuexd.cn/app/huiben/levels")
    return json.dumps(_pick(j), ensure_ascii=False)

@hnm_bp.route("/api/raz_levels")
def api_raz_levels():
    j = post_json("https://api.xuexd.cn/app/book/hnm_raz_levels/ver/3")
    return json.dumps(_pick(j), ensure_ascii=False)

@hnm_bp.route("/api/raz_books/<level>")
def api_raz_books(level):
    d = post_json(f"https://api.xuexd.cn/app/book/hnm_raz_level/level/{level}/ver/3")
    return json.dumps(_pick(d), ensure_ascii=False)

@hnm_bp.route("/api/raz_book/<path:entry_id>")
def api_raz_book(entry_id):
    return json.dumps(post_json(f"https://api.xuexd.cn/app/huiben/entry/id/{entry_id}/ver/3/hui"), ensure_ascii=False)

@hnm_bp.route("/api/books/<level>")
def api_books(level):
    d = post_json(f"https://api.xuexd.cn/app/huiben/level/ver/0/level/{level}")
    return json.dumps(_pick(d), ensure_ascii=False)

@hnm_bp.route("/api/book/<path:entry_id>")
def api_book(entry_id):
    return json.dumps(post_json(f"https://api.xuexd.cn/app/huiben/entry/id/{entry_id}/huiben/1"), ensure_ascii=False)

@hnm_bp.route("/api/quiz/<path:quiz_id>")
def api_quiz(quiz_id):
    return json.dumps(post_json(f"https://dudu.diiiapp.com/api/app/quiz_entry/id/{quiz_id}"), ensure_ascii=False)

@hnm_bp.route("/api/dict/<int:huiben_id>")
def api_dict(huiben_id):
    return json.dumps(post_json(f"https://dudu.diiiapp.com/app/app/words_translate/huiben_id/{huiben_id}"), ensure_ascii=False)

# ---- 首页入口 ----
@hnm_bp.route("/")
def hnm_index():
    return send_from_directory(hnm_bp.static_folder, "index.html")