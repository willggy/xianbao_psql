import os
import sys
import threading
import time
import base64
import hmac
import re
import secrets
import gc  # Manual GC after large HTML parses.
import ipaddress
import socket
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import quote, unquote, urlparse, urljoin
import psycopg
from psycopg.rows import dict_row
try:
    from psycopg_pool import ConnectionPool
except ImportError as exc:  # 依赖缺失时给出可操作的提示
    raise ImportError(
        "缺少依赖 psycopg-pool。请先安装依赖：pip install -r requirements.txt"
        "（或单独执行 pip install psycopg-pool）"
    ) from exc
import requests
from requests.adapters import HTTPAdapter
from flask import Flask, flash, render_template, request, Response, redirect, session, url_for
try:
    from flask_compress import Compress
    _compress = True
except Exception:
    _compress = False
from bs4 import BeautifulSoup, NavigableString
from waitress import serve

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # Keep startup working even if python-dotenv is not installed yet.
    pass

try:
    # Improve Windows console readability for Chinese logs.
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ==========================================
# 1. Basic config
# ==========================================
app = Flask(__name__)
if _compress:
    Compress(app)

# ===== 海尼曼点读蓝图挂载（独立 /hnm，不干扰线报逻辑）=====
from hnm_blue import hnm_bp
app.register_blueprint(hnm_bp)

# ===== Gemini Live WebSocket 代理蓝图挂载（独立 /gemini）=====
from gemini_live import gemini_bp, sock
sock.init_app(app)
app.register_blueprint(gemini_bp)

# Secrets and runtime config
SITE_TITLE = "古希腊掌管羊毛的神"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

# 本地调试开关。默认关闭：以前默认是开的，等于默认允许弱配置。
ALLOW_INSECURE_DEFAULTS = os.environ.get('ALLOW_INSECURE_DEFAULTS', '0').strip() == '1'

# SECRET_KEY：没配就每次启动随机生成（会话失效），绝不使用固定字符串。
# 固定密钥意味着任何人拿到源码就能伪造 session 直接进后台。
_secret_key_env = os.environ.get('SECRET_KEY', '').strip()
if _secret_key_env:
    app.secret_key = _secret_key_env
elif ALLOW_INSECURE_DEFAULTS:
    app.secret_key = 'xianbao_secret_key_888'   # 仅本地调试
else:
    app.secret_key = secrets.token_urlsafe(32)

# ADMIN_PASSWORD / CRON_SECRET：没配就是 None，对应功能直接关闭，不再回落到 '123'。
ADMIN_PASSWORD = (
    os.environ.get('ADMIN_PASSWORD', '').strip()
    or ('123' if ALLOW_INSECURE_DEFAULTS else None)
)
CRON_SECRET = (
    os.environ.get('CRON_SECRET', '').strip()
    or ('xianbao_secret_key_999' if ALLOW_INSECURE_DEFAULTS else None)
)

app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
PUBLISH_API_TOKEN = os.environ.get("PUBLISH_API_TOKEN", "").strip()
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "").strip()
WECHAT_WEBHOOK = os.environ.get("WECHAT_WEBHOOK", "").strip()
ALERT_ENABLED = os.environ.get("ALERT_ENABLED", "0").strip() == "1"
DETAIL_SIGNATURE_FETCH_ENABLED = os.environ.get("DETAIL_SIGNATURE_FETCH_ENABLED", "0").strip() == "1"
PUBLIC_BASE_URL = (
    os.environ.get("PUBLIC_BASE_URL", "").strip()
    or os.environ.get("RENDER_EXTERNAL_URL", "").strip()
).rstrip("/")

# Site config
SITES_CONFIG = {
    "xianbao": { 
        "name": "xianbao",
        "domain": "https://new.xianbao.fun", 
        "list_url": "https://new.xianbao.fun/", 
        "list_selector": "#mainbox > div.listbox tr, #mainbox > div.listbox li", 
        "content_selector": "#mainbox article .article-content, #art-fujia, #mainbox > article > div.art-content > div.art-copyright.br > div:nth-child(1)",
        "scrape_interval_min": 0,
        "max_items_per_run": 40,
        "original_url_selectors": [
            "a[href*='source']",
            "a[href*='from']",
            "a[href*='origin']",
            "a[href*='jump']"
        ],
        "original_url_regexes": [
            r'(https?://[^\s<"\']+)'
        ]
    },
    "iehou": { 
        "name": "爱猴线报", 
        "domain": "https://iehou.com", 
        "list_url": "https://iehou.com/", 
        "list_selector": "#body ul li",
        "content_selector": ".thread-content",
        "scrape_interval_min": 0,
        "max_items_per_run": 40,
        "original_url_selectors": [
            ".thread-content a[href]",
            "a[href*='url=']",
            "a[href*='target=']"
        ],
        "original_url_regexes": [
            r'(https?://[^\s<"\']+)'
        ]
    },
    "xianbao_icu": {
        "name": "xianbao_icu",
        "domain": "https://xianbao.icu",
        "list_url": "https://xianbao.icu/xianbao",  
        "list_selector": "main div div div:nth-child(3) > div:nth-child(2) a, main a[href*='/xianbao/detail'], main a[href*='/detail'], ul li a[href*='/detail']",
        # .el-card.comment 是评论块，位于 .xianbao-content 的兄弟节点
        # （同属 div.el-card__body），不显式指定就完全抓不到。
        "content_selector": ".xianbao-content, .el-card.comment, .article-content, .prose, .entry-content, .post-body, .detail-body, .markdown, .article-detail",
        "scrape_interval_min": 0,
        "max_items_per_run": 40,
        "original_url_selectors": [
            "a[href*='source']",
            "a[href*='origin']",
            ".article-content a[href]"
        ],
        "original_url_regexes": [
            r'(https?://[^\s<"\']+)'
        ]
   }
}

# 银行关键词
BANK_KEYWORDS = {
    "农行": ["农行", "农业银行", "nh"],
    "工行": ["工行", "工商银行", "gh"],
    "建行": ["建行", "建设银行", "CCB", "jh"],
    "中行": ["中行", "中国银行", "boc", "zh"],
}
ALL_BANK_VALS = [word for words in BANK_KEYWORDS.values() for word in words]

ALERT_GROUPS = {
    "农行": ["农行", "农业银行", "nh"],
    "工行": ["工行", "工商银行", "gh"],
    "建行": ["建行", "建设银行", "CCB", "jh"],
    "中行": ["中行", "中国银行", "boc", "zh"],
}
ALERT_ALL_VALS = [word for words in ALERT_GROUPS.values() for word in words]
TITLE_SIMILARITY_THRESHOLD = 0.85
# 标题/正文签名判重的回看窗口与池子规模上限。
# 窗口 1 小时（原来 30 分钟，跨站转发同一条线报常隔 40~90 分钟，导致首页重复）；
# 上限 300 条保证单轮比对的字符串条数封顶 —— 判重耗时随池子大小线性涨，
# 不设上限的话入库量一涨，单轮耗时跟着涨（见 idx_articles_update_sort 索引）。
TITLE_DEDUPE_WINDOW = "1 hour"
TITLE_DEDUPE_POOL_LIMIT = 300
# 「短的完整包含在长的里就算重」：只对长度差 > 8、本来会被直接跳过的配对生效，
# 且短的一方至少这么多归一化字符，否则「工行」这类两字短标题会被误吞进任何长标题。
TITLE_CONTAINMENT_MIN_LEN = 8
# 判重命中日志每轮最多打多少条（防刷屏），超出只累计计数。
DEDUPE_LOG_MAX_PER_RUN = 30
SITE_LOG_NAMES = {
    "xianbao": "线报库",
    "iehou": "爱猴线报",
    "xianbao_icu": "鲸线报",
}

# Local path helpers
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "xianbao.db")

PER_PAGE = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
}

# Shared requests session
session_req = requests.Session()
session_req.headers.update(HEADERS)
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=1)
session_req.mount('http://', adapter)
session_req.mount('https://', adapter)

scrape_lock = threading.Lock()

def get_beijing_now():
    # Return a naive Beijing-time datetime so existing DB arithmetic keeps working.
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)


def _warn(msg: str):
    print(f"[SECURITY WARNING] {msg}")


def _security_audit():
    """在模块导入时执行（gunicorn 下 __main__ 不会跑）。

    只打警告，不退出进程：公开页面照常工作，只把管理后台和 cron 关掉。
    这样即使漏配环境变量，站点也不会直接下线。
    """
    problems = []

    if not DATABASE_URL:
        problems.append("DATABASE_URL 未配置：数据库相关功能不可用")

    if not os.environ.get('SECRET_KEY', '').strip():
        problems.append("SECRET_KEY 未配置：已随机生成，每次重启会清空所有登录状态")

    if not ADMIN_PASSWORD:
        problems.append("ADMIN_PASSWORD 未配置：/admin 登录已禁用（返回 503）")

    if not CRON_SECRET:
        problems.append("CRON_SECRET 未配置：/cron/scrape 已禁用（返回 503）")

    if ALLOW_INSECURE_DEFAULTS:
        problems.append("ALLOW_INSECURE_DEFAULTS=1：正在使用不安全的默认值，仅限本地调试")

    for p in problems:
        _warn(p)

    if problems:
        _warn("以上问题不阻止启动；公开页面正常，管理后台与 cron 受限。")
    else:
        print("[SECURITY] 配置检查通过")


# 导入即执行：覆盖 gunicorn app:app 的启动路径
_security_audit()

# Last user activity timestamp
LAST_ACTIVE_TIME = get_beijing_now()

# ==========================================
# 2. Database and helpers
# ==========================================

# Simple TTL cache for GET pages (thread-safe, max 500 entries)
_template_cache = {}
_template_cache_order = []
_template_cache_lock = threading.Lock()
PAGE_CACHE_TTL = 30  # seconds
PAGE_CACHE_MAX = 500

def invalidate_page_cache():
    """清空首页/详情页的 HTML 缓存。

    新文章入库后必须调用：否则用户点了「有新线报」刷新，
    拿到的仍是 TTL 内的旧页面，看起来像刷新没生效。
    """
    with _template_cache_lock:
        _template_cache.clear()
        _template_cache_order.clear()


def render_cached(ttl=PAGE_CACHE_TTL):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if request.method != "GET" or session.get("is_logged_in"):
                return f(*args, **kwargs)
            key = request.full_path
            now = time.time()
            entry = _template_cache.get(key)
            if entry and now - entry["ts"] < ttl:
                return entry["html"]
            html = f(*args, **kwargs)
            with _template_cache_lock:
                _template_cache[key] = {"html": html, "ts": now}
                if key not in _template_cache_order:
                    _template_cache_order.append(key)
                if len(_template_cache) > PAGE_CACHE_MAX:
                    old = _template_cache_order.pop(0)
                    _template_cache.pop(old, None)
            return html
        return wrapper
    return decorator

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

# ---- Connection pool (lazy open; created on first getconn call) ----
_db_pool = None
_db_pool_lock = threading.Lock()


def _get_pool():
    """Return the process-wide connection pool, opening it on first use."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            if not DATABASE_URL:
                raise RuntimeError(
                    "DATABASE_URL not set. Configure env var DATABASE_URL before start."
                )
            pool = ConnectionPool(
                conninfo=DATABASE_URL,
                kwargs={"row_factory": dict_row},
                min_size=1,
                max_size=20,
                open=False,          # 延迟到第一次 getconn 时才建立
                timeout=15,          # 取连接最多等待 15s
                reconnect_timeout=30,
                max_lifetime=1800,   # 30 分钟回收，避免长连接被中间设备掐断
                max_idle=300,
            )
            pool.open(wait=False)
            _db_pool = pool
    return _db_pool


def get_db_connection():
    """从连接池取一个连接。调用方用完后必须调用 close_db_connection(conn) 归还。"""
    return _get_pool().getconn()


def close_db_connection(conn):
    """回滚未提交事务并把连接归还连接池（不是真正关闭 TCP）。"""
    if conn is None:
        return
    try:
        # 池不做 reset，这里主动回滚，避免把未提交/出错的事务状态带给下一个调用方。
        if not conn.closed:
            conn.rollback()
    except Exception:
        # 连接已损坏时直接丢弃，不归还池子。
        try:
            conn.close()
        except Exception:
            pass
        return
    try:
        _get_pool().putconn(conn)
    except Exception as e:
        print(f"[WARN] putconn failed: {e}")


def escape_like(value: str) -> str:
    """转义 LIKE/ILIKE 通配符，让用户输入按字面量匹配。

    PostgreSQL 的 LIKE 默认转义符是反斜杠。不转义时，用户输入单个 '%'
    会让模式变成 '%%%'，三个 ILIKE 条件恒真，退化为全表扫描 + 全表排序。
    """
    return (
        (value or "")
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def ensure_article_feature_columns(conn):
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS is_featured INTEGER NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS featured_at TIMESTAMP")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS featured_notified INTEGER NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS token_only_signature TEXT")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS content_prefetched INTEGER NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS prefetch_keyword TEXT")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS alert_keyword TEXT")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS notified INTEGER NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS notified_at TIMESTAMP")
    conn.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS notify_error TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_bank_top ON articles(match_keyword, is_top, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_featured ON articles(is_featured, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_cleanup ON articles(site_source, is_featured, updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_token_only_signature ON articles(token_only_signature)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_update_sort ON articles(updated_at DESC, id DESC)")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_articles_home_cover
        ON articles (is_top DESC, updated_at DESC, id DESC)
        INCLUDE (title, url, site_source, match_keyword, original_time)
    """)
    conn.commit()


def ensure_config_rules_schema(conn):
    try:
        conn.execute("ALTER TABLE config_rules ADD COLUMN IF NOT EXISTS alert_group TEXT")
        conn.commit()
        return True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[WARN] ensure_config_rules_schema failed: {e}")
        return False


def make_links_clickable(text):
    # Match plain http/https links but avoid wrapping URLs already inside href.
    pattern = re.compile(r'(?<!href=")(https?://[^\s"<]+)', re.IGNORECASE)
    return pattern.sub(r'<a href="\1" target="_blank" rel="noopener noreferrer" class="content-link">\1</a>', text)

def extract_original_url(html_content, fallback_url="", site_key=""):
    """Extract source/original URL from article HTML using per-site config first.

    只想要地址的调用方走这里；需要知道「是真实原始地址还是退回本站地址」的
    用 extract_original_url_info。
    """
    return extract_original_url_info(html_content, fallback_url=fallback_url, site_key=site_key)[0]


# 「栏目 / 导航 / 用户页 / 评论列表」这类链接的特征：命中就不是活动原始地址。
# 三个线报站的正文里都混着评论作者页、分类页、翻页链接（正文选择器把评论块
# 一起抓了下来，见 content_selector 里的 #art-fujia / .el-card.comment）。
_NAV_PATH_HINTS = (
    "/record/weibo/",      # 线报酷：评论作者页
    "/category/", "/tag/", "/page-", "/list-", "/search",
    "?author=", "&author=",  # 鲸线报：同作者线报
    "/user-", "/member-", "/login", "/register", "/mod-support",
    "/page-tools", "/page-dayhot", "/page-weekhot",
)
_ASSET_EXT_RE = re.compile(r"\.(?:jpg|jpeg|png|gif|webp|bmp|ico|css|js|svg|woff2?)(?:$|[?#])", re.I)
# 锚文本 / 正文标签词：挨着这些词的链接最可信。
_SOURCE_LABEL_RE = re.compile(
    r"(?:来源网址|原文链接|原文地址|来源地址|本文链接|本文地址|阅读原文|查看原文|"
    r"source|origin|original|from|link|jump)",
    re.I,
)


def _site_hosts(site_key="", fallback_url=""):
    """本站 / 线报站的域名集合：命中它的链接都是「站内链接」，不是活动原始地址。"""
    hosts = set()
    cfg = SITES_CONFIG.get(site_key, {}) if site_key else {}
    for base in (cfg.get("domain", ""), fallback_url, PUBLIC_BASE_URL):
        if not base:
            continue
        try:
            host = (urlparse(base).hostname or "").lower()
        except Exception:
            continue
        if host.startswith("www."):
            host = host[4:]
        if host:
            hosts.add(host)
    return hosts


def _is_site_link(href, site_hosts):
    """站内链接（含我们自己的图片代理）——不能当原始地址。"""
    if not href:
        return True
    if href.startswith("/"):
        return True
    try:
        host = (urlparse(href).hostname or "").lower()
    except Exception:
        return True
    if not host:
        return True
    for sh in site_hosts:
        if host == sh or host.endswith("." + sh):
            return True
    return False


def _is_nav_link(href):
    try:
        p = urlparse(href)
    except Exception:
        return True
    path = (p.path or "").lower()
    query = (p.query or "").lower()
    probe = f"{path}?{query}"
    if any(hint in probe for hint in _NAV_PATH_HINTS):
        return True
    if _ASSET_EXT_RE.search(path):
        return True
    # 首页 / 只有一两个路径段的栏目页：正文里不会只写一个域名就把活动放这儿
    if not path.strip("/"):
        return True
    return False


def _acceptable_original(href, site_hosts):
    return bool(href) and not _is_site_link(href, site_hosts) and not _is_nav_link(href)


def extract_original_url_info(html_content, fallback_url="", site_key=""):
    """抽活动的原始地址。返回 (地址, 是否真实原始地址, 依据)。

    原来的顺序有三个坑，都会让页面显示成「线报站自己的详情页地址」：
      1. 站点配的 original_url_selectors（a[href*='source'] 这类）先跑，
         命中的常是正文评论块里的站内链接 —— 现在一律先过 _acceptable_original。
      2. 带标签的链接（「来源网址：https://…」）没有优先权，被通用规则抢先。
         标签词命中的其实是最可靠的证据，提到最前。
      3. 第 3、4 档兜底取「第一个 http 链接」，而正文第一个链接经常是图床 /
         作者页 —— 现在同样过滤站内与导航。
    三档都取不到才退回本站地址（is_real=False，页面会标注清楚）。
    """
    if not html_content:
        return fallback_url, False, "无正文缓存"

    try:
        soup = BeautifulSoup(html_content, "lxml")
    except Exception:
        soup = BeautifulSoup(html_content, "html.parser")

    cfg = SITES_CONFIG.get(site_key, {}) if site_key else {}
    selectors = cfg.get("original_url_selectors", []) or []
    regexes = cfg.get("original_url_regexes", []) or []
    site_hosts = _site_hosts(site_key, fallback_url)

    def _normalize_href(href: str) -> str:
        href = (href or "").strip()
        if not href:
            return ""
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            # 相对链接一律是站内的（我们自己的 /img_proxy 也是），
            # 不再 urljoin 成站内绝对地址，省得它冒充原始地址。
            return ""
        return href if href.startswith(("http://", "https://")) else ""

    # 正文里所有 <a> 的 href，用来把「文本抽到的被截断地址」还原成完整链接：
    # 线报站常把长 URL 显示成 "https://im.icbc.com.cn/ICBCMPSer ... tivityId=800000712&"，
    # 从文本正则抽到的是这串省略号版本，点不开，必须以 href 为准。
    anchor_hrefs = []
    for a in soup.select("a[href]"):
        h = _normalize_href(a.get("href", ""))
        if h:
            anchor_hrefs.append(h)

    def _full_form(cand):
        for h in anchor_hrefs:
            if h == cand or (cand and h.startswith(cand[:24])):
                return h
        return cand

    # 1) 带「来源 / 原文 / source」标签的链接：标签挨着的第一个可用外链最可信。
    #    评论块里的链接不算 —— 正文选择器会把评论一起抓下来
    #    （#art-fujia / .el-card.comment），评论里有人贴「原文链接」不该顶掉真正的来源行。
    _COMMENT_CONTAINERS = ("#art-fujia", ".art-fujia", ".el-card.comment", ".comment-list", "#comment")
    comment_nodes = set()
    for sel in _COMMENT_CONTAINERS:
        try:
            comment_nodes.update(id(n) for n in soup.select(sel))
        except Exception:
            continue

    def _in_comment(node):
        return any(id(anc) in comment_nodes for anc in node.parents)

    labelled = []
    for a in soup.select("a[href]"):
        href = _normalize_href(a.get("href", ""))
        if not href:
            continue
        if comment_nodes and _in_comment(a):
            continue
        anchor_text = a.get_text(" ", strip=True)
        parent_text = ""
        try:
            parent = a.parent
            parent_text = parent.get_text(" ", strip=True) if parent is not None else ""
        except Exception:
            parent_text = ""
        if _SOURCE_LABEL_RE.search(anchor_text) or _SOURCE_LABEL_RE.search(parent_text[:120]):
            labelled.append(href)

    # 2) 只有「来源网址：」这种显式标签的正则（模板文字里抽地址用）。
    #    站点自己配的 original_url_regexes 多是裸 URL 通配，不算标签证据，
    #    放到第 4 档，否则它会抢在真实链接前面、还可能抽到被省略号截断的文本。
    text_blob = soup.get_text(" ", strip=True)
    label_patterns = [
        r"(?:来源网址|原文链接|原文地址|来源地址|本文链接|本文地址|阅读原文|查看原文)"
        r"\s*[:：]?\s*(https?://[^\s<>\"']+)",
        r"(?:source|origin|original|from|jump)\s*[:：]\s*(https?://[^\s<>\"']+)",
    ]
    for pat in label_patterns:
        try:
            m = re.search(pat, text_blob, flags=re.IGNORECASE)
            if not m:
                continue
            cands = [m.group(i) for i in range(1, (m.lastindex or 0) + 1)] or [m.group(0)]
            for cand in cands:
                cand = (cand or "").strip().rstrip(".,;:!?)]（）)，。；")
                if cand.startswith(("http://", "https://")):
                    labelled.append(_full_form(cand))
                    break
        except Exception:
            continue
    for href in labelled:
        if _acceptable_original(href, site_hosts):
            return href, True, "带来源/原文标签的链接"

    # 3) 站点级选择器：优先级高于通用规则，但必须先排除站内和导航链接。
    for sel in selectors:
        try:
            for a in soup.select(sel):
                href = _normalize_href(a.get("href", ""))
                if _acceptable_original(href, site_hosts):
                    return href, True, f"站点选择器 {sel}"
        except Exception:
            continue

    # 4) 正文里的外链，按出现顺序。线报正文第一个外链通常就是活动页。
    for a in soup.select("a[href]"):
        href = _normalize_href(a.get("href", ""))
        if _acceptable_original(href, site_hosts):
            return href, True, "正文第一个外链"

    # 5) 文本兜底：站点配的 regex + 通用 URL 模式（同样过滤站内/导航/图床）。
    for pat in list(regexes) + [r"(https?://[^\s<>\"']+)"]:
        try:
            for m in re.finditer(pat, text_blob):
                cands = [m.group(i) for i in range(1, (m.lastindex or 0) + 1)] or [m.group(0)]
                for cand in cands:
                    cand = (cand or "").strip().rstrip(".,;:!?)]（）)，。；")
                    if not cand.startswith(("http://", "https://")):
                        continue
                    cand = _full_form(cand)
                    if _acceptable_original(cand, site_hosts):
                        return cand, True, "正文文本中的链接"
        except Exception:
            continue

    return fallback_url, False, "未能识别原始地址，退回本站地址"


def safe_extract_original_url(html_content, fallback_url="", site_key=""):
    try:
        return extract_original_url(html_content, fallback_url=fallback_url, site_key=site_key)
    except Exception as e:
        print(f"[WARN] extract_original_url failed: {e}")
        return fallback_url


def normalize_image_url(url: str) -> str:
    """Normalize unstable image URLs from specific hosts."""
    if not url:
        return url
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.endswith("pic.xiaodigu.cn"):
            return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return url
    return url

TRUSTED_IMAGE_HOSTS = {
    "pic.xiaodigu.cn",
}

# 裸 URL 识别：排除空白、引号、尖括号，以及 CJK 标点/汉字/全角字符，
# 这样 "https://a.com/x。下一句" 不会把中文吞进链接里。
_BARE_URL_RE = re.compile(
    r"""https?://[^\s<>"'\u3000-\u303f\u3400-\u4dbf\u4e00-\u9fff\uff00-\uffef]+"""
)
_URL_TRAILING = ".,;:!?"

# 这些容器里的文本不做链接化：已有链接不能嵌套，代码块要保持原样。
_LINKIFY_SKIP_TAGS = {"a", "script", "style", "pre", "code", "textarea"}


def _linkify_urls(soup):
    """把正文纯文本里的裸 URL 变成可点击链接。

    原来是用 re.sub 扫整段 HTML，且要求 URL 前面必须是半角 ':' 或 '?'。
    但源站写的是中文全角冒号「来源网址：https://…」，匹配不上，于是：
      - 半角冒号 / 源站自带的 <a>  -> 能点
      - 全角冒号 / 行首裸链接      -> 点不了
    这就是「有的能点有的不能点」。改成在文本节点上处理：
    不依赖前面的标点，也不会误伤标签属性里的 URL。
    """
    for node in list(soup.find_all(string=True)):
        parent = getattr(node, "parent", None)
        if parent is None or getattr(parent, "name", "") in _LINKIFY_SKIP_TAGS:
            continue
        text = str(node)
        if "://" not in text:
            continue
        urls = _BARE_URL_RE.findall(text)
        if not urls:
            continue
        parts = _BARE_URL_RE.split(text)
        new_nodes = []
        for i, chunk in enumerate(parts):
            if chunk:
                new_nodes.append(NavigableString(chunk))
            if i >= len(urls):
                continue
            raw_url = urls[i]
            href = raw_url.rstrip(_URL_TRAILING)
            if not href:
                new_nodes.append(NavigableString(raw_url))
                continue
            a_tag = soup.new_tag("a")
            a_tag["href"] = href
            a_tag.string = href
            new_nodes.append(a_tag)
            tail = raw_url[len(href):]
            if tail:
                new_nodes.append(NavigableString(tail))
        if not new_nodes:
            continue
        try:
            node.replace_with(*new_nodes)
        except Exception:
            continue


# 评论头像：抓取时直接丢掉。
# 线报酷是「文字头像」<span class="pingluntoux xb-cmtav">不</span>，
# 源站 CSS 不在我们这边，它会渲染成昵称前一个多余的孤字（"不 不知道叫什么3232"）；
# icu 是图片头像，40px 尺寸同样依赖 Element Plus 的样式表，
# 丢了之后会被 .article-content img 撑成正文宽度的 72%。
_COMMENT_AVATAR_SELECTORS = (".pingluntoux", ".xb-cmtav", ".el-avatar")


def _drop_comment_avatars(soup):
    removed = 0
    for sel in _COMMENT_AVATAR_SELECTORS:
        try:
            els = soup.select(sel)
        except Exception:
            continue
        for el in els:
            el.decompose()
            removed += 1
    # 头像所在的 flex 单元被拆空后，留着会多出一段 column-gap 的缩进
    for el in soup.select(".el-space__item"):
        if not el.get_text(strip=True) and not el.find_all(True):
            el.decompose()
    return removed


# 原网页没有评论时，源站返回的不是空标签，而是一个带占位文字的整块
# （鲸线报：<div class="el-card comment">暂无评论</div>；
#   线报酷：<div id="art-fujia">原帖还没有人评论，等待重新抓取中~</div>）。
# 原样渲染到详情页就是一大片虚线框空白。这里把「没有真评论」的评论块整个去掉；
# 有真实评论（存在 .li 评论条目 / 评论文字）的照常保留。
def _drop_empty_comment_blocks(soup):
    dropped = 0
    # 鲸线报（Element Plus 评论卡片）
    for el in soup.select(".el-card.comment"):
        text = re.sub(r"[\s　]+", "", el.get_text(strip=True))
        if text in ("", "暂无评论", "评论列表", "评论列表暂无评论", "还没有评论"):
            el.decompose()
            dropped += 1
    # 线报酷（附加信息块）：带 .no-comment 占位，或评论列表里一条评论都没有
    for el in soup.select("#art-fujia, .art-fujia"):
        if el.select(".no-comment"):
            el.decompose()
            dropped += 1
            continue
        if not el.select(".comment-list .li, .ul .li, .c-neirong"):
            el.decompose()
            dropped += 1
    return dropped


def clean_html(html_content, site_key):
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, "lxml")
    _drop_comment_avatars(soup)
    _drop_empty_comment_blocks(soup)

    site_cfg = SITES_CONFIG.get(site_key, {})
    site_domain = site_cfg.get("domain", "")

    for text_node in soup.find_all(string=True):
        parent = getattr(text_node, "parent", None)
        if not parent or getattr(parent, "name", "") in {"script", "style", "pre", "code"}:
            continue
        text = str(text_node).strip()
        tokens = extract_command_tokens(text)
        if not tokens:
            continue
        pre_tag = soup.new_tag("pre")
        code_tag = soup.new_tag("code")
        code_tag.string = "\n".join(tokens)
        pre_tag.append(code_tag)
        text_node.replace_with(pre_tag)

    # 命令 token 先包进 <pre>（不会被链接化），再统一把裸 URL 变成链接。
    _linkify_urls(soup)

    for tag in soup.find_all(True):

        # Image handling
        if tag.name == 'img':
            src = (
                tag.get('src', '').strip()
                or tag.get('data-src', '').strip()
                or tag.get('data-original', '').strip()
                or tag.get('data-lazy-src', '').strip()
            )

            # srcset often looks like: "url1 1x, url2 2x"
            if not src:
                srcset = tag.get('srcset', '').strip() or tag.get('data-srcset', '').strip()
                if srcset:
                    first = srcset.split(',')[0].strip()
                    src = first.split(' ')[0].strip()

            if not src:
                continue

            # Avoid double-wrapping img_proxy URLs.
            if src.startswith("/img_proxy"):
                continue

            # Normalize relative image URLs.
            if src.startswith('//'):  # //img.xx.com/xx.jpg
                src = 'https:' + src

            elif src.startswith('/'):  # /upload/xxx.jpg
                if site_domain:
                    src = urljoin(site_domain, src)
                else:
                    continue

            elif src.startswith('./'):  # ./images/xxx.jpg
                if site_domain:
                    src = urljoin(site_domain + '/', src)
                else:
                    continue

            elif src.startswith('../'):  # ../xx/xx.jpg
                if site_domain:
                    src = urljoin(site_domain + '/', src)
                else:
                    continue

            # Keep existing percent-encoding, but proxy non-whitelisted hosts.
            # Keep existing percent-encoding, but encode query separators (&, =)
            # inside nested URLs so outer /img_proxy query string will not truncate.
            src = normalize_image_url(src)
            src_host = (urlparse(src).hostname or "").lower()
            if src_host.endswith("pic.xiaodigu.cn"):
                proxy_url = src
            else:
                proxy_url = "/img_proxy?url=" + quote(src, safe=':/?%')

            # 头像这类小图必须单独处理：detail.html 里 .article-content img
            # 会把图片撑到正文宽度的 72%，40px 的评论头像会被放大成整幅大图。
            is_avatar = False
            for ancestor in list(tag.parents)[:4]:
                if any("avatar" in str(c).lower() for c in (ancestor.get("class") or [])):
                    is_avatar = True
                    break

            if is_avatar:
                img_style = ("width:40px; height:40px; border-radius:50%; object-fit:cover;"
                             " display:inline-block; margin:0 8px 0 0; vertical-align:middle;")
            else:
                # 只留必要的兜底样式。原来这里还写死了 border-radius / margin:10px 0，
                # 内联样式优先级高于 CSS，会把 detail.html 里 .article-content img
                # 的 margin:20px auto（居中）和 12px 圆角整个覆盖掉，图片一直不居中。
                img_style = "max-width:100%; height:auto;"

            new_attrs = {'src': proxy_url, 'loading': 'lazy', 'style': img_style}
            alt_text = (tag.get('alt') or '').strip()
            if alt_text:
                new_attrs['alt'] = alt_text[:120]
            tag.attrs = new_attrs

        # Link handling
        elif tag.name == 'a':
            href = tag.get('href', '').strip()
            if not href:
                continue

            # Avoid self-referencing img_proxy links.
            if href.startswith('/img_proxy'):
                continue

            # Normalize relative links.
            if href.startswith('//'):
                href = 'https:' + href
            elif href.startswith('/'):
                if site_domain:
                    href = urljoin(site_domain, href)

            # Preserve standard clickable link styling.
            tag.attrs = {
                'href': href,
                'target': '_blank',
                'rel': 'noopener noreferrer',
                'style': 'color:#007aff; text-decoration:underline; word-break:break-all;'
            }

    # 用 body 内容返回片段：str(soup) 会带出 lxml 补的 <html><body> 外壳，
    # 这段 HTML 最终是被塞进 <div class="article-content"> 里的，
    # 嵌套的 html/body 虽然会被浏览器忽略，但属于脏输出且白白撑大缓存体积。
    body = soup.body
    result = body.decode_contents() if body is not None else str(soup)
    soup.decompose()
    return result


# ===== 正文抓取辅助 =====
# 源站改版后经常出现「正文选择器 MISS、但来源块选择器还在」的情况，
# 这时如果照旧写库，就会把一行「来源网址：xxx」当成整篇文章缓存下来。

_BOILERPLATE_RE = re.compile(
    r"^(来源网址|原文链接|原文地址|来源地址|免责声明|本文由系统自动重新抓取)"
)
_PUNCT_RE = re.compile(r"[\s:：,，。.、;；|()（）\[\]<>\-—_/\\]+")


def _is_boilerplate_only(html_content) -> bool:
    """判断片段是否只是模板文字（来源网址 / 免责声明），而不是真正正文。

    不能用「总长度」判断：脏数据里常带一条很长的 URL，
    「来源网址：https://...」总长可能七八十字，但没有任何实质内容。
    所以这里先剥掉标签和 URL，再看还剩多少真文字。
    """
    if not html_content:
        return True
    try:
        text = BeautifulSoup(html_content, "html.parser").get_text(" ", strip=True)
    except Exception:
        return False
    if not text:
        return True
    if len(text) > 300:  # 这么长不可能是纯模板文字
        return False
    m = _BOILERPLATE_RE.match(text)
    if not m:
        return False
    rest = re.sub(r"https?://\S+", "", text[m.end():])
    return len(_PUNCT_RE.sub("", rest)) < 10


def _collect_content_nodes(soup, selectors):
    """按顺序收集命中节点，跳过已被外层命中节点包含的子节点。

    配置里常写多个选择器兜底（例如 div.xianbao-content, .article-content），
    后者往往是前者的子节点，直接 join 会把正文重复拼接一遍。

    允许直接传逗号分隔的字符串：否则传成 str 时 for 会逐字符迭代，
    "a"/"b"/"p" 这类字母会被当成标签名命中 <a>/<b>/<p>，静默产出垃圾正文。
    """
    if isinstance(selectors, str):
        selectors = selectors.split(",")
    picked = []
    for sel in selectors:
        sel = sel.strip()
        if not sel:
            continue
        try:
            node = soup.select_one(sel)
        except Exception:
            continue
        if node is None:
            continue
        if any(node is p or p in node.parents for p in picked):
            continue
        if not node.get_text(strip=True) and not node.select("img"):
            continue
        picked.append(node)
    return picked



def record_visit():
    ua = request.headers.get('User-Agent', '')
    if 'HealthCheck' in ua or 'Zeabur' in ua: return
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    
    global LAST_ACTIVE_TIME
    LAST_ACTIVE_TIME = get_beijing_now()
    
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            '''INSERT INTO visit_stats (ip, visit_count, last_visit)
               VALUES (%s, 1, CURRENT_TIMESTAMP)
               ON CONFLICT (ip) DO UPDATE
               SET visit_count = visit_stats.visit_count + 1,
                   last_visit = CURRENT_TIMESTAMP''',
            (ip,),
        )
        conn.commit()
    except Exception as e:
        print(f"[WARN] record_visit failed: {e}")
    finally:
        close_db_connection(conn)

def upload_to_img_cdn(img_data, image_ext="png"):
    image_ext = (image_ext or "png").strip().lower()
    if image_ext == "jpg":
        image_ext = "jpeg"

    mime_map = {
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "tiff": "image/tiff",
    }
    mime_type = mime_map.get(image_ext, "image/png")
    fallback_data_url = f"data:{mime_type};base64,{base64.b64encode(img_data).decode()}"

    try:
        resp = requests.post(
            "https://img.scdn.io/api/v1.php",
            files={"image": (f"upload.{image_ext}", img_data, mime_type)},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        uploaded_url = (
            data.get("url")
            or data.get("imgurl")
            or data.get("data", {}).get("url")
            or data.get("image", {}).get("url")
        )
        if uploaded_url and isinstance(uploaded_url, str):
            return uploaded_url.strip()

        print(f"[IMG_UPLOAD WARN] Unexpected response: {data}")
    except Exception as e:
        print(f"[IMG_UPLOAD ERROR] {e}")

    return fallback_data_url


def process_publish_content(raw_content):
    raw_content = raw_content or ""

    def img_replacer(match):
        try:
            image_ext = (match.group(1) or "png").split(";")[0].strip().lower()
            cdn = upload_to_img_cdn(base64.b64decode(match.group(2)), image_ext=image_ext)
            return f'src="{cdn}"' if cdn else match.group(0)
        except Exception:
            return match.group(0)

    return re.sub(r'src="data:image\/(.*?);base64,(.*?)"', img_replacer, raw_content)


def create_user_article(title, raw_content, is_top=0, match_keyword="羊毛精选"):
    title = (title or "").strip()
    raw_content = raw_content or ""
    if not title or not raw_content.strip():
        raise ValueError("title and content are required")

    processed = process_publish_content(raw_content)
    fake_url = f"user://{int(time.time())}-{os.urandom(4).hex()}"
    original_time = get_beijing_now().strftime("%Y-%m-%d %H:%M")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO articles (title, url, site_source, match_keyword, original_time, is_top) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                (title, fake_url, "user", match_keyword, original_time, 1 if is_top else 0),
            )
            article_id = cur.fetchone()["id"]
        conn.execute(
            "INSERT INTO article_content (url, content) VALUES (%s, %s) "
            "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content, updated_at = CURRENT_TIMESTAMP",
            (fake_url, processed),
        )
        conn.commit()
        return {
            "id": article_id,
            "url": fake_url,
            "view_url": build_article_view_url(article_id),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)

# ==========================================
# 3. Routes
# ==========================================

def _latest_article_id(conn, tag=None, q=None):
    """当前视图下「最新一篇文章」的 id，供前端判断有没有新内容。

    必须和 /api/check_update 用完全相同的口径：之前 index() 传的是
    当前排序结果的第一条（有置顶/筛选/翻页时并不是最大 id），
    而接口返回全表最大 id，两者永远不相等 -> 首页每 2 分钟无限 reload。
    """
    if q:
        # 搜索要扫 article_content.content，%kw% 用不上索引；
        # 每 2 分钟一次的轮询不值得付这个钱，退化成全局最新。
        row = conn.execute("SELECT COALESCE(MAX(id), 0) AS id FROM articles").fetchone()
    elif tag == "羊毛精选":
        row = conn.execute(
            "SELECT id FROM articles WHERE is_featured = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()
    elif tag:
        row = conn.execute(
            "SELECT id FROM articles WHERE match_keyword = %s ORDER BY id DESC LIMIT 1",
            (tag,),
        ).fetchone()
    else:
        row = conn.execute("SELECT id FROM articles ORDER BY id DESC LIMIT 1").fetchone()
    return (row or {}).get("id", 0) or 0


@app.route('/')
@render_cached()
def index():
    record_visit()
    now = get_beijing_now()

    # Compute the next 2-minute refresh boundary.
    next_interval = ((now.minute // 2) + 1) * 2
    
    if next_interval >= 60:
        next_refresh_obj = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_refresh_obj = now.replace(minute=next_interval, second=0, microsecond=0)

    next_refresh_time = next_refresh_obj.strftime("%H:%M")
    # ----------------------------

    tag = request.args.get('tag')
    q = request.args.get('q')
    page = request.args.get('page', 1, type=int)
    
    conn = get_db_connection()
    try:
        where = "WHERE 1=1"
        params = []
        if tag:
            if tag == '羊毛精选':
                where += " AND articles.is_featured = 1"
            else:
                where += " AND articles.match_keyword = %s"
                params.append(tag)
        total_from_join = False
        if q:
            # 每个关键词会展开成 3 个 ILIKE 条件，限制数量避免超长 q 造出巨型表达式。
            keywords = [escape_like(kw) for kw in q.strip().split()[:10]]
            for kw in keywords:
                where += " AND (articles.title ILIKE %s OR articles.match_keyword ILIKE %s OR ac.content ILIKE %s)"
                params += [f"%{kw}%", f"%{kw}%", f"%{kw}%"]
            order_sql = "ORDER BY CASE WHEN articles.title ILIKE %s THEN 0 ELSE 1 END, articles.is_top DESC, articles.updated_at DESC, articles.id DESC"
            params.append(f"%{keywords[0]}%")
            from_sql = "FROM articles LEFT JOIN article_content ac ON ac.url = articles.url"
            total_from_join = True
        else:
            order_sql = "ORDER BY articles.is_top DESC, articles.updated_at DESC, articles.id DESC"
            from_sql = "FROM articles"

        # 只取模板需要的列，减少传输/解析开销
        article_cols = (
            "articles.id, articles.title, articles.url, articles.site_source, "
            "articles.match_keyword, articles.is_top, articles.original_time, articles.updated_at"
        )
        articles = conn.execute(
            f'SELECT {article_cols} {from_sql} {where} {order_sql} LIMIT %s OFFSET %s',
            params + [PER_PAGE, (page-1)*PER_PAGE],
        ).fetchall()

        if total_from_join:
            total_sql = f'SELECT COUNT(*) {from_sql} {where}'
            total = conn.execute(total_sql, params[:-1]).fetchone()["count"]
        else:
            total = conn.execute(f'SELECT COUNT(*) {from_sql} {where}', params).fetchone()["count"]

        latest_id = _latest_article_id(conn, tag, q)
    finally:
        close_db_connection(conn)

    return render_template('index.html', 
                           articles=articles, 
                           next_refresh_time=next_refresh_time,
                           bank_list=list(BANK_KEYWORDS.keys()), 
                           current_tag=tag, 
                           q=q, 
                           current_page=page, 
                           total_pages=(total+PER_PAGE-1)//PER_PAGE,
                           latest_id=latest_id,
                           today_str=now.strftime("%Y-%m-%d"))

@app.route("/view")
@render_cached(ttl=180)
def view():
    article_id = request.args.get("id", type=int)
    conn = get_db_connection()
    try:
        row = conn.execute(
            """SELECT a.*, ac.content AS article_content
               FROM articles a
               LEFT JOIN article_content ac ON ac.url = a.url
               WHERE a.id = %s""",
            (article_id,)
        ).fetchone()
        if not row:
            return "内容不存在", 404

        url, site_key, title = row["url"], row["site_source"], row["title"]
        # 默认是线报站自己的详情页地址（本站地址），不是活动原始地址；
        # original_is_real 决定页面把它标成「原始地址」还是「本站页面」。
        original_url = url
        original_is_real = False
        original_reason = "无正文可提取"
        cached_html = row["article_content"]
        if cached_html and site_key in SITES_CONFIG and _is_boilerplate_only(cached_html):
            # 历史脏数据：库里只存了「来源网址 / 免责声明」一行。
            # 当作没有缓存，走实时抓取并覆盖这一行，实现访问即自愈。
            # 但这一行本身就带着活动原始地址（「来源网址：https://…」），
            # 先把它抽出来再用 —— 以前是整段丢掉，original_url 永远退成本站地址。
            boiler_url, boiler_real, boiler_reason = extract_original_url_info(
                cached_html, fallback_url=url, site_key=site_key
            )
            if boiler_real:
                original_url, original_is_real, original_reason = boiler_url, True, f"模板来源行（{boiler_reason}）"
            print(f"[CONTENT HEAL] 丢弃疑似脏缓存: {url}")
            cached_html = None
        content = ""

        if cached_html:
            original_url, original_is_real, original_reason = extract_original_url_info(
                cached_html, fallback_url=url, site_key=site_key
            )
            if site_key == "user" or site_key not in SITES_CONFIG:
                content = cached_html
            else:
                content = clean_html(cached_html, site_key)
        elif site_key in SITES_CONFIG:
            try:
                r = session_req.get(url, timeout=10)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, "html.parser")

                selectors = SITES_CONFIG[site_key]["content_selector"].split(',')
                content_nodes = _collect_content_nodes(soup, selectors)

                if content_nodes:
                    full_raw_content = "".join(str(n) for n in content_nodes)
                    if _is_boilerplate_only(full_raw_content):
                        # 正文选择器已失效，只抓到「来源网址 / 免责声明」这类模板文字。
                        # 不写库，避免把模板文字当成正文长期缓存下来。
                        # 同样先从这一行里提原始地址：提不到才退回本站地址。
                        b_url, b_real, b_reason = extract_original_url_info(
                            full_raw_content, fallback_url=url, site_key=site_key
                        )
                        if b_real:
                            original_url, original_is_real, original_reason = b_url, True, f"模板来源行（{b_reason}）"
                        print(f"[CONTENT MISS] {site_key} 未抓到有效正文: {url}")
                        content = "暂无内容"
                    else:
                        conn.execute(
                            "INSERT INTO article_content(url, content) VALUES(%s, %s) "
                            "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content, updated_at = CURRENT_TIMESTAMP",
                            (url, full_raw_content),
                        )
                        conn.commit()
                        original_url, original_is_real, original_reason = extract_original_url_info(
                            full_raw_content, fallback_url=url, site_key=site_key
                        )
                        content = clean_html(full_raw_content, site_key)
                else:
                    print(f"[CONTENT MISS] {site_key} 无选择器命中: {url}")
                    content = "暂无内容"

            except Exception as e:
                print(f"Error fetching content: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                content = "加载原文失败，请稍后重试或打开原文链接。"

        # 自己发布的文章 url 是 user://… 这种内部标识，不是能点的地址：不显示。
        if not str(original_url or "").startswith(("http://", "https://")):
            original_url, original_is_real = "", False
        if original_reason:
            print(f"[ORIGINAL URL] {site_key} id={article_id} real={original_is_real} | {original_reason} | {original_url}")
        return render_template(
            "detail.html",
            title=title,
            content=content,
            original_url=original_url,
            original_is_real=original_is_real,
            time=row['original_time'],
        )
    finally:
        close_db_connection(conn)

@app.route('/admin')
@login_required
def admin_panel():
    conn = get_db_connection()
    # Initialize defaults to avoid UnboundLocalError.
    whitelist, blacklist, alertlist, my_articles = [], [], [], []
    total_arts, total_visits = 0, 0
    last_update = "尚未开始抓取"
    
    try:
        # Load admin data from database.
        whitelist = conn.execute("SELECT * FROM config_rules WHERE rule_type='white'").fetchall()
        blacklist = conn.execute("SELECT * FROM config_rules WHERE rule_type='black'").fetchall()
        alertlist = conn.execute(
            "SELECT * FROM config_rules "
            "WHERE rule_type='alert' "
            "ORDER BY "
            "CASE WHEN COALESCE(alert_group, '') = '' THEN 1 ELSE 0 END, "
            "alert_group ASC, keyword ASC, id DESC"
        ).fetchall()
        my_articles = conn.execute("SELECT id, title, is_top, updated_at FROM articles WHERE site_source='user' ORDER BY is_top DESC, id DESC").fetchall()
        
        last_log = conn.execute('SELECT last_scrape FROM scrape_log ORDER BY id DESC LIMIT 1').fetchone()
        if last_log:
            last_update = last_log["last_scrape"]
            
        # psycopg returns mapping rows here, so use the aliased key.
        res_count = conn.execute("SELECT COUNT(*) as cnt FROM articles").fetchone()
        total_arts = res_count["cnt"] if res_count else 0
        
        res_visits = conn.execute("SELECT SUM(visit_count) as s FROM visit_stats").fetchone()
        total_visits = res_visits["s"] if res_visits and res_visits["s"] else 0

    except Exception as e:
        print(f"admin data load failed: {e}")
    finally:
        close_db_connection(conn)

    # Render with safe defaults even if the query failed.
    stats = {
        'total_articles': total_arts, 
        'total_visits': total_visits, 
        'last_update': last_update
    }
    return render_template(
        'admin.html',
        whitelist=whitelist,
        blacklist=blacklist,
        alertlist=alertlist,
        my_articles=my_articles,
        stats=stats,
        alert_groups=ALERT_GROUPS,
    )


@app.route('/admin/featured')
@login_required
def admin_featured():
    status = request.args.get('status', 'all').strip() or 'all'
    bank = request.args.get('bank', '').strip()
    q = request.args.get('q', '').strip()

    where = ["site_source != 'user'"]
    params = []

    if status == 'featured':
        where.append("is_featured = 1")
    elif status == 'normal':
        where.append("is_featured = 0")

    if bank:
        where.append("match_keyword = %s")
        params.append(bank)

    if q:
        where.append("title LIKE %s")
        params.append(f"%{escape_like(q)}%")

    conn = get_db_connection()
    try:
        featured_articles = conn.execute(
            "SELECT id, title, match_keyword, original_time, updated_at, is_top, is_featured, featured_at "
            f"FROM articles WHERE {' AND '.join(where)} "
            "ORDER BY is_featured DESC, COALESCE(featured_at, updated_at) DESC, id DESC LIMIT 300",
            params,
        ).fetchall()
    finally:
        close_db_connection(conn)

    return render_template(
        'admin_featured.html',
        articles=featured_articles,
        bank_list=list(BANK_KEYWORDS.keys()),
        current_status=status,
        current_bank=bank,
        q=q,
    )


@app.route('/admin/refresh', methods=['GET', 'POST'])
@login_required  # manual refresh requires login
def admin_refresh():
    now = get_beijing_now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] admin manual refresh triggered")

    summary = scrape_all_sites()
    status = (summary or {}).get("status", "error")

    if status == "success":
        site_stats = (summary or {}).get("site_stats", {})
        site_desc = ", ".join(
            [f"{k} +{int(v.get('new', 0))}" for k, v in site_stats.items()]
        ) or "No site stats"
        notified = int(summary.get("notified", 0))
        skipped_desc = ", ".join(summary.get("skipped_sites", [])) or "none"
        flash(
            f"Scrape success: +{int(summary.get('total_new', 0))} new, notify {notified}, {summary.get('duration_sec', 0):.1f}s. {site_desc}. skipped: {skipped_desc}",
            "success",
        )
    elif status == "skipped":
        reason = summary.get("reason", "skipped")
        flash(f"Scrape skipped: {reason}, {summary.get('duration_sec', 0):.1f}s", "warning")
    else:
        err = summary.get("error", "unknown error")
        flash(f"Scrape failed: {err}", "danger")

    return redirect(url_for('admin_panel'))  # back to admin panel

@app.route('/publish', methods=['GET', 'POST'])
@login_required
def publish():
    if request.method == 'POST':
        title = request.form.get('title')
        raw_content = request.form.get('content')
        is_top = 1 if request.form.get('publish_mode') == 'top' else 0
        create_user_article(title, raw_content, is_top=is_top)
        return redirect('/')
    return render_template('publish.html')


@app.route('/api/publish', methods=['POST'])
def api_publish():
    auth_header = request.headers.get("Authorization", "").strip()
    token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else request.headers.get("X-API-Token", "").strip()
    if not PUBLISH_API_TOKEN or token != PUBLISH_API_TOKEN:
        return {"status": "error", "message": "Unauthorized"}, 401

    data = request.get_json(silent=True) or {}
    title = data.get("title", "")
    content = data.get("content", "")
    is_top = bool(data.get("is_top", False))
    match_keyword = (data.get("match_keyword") or "羊毛精选").strip()

    try:
        article = create_user_article(title, content, is_top=is_top, match_keyword=match_keyword)
    except ValueError as e:
        return {"status": "error", "message": str(e)}, 400
    except Exception as e:
        print(f"[API_PUBLISH ERROR] {e}")
        return {"status": "error", "message": "publish failed"}, 500

    return {
        "status": "success",
        "id": article["id"],
        "url": article["url"],
        "view_url": article["view_url"],
    }, 201

@app.route('/article/edit/<int:aid>', methods=['GET', 'POST'])
@login_required
def edit_article(aid):
    conn = get_db_connection()
    try:
        if request.method == 'POST':
            title = request.form.get('title')
            raw_content = request.form.get('content')
            is_top = 1 if request.form.get('publish_mode') == 'top' else 0

            def img_replacer(match):
                try:
                    image_ext = (match.group(1) or "png").split(";")[0].strip().lower()
                    cdn = upload_to_img_cdn(base64.b64decode(match.group(2)), image_ext=image_ext)
                    return f'src="{cdn}"' if cdn else match.group(0)
                except Exception as e:
                    print(f"[WARN] edit img upload failed: {e}")
                    return match.group(0)

            processed = re.sub(r'src="data:image\/(.*?);base64,(.*?)"', img_replacer, raw_content)
            row = conn.execute("SELECT url FROM articles WHERE id=%s", (aid,)).fetchone()
            if row:
                conn.execute("UPDATE articles SET title=%s, is_top=%s WHERE id=%s", (title, is_top, aid))
                conn.execute("UPDATE article_content SET content=%s WHERE url=%s", (processed, row['url']))
                conn.commit()
            return redirect('/admin')

        article = conn.execute("SELECT * FROM articles WHERE id=%s AND site_source='user'", (aid,)).fetchone()
        if not article:
            return "未找到文章", 404
        content_row = conn.execute("SELECT content FROM article_content WHERE url=%s", (article['url'],)).fetchone()
        content = content_row['content'] if content_row else ""
        return render_template('edit.html', article=article, content=content)
    finally:
        close_db_connection(conn)


@app.route('/article/top/<int:aid>')
@login_required
def toggle_top(aid):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE articles SET is_top = 1 - is_top WHERE id=%s", (aid,))
        conn.commit()
    finally:
        close_db_connection(conn)
    return redirect('/admin')


@app.route('/article/feature/<int:aid>')
@login_required
def toggle_featured(aid):
    conn = get_db_connection()
    should_notify = False
    try:
        row = conn.execute("SELECT id, title, url, site_source, match_keyword, is_featured, featured_notified FROM articles WHERE id=%s", (aid,)).fetchone()
        if not row:
            return redirect(url_for('admin_featured'))

        is_now_featured = row['is_featured'] == 0

        conn.execute(
            "UPDATE articles SET is_featured = 1 - is_featured, featured_at = CASE WHEN is_featured = 0 THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=%s",
            (aid,),
        )
        conn.commit()

        should_notify = bool(is_now_featured and not row['featured_notified'] and row['site_source'] != 'user')
        notify_row = row
    finally:
        close_db_connection(conn)

    if should_notify:
        # 通知是外部 HTTP 调用，放在连接归还之后做，避免长时间占用池内连接。
        conn = get_db_connection()
        try:
            notify_url = build_article_view_url(notify_row['id']) or notify_row['url']
            cleaned_title = strip_command_token(notify_row['title'])
            preview_title = build_preview_text(cleaned_title, limit=20)
            command_token = "\n".join(extract_command_tokens(notify_row['title'])) or fetch_article_command_token(notify_row['url'], notify_row['site_source'])
            _send_one_notification("线报-精选", notify_url, preview_title, command_token)
            conn.execute("UPDATE articles SET featured_notified = 1 WHERE id=%s", (aid,))
            conn.commit()
        except Exception as e:
            print(f"featured notify error: {e}")
        finally:
            close_db_connection(conn)

    next_url = request.args.get('next') or url_for('admin_featured')
    return redirect(next_url)


@app.route('/article/delete/<int:aid>')
@login_required
def delete_article(aid):
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT url FROM articles WHERE id=%s", (aid,)).fetchone()
        if row:
            conn.execute("DELETE FROM articles WHERE id=%s", (aid,))
            conn.execute("DELETE FROM article_content WHERE url=%s", (row['url'],))
            conn.commit()
    finally:
        close_db_connection(conn)
    return redirect('/admin')

@app.route('/api/rule', methods=['POST'])
@login_required
def api_rule():
    action = request.form.get('action')
    rtype = request.form.get('type')
    scope = request.form.get('scope', 'title')
    kw = request.form.get('keyword', '').strip()
    alert_group = request.form.get('alert_group', '').strip()
    rid = request.form.get('id')
    conn = get_db_connection()
    try:
        if action == 'add' and kw:
            conn.execute(
                "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (keyword, match_scope) DO UPDATE SET alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group)",
                (rtype, kw, scope, alert_group if rtype == "alert" else None),
            )
        elif action == 'delete' and rid:
            conn.execute("DELETE FROM config_rules WHERE id=%s", (rid,))
        conn.commit()
    except Exception as e:
        print(f"rule operation failed: {e}")
    finally:
        close_db_connection(conn)
    return redirect(url_for('admin_panel'))

@app.route('/admin/sync-bank-alerts', methods=['POST'])
@login_required
def sync_bank_alerts():
    conn = get_db_connection()
    added = 0
    try:
        for bank_name, keywords in BANK_KEYWORDS.items():
            for keyword in keywords:
                kw = (keyword or "").strip()
                if not kw:
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT (keyword, match_scope) DO UPDATE SET "
                        "alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group) "
                        "WHERE config_rules.rule_type = 'alert'",
                        ("alert", kw, "title", bank_name),
                    )
                    added += cur.rowcount
        conn.commit()
        flash(f"Bank keywords synced to alert rules: +{added}", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Sync bank alert rules failed: {e}", "danger")
    finally:
        close_db_connection(conn)
    return redirect(url_for('admin_panel'))


@app.route('/admin/sync-alert-group/<group_name>', methods=['POST'])
@login_required
def sync_alert_group(group_name):
    aliases = ALERT_GROUPS.get(group_name)
    if not aliases:
        flash(f"Unknown alert group: {group_name}", "warning")
        return redirect(url_for('admin_panel'))

    conn = get_db_connection()
    added = 0
    try:
        for keyword in aliases:
            kw = (keyword or "").strip()
            if not kw:
                continue
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (keyword, match_scope) DO UPDATE SET "
                    "alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group) "
                    "WHERE config_rules.rule_type = 'alert'",
                    ("alert", kw, "title", group_name),
                )
                added += cur.rowcount
        conn.commit()
        flash(f"Synced alert group {group_name}: +{added}", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Sync alert group failed: {e}", "danger")
    finally:
        close_db_connection(conn)
    return redirect(url_for('admin_panel'))

@app.route('/logs')
@login_required
def show_logs():
    conn = get_db_connection()
    try:
        logs = conn.execute('SELECT last_scrape FROM scrape_log ORDER BY created_at DESC, id DESC LIMIT 50').fetchall()
        visitors = conn.execute('SELECT * FROM visit_stats ORDER BY last_visit DESC LIMIT 30').fetchall()
    finally:
        close_db_connection(conn)
    return render_template('logs.html', logs=logs, visitors=visitors)


@app.route('/api/check_update')
def check_update():
    """Lightweight polling endpoint used by the homepage."""
    tag = request.args.get('tag')
    q = request.args.get('q')
    conn = get_db_connection()
    try:
        return {"last_id": _latest_article_id(conn, tag, q)}
    finally:
        close_db_connection(conn)


@app.route('/img_proxy')
def img_proxy():
    raw = request.args.get('url', '').strip()
    if not raw:
        return "", 404

    # request.args has already URL-decoded once. Decoding again may break
    # signed/processed image params such as %7Cwatermark.
    url = raw
    # If caller passed fully encoded URL (e.g. https%3A%2F%2F...), decode once.
    if not url.startswith(("http://", "https://")) and ("%3A" in url or "%2F" in url):
        try:
            url = unquote(url)
        except Exception:
            pass

    if url.startswith("/img_proxy"):
        print("[WARN] Blocked nested img_proxy:", url)
        return "", 404

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        print("[WARN] Blocked invalid scheme:", url)
        return "", 404

    def _is_ip_private_or_disallowed(ip: str) -> bool:
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return True
        return any([
            addr.is_private,
            addr.is_loopback,
            addr.is_link_local,
            addr.is_reserved,
            addr.is_multicast,
            getattr(addr, "is_unspecified", False),
        ])

    def _host_resolves_to_disallowed_ip(host: str) -> bool:
        host = (host or "").strip().lower()
        if not host:
            return True
        if host in {"localhost"}:
            return True

        # Literal IP host.
        try:
            ipaddress.ip_address(host)
            return _is_ip_private_or_disallowed(host)
        except ValueError:
            pass

        # Resolve A/AAAA records and reject if any address is private/reserved.
        try:
            infos = socket.getaddrinfo(host, None)
        except Exception:
            return True

        resolved = set()
        for info in infos:
            sockaddr = info[4]
            if isinstance(sockaddr, tuple) and sockaddr:
                resolved.add(sockaddr[0])
        if not resolved:
            return True

        return any(_is_ip_private_or_disallowed(ip) for ip in resolved)

    host = parsed.hostname or ""
    if host.lower() == "pic.xiaodigu.cn" and parsed.scheme == "https":
        url = "http://" + url[len("https://"):]
        parsed = urlparse(url)
        host = parsed.hostname or host
    trusted_host = host.lower() in TRUSTED_IMAGE_HOSTS
    if not trusted_host and _host_resolves_to_disallowed_ip(host):
        print("[WARN] Blocked SSRF host:", host, "url:", url)
        return "", 404

    try:
        dynamic_referer = f"{parsed.scheme}://{host}/" if host else ""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            # Use target site as Referer to reduce anti-hotlink false negatives.
            "Referer": dynamic_referer,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="122", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "image",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site"
        }

        # Stream upstream image data to reduce RAM usage.
        # Some image hosts reject unknown Referer values; retry once without Referer.
        r = session_req.get(url, headers=headers, timeout=15, stream=True, allow_redirects=True)
        if r.status_code in (401, 403, 404):
            try:
                r.close()
            except Exception:
                pass
            headers_no_referer = dict(headers)
            headers_no_referer.pop("Referer", None)
            r = session_req.get(url, headers=headers_no_referer, timeout=15, stream=True, allow_redirects=True)

        # Re-check redirects so upstream cannot bounce us into internal hosts.
        final_url = getattr(r, "url", "") or url
        final_parsed = urlparse(final_url)
        final_host = final_parsed.hostname or ""
        final_trusted = final_host.lower() in TRUSTED_IMAGE_HOSTS
        if final_parsed.scheme not in ("http", "https") or (not final_trusted and _host_resolves_to_disallowed_ip(final_host)):
            print("[WARN] Blocked SSRF redirect:", final_url)
            try:
                r.close()
            except Exception:
                pass
            return "", 404
        
        if r.status_code != 200:
            print(f"[IMG_PROXY] {url} returned {r.status_code}")
            return Response(
                "",
                status=r.status_code,
                headers={
                    "Cache-Control": "no-store, max-age=0",
                    "Pragma": "no-cache",
                },
            )

        content_type = r.headers.get("Content-Type", "image/jpeg")
        
        # Verify upstream content type is image-like.
        if not content_type or not any(img_type in content_type.lower() for img_type in ['image/', 'application/octet-stream']):
            if trusted_host and url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
                content_type = "image/jpeg"
            else:
                print(f"[WARN] Content-Type is not image-like: {content_type}")
                return "", 404

        # Yield image chunks directly instead of buffering the full body.
        def generate():
            try:
                for chunk in r.iter_content(chunk_size=4096):
                    yield chunk
            finally:
                try:
                    r.close()
                except Exception:
                    pass

        resp = Response(generate(), content_type=content_type, status=200)
        resp.headers["Cache-Control"] = "public, max-age=86400, s-maxage=86400, stale-while-revalidate=600"
        resp.headers["CDN-Cache-Control"] = "public, s-maxage=86400, stale-while-revalidate=600"
        resp.headers["Vary"] = "Accept-Encoding"
        upstream_etag = r.headers.get("ETag")
        if upstream_etag:
            resp.headers["ETag"] = upstream_etag
        upstream_last_modified = r.headers.get("Last-Modified")
        if upstream_last_modified:
            resp.headers["Last-Modified"] = upstream_last_modified
        return resp

    except Exception as e:
        print(f"[IMG_PROXY ERROR] {url}: {e}")
        transparent_png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y1GNnUAAAAASUVORK5CYII=")
        return Response(
            transparent_png,
            content_type="image/png",
            headers={
                "Cache-Control": "no-store, max-age=0",
                "Pragma": "no-cache",
            },
        )


@app.route('/login', methods=['GET', 'POST'])
def login():
    if not ADMIN_PASSWORD:
        return (
            "管理后台登录已禁用：ADMIN_PASSWORD 未配置。"
            "请在环境变量中设置 ADMIN_PASSWORD 后重新部署。",
            503,
        )
    if request.method == 'POST':
        submitted = (request.form.get('password') or '').encode('utf-8')
        if hmac.compare_digest(submitted, ADMIN_PASSWORD.encode('utf-8')):
            session['is_logged_in'] = True
            return redirect('/admin')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear(); return redirect('/')

# cron 触发的最小间隔保护（秒）。默认 60s，远小于常见的 2 分钟调度节奏。
_cron_last_start_ts = 0.0
_cron_guard_lock = threading.Lock()
_cron_query_secret_warned = False  # query 传密钥的提醒每个进程只打一次
CRON_MIN_INTERVAL_SEC = float(os.environ.get("CRON_MIN_INTERVAL_SEC", "60"))


def _extract_cron_secret():
    """取 cron 密钥，返回 (secret, via_query)。

    优先请求头，其次 query/表单。走 query 时返回 via_query=True，用于打迁移提醒。
    """
    header_secret = (request.headers.get('X-Cron-Secret') or '').strip()
    if header_secret:
        return header_secret, False

    auth = (request.headers.get('Authorization') or '').strip()
    if auth:
        if auth.lower().startswith('bearer '):
            auth = auth[7:].strip()
        return auth, False

    query_secret = (request.args.get('secret') or '').strip()
    if query_secret:
        return query_secret, True

    form_secret = (request.form.get('secret') or '').strip()
    if form_secret:
        return form_secret, True

    return '', False


@app.route('/cron/scrape', methods=['GET', 'POST'])
def cron_scrape():
    global _cron_last_start_ts, _cron_query_secret_warned

    if not CRON_SECRET:
        return {
            "error": "CRON_SECRET not configured; cron endpoint disabled",
            "hint": "在环境变量中设置 CRON_SECRET 后重新部署",
        }, 503

    provided_secret, via_query = _extract_cron_secret()
    if not provided_secret or not hmac.compare_digest(
        provided_secret.encode('utf-8'), CRON_SECRET.encode('utf-8')
    ):
        caller = request.headers.get('X-Forwarded-For', request.remote_addr)
        print(f"[CRON] unauthorized from {caller}")
        return {"error": "Unauthorized"}, 401

    # 每个进程只提醒一次，避免 2 分钟一次的调度刷屏。
    if via_query and not _cron_query_secret_warned:
        _cron_query_secret_warned = True
        print("[CRON][SECURITY] secret 通过 URL query 传递，会进访问日志；建议改用 X-Cron-Secret 请求头")

    # 最小间隔：密钥一旦泄露，也限制被无限次触发的爆炸半径。
    now_ts = time.time()
    with _cron_guard_lock:
        elapsed = now_ts - _cron_last_start_ts
        if _cron_last_start_ts and elapsed < CRON_MIN_INTERVAL_SEC:
            return {
                "status": "skipped",
                "reason": "too_frequent",
                "retry_after_sec": round(CRON_MIN_INTERVAL_SEC - elapsed, 1),
            }, 200
        _cron_last_start_ts = now_ts

    now = get_beijing_now()
    caller_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    print(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Cron triggered by: "
        f"{request.headers.get('User-Agent', 'Unknown')} from {caller_ip}"
    )
    # Optional: skip cron during recent user activity to reduce contention.
    # if (now - LAST_ACTIVE_TIME).total_seconds() < 300:
    #     print(f"[{now}] Skip cron: recent activity detected")
    #     return {"status": "skipped", "reason": "recent activity"}, 200

    try:
        summary = scrape_all_sites()
        summary["executed_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        return summary, 200
    except Exception as e:
        print(f"Cron error: {e}")
        return {"status": "error", "message": str(e)}, 500

# ==========================================
# 4. Scraping
# ==========================================

def normalize_title(title_text):
    """Normalize title by removing punctuation/whitespace and lower-casing."""
    if not title_text:
        return ""
    t = title_text.lower()
    t = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", t)
    return t
def is_similar_title(norm_title, norm_titles, threshold=TITLE_SIMILARITY_THRESHOLD):
    return find_duplicate_title(norm_title, norm_titles, threshold=threshold)[0] is not None

def find_duplicate_title(norm_title, norm_titles, threshold=TITLE_SIMILARITY_THRESHOLD):
    """返回 (撞上的标题, 命中的规则)，没撞上返回 (None, None)。

    规则顺序与原来一致：完全相同 → 长度差 > 8 跳过 → 相似度 ≥ 0.85；
    只在「长度差 > 8 被跳过」这一路补一条包含判定：短的一方完整出现在
    长的一方里就算重。治的是「原标题 + 尾巴话」这类只差一个字却被长度门槛
    顶出门槛的情况。短标题必须够长（≥ TITLE_CONTAINMENT_MIN_LEN 个归一化字符）
    才参与，否则「工行」这种两字标题会被任何长标题误吞。
    """
    if not norm_title:
        return None, None
    for existing in norm_titles:
        if not existing:
            continue
        if norm_title == existing:
            return existing, "标题完全相同"
        length_gap = abs(len(norm_title) - len(existing))
        if length_gap > 8:
            shorter, longer = (
                (norm_title, existing) if len(norm_title) <= len(existing) else (existing, norm_title)
            )
            if len(shorter) >= TITLE_CONTAINMENT_MIN_LEN and shorter in longer:
                return existing, f"短标题完整包含在长标题内（长度差 {length_gap}，本会被跳过）"
            continue
        ratio = SequenceMatcher(None, norm_title, existing).ratio()
        if ratio >= threshold:
            return existing, f"相似度 {ratio:.3f} ≥ {threshold}"
    return None, None

def ensure_runtime_tables(conn):
    ensure_article_feature_columns(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_state (
            site_key TEXT PRIMARY KEY,
            last_run_at TIMESTAMP,
            last_seen_url TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_log (
            id BIGSERIAL PRIMARY KEY,
            last_scrape TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()

def load_scrape_state(conn):
    rows = conn.execute("SELECT site_key, last_run_at, last_seen_url FROM scrape_state").fetchall()
    return {row["site_key"]: row for row in rows}

def is_site_due(cfg, state_row, now_beijing):
    interval_min = int(cfg.get("scrape_interval_min", 3))
    if not state_row or not state_row.get("last_run_at"):
        return True
    last_run_at = state_row["last_run_at"]
    return (now_beijing - last_run_at).total_seconds() >= interval_min * 60

def update_scrape_state(conn, site_key, last_seen_url, run_at):
    conn.execute(
        """
        INSERT INTO scrape_state (site_key, last_run_at, last_seen_url, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (site_key) DO UPDATE
        SET last_run_at = EXCLUDED.last_run_at,
            last_seen_url = COALESCE(EXCLUDED.last_seen_url, scrape_state.last_seen_url),
            updated_at = CURRENT_TIMESTAMP
        """,
        (site_key, run_at, last_seen_url),
    )


def get_rotating_site(now_beijing):
    rotating_sites = ("iehou", "xianbao_icu")
    slot = (now_beijing.hour * 60 + now_beijing.minute) // 2
    return rotating_sites[slot % len(rotating_sites)]

def match_alert_group(title_lower, url, title_alert, url_alert):
    def _match_title_rule(rule):
        if isinstance(rule, dict):
            kw = (rule.get("keyword") or "").strip()
            group = (rule.get("alert_group") or "").strip()
        else:
            kw = (rule or "").strip()
            group = ""
        if not kw:
            return None
        kw_lower = kw.lower()
        kw_norm = normalize_title(kw)
        normalized_title = normalize_title(title_lower)
        if (kw_lower and kw_lower in title_lower) or (kw_norm and kw_norm in normalized_title):
            return group or kw
        return None

    def _match_url_rule(rule):
        if isinstance(rule, dict):
            kw = (rule.get("keyword") or "").strip()
            group = (rule.get("alert_group") or "").strip()
        else:
            kw = (rule or "").strip()
            group = ""
        if kw and kw in url:
            return group or kw
        return None

    matched_title = next((m for m in (_match_title_rule(r) for r in title_alert) if m), None)
    matched_url = next((m for m in (_match_url_rule(r) for r in url_alert) if m), None)
    matched = matched_title or matched_url
    if not matched:
        return None

    matched_lower = matched.lower()
    # If group name is explicitly configured in config_rules, use it directly.
    if matched in ALERT_GROUPS:
        return matched
    for group_name, aliases in ALERT_GROUPS.items():
        for alias in aliases:
            alias_norm = normalize_title(alias)
            matched_norm = normalize_title(matched_lower)
            if alias.lower() == matched_lower or (alias_norm and alias_norm == matched_norm):
                return group_name
    # Config-rule match is enough to push; fallback to matched value.
    return matched


# 关键词规则语法（向后兼容，老配置一个字符都不用改）：
#   红包            纯子串匹配，和以前完全一样
#   红包+领          标题必须同时含「红包」和「领」，顺序无关
#   红包+领+抽       多个必含词，全都要有
#   红包-消灭-返     含「红包」但不含任何一个排除词
# 命中后返回的是基础词（第一个 + 或 - 之前那一段），
# 这样 match_keyword / 首页 tag / 预警分组 / 正文预取判断都不受影响。
_KEYWORD_RULE_SPLIT_RE = re.compile(r"([+\-])")


def _parse_keyword_rule(rule):
    """把 "红包+领-消灭" 拆成 ("红包", ["领"], ["消灭"])。"""
    rule = (rule or "").strip()
    if not rule:
        return "", [], []
    parts = _KEYWORD_RULE_SPLIT_RE.split(rule)
    base = (parts[0] or "").strip()
    required, excluded = [], []
    for i in range(1, len(parts) - 1, 2):
        operator = parts[i]
        term = (parts[i + 1] or "").strip()
        if not term:
            continue
        (required if operator == "+" else excluded).append(term)
    return base, required, excluded


def _keyword_in_title(term, title_lower, title_norm):
    term_lower = (term or "").lower()
    if term_lower and term_lower in title_lower:
        return True
    term_norm = normalize_title(term or "")
    return bool(term_norm) and term_norm in title_norm


def keyword_match_in_title(title, keywords):
    title_lower = (title or "").lower()
    title_norm = normalize_title(title or "")
    for kw in keywords:
        base, required, excluded = _parse_keyword_rule(kw)
        if not base:
            continue
        if not _keyword_in_title(base, title_lower, title_norm):
            continue
        # 必含词：任何一个缺失都不算命中
        if any(not _keyword_in_title(t, title_lower, title_norm) for t in required):
            continue
        # 排除词：出现任何一个就否决
        if any(_keyword_in_title(t, title_lower, title_norm) for t in excluded):
            continue
        return base
    return None


def any_keyword_match_in_title(title, keywords):
    return keyword_match_in_title(title, keywords) is not None


# 列表页里混着的栏目 / 分页 / 登录页，不是文章详情页。
# 例：iehou 列表里「24小时热门」指向 /page-dayhot.htm，标题够长会被当成文章。
_NON_ARTICLE_PATH_RE = re.compile(
    r"^/(?:page-[^/]*\.htm|index(?:-\d+)?\.htm|user-[^/]*\.htm|portal/)",
    re.IGNORECASE,
)


def _is_non_article_url(url):
    """判断链接是不是列表页里的导航/分页/栏目页，而不是文章。"""
    try:
        path = urlparse(url).path or "/"
    except Exception:
        return False
    if path in ("", "/"):
        return True
    return bool(_NON_ARTICLE_PATH_RE.match(path))


def fetch_site_candidates(skey, cfg, last_seen_url, url_black=None, title_black=None,
                          known_urls=None):
    # 黑名单必须在「截断到 max_items_per_run 之前」生效。
    # new.xianbao.fun 首页 101 条里有 80 条是 /haodan/ 灌水帖，
    # 若等到入库前才过滤，40 个候选名额会被全部吃掉，
    # 真正想要的线报被挤出窗口后，下一轮 last_seen_url 已越过它，就永久抓不到了。
    url_black = url_black or []
    title_black = title_black or []
    known_urls = known_urls or set()

    result = {
        "site_key": skey,
        "site_name": cfg.get("name", skey),
        "candidates": [],
        "last_seen_url": last_seen_url or "",
        "matched_items": 0,
        "candidate_count": 0,
        "status": "ok",
    }

    try:
        print(f"\n=== start scrape: {skey} ===")
        r = session_req.get(cfg["list_url"], timeout=15)
        print(f"  status: {r.status_code}")
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        if skey == "xianbao_icu":
            # Avoid heavyweight CSS selectors on xianbao_icu. The page can be
            # large enough that nth-child descendant selectors trigger long
            # SoupSieve walks and get the worker killed by the platform.
            items = []
            seen_urls = set()
            for a in soup.find_all("a", href=True):
                href = (a.get("href") or "").strip()
                if not href:
                    continue
                if "/xianbao/detail" not in href and "/detail" not in href:
                    continue
                url = href if href.startswith("http") else (cfg["domain"] + (href if href.startswith("/") else "/" + href))
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                items.append(a)
        else:
            items = soup.select(cfg["list_selector"])
        result["matched_items"] = len(items)
        print(f"  matched items: {len(items)}")

        max_items = int(cfg.get("max_items_per_run", 12))
        newest_url = ""

        for item in items:
            if item.name == "a":
                a = item
            else:
                a = item.select_one(
                    "a[href*='view'], a[href*='thread'], a[href*='post'], a[href*='/detail'], a[href*='/xianbao/detail']"
                ) or item.find("a")

            if not a:
                continue

            title = a.get_text(strip=True).strip()
            if not title or len(title) < 5:
                continue

            href = a.get("href", "")
            url = href if href.startswith("http") else (cfg["domain"] + (href if href.startswith("/") else "/" + href))
            if _is_non_article_url(url):
                # 导航/分页/栏目页：不能让它参与候选，更不能成为 last_seen_url
                continue
            if url_black and any(b in url for b in url_black):
                continue
            if title_black and any_keyword_match_in_title(title, title_black):
                continue
            if not newest_url:
                newest_url = url
                result["last_seen_url"] = newest_url

            # 增量判定：不能用「碰到上轮那条 URL 就停」，也不能用「连续 N 条已入库就停」。
            # 列表顶部可能是置顶公告（new.xianbao.fun 的 /gonggao/ 就是），它永远是
            # 「已入库」状态；只要设断点，就可能在第 1~2 条上误判成「已经追上历史」，
            # 导致下面的新线报永远扫不到（这正是线报库卡了将近一周的原因）。
            # 所以这里不设断点：逐条扫完整页，已入库的直接跳过。
            # 整页也就百来条字符串比较，开销可忽略；真正的开销是正文预取，
            # 而预取只针对候选（即未入库的新文章），不会被已入库的旧文放大。
            if known_urls and url in known_urls:
                continue

            result["candidates"].append({"title": title, "url": url})
            if len(result["candidates"]) >= max_items:
                break

        result["candidate_count"] = len(result["candidates"])
        print(f"  candidates after filters: {result['candidate_count']}")
        soup.decompose()
        gc.collect()
        return result
    except Exception as e:
        print(f"scrape error on {skey}: {e}")
        result["status"] = "error"
        result["error"] = str(e)
        return result

def build_article_view_url(article_id):
    if not article_id:
        return ""
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}/view?id={article_id}"
    return f"/view?id={article_id}"


def build_preview_text(text, limit=20):
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit] + "..."


COMMAND_TOKEN_RE = re.compile(r"(#小程序://\S+|#小程序\s*//\S+|mp://\S+)")
COMMAND_TOKEN_CORE_RE = re.compile(r"([A-Za-z0-9]{8,})")


def normalize_command_token(token):
    token = (token or "").strip()
    if not token:
        return ""
    core_matches = COMMAND_TOKEN_CORE_RE.findall(token)
    if core_matches:
        return f"mp://{core_matches[-1]}"
    return token


def extract_command_token(text):
    if not text:
        return ""
    match = COMMAND_TOKEN_RE.search(text)
    return match.group(1) if match else ""


def extract_command_tokens(text):
    if not text:
        return []
    return COMMAND_TOKEN_RE.findall(text)


def extract_normalized_command_tokens(text):
    if not text:
        return []
    normalized = []
    for token in COMMAND_TOKEN_RE.findall(text):
        normalized_token = normalize_command_token(token)
        if normalized_token:
            normalized.append(normalized_token)
    return normalized


def get_command_token_signature(text):
    tokens = extract_normalized_command_tokens(text)
    if not tokens:
        return ""
    return "\n".join(tokens)


def get_command_text_signature(text):
    return normalize_title(strip_command_token(text or ""))


def get_command_text_score(text):
    return len(get_command_text_signature(text))


def get_token_only_signature(text):
    tokens = extract_normalized_command_tokens(text)
    if not tokens:
        return ""
    stripped = strip_command_token(text)
    if stripped:
        return ""
    return "\n".join(tokens)


def strip_command_token(text):
    stripped = COMMAND_TOKEN_RE.sub("", text or "")
    stripped = re.sub(r"\s+", " ", stripped).strip(" -—|")
    return stripped.strip()


def fetch_article_command_token(url, site_key):
    if not url or site_key not in SITES_CONFIG:
        return ""
    try:
        r = session_req.get(url, timeout=10)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        selectors = SITES_CONFIG[site_key]["content_selector"].split(",")
        parts = []
        for sel in selectors:
            node = soup.select_one(sel.strip())
            if node:
                text = node.get_text(" ", strip=True)
                if text:
                    parts.append(text)
        soup.decompose()
        if not parts:
            return ""
        return "\n".join(extract_command_tokens(" ".join(parts)))
    except Exception:
        return ""


def fetch_article_token_only_signature(url, site_key):
    if not url or site_key not in SITES_CONFIG:
        return ""
    try:
        r = session_req.get(url, timeout=4)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        selectors = SITES_CONFIG[site_key]["content_selector"].split(",")
        parts = []
        for sel in selectors:
            node = soup.select_one(sel.strip())
            if node:
                text = node.get_text(" ", strip=True)
                if text:
                    parts.append(text)
        soup.decompose()
        if not parts:
            return ""
        return get_token_only_signature(" ".join(parts))
    except Exception:
        return ""


def fetch_article_command_meta(url, site_key):
    if not url or site_key not in SITES_CONFIG:
        return {"token_signature": "", "text_signature": "", "text_score": 0}
    try:
        r = session_req.get(url, timeout=4)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        selectors = SITES_CONFIG[site_key]["content_selector"].split(",")
        parts = []
        for sel in selectors:
            node = soup.select_one(sel.strip())
            if node:
                text = node.get_text(" ", strip=True)
                if text:
                    parts.append(text)
        soup.decompose()
        if not parts:
            return {"token_signature": "", "text_signature": "", "text_score": 0}
        joined = " ".join(parts)
        text_signature = get_command_text_signature(joined)
        return {
            "token_signature": get_command_token_signature(joined),
            "text_signature": text_signature,
            "text_score": len(text_signature),
        }
    except Exception:
        return {"token_signature": "", "text_signature": "", "text_score": 0}


def _send_one_notification(notify_title, notify_url, preview_title, preview_body):
    if not ALERT_ENABLED:
        return
    text_parts = [notify_title, preview_title]
    if preview_body:
        text_parts.append(preview_body)
    text_parts.append(notify_url)
    text = "\n".join(text_parts)

    if FEISHU_WEBHOOK:
        try:
            content_rows = [
                [{"tag": "text", "text": preview_title}],
            ]
            if preview_body:
                content_rows.append([{"tag": "text", "text": preview_body}])
            content_rows.append([{"tag": "a", "text": "查看线报", "href": notify_url}])
            requests.post(FEISHU_WEBHOOK, json={
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": notify_title,
                            "content": content_rows,
                        }
                    }
                },
            }, timeout=8)
        except Exception as e:
            print(f"feishu notify error: {e}")

    if WECHAT_WEBHOOK:
        try:
            requests.post(WECHAT_WEBHOOK, json={"msgtype": "text", "text": {"content": text}}, timeout=8)
        except Exception as e:
            print(f"wechat notify error: {e}")


def send_match_notifications(new_articles):
    if not ALERT_ENABLED or not new_articles:
        return []

    results = []
    for article in new_articles:
        notify_title = f"线报-{article['alert_keyword']}"
        notify_url = article.get("view_url") or article["url"]
        command_token = article.get("command_token", "")
        cleaned_title = strip_command_token(article["title"])
        preview_title = build_preview_text(cleaned_title or article["title"], limit=20)
        try:
            _send_one_notification(notify_title, notify_url, preview_title, command_token)
            results.append({"id": article["id"], "ok": True, "error": ""})
        except Exception as e:
            results.append({"id": article["id"], "ok": False, "error": str(e)})

    return results

def scrape_all_sites():
    global LAST_ACTIVE_TIME
    started_at = time.time()

    if scrape_lock.locked():
        print("scrape lock busy, skip this run")
        return {
            "status": "skipped",
            "reason": "lock_busy",
            "site_stats": {},
            "total_new": 0,
            "duration_sec": round(time.time() - started_at, 2),
        }

    with scrape_lock:
        conn = None
        site_stats = {}
        try:
            now_beijing = get_beijing_now()
            conn = get_db_connection()
            # DDL 已在启动时执行，此处只做数据操作
            rules = conn.execute("SELECT * FROM config_rules").fetchall()
            title_white = [r['keyword'] for r in rules if r['rule_type'] == 'white' and r['match_scope'] == 'title']
            title_black = [r['keyword'] for r in rules if r['rule_type'] == 'black' and r['match_scope'] == 'title']
            url_black = [r['keyword'] for r in rules if r['rule_type'] == 'black' and r['match_scope'] == 'url']
            title_alert_keywords = [r['keyword'] for r in rules if r['rule_type'] == 'alert' and r['match_scope'] == 'title']
            title_alert = [
                {"keyword": r["keyword"], "alert_group": (r.get("alert_group") or "").strip()}
                for r in rules if r['rule_type'] == 'alert' and r['match_scope'] == 'title'
            ]
            url_alert = [
                {"keyword": r["keyword"], "alert_group": (r.get("alert_group") or "").strip()}
                for r in rules if r['rule_type'] == 'alert' and r['match_scope'] == 'url'
            ]

            # Alert keywords also qualify items for scraping; white remains a separate concept.
            base_keywords = list(dict.fromkeys(ALL_BANK_VALS + title_white + title_alert_keywords))
            state_map = load_scrape_state(conn)
            due_sites = []
            skipped_sites = []
            log_stats = {}
            rotating_site = get_rotating_site(now_beijing)

            for skey, cfg in SITES_CONFIG.items():
                state_row = state_map.get(skey)
                if skey in {"iehou", "xianbao_icu"} and skey != rotating_site:
                    skipped_sites.append(skey)
                    site_stats[skey] = {"name": cfg.get("name", skey), "new": 0, "status": "rotation_skipped"}
                    log_stats[SITE_LOG_NAMES.get(skey, skey)] = "rotation_skipped"
                elif is_site_due(cfg, state_row, now_beijing):
                    due_sites.append((skey, cfg, (state_row or {}).get("last_seen_url") or ""))
                else:
                    skipped_sites.append(skey)
                    site_stats[skey] = {"name": cfg.get("name", skey), "new": 0, "status": "skipped"}
                    log_stats[SITE_LOG_NAMES.get(skey, skey)] = "skipped"

            seen_titles_this_run = set()
            current_run_token_best = {}
            current_run_body_best = {}
            recent_token_text_pairs = set()
            # (口令签名, 文字签名) -> 库里那条标题，只为判重日志能报出撞上了谁
            recent_signature_titles = {}
            # 标题判重和正文签名判重共用这一批回看记录（recent_norm_titles /
            # recent_token_text_pairs 都来自它），所以放宽窗口一次同时改掉了两处：
            # 原来 30 分钟，同一条线报被两个站相隔一小时转发就会各进一条。
            # 条数上限 300：判重是「每条候选 × 池子」比字符串，耗时随池子线性涨，
            # 封顶之后单轮开销与入库量脱钩。ORDER BY 走 idx_articles_update_sort。
            recent_articles = conn.execute(
                "SELECT title, token_only_signature FROM articles "
                f"WHERE updated_at > (now() - interval '{TITLE_DEDUPE_WINDOW}') "
                f"ORDER BY updated_at DESC, id DESC LIMIT {int(TITLE_DEDUPE_POOL_LIMIT)}"
            ).fetchall()
            # 判重命中不能静默丢：记下「被判掉的标题 + 凭什么判的 + 撞上库里哪条」，
            # 跑几天核对有没有误吞真线报。只打日志，不建表。
            dedupe_stats = {"title": 0, "signature": 0, "printed": 0}
            # 本轮已保留条目对应的标题（按签名），供日志报出「撞上本轮哪条」
            run_body_titles = {}
            run_token_titles = {}

            def _log_dedupe(kind, dropped_title, rule, matched_title):
                dedupe_stats[kind] += 1
                if dedupe_stats["printed"] >= DEDUPE_LOG_MAX_PER_RUN:
                    return
                dedupe_stats["printed"] += 1
                print(
                    f"[DEDUPE][{kind}] 丢弃：{dropped_title} "
                    f"| 依据：{rule} | 撞上：{matched_title}"
                )
                if dedupe_stats["printed"] == DEDUPE_LOG_MAX_PER_RUN:
                    print(f"[DEDUPE] 本轮判重日志已达上限 {DEDUPE_LOG_MAX_PER_RUN} 条，后续只计数")

            # 已入库 URL 集合：用于判断「是否已经追上历史」。
            # 窗口取 12 小时，远大于列表页覆盖的时间跨度，足够可靠；
            # 条数很少（这个站点一天也就几百条），开销可以忽略。
            known_urls = {
                row["url"] for row in conn.execute(
                    "SELECT url FROM articles WHERE updated_at > (now() - interval '12 hours')"
                ).fetchall()
            }
            recent_norm_titles = {normalize_title(row['title']) for row in recent_articles}
            # 归一化标题 -> 原始标题，只为判重日志能报出人话（不然打出的是去标点的串）
            recent_title_by_norm = {normalize_title(row['title']): row['title'] for row in recent_articles}
            run_title_by_norm = {}
            for row in recent_articles:
                token_signature = row.get('token_only_signature')
                if not token_signature:
                    token_signature = get_command_token_signature(row['title'])
                if not token_signature:
                    continue
                text_signature = get_command_text_signature(row['title'])
                recent_token_text_pairs.add((token_signature, text_signature))
                # 判重日志要能说出「撞上库里哪条」，这里按签名反查标题。
                recent_signature_titles.setdefault((token_signature, text_signature), row['title'])
            inserted_articles = []

            if due_sites:
                site_results = {}
                with ThreadPoolExecutor(max_workers=len(due_sites)) as executor:
                    future_map = {
                        executor.submit(
                            fetch_site_candidates, skey, cfg, last_seen_url,
                            url_black, title_black, known_urls
                        ): (skey, cfg, last_seen_url)
                        for skey, cfg, last_seen_url in due_sites
                    }
                    for future in as_completed(future_map):
                        skey, cfg, last_seen_url = future_map[future]
                        try:
                            site_results[skey] = future.result()
                        except Exception as e:
                            site_results[skey] = {
                                "site_key": skey,
                                "site_name": cfg.get("name", skey),
                                "candidates": [],
                                "last_seen_url": last_seen_url or "",
                                "matched_items": 0,
                                "status": "error",
                                "error": str(e),
                            }

                for skey, cfg, last_seen_url in due_sites:
                    site_order = len(site_stats)  # 0=线报库, 1=爱猴线报, 2=鲸线报 按处理顺序
                    result = site_results.get(skey) or {
                        "site_key": skey,
                        "site_name": cfg.get("name", skey),
                        "candidates": [],
                        "last_seen_url": last_seen_url or "",
                        "matched_items": 0,
                        "status": "error",
                        "error": "missing site result",
                    }
                    count = 0

                    if result.get("status") != "ok":
                        site_stats[skey] = {
                            "name": cfg.get("name", skey),
                            "new": 0,
                            "status": "error",
                            "error": result.get("error", "unknown error"),
                        }
                        log_stats[SITE_LOG_NAMES.get(skey, skey)] = "error"
                        update_scrape_state(conn, skey, result.get("last_seen_url") or None, now_beijing)
                        continue

                    # Prefetch article bodies in parallel for candidates likely to need alert matching.
                    body_cache = {}
                    pre_fetch = []
                    for item in result["candidates"]:
                        kw = keyword_match_in_title(item["title"], base_keywords)
                        if kw and kw in ALERT_ALL_VALS and skey in SITES_CONFIG:
                            pre_fetch.append((item["url"], skey, kw))
                    if pre_fetch:
                        with ThreadPoolExecutor(max_workers=3) as exec:
                            def fetch_and_parse(url, skey):
                                try:
                                    r = session_req.get(url, timeout=10)
                                    r.encoding = "utf-8"
                                    raw_text = r.text
                                    del r
                                    soup = BeautifulSoup(raw_text, "html.parser")
                                    del raw_text
                                    selectors = SITES_CONFIG[skey]["content_selector"].split(",")
                                    # 必须在 decompose() 之前转成字符串，否则 Tag 会失效
                                    raw_html = "".join(
                                        str(n) for n in _collect_content_nodes(soup, selectors)
                                    )
                                    soup.decompose()
                                    del soup
                                    if raw_html:
                                        if _is_boilerplate_only(raw_html):
                                            del raw_html
                                            gc.collect()
                                            return None
                                        inner = BeautifulSoup(raw_html, "html.parser")
                                        text = inner.get_text(" ", strip=True)
                                        inner.decompose()
                                        del inner
                                        result = {
                                            "raw_html": raw_html,
                                            "token_set": set(extract_normalized_command_tokens(text)),
                                            "text_only": strip_command_token(text),
                                        }
                                        del raw_html, text
                                        gc.collect()
                                        return result
                                    gc.collect()
                                except Exception:
                                    pass
                                return None
                            future_map = {exec.submit(fetch_and_parse, url, skey): (url, kw) for url, skey, kw in pre_fetch}
                            for f in as_completed(future_map):
                                url, kw = future_map[f]
                                body_cache[url] = {
                                    "data": f.result(),
                                    "prefetch_keyword": kw,
                                }

                    for item in result["candidates"]:
                        title = item["title"]
                        url = item["url"]
                        lower_t = title.lower()
                        lower_url = url.lower()
                        norm_title = normalize_title(title)

                        if not norm_title:
                            continue
                        dup_pool, dup_rule, dup_matched = None, None, None
                        dup_matched, dup_rule = find_duplicate_title(norm_title, seen_titles_this_run)
                        if dup_matched is None:
                            dup_matched, dup_rule = find_duplicate_title(norm_title, recent_norm_titles)
                            dup_pool = "近1小时已入库"
                        else:
                            dup_pool = "本轮已收"
                        if dup_matched is not None:
                            dup_label = run_title_by_norm.get(dup_matched) or recent_title_by_norm.get(dup_matched) or dup_matched
                            _log_dedupe("title", title, f"{dup_rule}（{dup_pool}）", dup_label)
                            continue
                        token_signature = get_command_token_signature(title)
                        text_signature = get_command_text_signature(title)
                        text_score = len(text_signature)

                        kw = keyword_match_in_title(title, base_keywords)
                        if not kw:
                            continue

                        seen_titles_this_run.add(norm_title)
                        run_title_by_norm[norm_title] = title
                        tag = kw
                        for b_name, b_v in BANK_KEYWORDS.items():
                            if kw in b_v:
                                tag = b_name
                                break

                        body_token_set = set()
                        body_text_only = ""
                        body_raw_html = ""
                        cached = body_cache.get(url)
                        prefetch_keyword = ""
                        if cached and cached.get("data"):
                            prefetch_keyword = cached.get("prefetch_keyword") or ""
                            body_raw_html = cached["data"]["raw_html"]
                            body_token_set = cached["data"]["token_set"]
                            body_text_only = cached["data"]["text_only"]

                        if body_token_set:
                            body_sig = "\n".join(sorted(body_token_set))
                            body_text_len = len(body_text_only)
                            body_text_sig = normalize_title(body_text_only)

                            # Cross-run dedupe: same token set and same normalized body text.
                            if (body_sig, body_text_sig) in recent_token_text_pairs:
                                _log_dedupe(
                                    "signature", title, "正文口令+正文文字与库里相同",
                                    recent_signature_titles.get((body_sig, body_text_sig), ""),
                                )
                                continue

                            # Same-run dedupe: same token set.
                            # 优先保留抓取顺序在前（site_order 更小）的；同站点则留文字多的。
                            best_seen = current_run_body_best.get(body_sig)
                            if best_seen:
                                if best_seen["site_order"] < site_order:
                                    _log_dedupe("signature", title, f"本轮正文口令相同，站点 {best_seen['site_order']} 在前", run_body_titles.get(body_sig, ""))
                                    continue
                                if best_seen["site_order"] == site_order and best_seen["text_score"] >= body_text_len:
                                    _log_dedupe("signature", title, f"本轮正文口令相同且文字更少（{body_text_len} ≤ {best_seen['text_score']}）", run_body_titles.get(body_sig, ""))
                                    continue
                                # 当前文章胜出（站点更靠前，或同站点文字更多）
                                old_id = best_seen.get("article_id")
                                if old_id:
                                    conn.execute("DELETE FROM article_content WHERE url=(SELECT url FROM articles WHERE id=%s)", (old_id,))
                                    conn.execute("DELETE FROM articles WHERE id=%s", (old_id,))
                                    inserted_articles[:] = [a for a in inserted_articles if a["id"] != old_id]
                                current_run_body_best[body_sig] = {"text_score": body_text_len, "site_order": site_order, "article_id": None}
                                run_body_titles[body_sig] = title
                            else:
                                # Same-run partial-overlap dedupe: keep the entry with more tokens / text.
                                is_weaker = False
                                weaker_than_sig = ""
                                weaker_reason = ""
                                for existing_sig, existing_data in current_run_body_best.items():
                                    existing_set = set(existing_sig.split("\n"))
                                    common = body_token_set & existing_set
                                    if not common:
                                        continue
                                    if len(body_token_set) < len(existing_set):
                                        is_weaker = True
                                        weaker_than_sig = existing_sig
                                        weaker_reason = f"口令更少（{len(body_token_set)} < {len(existing_set)}）"
                                        break
                                    if len(body_token_set) == len(existing_set):
                                        if existing_data.get("site_order", 0) < site_order:
                                            is_weaker = True
                                            weaker_than_sig = existing_sig
                                            weaker_reason = "口令数相同，对方站点在前"
                                            break
                                        if existing_data.get("site_order", 0) == site_order and body_text_len <= existing_data["text_score"]:
                                            is_weaker = True
                                            weaker_than_sig = existing_sig
                                            weaker_reason = f"口令数相同且文字更少（{body_text_len} ≤ {existing_data['text_score']}）"
                                            break
                                if is_weaker:
                                    _log_dedupe("signature", title, f"本轮正文口令重叠，{weaker_reason}", run_body_titles.get(weaker_than_sig, ""))
                                    continue
                                current_run_body_best[body_sig] = {"text_score": body_text_len, "site_order": site_order}
                                run_body_titles[body_sig] = title
                        elif token_signature:
                            if (token_signature, text_signature) in recent_token_text_pairs:
                                _log_dedupe(
                                    "signature", title, "标题口令+文字与库里相同",
                                    recent_signature_titles.get((token_signature, text_signature), ""),
                                )
                                continue
                            best_seen = current_run_token_best.get(token_signature)
                            if best_seen:
                                if best_seen["site_order"] < site_order:
                                    _log_dedupe("signature", title, f"本轮标题口令相同，站点 {best_seen['site_order']} 在前", run_token_titles.get(token_signature, ""))
                                    continue
                                if best_seen["site_order"] == site_order and best_seen["text_score"] >= text_score:
                                    _log_dedupe("signature", title, f"本轮标题口令相同且文字更少（{text_score} ≤ {best_seen['text_score']}）", run_token_titles.get(token_signature, ""))
                                    continue
                                old_id = best_seen.get("article_id")
                                if old_id:
                                    conn.execute("DELETE FROM articles WHERE id=%s", (old_id,))
                                    inserted_articles[:] = [a for a in inserted_articles if a["id"] != old_id]
                            current_run_token_best[token_signature] = {
                                "text_score": text_score,
                                "text_signature": text_signature,
                                "site_order": site_order,
                                "article_id": None,
                            }
                            run_token_titles[token_signature] = title

                        if 'jd.com' in lower_url or 'tb.cn' in lower_url or 'jd.com' in lower_t or 'tb.cn' in lower_t:
                            continue
                        if any(b in url for b in url_black) or any_keyword_match_in_title(title, title_black):
                            continue

                        with conn.cursor() as cur:
                            cur.execute(
                                'INSERT INTO articles (title, url, site_source, match_keyword, original_time, token_only_signature) '
                                'VALUES (%s, %s, %s, %s, %s, %s) '
                                'ON CONFLICT (url) DO NOTHING RETURNING id',
                                (title, url, skey, tag, now_beijing.strftime("%Y-%m-%d %H:%M"), token_signature or None),
                            )
                            inserted_row = cur.fetchone()
                            if inserted_row:
                                article_id = inserted_row["id"]
                                count += 1
                                conn.execute(
                                    "UPDATE articles SET prefetch_keyword=%s WHERE id=%s",
                                    (prefetch_keyword or None, article_id),
                                )
                                if body_raw_html:
                                    conn.execute(
                                        "INSERT INTO article_content (url, content) VALUES (%s, %s) "
                                        "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content, updated_at = CURRENT_TIMESTAMP",
                                        (url, body_raw_html),
                                    )
                                    conn.execute(
                                        "UPDATE articles SET content_prefetched=1 WHERE id=%s",
                                        (article_id,),
                                    )
                                    if not token_signature and body_token_set:
                                        body_sig = "\n".join(sorted(body_token_set))
                                        conn.execute(
                                            "UPDATE articles SET token_only_signature=%s WHERE id=%s",
                                            (body_sig, article_id),
                                        )
                                    # 记录 article_id，供同批次替换用
                                    if body_token_set:
                                        body_sig = "\n".join(sorted(body_token_set))
                                        current_run_body_best[body_sig] = {"text_score": len(body_text_only), "site_order": site_order, "article_id": article_id}
                                if token_signature and token_signature in current_run_token_best:
                                    current_run_token_best[token_signature]["article_id"] = article_id
                                matched_alert = match_alert_group(lower_t, url, title_alert, url_alert)
                                if matched_alert:
                                    conn.execute(
                                        "UPDATE articles SET alert_keyword=%s WHERE id=%s",
                                        (matched_alert, article_id),
                                    )
                                if matched_alert:
                                    inserted_articles.append(
                                        {
                                            "id": article_id,
                                            "view_url": build_article_view_url(article_id),
                                            "title": title,
                                            "command_token": "\n".join(extract_command_tokens(title)) or fetch_article_command_token(url, skey),
                                            "url": url,
                                            "tag": tag,
                                            "site_key": skey,
                                            "alert_keyword": matched_alert,
                                        }
                                    )

                    matched_items = int(result.get("matched_items", 0) or 0)
                    candidate_count = int(result.get("candidate_count", 0) or 0)
                    site_stats[skey] = {
                        "name": cfg.get("name", skey),
                        "new": count,
                        "matched": matched_items,          # 列表页命中的行数
                        "candidates": candidate_count,     # 过滤后真正处理的条数
                        "status": "ok",
                    }
                    # 记录成 "新增/候选"。用 candidates 而不是 matched：
                    # matched 只反映列表页有多少行，候选为 0 时它依然是 101，
                    # 会把「一条都没抓到」伪装成正常。
                    log_stats[SITE_LOG_NAMES.get(skey, skey)] = f"{count}/{candidate_count}"
                    update_scrape_state(conn, skey, result.get("last_seen_url") or None, now_beijing)
                    print(f"  {skey} new items: {count} "
                          f"(rows {matched_items}, candidates {candidate_count})")

            # Commit freshly inserted articles before non-critical side effects.
            conn.commit()
            notify_results = send_match_notifications(inserted_articles)
            notified = sum(1 for r in notify_results if r.get("ok"))
            for r in notify_results:
                conn.execute(
                    "UPDATE articles SET notified=%s, notified_at=%s, notify_error=%s WHERE id=%s",
                    (
                        1 if r.get("ok") else 0,
                        now_beijing if r.get("ok") else None,
                        None if r.get("ok") else (r.get("error") or "")[:500],
                        r["id"],
                    ),
                )
            conn.execute(
                "DELETE FROM article_content ac WHERE EXISTS ("
                "SELECT 1 FROM articles a WHERE a.url = ac.url "
                "AND a.site_source != 'user' "
                "AND COALESCE(a.is_featured, 0) = 0 "
                "AND a.updated_at < (now() - interval '7 days')"
                ")"
            )
            conn.execute("DELETE FROM articles WHERE site_source != 'user' AND COALESCE(is_featured, 0) = 0 AND updated_at < (now() - interval '7 days')")
            # 访客表只增不减，这里顺手清掉 90 天没来过的 IP，
            # 避免 admin 页的 SUM(visit_count) 随时间退化成全表扫。
            conn.execute("DELETE FROM visit_stats WHERE last_visit < (now() - interval '90 days')")
            conn.execute(
                'INSERT INTO scrape_log(last_scrape) VALUES(%s)',
                (f"[{now_beijing.strftime('%m-%d %H:%M')}] {log_stats} 推送：{notified}",),
            )
            conn.execute(
                'DELETE FROM scrape_log WHERE id NOT IN ('
                'SELECT id FROM scrape_log ORDER BY id DESC LIMIT 1000'
                ')'
            )
            conn.commit()

            total_new = sum(int(v.get("new", 0)) for v in site_stats.values())
            # 判重统计：跑几天靠这行核对有没有误吞真线报（明细见 [DEDUPE] 日志）。
            # 顺手带上池子规模，窗口/上限改完后单轮比对的条数就封顶在这里。
            print(
                f"[DEDUPE] 本轮判重：标题 {dedupe_stats['title']} 条 / "
                f"签名 {dedupe_stats['signature']} 条；"
                f"回看池 {len(recent_articles)}/{TITLE_DEDUPE_POOL_LIMIT} 条"
                f"（窗口 {TITLE_DEDUPE_WINDOW}），本轮新增 {total_new} 条"
            )
            if total_new:
                # 有新文章入库就清掉页面缓存，让「有新线报」点开就是新的
                invalidate_page_cache()
            return {
                "status": "success",
                "site_stats": site_stats,
                "total_new": total_new,
                "notified": notified,
                "skipped_sites": skipped_sites,
                "duration_sec": round(time.time() - started_at, 2),
            }

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(f"Scrape Loop Error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "site_stats": site_stats,
                "total_new": sum(int(v.get("new", 0)) for v in site_stats.values()) if site_stats else 0,
                "duration_sec": round(time.time() - started_at, 2),
            }
        finally:
            try:
                if conn:
                    close_db_connection(conn)
            except Exception:
                pass


# ==========================================
# 5. One-time schema bootstrap
# ==========================================
# DDL 只在进程内执行一次，不再挂在每个请求上。生产环境用 gunicorn app:app
# 启动时 __main__ 分支不会执行，所以这里用 before_request + 一次性标志兜底。
_schema_ready = False
_schema_lock = threading.Lock()
_schema_next_try = 0.0
SCHEMA_RETRY_INTERVAL = 30  # 秒；初始化失败后隔多久再试


def _startup_ensure_schema():
    """执行一次 DDL 检查（幂等）。失败不抛出，避免应用起不来。"""
    global _schema_ready, _schema_next_try
    if _schema_ready:
        return
    if not DATABASE_URL:
        _schema_ready = True
        return
    conn = None
    try:
        conn = get_db_connection()
        # ensure_runtime_tables 内部已包含 ensure_article_feature_columns。
        ensure_config_rules_schema(conn)
        ensure_runtime_tables(conn)
        _schema_ready = True
        print("[SCHEMA] bootstrap done")
    except Exception as e:
        _schema_next_try = time.time() + SCHEMA_RETRY_INTERVAL
        print(f"[SCHEMA WARN] bootstrap failed, retry in {SCHEMA_RETRY_INTERVAL}s: {e}")
    finally:
        close_db_connection(conn)


@app.before_request
def _ensure_schema_once():
    # 非阻塞抢锁：DB 不可用时不要让所有请求排队等同一个初始化。
    if _schema_ready or time.time() < _schema_next_try:
        return
    if not _schema_lock.acquire(blocking=False):
        return
    try:
        _startup_ensure_schema()
    finally:
        _schema_lock.release()


if __name__ == '__main__':
    # _security_audit() 已在模块导入时执行过
    with _schema_lock:
        _startup_ensure_schema()
    print("Serving on port 8080...")
    serve(app, host='0.0.0.0', port=8080, threads=80)

