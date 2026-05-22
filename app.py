import os
import sys
import threading
import time
import base64
import re
import gc  # Manual GC after large HTML parses.
import ipaddress
import socket
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import wraps, lru_cache
from urllib.parse import quote, unquote, urlparse, urljoin
import psycopg
from psycopg.rows import dict_row
import requests
from requests.adapters import HTTPAdapter
from flask import Flask, flash, render_template, request, Response, redirect, session, url_for
from bs4 import BeautifulSoup
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

# Secrets and runtime config
SITE_TITLE = "古希腊掌管羊毛的神"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
app.secret_key = os.environ.get('SECRET_KEY', 'xianbao_secret_key_888') 
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', '123')  
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 
CRON_SECRET = os.environ.get('CRON_SECRET', 'xianbao_secret_key_999')
PUBLISH_API_TOKEN = os.environ.get("PUBLISH_API_TOKEN", "").strip()
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "").strip()
WECHAT_WEBHOOK = os.environ.get("WECHAT_WEBHOOK", "").strip()
ALERT_ENABLED = os.environ.get("ALERT_ENABLED", "0").strip() == "1"
DETAIL_SIGNATURE_FETCH_ENABLED = os.environ.get("DETAIL_SIGNATURE_FETCH_ENABLED", "0").strip() == "1"
PUBLIC_BASE_URL = (
    os.environ.get("PUBLIC_BASE_URL", "").strip()
    or os.environ.get("RENDER_EXTERNAL_URL", "").strip()
).rstrip("/")
# Local/dev safety switch
ALLOW_INSECURE_DEFAULTS = os.environ.get('ALLOW_INSECURE_DEFAULTS', '1').strip() == '1'

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
        "content_selector": "main > div:nth-of-type(2) > div > div, .prose, .prose-max, .content, .entry-content, .post-body, .detail-body, .markdown, .article-detail, .text",
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


def ensure_secure_config_or_exit():
    """Fail fast on insecure defaults in production-like environments."""
    problems = []

    if not os.environ.get('SECRET_KEY') or app.secret_key == 'xianbao_secret_key_888':
        problems.append("SECRET_KEY is missing or still using default")

    if not os.environ.get('ADMIN_PASSWORD') or ADMIN_PASSWORD == '123':
        problems.append("ADMIN_PASSWORD is missing or still using default")

    if not os.environ.get('CRON_SECRET') or CRON_SECRET == 'xianbao_secret_key_999':
        problems.append("CRON_SECRET is missing or still using default")

    if not os.environ.get('DATABASE_URL'):
        problems.append("DATABASE_URL is missing (Supabase Postgres DSN)")

    if not problems:
        return

    msg = (
        "Detected insecure defaults; startup aborted.\n"
        + "\n".join([f"- {p}" for p in problems])
        + "\n\nSet env vars SECRET_KEY, ADMIN_PASSWORD, CRON_SECRET, DATABASE_URL."
        + "\nFor local debugging only, set ALLOW_INSECURE_DEFAULTS=1."
    )

    if ALLOW_INSECURE_DEFAULTS:
        _warn(msg)
        return

    raise RuntimeError(msg)

# Last user activity timestamp
LAST_ACTIVE_TIME = get_beijing_now()

# ==========================================
# 2. Database and helpers
# ==========================================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

# Local connection helper
def get_db_connection():
    dsn = DATABASE_URL
    if not dsn:
        raise RuntimeError("DATABASE_URL not set. Configure env var DATABASE_URL before start.")
    return psycopg.connect(dsn, row_factory=dict_row)


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
    """Extract source/original URL from article HTML using per-site config first."""
    if not html_content:
        return fallback_url

    try:
        soup = BeautifulSoup(html_content, "lxml")
    except Exception:
        soup = BeautifulSoup(html_content, "html.parser")

    cfg = SITES_CONFIG.get(site_key, {}) if site_key else {}
    selectors = cfg.get("original_url_selectors", []) or []
    regexes = cfg.get("original_url_regexes", []) or []

    def _normalize_href(href: str) -> str:
        href = (href or "").strip()
        if not href:
            return ""
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin(fallback_url, href) if fallback_url else href
        return href if href.startswith(("http://", "https://")) else ""

    # 1) Site-level selectors (highest priority)
    for sel in selectors:
        try:
            for a in soup.select(sel):
                href = _normalize_href(a.get("href", ""))
                if href:
                    return href
        except Exception:
            continue

    # 2) Keyword anchor text
    keywords = ("source", "origin", "original", "from", "link", "jump")
    for a in soup.select("a[href]"):
        href = _normalize_href(a.get("href", ""))
        if not href:
            continue
        t = a.get_text(" ", strip=True).lower()
        if any(k.lower() in t for k in keywords):
            return href

    # 3) Site-level regex + defaults
    text_blob = soup.get_text(" ", strip=True)
    default_patterns = [
        r"(?:source|origin|original|from|link)\s*[:锛歖?\s*(https?://[^\s<>\"']+)",
        r"(https?://[^\s<>\"']+)",
    ]
    for pat in list(regexes) + default_patterns:
        try:
            m = re.search(pat, text_blob, flags=re.IGNORECASE)
            if not m:
                continue
            if m.lastindex:
                for i in range(1, m.lastindex + 1):
                    cand = (m.group(i) or "").strip()
                    if cand.startswith(("http://", "https://")):
                        return cand
            cand = (m.group(0) or "").strip()
            if cand.startswith(("http://", "https://")):
                return cand
        except Exception:
            continue

    # 4) Fallback first http(s) link
    for a in soup.select("a[href]"):
        href = _normalize_href(a.get("href", ""))
        if href:
            return href

    return fallback_url


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

def clean_html(html_content, site_key):
    if not html_content:
        return ""

    # Convert plain URL after a colon (e.g. "????: https://...") into links.
    html_content = re.sub(
        r'([:?]\s*)(https?://[^\s<"]+)',
        r'\1<a href="\2" target="_blank" rel="noopener noreferrer" style="color:#007aff; text-decoration:underline; word-break:break-all;">\2</a>',
        html_content,
        flags=re.IGNORECASE,
    )

    soup = BeautifulSoup(html_content, "lxml")

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

            tag.attrs = {
                'src': proxy_url,
                'loading': 'lazy',
                'style': 'max-width:100%; height:auto; border-radius:8px; margin:10px 0;'
            }

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

    # Capture output before releasing the soup tree.
    result = str(soup)
    soup.decompose()
    return result



def record_visit():
    ua = request.headers.get('User-Agent', '')
    if 'HealthCheck' in ua or 'Zeabur' in ua: return
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    
    global LAST_ACTIVE_TIME
    LAST_ACTIVE_TIME = get_beijing_now()
    
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
        conn.commit(); conn.close()
    except: pass

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
        conn.close()

# ==========================================
# 3. Routes
# ==========================================

@app.route('/')
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
    ensure_article_feature_columns(conn)
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
        keywords = q.strip().split()
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
    
    articles = conn.execute(
        f'SELECT articles.* {from_sql} {where} {order_sql} LIMIT %s OFFSET %s',
        params + [PER_PAGE, (page-1)*PER_PAGE],
    ).fetchall()
    
    if total_from_join:
        total_sql = f'SELECT COUNT(*) {from_sql} {where}'
        total = conn.execute(total_sql, params).fetchone()["count"]
    else:
        total = conn.execute(f'SELECT COUNT(*) {from_sql} {where}', params).fetchone()["count"]
    conn.close()

    return render_template('index.html', 
                           articles=articles, 
                           next_refresh_time=next_refresh_time,
                           bank_list=list(BANK_KEYWORDS.keys()), 
                           current_tag=tag, 
                           q=q, 
                           current_page=page, 
                           total_pages=(total+PER_PAGE-1)//PER_PAGE,
                           latest_id=articles[0]['id'] if articles else 0,
                           today_str=now.strftime("%Y-%m-%d"))

@app.route("/view")
def view():
    article_id = request.args.get("id", type=int)
    conn = get_db_connection()
    row = conn.execute("SELECT * FROM articles WHERE id=%s", (article_id,)).fetchone()
    if not row:
        return "内容不存在", 404
    
    url, site_key, title = row["url"], row["site_source"], row["title"]
    original_url = url
    cached = conn.execute("SELECT content FROM article_content WHERE url=%s", (url,)).fetchone()
    content = ""

    if cached and cached['content']:
        original_url = safe_extract_original_url(cached["content"], fallback_url=url, site_key=site_key)
        if site_key == "user" or site_key not in SITES_CONFIG:
            content = cached["content"]
        else:
            content = clean_html(cached["content"], site_key)
    elif site_key in SITES_CONFIG:
        try:
            r = session_req.get(url, timeout=10)
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Use two precise containers for xianbao_icu.
            if site_key == "xianbao_icu":
                content_parts = []
                
                # Main article body.
                node1 = soup.select_one('#__nuxt > div > section > main > div:nth-child(2) > div.el-col.el-col-24.el-col-xs-24.el-col-lg-16.is-guttered > div > div > div.article-content')
                if node1:
                    content_parts.append(str(node1))
                
                # Source / supplement block.
                node2 = soup.select_one('#__nuxt > div > section > main > div:nth-child(2) > div.el-col.el-col-24.el-col-xs-24.el-col-lg-16.is-guttered > div > div > div:nth-child(6) > div > div > div:nth-child(1)')
                if node2:
                    content_parts.append(str(node2))
                
                if content_parts:
                    # Join both blocks with a visible separator.
                    full_raw_content = "<br><br>".join(content_parts)
                    
                    # Clean common spacing noise.
                    full_raw_content = full_raw_content.replace('&nbsp;', ' ').replace('\xa0', ' ')
                    
                    # Turn source labels plus URLs into clickable links.
                    full_raw_content = re.sub(
                        r'(来源网址|原文链接|原文地址|来源地址)[:：]?\s*(https?://[^\s<"]+)',
                        r'<br><br>\1: <a href="\2" target="_blank" rel="noopener noreferrer" style="color:#0066cc; text-decoration:underline;">\2</a><br>',
                        full_raw_content,
                        flags=re.IGNORECASE | re.MULTILINE
                    )
                    
                    conn.execute(
                        "INSERT INTO article_content(url, content) VALUES(%s, %s) "
                        "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content, updated_at = CURRENT_TIMESTAMP",
                        (url, full_raw_content),
                    )
                    conn.commit()
                    original_url = safe_extract_original_url(full_raw_content, fallback_url=url, site_key=site_key)
                    content = clean_html(full_raw_content, site_key)
                else:
                    content = "暂无核心内容"
            else:
                # Keep the generic logic for other sites.
                selectors = SITES_CONFIG[site_key]["content_selector"].split(',')
                content_nodes = []
                for sel in selectors:
                    node = soup.select_one(sel.strip())
                    if node: content_nodes.append(str(node))
                
                if content_nodes:
                    full_raw_content = "".join(content_nodes)
                    conn.execute(
                        "INSERT INTO article_content(url, content) VALUES(%s, %s) "
                        "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content, updated_at = CURRENT_TIMESTAMP",
                        (url, full_raw_content),
                    )
                    conn.commit()
                    original_url = safe_extract_original_url(full_raw_content, fallback_url=url, site_key=site_key)
                    content = clean_html(full_raw_content, site_key)
                else:
                    content = "暂无内容"
                    
        except Exception as e:
            print(f"Error fetching content: {e}")
            content = "加载原文失败，请稍后重试或打开原文链接。"
    conn.close()
    return render_template("detail.html", title=title, content=content, original_url=original_url, time=row['original_time'])

@app.route('/admin')
@login_required
def admin_panel():
    conn = get_db_connection()
    ensure_article_feature_columns(conn)
    ensure_config_rules_schema(conn)
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
        conn.close()

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
    conn = get_db_connection()
    ensure_article_feature_columns(conn)

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
        params.append(f"%{q}%")

    featured_articles = conn.execute(
        "SELECT id, title, match_keyword, original_time, updated_at, is_top, is_featured, featured_at "
        f"FROM articles WHERE {' AND '.join(where)} "
        "ORDER BY is_featured DESC, COALESCE(featured_at, updated_at) DESC, id DESC LIMIT 300",
        params,
    ).fetchall()
    conn.close()

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
    if request.method == 'POST':
        title = request.form.get('title')
        raw_content = request.form.get('content')
        is_top = 1 if request.form.get('publish_mode') == 'top' else 0
        def img_replacer(match):
            try:
                image_ext = (match.group(1) or "png").split(";")[0].strip().lower()
                cdn = upload_to_img_cdn(base64.b64decode(match.group(2)), image_ext=image_ext)
                return f'src="{cdn}"' if cdn else match.group(0)
            except: return match.group(0)
            
        processed = re.sub(r'src="data:image\/(.*?);base64,(.*?)"', img_replacer, raw_content)
        row = conn.execute("SELECT url FROM articles WHERE id=%s", (aid,)).fetchone()
        if row:
            conn.execute("UPDATE articles SET title=%s, is_top=%s WHERE id=%s", (title, is_top, aid))
            conn.execute("UPDATE article_content SET content=%s WHERE url=%s", (processed, row['url']))
            conn.commit()
        conn.close()
        return redirect('/admin')

    article = conn.execute("SELECT * FROM articles WHERE id=%s AND site_source='user'", (aid,)).fetchone()
    if not article:
        return "未找到文章", 404
    content = conn.execute("SELECT content FROM article_content WHERE url=%s", (article['url'],)).fetchone()['content']
    conn.close()
    return render_template('edit.html', article=article, content=content)

@app.route('/article/top/<int:aid>')
@login_required
def toggle_top(aid):
    conn = get_db_connection()
    conn.execute("UPDATE articles SET is_top = 1 - is_top WHERE id=%s", (aid,))
    conn.commit(); conn.close()
    return redirect('/admin')


@app.route('/article/feature/<int:aid>')
@login_required
def toggle_featured(aid):
    conn = get_db_connection()
    ensure_article_feature_columns(conn)
    row = conn.execute("SELECT id, title, url, site_source, match_keyword, is_featured, featured_notified FROM articles WHERE id=%s", (aid,)).fetchone()
    if not row:
        conn.close()
        return redirect(url_for('admin_featured'))

    is_now_featured = row['is_featured'] == 0

    conn.execute(
        "UPDATE articles SET is_featured = 1 - is_featured, featured_at = CASE WHEN is_featured = 0 THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=%s",
        (aid,),
    )

    if is_now_featured and not row['featured_notified'] and row['site_source'] != 'user':
        try:
            notify_url = build_article_view_url(row['id']) or row['url']
            cleaned_title = strip_command_token(row['title'])
            preview_title = build_preview_text(cleaned_title, limit=20)
            command_token = "\n".join(extract_command_tokens(row['title'])) or fetch_article_command_token(row['url'], row['site_source'])
            _send_one_notification("线报-精选", notify_url, preview_title, command_token)
            conn.execute("UPDATE articles SET featured_notified = 1 WHERE id=%s", (aid,))
        except Exception as e:
            print(f"featured notify error: {e}")

    conn.commit()
    conn.close()
    next_url = request.args.get('next') or url_for('admin_featured')
    return redirect(next_url)

@app.route('/article/delete/<int:aid>')
@login_required
def delete_article(aid):
    conn = get_db_connection()
    row = conn.execute("SELECT url FROM articles WHERE id=%s", (aid,)).fetchone()
    if row:
        conn.execute("DELETE FROM articles WHERE id=%s", (aid,))
        conn.execute("DELETE FROM article_content WHERE url=%s", (row['url'],))
        conn.commit()
    conn.close()
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
    has_alert_group = ensure_config_rules_schema(conn)
    try:
        if action == 'add' and kw:
            if has_alert_group:
                conn.execute(
                    "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (keyword, match_scope) DO UPDATE SET alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group)",
                    (rtype, kw, scope, alert_group if rtype == "alert" else None),
                )
            else:
                conn.execute(
                    "INSERT INTO config_rules (rule_type, keyword, match_scope) VALUES (%s, %s, %s) "
                    "ON CONFLICT (keyword, match_scope) DO NOTHING",
                    (rtype, kw, scope),
                )
        elif action == 'delete' and rid:
            conn.execute("DELETE FROM config_rules WHERE id=%s", (rid,))
        conn.commit()
    except Exception as e:
        print(f"rule operation failed: {e}")
    finally:
        conn.close()
    return redirect(url_for('admin_panel'))

@app.route('/admin/sync-bank-alerts', methods=['POST'])
@login_required
def sync_bank_alerts():
    conn = get_db_connection()
    has_alert_group = ensure_config_rules_schema(conn)
    added = 0
    try:
        for bank_name, keywords in BANK_KEYWORDS.items():
            for keyword in keywords:
                kw = (keyword or "").strip()
                if not kw:
                    continue
                with conn.cursor() as cur:
                    if has_alert_group:
                        cur.execute(
                            "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT (keyword, match_scope) DO UPDATE SET "
                            "alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group) "
                            "WHERE config_rules.rule_type = 'alert'",
                            ("alert", kw, "title", bank_name),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO config_rules (rule_type, keyword, match_scope) VALUES (%s, %s, %s) "
                            "ON CONFLICT (keyword, match_scope) DO NOTHING",
                            ("alert", kw, "title"),
                        )
                    added += cur.rowcount
        conn.commit()
        flash(f"Bank keywords synced to alert rules: +{added}", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Sync bank alert rules failed: {e}", "danger")
    finally:
        conn.close()
    return redirect(url_for('admin_panel'))


@app.route('/admin/sync-alert-group/<group_name>', methods=['POST'])
@login_required
def sync_alert_group(group_name):
    aliases = ALERT_GROUPS.get(group_name)
    if not aliases:
        flash(f"Unknown alert group: {group_name}", "warning")
        return redirect(url_for('admin_panel'))

    conn = get_db_connection()
    has_alert_group = ensure_config_rules_schema(conn)
    added = 0
    try:
        for keyword in aliases:
            kw = (keyword or "").strip()
            if not kw:
                continue
            with conn.cursor() as cur:
                if has_alert_group:
                    cur.execute(
                        "INSERT INTO config_rules (rule_type, keyword, match_scope, alert_group) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT (keyword, match_scope) DO UPDATE SET "
                        "alert_group = COALESCE(NULLIF(EXCLUDED.alert_group, ''), config_rules.alert_group) "
                        "WHERE config_rules.rule_type = 'alert'",
                        ("alert", kw, "title", group_name),
                    )
                else:
                    cur.execute(
                        "INSERT INTO config_rules (rule_type, keyword, match_scope) VALUES (%s, %s, %s) "
                        "ON CONFLICT (keyword, match_scope) DO NOTHING",
                        ("alert", kw, "title"),
                    )
                added += cur.rowcount
        conn.commit()
        flash(f"Synced alert group {group_name}: +{added}", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Sync alert group failed: {e}", "danger")
    finally:
        conn.close()
    return redirect(url_for('admin_panel'))

@app.route('/logs')
@login_required
def show_logs():
    conn = get_db_connection()
    logs = conn.execute('SELECT last_scrape FROM scrape_log ORDER BY created_at DESC, id DESC LIMIT 50').fetchall()
    visitors = conn.execute('SELECT * FROM visit_stats ORDER BY last_visit DESC LIMIT 30').fetchall()
    conn.close()
    return render_template('logs.html', logs=logs, visitors=visitors)

@lru_cache(maxsize=200)
def fetch_image_cached(url):
    """
    Download and cache remote images.
    Returns `(bytes, content-type)`.
    """
    r = session_req.get(url, headers={"User-Agent": HEADERS["User-Agent"], "Referer": ""}, timeout=15)
    return r.content, r.headers.get("Content-Type", "image/jpeg")


@app.route('/api/check_update')
def check_update():
    """Lightweight polling endpoint used by the homepage."""
    conn = get_db_connection()
    row = conn.execute("SELECT id FROM articles ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return {"last_id": row['id'] if row else 0}


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
    if request.method == 'POST' and request.form.get('password') == ADMIN_PASSWORD:
        session['is_logged_in'] = True
        return redirect('/admin')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear(); return redirect('/')

@app.route('/cron/scrape', methods=['GET', 'POST'])
def cron_scrape():
    # Accept secret from header or query/body parameter.
    provided_secret = (
        request.headers.get('Authorization') or
        request.args.get('secret') or
        request.form.get('secret')
    )
    
    if provided_secret != CRON_SECRET:
        return {"error": "Unauthorized"}, 401
    
    now = get_beijing_now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Cron triggered by: {request.headers.get('User-Agent', 'Unknown')}")
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
    if not norm_title:
        return False
    for existing in norm_titles:
        if not existing:
            continue
        if norm_title == existing:
            return True
        if abs(len(norm_title) - len(existing)) > 8:
            continue
        if SequenceMatcher(None, norm_title, existing).ratio() >= threshold:
            return True
    return False

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


def keyword_match_in_title(title, keywords):
    title_lower = (title or "").lower()
    title_norm = normalize_title(title or "")
    for kw in keywords:
        kw_lower = (kw or "").lower()
        kw_norm = normalize_title(kw or "")
        if kw_lower and kw_lower in title_lower:
            return kw
        if kw_norm and kw_norm in title_norm:
            return kw
    return None


def any_keyword_match_in_title(title, keywords):
    return keyword_match_in_title(title, keywords) is not None

def fetch_site_candidates(skey, cfg, last_seen_url):
    result = {
        "site_key": skey,
        "site_name": cfg.get("name", skey),
        "candidates": [],
        "last_seen_url": last_seen_url or "",
        "matched_items": 0,
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
            if not newest_url:
                newest_url = url
                result["last_seen_url"] = newest_url

            if last_seen_url and url == last_seen_url:
                break

            result["candidates"].append({"title": title, "url": url})
            if len(result["candidates"]) >= max_items:
                break

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
            ensure_runtime_tables(conn)
            ensure_config_rules_schema(conn)

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
            recent_articles = conn.execute(
                "SELECT title, token_only_signature FROM articles WHERE updated_at > (now() - interval '30 minutes')"
            ).fetchall()
            recent_norm_titles = {normalize_title(row['title']) for row in recent_articles}
            for row in recent_articles:
                token_signature = row.get('token_only_signature')
                if not token_signature:
                    token_signature = get_command_token_signature(row['title'])
                if not token_signature:
                    continue
                text_signature = get_command_text_signature(row['title'])
                recent_token_text_pairs.add((token_signature, text_signature))
            inserted_articles = []

            if due_sites:
                site_results = {}
                with ThreadPoolExecutor(max_workers=len(due_sites)) as executor:
                    future_map = {
                        executor.submit(fetch_site_candidates, skey, cfg, last_seen_url): (skey, cfg, last_seen_url)
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
                                    soup = BeautifulSoup(r.text, "html.parser")
                                    selectors = SITES_CONFIG[skey]["content_selector"].split(",")
                                    content_nodes = []
                                    for sel in selectors:
                                        node = soup.select_one(sel.strip())
                                        if node: content_nodes.append(str(node))
                                    soup.decompose()
                                    if content_nodes:
                                        raw_html = "".join(content_nodes)
                                        text = BeautifulSoup(raw_html, "html.parser").get_text(" ", strip=True)
                                        return {
                                            "raw_html": raw_html,
                                            "token_set": set(extract_normalized_command_tokens(text)),
                                            "text_only": strip_command_token(text),
                                        }
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
                        if is_similar_title(norm_title, seen_titles_this_run) or is_similar_title(norm_title, recent_norm_titles):
                            continue
                        token_signature = get_command_token_signature(title)
                        text_signature = get_command_text_signature(title)
                        text_score = len(text_signature)

                        kw = keyword_match_in_title(title, base_keywords)
                        if not kw:
                            continue

                        seen_titles_this_run.add(norm_title)
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
                                continue

                            # Same-run dedupe: same token set, keep the richer text body.
                            best_seen = current_run_body_best.get(body_sig)
                            if best_seen:
                                if best_seen["text_score"] >= body_text_len:
                                    continue
                                current_run_body_best[body_sig] = {"text_score": body_text_len}
                            else:
                                # Same-run partial-overlap dedupe: keep the entry with more tokens / text.
                                is_weaker = False
                                for existing_sig, existing_data in current_run_body_best.items():
                                    existing_set = set(existing_sig.split("\n"))
                                    common = body_token_set & existing_set
                                    if not common:
                                        continue
                                    if len(body_token_set) < len(existing_set):
                                        is_weaker = True
                                        break
                                    if len(body_token_set) == len(existing_set) and body_text_len <= existing_data["text_score"]:
                                        is_weaker = True
                                        break
                                if is_weaker:
                                    continue
                                current_run_body_best[body_sig] = {"text_score": body_text_len}
                        elif token_signature:
                            if (token_signature, text_signature) in recent_token_text_pairs:
                                continue
                            best_seen = current_run_token_best.get(token_signature)
                            if best_seen and best_seen["text_score"] >= text_score:
                                continue
                            current_run_token_best[token_signature] = {
                                "text_score": text_score,
                                "text_signature": text_signature,
                            }

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

                    site_stats[skey] = {"name": cfg.get("name", skey), "new": count, "status": "ok"}
                    log_stats[SITE_LOG_NAMES.get(skey, skey)] = count
                    update_scrape_state(conn, skey, result.get("last_seen_url") or None, now_beijing)
                    print(f"  {skey} new items: {count}")

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
                    conn.close()
            except Exception:
                pass

if __name__ == '__main__':
    get_db_connection().close()
    ensure_secure_config_or_exit()
    print("Serving on port 8080...")
    serve(app, host='0.0.0.0', port=8080, threads=80)

