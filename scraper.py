# scraper.py
import traceback
from app import (
    scrape_all_sites,
    get_db_connection,
    close_db_connection,
    _startup_ensure_schema,
)

if __name__ == "__main__":
    print("Scraper started...")
    conn = None
    try:
        # 建表/加列等 DDL 只在入口执行一次，不在抓取循环里重复跑。
        _startup_ensure_schema()

        # 连接可用性探测（走连接池，用完归还）
        conn = get_db_connection()
    except Exception as e:
        print("Scraper init error:")
        traceback.print_exc()
    finally:
        close_db_connection(conn)

    try:
        # 强制抓取（忽略无人访问休眠）
        scrape_all_sites()
        print("Scraper finished successfully!")
    except Exception as e:
        print("Scraper error:")
        traceback.print_exc()
