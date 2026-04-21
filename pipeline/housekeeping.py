import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from utils import ts


def cleanup_expired_refresh_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM refresh_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def cleanup_expired_password_reset_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM password_reset_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def cleanup_expired_email_verification_tokens(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM email_verification_tokens WHERE expires_at < NOW()")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def run_housekeeping(conn):
    print(f"[{ts()}] Running housekeeping...")

    n = cleanup_expired_refresh_tokens(conn)
    print(f"[{ts()}]   Refresh tokens:          {n} expired row(s) removed")

    n = cleanup_expired_password_reset_tokens(conn)
    print(f"[{ts()}]   Password reset tokens:   {n} expired row(s) removed")

    n = cleanup_expired_email_verification_tokens(conn)
    print(f"[{ts()}]   Email verification tokens: {n} expired row(s) removed")

    print(f"[{ts()}] Housekeeping complete.")


if __name__ == "__main__":
    conn = get_conn()
    run_housekeeping(conn)
    conn.close()
