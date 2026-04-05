"""User accounts for multi-tenant tracker (bcrypt password hashes in SQLite)."""

from __future__ import annotations

import sqlite3

import bcrypt


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("ascii")


def verify_password(plain: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(
            plain.encode("utf-8"),
            password_hash.encode("ascii"),
        )
    except (ValueError, TypeError):
        return False


def normalize_email(email: str) -> str:
    return " ".join(str(email).strip().lower().split())


def create_user(conn: sqlite3.Connection, email: str, password: str) -> int:
    """Insert a user. Raises sqlite3.IntegrityError on duplicate email."""
    em = normalize_email(email)
    if not em or "@" not in em:
        raise ValueError("Invalid email")
    if not password:
        raise ValueError("Password required")
    ph = hash_password(password)
    cur = conn.execute(
        """
        INSERT INTO users (email, password_hash, created_at)
        VALUES (?, ?, datetime('now'))
        """,
        (em, ph),
    )
    conn.commit()
    return int(cur.lastrowid)


def authenticate(conn: sqlite3.Connection, email: str, password: str) -> int | None:
    """Return user id if credentials match, else None."""
    em = normalize_email(email)
    row = conn.execute(
        "SELECT id, password_hash FROM users WHERE email = ?",
        (em,),
    ).fetchone()
    if row is None:
        return None
    if not verify_password(password, str(row[1])):
        return None
    return int(row[0])


def user_id_for_email(conn: sqlite3.Connection, email: str) -> int | None:
    em = normalize_email(email)
    row = conn.execute("SELECT id FROM users WHERE email = ?", (em,)).fetchone()
    return int(row[0]) if row else None


def list_user_emails(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    rows = conn.execute("SELECT id, email FROM users ORDER BY id").fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_user_profile(conn: sqlite3.Connection, user_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, email, display_name FROM users WHERE id = ?",
        (int(user_id),),
    ).fetchone()
    if row is None:
        return None
    dn = row[2]
    return {
        "id": int(row[0]),
        "email": str(row[1]),
        "display_name": (str(dn).strip() if dn else None) or None,
    }


def update_display_name(
    conn: sqlite3.Connection, user_id: int, display_name: str | None
) -> None:
    v = (display_name or "").strip() or None
    conn.execute(
        "UPDATE users SET display_name = ? WHERE id = ?",
        (v, int(user_id)),
    )
    conn.commit()


def update_password(
    conn: sqlite3.Connection,
    user_id: int,
    old_password: str,
    new_password: str,
) -> tuple[bool, str]:
    row = conn.execute(
        "SELECT password_hash FROM users WHERE id = ?",
        (int(user_id),),
    ).fetchone()
    if row is None:
        return False, "User not found."
    if not verify_password(old_password, str(row[0])):
        return False, "Current password is incorrect."
    np = (new_password or "").strip()
    if len(np) < 6:
        return False, "New password must be at least 6 characters."
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (hash_password(np), int(user_id)),
    )
    conn.commit()
    return True, ""
