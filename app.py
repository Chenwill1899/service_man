#!/usr/bin/env python3
from __future__ import annotations

import hmac
import json
import os
import sqlite3
import socket
from contextlib import closing
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "server_usage.db"
INDEX_PATH = BASE_DIR / "index.html"
LOGIN_PATH = BASE_DIR / "login.html"
INPUT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
DISPLAY_DATETIME_FORMAT = "%Y-%m-%d %H:%M"
AUTH_USER = os.getenv("APP_AUTH_USER", "ausim")
SESSION_COOKIE = "server_usage_session"
SESSION_SECRET = os.getenv("APP_SESSION_SECRET", "server-usage-secret")
REVIEW_USER = "ausim"
REVIEW_PASSWORD = "ausim666"


def now_display() -> str:
    return datetime.now().strftime(DISPLAY_DATETIME_FORMAT)


def parse_input_datetime(value: str | None, field_name: str) -> str | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.strptime(value, INPUT_DATETIME_FORMAT)
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是 YYYY-MM-DDTHH:MM 格式") from exc
    return parsed.strftime(DISPLAY_DATETIME_FORMAT)


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                location TEXT NOT NULL,
                total_gpus INTEGER NOT NULL CHECK(total_gpus > 0)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                project_name TEXT NOT NULL,
                gpu_count INTEGER NOT NULL CHECK(gpu_count > 0),
                start_time TEXT NOT NULL,
                expected_end_time TEXT,
                actual_end_time TEXT,
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (server_id) REFERENCES servers(id)
            )
            """
        )

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(usage_records)").fetchall()
        }
        if "approval_status" not in columns:
            conn.execute(
                "ALTER TABLE usage_records ADD COLUMN approval_status TEXT NOT NULL DEFAULT 'approved'"
            )
        if "approved_at" not in columns:
            conn.execute("ALTER TABLE usage_records ADD COLUMN approved_at TEXT")
        if "approved_by" not in columns:
            conn.execute("ALTER TABLE usage_records ADD COLUMN approved_by TEXT")

        count = conn.execute("SELECT COUNT(*) AS count FROM servers").fetchone()["count"]
        if count == 0:
            conn.executemany(
                "INSERT INTO servers(name, location, total_gpus) VALUES (?, ?, ?)",
                [
                    ("4卡服务器", "s210机柜 A-02", 4),
                    ("2卡服务器", "s210机柜 B-01", 2),
                ],
            )

        conn.execute(
            "INSERT OR IGNORE INTO servers(name, location, total_gpus) VALUES (?, ?, ?)",
            ("2卡3090服务器", "s214", 2),
        )
        conn.commit()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_servers(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, name, location, total_gpus FROM servers ORDER BY total_gpus DESC, id ASC"
    ).fetchall()


def fetch_record(conn: sqlite3.Connection, record_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.total_gpus, servers.location
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        WHERE usage_records.id = ?
        """,
        (record_id,),
    ).fetchone()


def active_gpu_count(conn: sqlite3.Connection, server_id: int, exclude_record_id: int | None = None) -> int:
    query = """
        SELECT COALESCE(SUM(gpu_count), 0) AS busy
        FROM usage_records
        WHERE server_id = ?
          AND actual_end_time IS NULL
          AND approval_status = 'approved'
          AND start_time <= ?
    """
    params: tuple[int | str, ...]
    params = (server_id, now_display())
    if exclude_record_id is not None:
        query = """
            SELECT COALESCE(SUM(gpu_count), 0) AS busy
            FROM usage_records
            WHERE server_id = ?
              AND actual_end_time IS NULL
              AND approval_status = 'approved'
              AND start_time <= ?
              AND id != ?
        """
        params = (server_id, now_display(), exclude_record_id)
    row = conn.execute(query, params).fetchone()
    return int(row["busy"])


def ensure_capacity(conn: sqlite3.Connection, server_id: int, gpu_count: int, exclude_record_id: int | None = None) -> None:
    server = conn.execute(
        "SELECT id, total_gpus FROM servers WHERE id = ?",
        (server_id,),
    ).fetchone()
    if server is None:
        raise ValueError("服务器不存在")
    busy_gpu = active_gpu_count(conn, server_id, exclude_record_id)
    if busy_gpu + gpu_count > int(server["total_gpus"]):
        free_gpu = int(server["total_gpus"]) - busy_gpu
        raise ValueError(f"该服务器当前只剩 {free_gpu} 张 GPU")


def ensure_no_reservation_conflict(
    conn: sqlite3.Connection,
    server_id: int,
    start_time: str,
    expected_end_time: str | None,
    exclude_record_id: int | None = None,
) -> None:
    end_time = expected_end_time or start_time
    query = """
        SELECT id
        FROM usage_records
        WHERE server_id = ?
          AND actual_end_time IS NULL
          AND approval_status IN ('pending', 'approved')
          AND start_time < ?
          AND COALESCE(expected_end_time, start_time) > ?
    """
    params: tuple[int | str, ...] = (server_id, end_time, start_time)
    if exclude_record_id is not None:
        query += " AND id != ?"
        params = (server_id, end_time, start_time, exclude_record_id)

    conflict = conn.execute(query, params).fetchone()
    if conflict is not None:
        raise ValueError("该设备在所选时间段已被预约，请更换时间或服务器")


def validate_payload(conn: sqlite3.Connection, payload: dict, record_id: int | None = None) -> dict:
    required = ["server_id", "user_name", "project_name", "gpu_count", "start_time"]
    for field in required:
        if str(payload.get(field, "")).strip() == "":
            raise ValueError(f"{field} 不能为空")

    try:
        server_id = int(payload["server_id"])
        gpu_count = int(payload["gpu_count"])
    except (TypeError, ValueError) as exc:
        raise ValueError("server_id 和 gpu_count 必须是数字") from exc

    if gpu_count <= 0:
        raise ValueError("gpu_count 必须大于 0")

    server = conn.execute(
        "SELECT id, total_gpus FROM servers WHERE id = ?",
        (server_id,),
    ).fetchone()
    if server is None:
        raise ValueError("服务器不存在")

    start_time = parse_input_datetime(payload.get("start_time"), "start_time")
    expected_end_time = parse_input_datetime(payload.get("expected_end_time"), "expected_end_time")
    actual_end_time = parse_input_datetime(payload.get("actual_end_time"), "actual_end_time")

    if expected_end_time and expected_end_time < start_time:
        raise ValueError("预计结束时间不能早于开始时间")
    if actual_end_time and actual_end_time < start_time:
        raise ValueError("实际结束时间不能早于开始时间")

    if actual_end_time is None and expected_end_time is None:
        raise ValueError("预约必须填写预计结束时间")

    if actual_end_time is None:
        ensure_no_reservation_conflict(conn, server_id, start_time, expected_end_time, record_id)

    return {
        "server_id": server_id,
        "user_name": str(payload["user_name"]).strip(),
        "project_name": str(payload["project_name"]).strip(),
        "gpu_count": gpu_count,
        "start_time": start_time,
        "expected_end_time": expected_end_time,
        "actual_end_time": actual_end_time,
        "note": str(payload.get("note", "")).strip(),
    }


def row_status(row: sqlite3.Row) -> str:
    if row["approval_status"] == "pending":
        return "pending"
    if row["actual_end_time"]:
        return "completed"
    if row["expected_end_time"] and row["expected_end_time"] < now_display():
        return "overtime"
    return "active"


def serialize_record(row: sqlite3.Row) -> dict:
    return {
        "id": int(row["id"]),
        "server_id": int(row["server_id"]),
        "server_name": row["server_name"],
        "user_name": row["user_name"],
        "project_name": row["project_name"],
        "gpu_count": int(row["gpu_count"]),
        "start_time": row["start_time"],
        "expected_end_time": row["expected_end_time"],
        "actual_end_time": row["actual_end_time"],
        "note": row["note"],
        "status": row_status(row),
        "approval_status": row["approval_status"],
        "approved_at": row["approved_at"],
        "approved_by": row["approved_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def build_usage_export_text(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.location
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        ORDER BY usage_records.start_time DESC, usage_records.id DESC
        """
    ).fetchall()

    lines = [
        "服务器使用记录导出",
        f"导出时间: {now_display()}",
        f"记录总数: {len(rows)}",
        "",
    ]

    if not rows:
        lines.append("暂无记录")
        return "\n".join(lines) + "\n"

    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"[{index}] 记录ID: {int(row['id'])}",
                f"服务器: {row['server_name']} ({row['location']})",
                f"使用人: {row['user_name']}",
                f"项目: {row['project_name']}",
                f"占用卡数: {int(row['gpu_count'])}",
                f"开始时间: {row['start_time']}",
                f"预计结束: {row['expected_end_time'] or '未填写'}",
                f"实际结束: {row['actual_end_time'] or '进行中'}",
                f"状态: {row_status(row)}",
                f"备注: {row['note'] or '无'}",
                f"审核状态: {row['approval_status']}",
                f"审核人: {row['approved_by'] or '未审核'}",
                f"审核时间: {row['approved_at'] or '未审核'}",
                f"创建时间: {row['created_at']}",
                f"更新时间: {row['updated_at']}",
                "",
            ]
        )
    return "\n".join(lines)


def dashboard_payload(conn: sqlite3.Connection) -> dict:
    servers = fetch_servers(conn)
    active_rows = conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.location, servers.total_gpus
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        WHERE usage_records.actual_end_time IS NULL
          AND usage_records.approval_status = 'approved'
          AND usage_records.start_time <= ?
        ORDER BY usage_records.start_time ASC, usage_records.id ASC
        """,
        (now_display(),),
    ).fetchall()
    history_rows = conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.location, servers.total_gpus
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        WHERE usage_records.actual_end_time IS NOT NULL
          AND usage_records.approval_status = 'approved'
        ORDER BY usage_records.actual_end_time DESC, usage_records.id DESC
        LIMIT 100
        """
    ).fetchall()
    pending_rows = conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.location, servers.total_gpus
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        WHERE usage_records.actual_end_time IS NULL
          AND usage_records.approval_status = 'pending'
        ORDER BY usage_records.created_at ASC, usage_records.id ASC
        """
    ).fetchall()
    reservation_rows = conn.execute(
        """
        SELECT usage_records.*, servers.name AS server_name, servers.location, servers.total_gpus
        FROM usage_records
        JOIN servers ON servers.id = usage_records.server_id
        WHERE usage_records.actual_end_time IS NULL
          AND usage_records.approval_status = 'approved'
          AND usage_records.start_time > ?
        ORDER BY usage_records.start_time ASC, usage_records.id ASC
        """,
        (now_display(),),
    ).fetchall()

    rows_by_server: dict[int, list[sqlite3.Row]] = {}
    for row in active_rows:
        rows_by_server.setdefault(int(row["server_id"]), []).append(row)

    server_cards = []
    total_gpu = 0
    busy_gpu = 0
    for server in servers:
        server_id = int(server["id"])
        total = int(server["total_gpus"])
        total_gpu += total
        rows = rows_by_server.get(server_id, [])
        busy = sum(int(row["gpu_count"]) for row in rows)
        busy_gpu += busy
        free = total - busy

        slots = []
        index = 0
        for row in rows:
            for _ in range(int(row["gpu_count"])):
                slots.append(
                    {
                        "label": f"GPU {index}",
                        "status": "busy",
                        "user_name": row["user_name"],
                        "project_name": row["project_name"],
                        "record_id": int(row["id"]),
                    }
                )
                index += 1
        while index < total:
            slots.append(
                {
                    "label": f"GPU {index}",
                    "status": "free",
                    "user_name": "空闲",
                    "project_name": "可立即分配",
                    "record_id": None,
                }
            )
            index += 1

        server_cards.append(
            {
                "id": server_id,
                "name": server["name"],
                "location": server["location"],
                "total_gpus": total,
                "busy_gpus": busy,
                "free_gpus": free,
                "utilization": int((busy / total) * 100) if total else 0,
                "slots": slots,
            }
        )

    return {
        "updated_at": now_display(),
        "summary": {
            "server_count": len(server_cards),
            "total_gpus": total_gpu,
            "busy_gpus": busy_gpu,
            "free_gpus": total_gpu - busy_gpu,
            "active_count": len(active_rows),
            "utilization": int((busy_gpu / total_gpu) * 100) if total_gpu else 0,
        },
        "servers": server_cards,
        "active_records": [serialize_record(row) for row in active_rows],
        "history_records": [serialize_record(row) for row in history_rows],
        "pending_records": [serialize_record(row) for row in pending_rows],
        "reservation_records": [serialize_record(row) for row in reservation_rows],
    }


class AppHandler(BaseHTTPRequestHandler):
    server_version = "ServerUsage/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.redirect("/login", cookie=self.clear_session_cookie())
            return
        if parsed.path == "/login":
            self.serve_login(cookie=self.clear_session_cookie())
            return
        if parsed.path == "/app":
            if not self.ensure_authenticated():
                return
            self.serve_index()
            return
        if parsed.path == "/api/usages/export.txt":
            if not self.ensure_authenticated():
                return
            with closing(get_conn()) as conn:
                body = build_usage_export_text(conn).encode("utf-8")
            filename = f"usage-records-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/dashboard":
            if not self.ensure_authenticated():
                return
            with closing(get_conn()) as conn:
                self.write_json(HTTPStatus.OK, dashboard_payload(conn))
            return
        if parsed.path == "/api/servers":
            if not self.ensure_authenticated():
                return
            with closing(get_conn()) as conn:
                data = [
                    {
                        "id": int(row["id"]),
                        "name": row["name"],
                        "location": row["location"],
                        "total_gpus": int(row["total_gpus"]),
                    }
                    for row in fetch_servers(conn)
                ]
                self.write_json(HTTPStatus.OK, {"servers": data})
            return
        if parsed.path == "/api/usages":
            if not self.ensure_authenticated():
                return
            status_filter = parse_qs(parsed.query).get("status", ["all"])[0]
            with closing(get_conn()) as conn:
                rows = conn.execute(
                    """
                    SELECT usage_records.*, servers.name AS server_name
                    FROM usage_records
                    JOIN servers ON servers.id = usage_records.server_id
                    ORDER BY usage_records.start_time DESC, usage_records.id DESC
                    """
                ).fetchall()
                records = []
                for row in rows:
                    status = row_status(row)
                    if status_filter == "active" and status not in {"active", "overtime"}:
                        continue
                    if status_filter == "history" and status != "completed":
                        continue
                    record = serialize_record(row)
                    record["status"] = status
                    records.append(record)
                self.write_json(HTTPStatus.OK, {"records": records})
            return
        if parsed.path.startswith("/api/usages/"):
            if not self.ensure_authenticated():
                return
            record_id = self.extract_id(parsed.path)
            if record_id is None:
                self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                return
            with closing(get_conn()) as conn:
                row = fetch_record(conn, record_id)
                if row is None:
                    self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                    return
                self.write_json(HTTPStatus.OK, {"record": serialize_record(row)})
            return
        self.write_error_json(HTTPStatus.NOT_FOUND, "路径不存在")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/login":
            payload = self.read_json_body()
            if payload is None:
                return
            username = str(payload.get("username", "")).strip()
            if not hmac.compare_digest(username, AUTH_USER):
                self.write_error_json(HTTPStatus.FORBIDDEN, "账号不存在或无权访问")
                return
            self.write_json(
                HTTPStatus.OK,
                {"message": "登录成功", "redirect_to": "/app"},
                cookie=self.build_session_cookie(username),
            )
            return
        if parsed.path == "/api/logout":
            self.write_json(
                HTTPStatus.OK,
                {"message": "已退出登录", "redirect_to": "/login"},
                cookie=self.clear_session_cookie(),
            )
            return
        if parsed.path == "/api/usages":
            if not self.ensure_authenticated():
                return
            payload = self.read_json_body()
            if payload is None:
                return
            with closing(get_conn()) as conn:
                try:
                    valid = validate_payload(conn, payload)
                except ValueError as exc:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                    return
                now = now_display()
                cursor = conn.execute(
                    """
                    INSERT INTO usage_records(
                        server_id, user_name, project_name, gpu_count, start_time,
                        expected_end_time, actual_end_time, note,
                        approval_status, approved_at, approved_by,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        valid["server_id"],
                        valid["user_name"],
                        valid["project_name"],
                        valid["gpu_count"],
                        valid["start_time"],
                        valid["expected_end_time"],
                        valid["actual_end_time"],
                        valid["note"],
                        "pending",
                        None,
                        None,
                        now,
                        now,
                    ),
                )
                conn.commit()
                self.write_json(HTTPStatus.CREATED, {"id": int(cursor.lastrowid), "message": "申请已提交，等待管理员审核"})
            return

        if parsed.path.startswith("/api/usages/") and parsed.path.endswith("/approve"):
            if not self.ensure_authenticated():
                return
            record_id = self.extract_id(parsed.path.removesuffix("/approve"))
            if record_id is None:
                self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                return
            payload = self.read_json_body()
            if payload is None:
                return
            admin_user = str(payload.get("admin_user", "")).strip()
            admin_password = str(payload.get("admin_password", "")).strip()
            if not (
                hmac.compare_digest(admin_user, REVIEW_USER)
                and hmac.compare_digest(admin_password, REVIEW_PASSWORD)
            ):
                self.write_error_json(HTTPStatus.FORBIDDEN, "管理员账号或密码错误")
                return
            with closing(get_conn()) as conn:
                row = fetch_record(conn, record_id)
                if row is None:
                    self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                    return
                if row["approval_status"] == "approved":
                    self.write_error_json(HTTPStatus.BAD_REQUEST, "该记录已审核")
                    return
                if row["actual_end_time"]:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, "已结束记录不能审核")
                    return
                if row["start_time"] <= now_display():
                    try:
                        ensure_capacity(conn, int(row["server_id"]), int(row["gpu_count"]), record_id)
                    except ValueError as exc:
                        self.write_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                        return
                now = now_display()
                conn.execute(
                    """
                    UPDATE usage_records
                    SET approval_status = 'approved', approved_at = ?, approved_by = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, admin_user, now, record_id),
                )
                conn.commit()
                self.write_json(HTTPStatus.OK, {"message": "审核通过，已纳入管理"})
            return

        if parsed.path.startswith("/api/usages/") and parsed.path.endswith("/complete"):
            if not self.ensure_authenticated():
                return
            record_id = self.extract_id(parsed.path.removesuffix("/complete"))
            if record_id is None:
                self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                return
            payload = self.read_json_body(optional=True) or {}
            with closing(get_conn()) as conn:
                row = fetch_record(conn, record_id)
                if row is None:
                    self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                    return
                if row["actual_end_time"]:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, "这条记录已经结束")
                    return
                if row["approval_status"] != "approved":
                    self.write_error_json(HTTPStatus.BAD_REQUEST, "待审核记录不能结束，请先审核")
                    return
                try:
                    actual_end_time = parse_input_datetime(payload.get("actual_end_time"), "actual_end_time") or now_display()
                except ValueError as exc:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                    return
                if actual_end_time < row["start_time"]:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, "结束时间不能早于开始时间")
                    return
                conn.execute(
                    "UPDATE usage_records SET actual_end_time = ?, updated_at = ? WHERE id = ?",
                    (actual_end_time, now_display(), record_id),
                )
                conn.commit()
                self.write_json(HTTPStatus.OK, {"message": "记录已结束"})
            return

        self.write_error_json(HTTPStatus.NOT_FOUND, "路径不存在")

    def do_PUT(self) -> None:
        if not self.ensure_authenticated():
            return
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/usages/"):
            self.write_error_json(HTTPStatus.NOT_FOUND, "路径不存在")
            return
        record_id = self.extract_id(parsed.path)
        if record_id is None:
            self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
            return
        payload = self.read_json_body()
        if payload is None:
            return
        with closing(get_conn()) as conn:
            existing = fetch_record(conn, record_id)
            if existing is None:
                self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                return
            try:
                valid = validate_payload(conn, payload, record_id)
            except ValueError as exc:
                self.write_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                return
            should_check_capacity = (
                existing["approval_status"] == "approved"
                and valid["actual_end_time"] is None
                and valid["start_time"] <= now_display()
            )
            if should_check_capacity:
                try:
                    ensure_capacity(conn, valid["server_id"], valid["gpu_count"], record_id)
                except ValueError as exc:
                    self.write_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                    return
            conn.execute(
                """
                UPDATE usage_records
                SET server_id = ?, user_name = ?, project_name = ?, gpu_count = ?, start_time = ?,
                    expected_end_time = ?, actual_end_time = ?, note = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    valid["server_id"],
                    valid["user_name"],
                    valid["project_name"],
                    valid["gpu_count"],
                    valid["start_time"],
                    valid["expected_end_time"],
                    valid["actual_end_time"],
                    valid["note"],
                    now_display(),
                    record_id,
                ),
            )
            conn.commit()
            self.write_json(HTTPStatus.OK, {"message": "记录已更新"})

    def do_DELETE(self) -> None:
        if not self.ensure_authenticated():
            return
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/usages/"):
            self.write_error_json(HTTPStatus.NOT_FOUND, "路径不存在")
            return
        record_id = self.extract_id(parsed.path)
        if record_id is None:
            self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
            return
        with closing(get_conn()) as conn:
            existing = fetch_record(conn, record_id)
            if existing is None:
                self.write_error_json(HTTPStatus.NOT_FOUND, "记录不存在")
                return
            conn.execute("DELETE FROM usage_records WHERE id = ?", (record_id,))
            conn.commit()
            self.write_json(HTTPStatus.OK, {"message": "记录已删除"})

    def serve_index(self) -> None:
        if not INDEX_PATH.exists():
            self.write_error_json(HTTPStatus.NOT_FOUND, "首页不存在")
            return
        content = INDEX_PATH.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def serve_login(self, cookie: str | None = None) -> None:
        if not LOGIN_PATH.exists():
            self.write_error_json(HTTPStatus.NOT_FOUND, "登录页不存在")
            return
        content = LOGIN_PATH.read_bytes()
        self.send_response(HTTPStatus.OK)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def read_json_body(self, optional: bool = False) -> dict | None:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            if optional:
                return {}
            self.write_error_json(HTTPStatus.BAD_REQUEST, "请求体不能为空")
            return None
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self.write_error_json(HTTPStatus.BAD_REQUEST, "请求体必须是 JSON")
            return None

    def write_json(self, status: HTTPStatus, payload: dict, cookie: str | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_error_json(self, status: HTTPStatus, message: str) -> None:
        self.write_json(status, {"error": message})

    def ensure_authenticated(self) -> bool:
        if not self.is_authenticated():
            self.write_auth_required()
            return False
        return True

    def is_authenticated(self) -> bool:
        session_value = self.get_cookie_value(SESSION_COOKIE)
        if not session_value:
            return False
        username, sep, signature = session_value.partition(".")
        if sep != "." or not username or not signature:
            return False
        expected = hmac.new(
            SESSION_SECRET.encode("utf-8"),
            username.encode("utf-8"),
            "sha256",
        ).hexdigest()
        return hmac.compare_digest(username, AUTH_USER) and hmac.compare_digest(signature, expected)

    def write_auth_required(self) -> None:
        body = json.dumps({"error": "未授权，请先登录"}, ensure_ascii=False).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def build_session_cookie(self, username: str) -> str:
        signature = hmac.new(
            SESSION_SECRET.encode("utf-8"),
            username.encode("utf-8"),
            "sha256",
        ).hexdigest()
        return (
            f"{SESSION_COOKIE}={username}.{signature}; Path=/; HttpOnly; SameSite=Lax; Max-Age=43200"
        )

    def clear_session_cookie(self) -> str:
        return f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"

    def get_cookie_value(self, key: str) -> str | None:
        raw_cookie = self.headers.get("Cookie", "")
        if not raw_cookie:
            return None
        for part in raw_cookie.split(";"):
            name, _, value = part.strip().partition("=")
            if name == key:
                return value or None
        return None

    def redirect(self, location: str, cookie: str | None = None) -> None:
        self.send_response(HTTPStatus.FOUND)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Location", location)
        self.end_headers()

    def extract_id(self, path: str) -> int | None:
        tail = path.rstrip("/").split("/")[-1]
        try:
            return int(tail)
        except ValueError:
            return None

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    init_db()
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8282"))
    server = ThreadingHTTPServer((host, port), AppHandler)
    # Show a practical LAN URL when listening on all interfaces.
    display_host = host
    if host == "0.0.0.0":
        try:
            display_host = socket.gethostbyname(socket.gethostname())
        except OSError:
            display_host = "<your-lan-ip>"
    print(f"Server usage app running at http://{display_host}:{port}")
    print(f"Login user: {AUTH_USER}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
