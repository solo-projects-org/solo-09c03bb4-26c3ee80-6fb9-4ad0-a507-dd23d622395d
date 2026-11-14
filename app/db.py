"""SQLite 持久化层。

设计要点
========
* 全部写入经同一把可重入锁串行化，配合 ``check-then-write`` 逻辑保证
  重复实例不重复计数、批状态迁移只发生一次。
* ``instances`` 表对 ``(batch_id, sop_instance_uid)`` 建唯一索引：
  清单登记和回传登记都靠它天然去重。
* 文件与登记在同一事务边界内处理：调用方先把文件写好并 fsync，再调用
  ``register_instance``；登记失败时调用方负责删除已写文件，保证
  “文件写完且登记成功才回复 C-STORE 成功”。
* 剂量导出按 ``(study_instance_uid, version)`` 版本化：
  ``publish_dose_version`` 把版本行、事件行与“当前版本”指针放在
  **同一事务** 提交，读者永远只看到完整版本；并发修订由
  :class:`app.dose.DoseService` 的修订锁串行化，不会产生混合版本。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# ---- 批状态（终态不可变，迟到回传无法恢复失败批）----
STATE_PENDING = "PENDING"        # 已创建，尚未开始 C-FIND
STATE_FINDING = "FINDING"        # C-FIND 固定清单中
STATE_MOVING = "MOVING"          # C-MOVE 进行中，等待 C-STORE 回传
STATE_COMPLETED = "COMPLETED"    # 清单收齐且 C-MOVE 成功，可下载
STATE_FAILED = "FAILED"          # 终态失败（FIND/MOVE/保存/重启中断/缺件）

TERMINAL_STATES = {STATE_COMPLETED, STATE_FAILED}

# 进程级单例（main 启动时注入），供 storage 等模块反向访问
_DB_SINGLETON: "Database | None" = None


def set_db(db: "Database") -> None:
    global _DB_SINGLETON
    _DB_SINGLETON = db


def get_db() -> "Database":
    if _DB_SINGLETON is None:
        raise RuntimeError("数据库尚未初始化")
    return _DB_SINGLETON


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, timeout=30.0, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._create_schema()

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id                  TEXT PRIMARY KEY,
                    study_instance_uid  TEXT NOT NULL,
                    state               TEXT NOT NULL,
                    expected_count      INTEGER NOT NULL DEFAULT 0,
                    received_count      INTEGER NOT NULL DEFAULT 0,
                    failure_reason      TEXT,
                    zip_path            TEXT,
                    fault_inject        TEXT NOT NULL DEFAULT '',
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_batches_study
                    ON batches(study_instance_uid);

                CREATE TABLE IF NOT EXISTS instances (
                    batch_id            TEXT NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    sop_instance_uid    TEXT NOT NULL,
                    sop_class_uid       TEXT NOT NULL,
                    transfer_syntax     TEXT NOT NULL,
                    series_instance_uid TEXT NOT NULL DEFAULT '',
                    file_path           TEXT NOT NULL,
                    received_at         TEXT NOT NULL,
                    PRIMARY KEY (batch_id, sop_instance_uid)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id    TEXT,
                    kind        TEXT NOT NULL,
                    message     TEXT NOT NULL,
                    created_at  TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_batch
                    ON events(batch_id, id);

                -- 剂量导出：每个检查一个版本序列，版本不可变、只增不改。
                -- “当前版本”指针与版本行/事件行在同一事务提交，
                -- 读者按指针取到的永远是完整版本（汇总+原件同版）。
                CREATE TABLE IF NOT EXISTS dose_versions (
                    study_instance_uid  TEXT NOT NULL,
                    version             INTEGER NOT NULL,
                    batch_id            TEXT NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    export_id           TEXT NOT NULL DEFAULT '',
                    event_count         INTEGER NOT NULL,
                    series_count        INTEGER NOT NULL,
                    total_dlp_mgy_cm    REAL NOT NULL,
                    raw_path            TEXT NOT NULL,
                    created_at          TEXT NOT NULL,
                    PRIMARY KEY (study_instance_uid, version)
                );

                CREATE TABLE IF NOT EXISTS dose_events (
                    study_instance_uid      TEXT NOT NULL,
                    version                 INTEGER NOT NULL,
                    irradiation_event_uid   TEXT NOT NULL,
                    series_instance_uid     TEXT NOT NULL,
                    ctdi_vol_mgy            REAL NOT NULL,
                    dlp_mgy_cm              REAL NOT NULL,
                    seq                     INTEGER NOT NULL,
                    PRIMARY KEY (study_instance_uid, version, irradiation_event_uid),
                    FOREIGN KEY (study_instance_uid, version)
                        REFERENCES dose_versions(study_instance_uid, version)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS dose_current (
                    study_instance_uid  TEXT PRIMARY KEY,
                    version             INTEGER NOT NULL,
                    updated_at          TEXT NOT NULL
                );
                """
            )
            self._migrate()

    def _migrate(self) -> None:
        """老库就地升级：instances 补 series_instance_uid 列。"""
        cols = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(instances)")
        }
        if "series_instance_uid" not in cols:
            self._conn.execute(
                "ALTER TABLE instances ADD COLUMN "
                "series_instance_uid TEXT NOT NULL DEFAULT ''"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ events
    def add_event(self, kind: str, message: str, batch_id: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events(batch_id, kind, message, created_at) "
                "VALUES (?, ?, ?, ?)",
                (batch_id, kind, message, _now()),
            )

    def list_events(self, batch_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT kind, message, created_at FROM events "
                "WHERE batch_id = ? ORDER BY id",
                (batch_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ batches
    def create_batch(
        self, batch_id: str, study_instance_uid: str, fault_inject: str
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO batches(id, study_instance_uid, state, fault_inject, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, study_instance_uid, STATE_PENDING, fault_inject,
                 _now(), _now()),
            )
        self.add_event("BATCH_CREATED", f"批创建，Study={study_instance_uid}", batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY rowid DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def has_active_batch_for_study(self, study_instance_uid: str) -> bool:
        """同一 Study 已有未终态批时拒绝重复提交。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM batches WHERE study_instance_uid = ? "
                "AND state NOT IN (?, ?) LIMIT 1",
                (study_instance_uid, STATE_COMPLETED, STATE_FAILED),
            ).fetchone()
        return row is not None

    def transition_state(
        self,
        batch_id: str,
        expected: Iterable[str],
        new_state: str,
        *,
        failure_reason: str | None = None,
        expected_count: int | None = None,
        zip_path: str | None = None,
    ) -> bool:
        """仅当当前状态属于 ``expected`` 时迁移，返回是否发生了迁移。

        终态（COMPLETED / FAILED）不可再被改写——这是“迟到回传不能恢复
        失败批”的数据库级保证。
        """
        expected = set(expected)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["state"] not in expected:
                return False
            # 数据库级终态保护：COMPLETED / FAILED 一旦落定不可迁出，
            # 任何迟到事件都无法“复活”一个失败批。
            if row["state"] in TERMINAL_STATES and new_state != row["state"]:
                return False
            self._conn.execute(
                "UPDATE batches SET state = ?, failure_reason = COALESCE(?, failure_reason), "
                "zip_path = COALESCE(?, zip_path), updated_at = ? WHERE id = ?",
                (new_state, failure_reason, zip_path, _now(), batch_id),
            )
            if expected_count is not None:
                self._conn.execute(
                    "UPDATE batches SET expected_count = ? WHERE id = ?",
                    (expected_count, batch_id),
                )
        return True

    def fail_batch(self, batch_id: str, reason: str) -> bool:
        ok = self.transition_state(
            batch_id,
            [STATE_PENDING, STATE_FINDING, STATE_MOVING],
            STATE_FAILED,
            failure_reason=reason,
        )
        if ok:
            self.add_event("BATCH_FAILED", reason, batch_id)
        return ok

    def complete_batch(self, batch_id: str, zip_path: str) -> bool:
        ok = self.transition_state(
            batch_id, [STATE_MOVING], STATE_COMPLETED, zip_path=zip_path
        )
        if ok:
            self.add_event("BATCH_COMPLETED", "清单收齐，ZIP 已生成", batch_id)
        return ok

    # ------------------------------------------------------------------ manifest
    def set_manifest(
        self, batch_id: str, items: list[tuple[str, str, str, str]]
    ) -> None:
        """登记 C-FIND 固定下来的 SOP 清单。

        ``items`` 元素为 ``(sop_instance_uid, sop_class_uid, transfer_syntax,
        series_instance_uid)``。同一批只允许固定一次。
        """
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? LIMIT 1", (batch_id,)
            ).fetchone()
            if exists:
                raise RuntimeError("manifest already fixed")
            self._conn.executemany(
                "INSERT INTO instances(batch_id, sop_instance_uid, sop_class_uid, "
                "transfer_syntax, series_instance_uid, file_path, received_at) "
                "VALUES (?, ?, ?, ?, ?, '', '')",
                [(batch_id, sop, cls, ts, series) for sop, cls, ts, series in items],
            )
            self._conn.execute(
                "UPDATE batches SET expected_count = ?, updated_at = ? WHERE id = ?",
                (len(items), _now(), batch_id),
            )

    def manifest_uids(self, batch_id: str) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid FROM instances WHERE batch_id = ?",
                (batch_id,),
            ).fetchall()
        return {r["sop_instance_uid"] for r in rows}

    def is_manifest_uid(self, batch_id: str, sop_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? AND sop_instance_uid = ?",
                (batch_id, sop_uid),
            ).fetchone()
        return row is not None

    def is_already_received(self, batch_id: str, sop_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? AND sop_instance_uid = ? "
                "AND file_path != ''",
                (batch_id, sop_uid),
            ).fetchone()
        return row is not None

    def register_instance(
        self, batch_id: str, sop_uid: str, file_path: str, transfer_syntax: str
    ) -> bool:
        """登记一个已落盘实例。返回 False 表示重复投送（不重复计数）。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT file_path FROM instances WHERE batch_id = ? AND sop_instance_uid = ?",
                (batch_id, sop_uid),
            ).fetchone()
            if row is None:
                return False  # 不在清单内，调用方应拒绝
            if row["file_path"] != "":
                return False  # 重复实例
            self._conn.execute(
                "UPDATE instances SET file_path = ?, transfer_syntax = ?, "
                "received_at = ? WHERE batch_id = ? AND sop_instance_uid = ?",
                (file_path, transfer_syntax, _now(), batch_id, sop_uid),
            )
            self._conn.execute(
                "UPDATE batches SET received_count = received_count + 1, updated_at = ? "
                "WHERE id = ?",
                (_now(), batch_id),
            )
        return True

    def received_progress(self, batch_id: str) -> tuple[int, int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT expected_count, received_count FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
        if row is None:
            return (0, 0)
        return row["expected_count"], row["received_count"]

    def missing_uids(self, batch_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid FROM instances WHERE batch_id = ? AND file_path = ''",
                (batch_id,),
            ).fetchall()
        return [r["sop_instance_uid"] for r in rows]

    def received_files(self, batch_id: str) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid, file_path FROM instances "
                "WHERE batch_id = ? AND file_path != '' ORDER BY sop_instance_uid",
                (batch_id,),
            ).fetchall()
        return [(r["sop_instance_uid"], r["file_path"]) for r in rows]

    # ------------------------------------------------------------------ series
    def series_uids_for_batch(self, batch_id: str) -> set[str]:
        """批清单中登记过的全部 SeriesInstanceUID（空串除外）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT series_instance_uid FROM instances "
                "WHERE batch_id = ? AND series_instance_uid != ''",
                (batch_id,),
            ).fetchall()
        return {r["series_instance_uid"] for r in rows}

    def received_rows_missing_series(self, batch_id: str) -> list[tuple[str, str]]:
        """已收但缺序列标识的实例 ``(sop_instance_uid, file_path)``。

        老库升级或源端 C-FIND 未返回 SeriesInstanceUID 时出现；
        由 storage 层读 DICOM 文件头回填。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid, file_path FROM instances "
                "WHERE batch_id = ? AND file_path != '' AND series_instance_uid = ''",
                (batch_id,),
            ).fetchall()
        return [(r["sop_instance_uid"], r["file_path"]) for r in rows]

    def update_instance_series(
        self, batch_id: str, sop_uid: str, series_uid: str
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE instances SET series_instance_uid = ? "
                "WHERE batch_id = ? AND sop_instance_uid = ?",
                (series_uid, batch_id, sop_uid),
            )

    def find_batch_for_study(self, study_uid: str) -> dict[str, Any] | None:
        """按 StudyInstanceUID 路由 C-STORE 回传到对应批。

        用 rowid 而不是时间戳排序：同秒内创建多个批时顺序仍确定，
        永远命中最新的批。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE study_instance_uid = ? "
                "ORDER BY rowid DESC LIMIT 1",
                (study_uid,),
            ).fetchone()
        return dict(row) if row else None

    def latest_completed_batch_for_study(
        self, study_uid: str
    ) -> dict[str, Any] | None:
        """该检查最近一次成功取数的批（剂量归集只认已完成批）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE study_instance_uid = ? AND state = ? "
                "ORDER BY rowid DESC LIMIT 1",
                (study_uid, STATE_COMPLETED),
            ).fetchone()
        return dict(row) if row else None

    def has_any_batch_for_study(self, study_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM batches WHERE study_instance_uid = ? LIMIT 1",
                (study_uid,),
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------ dose
    def next_dose_version(self, study_uid: str) -> int:
        """下一个剂量版本号（调用方须持有修订锁，保证并发修订不撞号）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS v FROM dose_versions "
                "WHERE study_instance_uid = ?",
                (study_uid,),
            ).fetchone()
        return int(row["v"]) + 1

    def publish_dose_version(
        self,
        study_uid: str,
        version: int,
        batch_id: str,
        export_id: str,
        events: list[dict[str, Any]],
        total_dlp: float,
        series_count: int,
        raw_path: str,
    ) -> None:
        """把一版剂量导出发布为当前版本。

        版本行 + 事件行 + 当前版本指针在**同一事务**提交：
        任何时刻读者看到的都是完整版本，绝不出现“汇总是新版、
        原件是旧版”的混合状态。事务失败整体回滚，当前版本不变。
        """
        now = _now()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO dose_versions(study_instance_uid, version, batch_id, "
                "export_id, event_count, series_count, total_dlp_mgy_cm, "
                "raw_path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (study_uid, version, batch_id, export_id, len(events),
                 series_count, total_dlp, raw_path, now),
            )
            self._conn.executemany(
                "INSERT INTO dose_events(study_instance_uid, version, "
                "irradiation_event_uid, series_instance_uid, ctdi_vol_mgy, "
                "dlp_mgy_cm, seq) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (study_uid, version, e["irradiation_event_uid"],
                     e["series_instance_uid"], e["ctdi_vol_mgy"],
                     e["dlp_mgy_cm"], seq)
                    for seq, e in enumerate(events)
                ],
            )
            self._conn.execute(
                "INSERT INTO dose_current(study_instance_uid, version, updated_at) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(study_instance_uid) DO UPDATE SET "
                "version = excluded.version, updated_at = excluded.updated_at",
                (study_uid, version, now),
            )
        self.add_event(
            "DOSE_PUBLISHED",
            f"剂量导出第 {version} 版已发布：{len(events)} 个照射事件，"
            f"DLP 合计 {total_dlp:.1f} mGy·cm（汇总与原件同版原子生效）",
            batch_id,
        )

    def current_dose_version(self, study_uid: str) -> dict[str, Any] | None:
        """当前版本指针指向的版本行。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT v.* FROM dose_current c "
                "JOIN dose_versions v ON v.study_instance_uid = c.study_instance_uid "
                "AND v.version = c.version "
                "WHERE c.study_instance_uid = ?",
                (study_uid,),
            ).fetchone()
        return dict(row) if row else None

    def dose_version(
        self, study_uid: str, version: int
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM dose_versions "
                "WHERE study_instance_uid = ? AND version = ?",
                (study_uid, version),
            ).fetchone()
        return dict(row) if row else None

    def dose_events(
        self, study_uid: str, version: int
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT irradiation_event_uid, series_instance_uid, "
                "ctdi_vol_mgy, dlp_mgy_cm, seq FROM dose_events "
                "WHERE study_instance_uid = ? AND version = ? ORDER BY seq",
                (study_uid, version),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_dose_versions(self, study_uid: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM dose_versions WHERE study_instance_uid = ? "
                "ORDER BY version DESC",
                (study_uid,),
            ).fetchall()
        return [dict(r) for r in rows]

    def all_current_doses(self) -> list[dict[str, Any]]:
        """全部已归集检查的当前剂量版本（列表页用）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT c.study_instance_uid, c.version, c.updated_at, "
                "v.batch_id, v.event_count, v.series_count, v.total_dlp_mgy_cm "
                "FROM dose_current c "
                "JOIN dose_versions v ON v.study_instance_uid = c.study_instance_uid "
                "AND v.version = c.version "
                "ORDER BY c.updated_at DESC",
            ).fetchall()
        return [dict(r) for r in rows]

    def all_dose_versions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM dose_versions ORDER BY study_instance_uid, version"
            ).fetchall()
        return [dict(r) for r in rows]

    def drop_dose_version(self, study_uid: str, version: int) -> None:
        """删除残缺版本（如原件文件损坏），并把当前指针回退到最新完整版。

        仅用于启动恢复；正常流程中版本不可变、不删除。
        """
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM dose_versions WHERE study_instance_uid = ? AND version = ?",
                (study_uid, version),
            )
            row = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS v FROM dose_versions "
                "WHERE study_instance_uid = ?",
                (study_uid,),
            ).fetchone()
            remaining = int(row["v"])
            if remaining:
                self._conn.execute(
                    "INSERT INTO dose_current(study_instance_uid, version, updated_at) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(study_instance_uid) DO UPDATE SET "
                    "version = excluded.version, updated_at = excluded.updated_at",
                    (study_uid, remaining, _now()),
                )
            else:
                self._conn.execute(
                    "DELETE FROM dose_current WHERE study_instance_uid = ?",
                    (study_uid,),
                )

    # ------------------------------------------------------------------ recovery
    def non_terminal_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches WHERE state NOT IN (?, ?)",
                (STATE_COMPLETED, STATE_FAILED),
            ).fetchall()
        return [dict(r) for r in rows]

    def all_completed_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches WHERE state = ?", (STATE_COMPLETED,)
            ).fetchall()
        return [dict(r) for r in rows]
