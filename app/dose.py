"""CT 剂量导出归集：版本化接收、按序列汇总、原子发布。

业务规则
========
* **只受理已完成批**：检查必须已有 COMPLETED 的取数批；导出中的
  StudyInstanceUID 定位批，每个照射事件的 SeriesInstanceUID 必须出现在
  该批已登记的 DICOM 清单中。
* **同一照射事件重复出现**：归属（检查+序列）与剂量（CTDIvol、DLP）
  完全一致的重复条目只计一次；任一字段不一致即视为冲突，
  **整份导出拒绝**，不产生任何版本。
* **修订即换版**：每次受理生成新的单调递增版本，整份替换旧版；
  汇总（DB 行）与原件（JSON 文件）作为同一版原子发布——
  先写原件文件并 fsync，再在单个事务里提交版本行+事件行+当前版本指针。
* **失败与重启保旧版**：解析、写盘或登记任一步失败，以及发布中途重启，
  当前版本指针保持不变，读者始终看到上一次的完整版本；
  两个并发修订由每检查一把修订锁串行化，不会产生混合版本。
"""
from __future__ import annotations

import logging
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import storage
from .db import Database

log = logging.getLogger("dose")


class DoseError(Exception):
    """剂量归集失败基类。``status_code`` 供路由映射 HTTP 状态。"""

    status_code = 500

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NoBatchForStudyError(DoseError):
    """该检查从未取数（无任何批）。"""

    status_code = 404


class BatchNotCompletedError(DoseError):
    """该检查有批但无已完成批，剂量导出只受理已完成批。"""

    status_code = 409


class DoseValidationError(DoseError):
    """导出内容校验失败（空事件、冲突重复、序列不一致等），整份拒绝。"""

    status_code = 422


class DoseStoreError(DoseError):
    """原件写盘或版本登记失败；当前版本保持不变。"""

    status_code = 500


def _dec(value: float) -> Decimal:
    """剂量数值的规范化比较形式（10.0 与 10.00 视为同一剂量）。"""
    return Decimal(str(value))


class DoseService:
    """剂量导出的校验、去重、版本化发布与查询。"""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _revision_lock(self, study_uid: str) -> threading.Lock:
        """每检查一把修订锁：并发修订串行化，版本号与发布不会交错。"""
        with self._locks_guard:
            return self._locks.setdefault(study_uid, threading.Lock())

    # ------------------------------------------------------------ 校验与去重
    def _completed_batch(self, study_uid: str) -> dict[str, Any]:
        batch = self.db.latest_completed_batch_for_study(study_uid)
        if batch is not None:
            return batch
        if self.db.has_any_batch_for_study(study_uid):
            raise BatchNotCompletedError(
                "该检查的取数批尚未完成（或已失败），剂量导出只受理已完成批"
            )
        raise NoBatchForStudyError("该检查尚未取数，无对应批")

    @staticmethod
    def _dedup(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按照射事件 UID 去重。

        同一 UID 多次出现时，仅当序列归属与 CTDIvol/DLP 完全一致才计一次；
        否则整份导出拒绝（调用方不落任何数据）。
        """
        seen: dict[str, dict[str, Any]] = {}
        for e in events:
            uid = e["irradiation_event_uid"]
            if uid in seen:
                first = seen[uid]
                same = (
                    first["series_instance_uid"] == e["series_instance_uid"]
                    and _dec(first["ctdi_vol_mgy"]) == _dec(e["ctdi_vol_mgy"])
                    and _dec(first["dlp_mgy_cm"]) == _dec(e["dlp_mgy_cm"])
                )
                if not same:
                    raise DoseValidationError(
                        f"照射事件 {uid} 重复出现但归属或剂量不一致"
                        f"（{first['series_instance_uid']}/"
                        f"{first['ctdi_vol_mgy']}/{first['dlp_mgy_cm']} vs "
                        f"{e['series_instance_uid']}/{e['ctdi_vol_mgy']}/"
                        f"{e['dlp_mgy_cm']}），整份导出拒绝"
                    )
                continue  # 完全一致的重复：只计一次
            seen[uid] = e
        return list(seen.values())

    def _check_series(
        self, batch: dict[str, Any], events: list[dict[str, Any]]
    ) -> None:
        """每个事件的序列必须与已登记 DICOM 一致。"""
        batch_id = batch["id"]
        # 老库或源端未返回序列号时，先从已落盘文件头回填
        if self.db.received_rows_missing_series(batch_id):
            storage.backfill_series_for_batch(self.db, batch_id)
        registered = self.db.series_uids_for_batch(batch_id)
        unknown = sorted(
            {e["series_instance_uid"] for e in events} - registered
        )
        if unknown:
            raise DoseValidationError(
                f"序列与已登记 DICOM 不一致：{unknown} "
                f"不在批 {batch_id} 的清单中，整份导出拒绝"
            )

    # ------------------------------------------------------------ 发布
    def submit(
        self,
        study_uid: str,
        export_id: str,
        events: list[dict[str, Any]],
        raw_body: bytes,
    ) -> dict[str, Any]:
        """受理一份剂量导出，发布为新版本。失败时当前版本不变。"""
        db = self.db
        study_uid = study_uid.strip()
        if not study_uid:
            raise DoseValidationError("study_instance_uid 不能为空")
        if not events:
            raise DoseValidationError("events 不能为空")

        batch = self._completed_batch(study_uid)
        batch_id = batch["id"]
        try:
            deduped = self._dedup(events)
            self._check_series(batch, deduped)
        except DoseValidationError as exc:
            db.add_event("DOSE_REJECTED", f"剂量导出被拒：{exc.message}", batch_id)
            raise

        total_dlp = float(sum(e["dlp_mgy_cm"] for e in deduped))
        series_count = len({e["series_instance_uid"] for e in deduped})

        # 修订锁：同一检查的并发修订在此串行，版本号分配与发布不会交错
        with self._revision_lock(study_uid):
            version = db.next_dose_version(study_uid)
            try:
                raw_path = storage.write_dose_raw(study_uid, version, raw_body)
            except storage.StorageError as exc:
                db.add_event("DOSE_REJECTED",
                             f"剂量原件写盘失败：{exc}", batch_id)
                raise DoseStoreError(str(exc)) from exc
            try:
                db.publish_dose_version(
                    study_uid, version, batch_id, export_id, deduped,
                    total_dlp, series_count, str(raw_path),
                )
            except Exception as exc:
                # 登记失败：删除已写原件，当前版本保持旧版
                storage.remove_dose_raw(raw_path)
                db.add_event("DOSE_REJECTED",
                             f"剂量版本登记失败：{exc}", batch_id)
                log.exception("剂量版本登记失败 study=%s v=%d", study_uid, version)
                raise DoseStoreError(f"剂量版本登记失败：{exc}") from exc

        log.info(
            "剂量导出已发布 study=%s v=%d 事件=%d DLP合计=%.1f",
            study_uid, version, len(deduped), total_dlp,
        )
        return self.version_view(study_uid, version)

    # ------------------------------------------------------------ 查询
    def version_view(self, study_uid: str, version: int) -> dict[str, Any]:
        row = self.db.dose_version(study_uid, version)
        if row is None:
            raise NoBatchForStudyError(
                f"检查 {study_uid} 不存在剂量版本 v{version}"
            )
        return self._build_view(row)

    def current_view(self, study_uid: str) -> dict[str, Any]:
        row = self.db.current_dose_version(study_uid)
        if row is None:
            raise NoBatchForStudyError(f"检查 {study_uid} 尚未归集剂量导出")
        return self._build_view(row)

    def _build_view(self, row: dict[str, Any]) -> dict[str, Any]:
        study_uid = row["study_instance_uid"]
        version = row["version"]
        events = self.db.dose_events(study_uid, version)
        by_series: dict[str, list[dict[str, Any]]] = {}
        for e in events:
            by_series.setdefault(e["series_instance_uid"], []).append(e)
        series_views = []
        for series_uid in sorted(by_series):
            items = by_series[series_uid]
            series_views.append({
                "series_instance_uid": series_uid,
                "event_count": len(items),
                "total_dlp_mgy_cm": round(
                    sum(i["dlp_mgy_cm"] for i in items), 6),
                "max_ctdi_vol_mgy": max(i["ctdi_vol_mgy"] for i in items),
                "events": [
                    {
                        "irradiation_event_uid": i["irradiation_event_uid"],
                        "ctdi_vol_mgy": i["ctdi_vol_mgy"],
                        "dlp_mgy_cm": i["dlp_mgy_cm"],
                    }
                    for i in items
                ],
            })
        return {
            "study_instance_uid": study_uid,
            "version": version,
            "batch_id": row["batch_id"],
            "export_id": row["export_id"],
            "created_at": row["created_at"],
            "event_count": row["event_count"],
            "series_count": row["series_count"],
            "total_dlp_mgy_cm": round(row["total_dlp_mgy_cm"], 6),
            "series": series_views,
            "raw_download": f"/dose-exports/{study_uid}/versions/{version}/raw",
        }

    def versions(self, study_uid: str) -> list[dict[str, Any]]:
        current = self.db.current_dose_version(study_uid)
        current_version = current["version"] if current else None
        return [
            {
                "version": r["version"],
                "batch_id": r["batch_id"],
                "export_id": r["export_id"],
                "event_count": r["event_count"],
                "series_count": r["series_count"],
                "total_dlp_mgy_cm": round(r["total_dlp_mgy_cm"], 6),
                "created_at": r["created_at"],
                "is_current": r["version"] == current_version,
            }
            for r in self.db.list_dose_versions(study_uid)
        ]

    def raw_path(self, study_uid: str, version: int) -> Path:
        row = self.db.dose_version(study_uid, version)
        if row is None:
            raise NoBatchForStudyError(
                f"检查 {study_uid} 不存在剂量版本 v{version}"
            )
        path = Path(row["raw_path"])
        if not path.exists():
            raise DoseStoreError(
                f"剂量版本 v{version} 的原件文件缺失（数据卷可能已损坏）"
            )
        return path

    def current_raw_path(self, study_uid: str) -> Path:
        row = self.db.current_dose_version(study_uid)
        if row is None:
            raise NoBatchForStudyError(f"检查 {study_uid} 尚未归集剂量导出")
        return self.raw_path(study_uid, row["version"])

    # ------------------------------------------------------------ 启动恢复
    def recover(self) -> None:
        """启动对账：残缺版本回退指针，孤儿原件清理。

        * 发布顺序是“先写原件、再提交事务”，崩溃只会留下未被引用的
          孤儿文件——直接删除，已发布版本不受影响。
        * 若已发布版本的原件文件缺失（数据卷损坏），删除该版本行并把
          当前指针回退到最新的完整版本，保证读者不会拿到混合版本。
        """
        db = self.db
        for row in db.all_dose_versions():
            if Path(row["raw_path"]).exists():
                continue
            log.warning(
                "剂量版本原件缺失，回退 study=%s v=%d",
                row["study_instance_uid"], row["version"],
            )
            db.drop_dose_version(row["study_instance_uid"], row["version"])
            db.add_event(
                "DOSE_ROLLED_BACK",
                f"剂量版本 v{row['version']} 原件缺失，当前版本回退到最新完整版",
                row["batch_id"],
            )
        valid = {r["raw_path"] for r in db.all_dose_versions()}
        removed = storage.cleanup_dose_orphans(valid)
        if removed:
            log.info("清理未登记的剂量孤儿文件 %d 个", len(removed))
