"""虚构 CT 剂量导出生成器（与虚构合成 CT 配套，无任何真实数据）。

扫描仪侧产物：每个检查一份 JSON，包含该检查各序列的照射事件
（CTDIvol / DLP）。演示与测试用它确定性地构造导出、修订版与
各种异常变体；同一输入永远得到同一输出。
"""
from __future__ import annotations

import copy
import hashlib
from typing import Any


def _scaled(seed_hex: str, lo: float, hi: float) -> float:
    """把哈希片段映射到 [lo, hi] 区间，保留 1 位小数。"""
    frac = int(seed_hex, 16) / 0xFFFFFFFF
    return round(lo + frac * (hi - lo), 1)


def build_dose_export(
    study_uid: str,
    series_uids: list[str],
    export_id: str = "",
    variant: int = 0,
) -> dict[str, Any]:
    """构造一份虚构剂量导出。

    ``variant`` 区分修订版：事件 UID 不变、剂量不同，模拟扫描仪
    对同一检查重新导出的“修订件”。
    """
    events: list[dict[str, Any]] = []
    for s_idx, series_uid in enumerate(series_uids, start=1):
        n_events = 2 + (s_idx % 2)  # 每个序列 2~3 个照射事件
        for e_idx in range(1, n_events + 1):
            seed = hashlib.sha256(
                f"{study_uid}|{series_uid}|{e_idx}|{variant}".encode()
            ).hexdigest()
            events.append({
                "irradiation_event_uid": f"{series_uid}.900{e_idx}",
                "series_instance_uid": series_uid,
                "ctdi_vol_mgy": _scaled(seed[:8], 2.0, 25.0),
                "dlp_mgy_cm": _scaled(seed[8:16], 80.0, 900.0),
            })
    return {
        "study_instance_uid": study_uid,
        "export_id": export_id
        or f"SIM-EXPORT-{study_uid.rsplit('.', 1)[-1]}-r{variant + 1}",
        "device": "Synthetic CT Scanner SIM-CT-01",
        "exported_at": "2026-11-01T10:30:00+00:00",
        "events": events,
    }


def revised_export(export: dict[str, Any]) -> dict[str, Any]:
    """基于已有导出生成修订版（事件相同、剂量重算、导出头更新）。"""
    try:
        current_variant = int(
            str(export.get("export_id", "")).rsplit("r", 1)[-1])
    except (ValueError, IndexError):
        current_variant = 1
    return build_dose_export(
        export["study_instance_uid"],
        sorted({e["series_instance_uid"] for e in export["events"]}),
        variant=current_variant,
    )


def with_identical_duplicates(export: dict[str, Any]) -> dict[str, Any]:
    """变体：每个事件原样重复一次（归属与剂量完全一致，应只计一次）。"""
    out = copy.deepcopy(export)
    out["events"] = [
        copy.deepcopy(e) for e in export["events"] for _ in (0, 1)
    ]
    return out


def with_conflicting_duplicate(export: dict[str, Any]) -> dict[str, Any]:
    """变体：首个事件被重复但 DLP 不同（冲突，应整份拒绝）。"""
    out = copy.deepcopy(export)
    clash = copy.deepcopy(export["events"][0])
    clash["dlp_mgy_cm"] = round(clash["dlp_mgy_cm"] + 37.5, 1)
    out["events"] = [export["events"][0], clash, *export["events"][1:]]
    return out


def with_unknown_series(export: dict[str, Any], bogus_series_uid: str) -> dict[str, Any]:
    """变体：混入一个未登记序列的事件（应整份拒绝）。"""
    out = copy.deepcopy(export)
    rogue = copy.deepcopy(export["events"][0])
    rogue["irradiation_event_uid"] = f"{bogus_series_uid}.9001"
    rogue["series_instance_uid"] = bogus_series_uid
    out["events"] = [*out["events"], rogue]
    return out
