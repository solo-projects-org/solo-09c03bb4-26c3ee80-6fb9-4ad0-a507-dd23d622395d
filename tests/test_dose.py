"""剂量归集：校验、去重、版本化发布、并发与崩溃恢复的单元测试（无网络）。"""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest
from pydicom.uid import CTImageStorage, ExplicitVRLittleEndian

from app import storage
from app.config import Settings
from app.db import (
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_FINDING,
    STATE_MOVING,
    STATE_PENDING,
    Database,
    set_db,
)
from app.dose import (
    BatchNotCompletedError,
    DoseService,
    DoseStoreError,
    DoseValidationError,
    NoBatchForStudyError,
)
from simulator.generate_data import build_ct_dataset

CT = str(CTImageStorage)
TS = str(ExplicitVRLittleEndian)
STUDY = "1.2.826.0.1.3680043.8.498.1001"
SERIES_A = "1.2.826.0.1.3680043.8.498.2001"
SERIES_B = "1.2.826.0.1.3680043.8.498.2511"


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(Settings, "DATA_DIR", data)
    monkeypatch.setattr(Settings, "DB_PATH", data / "retrieval.db")
    monkeypatch.setattr(Settings, "BATCHES_DIR", data / "batches")
    monkeypatch.setattr(Settings, "ZIP_DIR", data / "zips")
    monkeypatch.setattr(Settings, "DOSE_DIR", data / "dose")
    Settings.ensure_dirs()
    db = Database(Settings.DB_PATH)
    set_db(db)
    yield data, db
    db.close()


@pytest.fixture()
def svc(workspace):
    _, db = workspace
    return DoseService(db)


def _completed_batch(db, study=STUDY, batch_id="b1",
                     series=(SERIES_A, SERIES_B)):
    db.create_batch(batch_id, study, "")
    db.transition_state(batch_id, [STATE_PENDING], STATE_FINDING)
    manifest = [
        (f"{study}.7.{i}", CT, TS, s)
        for i, s in enumerate(series)
    ]
    db.set_manifest(batch_id, manifest)
    db.transition_state(batch_id, [STATE_FINDING], STATE_MOVING)
    db.transition_state(batch_id, [STATE_MOVING], STATE_COMPLETED,
                        zip_path="/z.zip")
    return batch_id


def _export(study=STUDY, series=(SERIES_A, SERIES_B), dlp_base=100.0):
    events = []
    for s in series:
        for i in range(2):
            events.append({
                "irradiation_event_uid": f"{s}.ev{i}",
                "series_instance_uid": s,
                "ctdi_vol_mgy": 10.0 + i,
                "dlp_mgy_cm": dlp_base * (i + 1),
            })
    return {"study_instance_uid": study, "export_id": "E-1", "events": events}


def _submit(svc, export):
    raw = json.dumps(export).encode()
    return svc.submit(export["study_instance_uid"],
                      export.get("export_id", ""),
                      export["events"], raw)


# ---------------------------------------------------------------- 受理与汇总
def test_submit_publishes_version_and_summary(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    export = _export()
    view = _submit(svc, export)

    assert view["version"] == 1
    assert view["study_instance_uid"] == STUDY
    assert view["event_count"] == 4
    assert view["series_count"] == 2
    # 每序列 100+200=300，两序列合计 600
    assert view["total_dlp_mgy_cm"] == 600.0
    by_series = {s["series_instance_uid"]: s for s in view["series"]}
    assert set(by_series) == {SERIES_A, SERIES_B}
    assert by_series[SERIES_A]["total_dlp_mgy_cm"] == 300.0
    assert by_series[SERIES_A]["event_count"] == 2
    assert by_series[SERIES_A]["max_ctdi_vol_mgy"] == 11.0

    # 原件按提交字节原样留存
    raw_path = storage.dose_raw_path(STUDY, 1)
    assert raw_path.read_bytes() == json.dumps(export).encode()

    # 当前版本视图与版本历史一致
    assert svc.current_view(STUDY)["version"] == 1
    versions = svc.versions(STUDY)
    assert len(versions) == 1 and versions[0]["is_current"]


def test_identical_duplicates_counted_once(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    export = _export()
    # 每个事件原样重复一次（归属与剂量完全一致）
    export["events"] = [dict(e) for e in export["events"] for _ in (0, 1)]
    view = _submit(svc, export)
    assert view["event_count"] == 4
    assert view["total_dlp_mgy_cm"] == 600.0


def test_conflicting_duplicate_rejects_whole_export(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    export = _export()
    clash = dict(export["events"][0])
    clash["dlp_mgy_cm"] += 50.0  # 同一事件 UID，剂量不一致
    export["events"].append(clash)

    with pytest.raises(DoseValidationError):
        _submit(svc, export)
    # 整份拒绝：无版本、无事件、无原件文件
    assert db.list_dose_versions(STUDY) == []
    assert db.current_dose_version(STUDY) is None
    assert not list(Settings.DOSE_DIR.rglob("*.json"))
    kinds = [e["kind"] for e in db.list_events("b1")]
    assert "DOSE_REJECTED" in kinds


def test_conflicting_series_attribution_rejected(workspace, svc):
    """同一事件 UID 归属到不同序列也是冲突，整份拒绝。"""
    _, db = workspace
    _completed_batch(db)
    export = _export()
    clash = dict(export["events"][0])
    clash["series_instance_uid"] = SERIES_B  # 同 UID 不同序列归属
    export["events"].append(clash)
    with pytest.raises(DoseValidationError):
        _submit(svc, export)
    assert db.list_dose_versions(STUDY) == []


def test_unknown_series_rejected(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    export = _export()
    export["events"].append({
        "irradiation_event_uid": "1.2.3.4.5.9001",
        "series_instance_uid": "1.2.3.4.5",  # 未登记的序列
        "ctdi_vol_mgy": 5.0,
        "dlp_mgy_cm": 120.0,
    })
    with pytest.raises(DoseValidationError) as exc:
        _submit(svc, export)
    assert "序列" in str(exc.value)
    assert db.list_dose_versions(STUDY) == []


def test_only_completed_batches_accepted(workspace, svc):
    _, db = workspace
    # 无批 -> 404
    with pytest.raises(NoBatchForStudyError):
        _submit(svc, _export(study="1.2.3.9"))
    # 进行中批 -> 409
    db.create_batch("b-moving", STUDY, "")
    db.transition_state("b-moving", [STATE_PENDING], STATE_FINDING)
    with pytest.raises(BatchNotCompletedError):
        _submit(svc, _export())
    # 失败批 -> 409
    db.fail_batch("b-moving", "boom")
    with pytest.raises(BatchNotCompletedError):
        _submit(svc, _export())
    # 完成后受理
    db.create_batch("b-ok", STUDY, "")
    db.transition_state("b-ok", [STATE_PENDING], STATE_FINDING)
    db.set_manifest("b-ok", [(f"{STUDY}.7.0", CT, TS, SERIES_A)])
    db.transition_state("b-ok", [STATE_FINDING], STATE_MOVING)
    db.transition_state("b-ok", [STATE_MOVING], STATE_COMPLETED,
                        zip_path="/z.zip")
    view = _submit(svc, _export(series=(SERIES_A,)))
    assert view["version"] == 1
    assert view["batch_id"] == "b-ok"


def test_empty_and_blank_rejected(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    with pytest.raises(DoseValidationError):
        svc.submit(STUDY, "", [], b"{}")
    with pytest.raises(DoseValidationError):
        svc.submit("   ", "", _export()["events"], b"{}")


# ---------------------------------------------------------------- 修订与版本
def test_revision_supersedes_and_old_versions_stay(workspace, svc):
    _, db = workspace
    _completed_batch(db)
    v1 = _submit(svc, _export(dlp_base=100.0))
    v2 = _submit(svc, _export(dlp_base=200.0))  # 修订：剂量不同

    assert v1["version"] == 1 and v2["version"] == 2
    assert svc.current_view(STUDY)["version"] == 2
    assert svc.current_view(STUDY)["total_dlp_mgy_cm"] == 1200.0
    # 旧版本不可变、仍可查
    old = svc.version_view(STUDY, 1)
    assert old["total_dlp_mgy_cm"] == 600.0
    # 两版原件各自完整留存
    assert storage.dose_raw_path(STUDY, 1).exists()
    assert storage.dose_raw_path(STUDY, 2).exists()
    assert (storage.dose_raw_path(STUDY, 1).read_bytes()
            != storage.dose_raw_path(STUDY, 2).read_bytes())
    flags = {v["version"]: v["is_current"] for v in svc.versions(STUDY)}
    assert flags == {1: False, 2: True}


def test_concurrent_revisions_never_mix(workspace, svc):
    """两个并发修订：版本号不撞、指针指向完整版本、原件与汇总同版。"""
    _, db = workspace
    _completed_batch(db)
    barrier = threading.Barrier(2)
    results: list[dict] = []
    errors: list[Exception] = []

    def _revise(dlp_base):
        try:
            barrier.wait(timeout=10)
            results.append(_submit(svc, _export(dlp_base=dlp_base)))
        except Exception as exc:  # pragma: no cover - 失败即测试失败
            errors.append(exc)

    threads = [threading.Thread(target=_revise, args=(100.0,)),
               threading.Thread(target=_revise, args=(200.0,))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    assert sorted(v["version"] for v in results) == [1, 2]
    current = svc.current_view(STUDY)
    assert current["version"] == 2
    # 当前版本的汇总与其原件内容一致（非混合版本）
    raw = json.loads(storage.dose_raw_path(STUDY, 2).read_bytes())
    raw_total = sum(e["dlp_mgy_cm"] for e in raw["events"])
    assert current["total_dlp_mgy_cm"] == raw_total
    assert db.dose_version(STUDY, 2)["raw_path"].endswith("v2.json")


# ---------------------------------------------------------------- 失败保旧版
def test_publish_failure_keeps_previous_version(workspace, svc, monkeypatch):
    _, db = workspace
    _completed_batch(db)
    _submit(svc, _export(dlp_base=100.0))

    def _boom(*args, **kwargs):
        raise RuntimeError("db locked")

    monkeypatch.setattr(db, "publish_dose_version", _boom)
    with pytest.raises(DoseStoreError):
        _submit(svc, _export(dlp_base=200.0))
    # 无需撤销：后续断言不经过 publish；teardown 时 monkeypatch 自动还原

    # 当前版本仍是 v1，v2 未登记且原件已清理
    assert svc.current_view(STUDY)["version"] == 1
    assert db.list_dose_versions(STUDY)[0]["version"] == 1
    assert not storage.dose_raw_path(STUDY, 2).exists()
    assert storage.dose_raw_path(STUDY, 1).exists()


def test_raw_write_failure_keeps_previous_version(workspace, svc, monkeypatch):
    _, db = workspace
    _completed_batch(db)
    _submit(svc, _export(dlp_base=100.0))

    def _boom(*args, **kwargs):
        raise storage.StorageError("disk full")

    monkeypatch.setattr(storage, "write_dose_raw", _boom)
    with pytest.raises(DoseStoreError):
        _submit(svc, _export(dlp_base=200.0))

    assert svc.current_view(STUDY)["version"] == 1
    assert len(db.list_dose_versions(STUDY)) == 1


# ---------------------------------------------------------------- 启动恢复
def test_recovery_rolls_back_broken_and_cleans_orphans(workspace, svc):
    data, db = workspace
    _completed_batch(db)
    _submit(svc, _export(dlp_base=100.0))
    _submit(svc, _export(dlp_base=200.0))
    assert svc.current_view(STUDY)["version"] == 2

    # 模拟数据卷损坏：v2 原件丢失；另有崩溃残留的孤儿文件
    storage.dose_raw_path(STUDY, 2).unlink()
    orphan = Settings.DOSE_DIR / STUDY / "v3.json"
    orphan.write_text("{}")
    stray = Settings.DOSE_DIR / STUDY / "v3.json.part"
    stray.write_text("{}")

    DoseService(db).recover()

    # 指针回退到最新完整版；孤儿文件被清理；v1 完好
    assert svc.current_view(STUDY)["version"] == 1
    assert [v["version"] for v in svc.versions(STUDY)] == [1]
    assert not orphan.exists() and not stray.exists()
    assert storage.dose_raw_path(STUDY, 1).exists()
    kinds = [e["kind"] for e in db.list_events("b1")]
    assert "DOSE_ROLLED_BACK" in kinds


def test_orphan_from_crash_before_commit_is_cleaned(workspace, svc):
    """文件写完但事务未提交（进程被杀）：孤儿文件不影响当前版本。"""
    _, db = workspace
    _completed_batch(db)
    _submit(svc, _export(dlp_base=100.0))
    # 手工制造“写完未登记”的孤儿 v2
    storage.write_dose_raw(STUDY, 2, json.dumps(_export(dlp_base=999.0)).encode())
    assert storage.dose_raw_path(STUDY, 2).exists()
    assert svc.current_view(STUDY)["version"] == 1

    DoseService(db).recover()
    assert not storage.dose_raw_path(STUDY, 2).exists()
    assert svc.current_view(STUDY)["version"] == 1


# ---------------------------------------------------------------- 序列回填
def test_series_backfilled_from_registered_dicom(workspace, svc):
    """清单缺序列号（老库/源端未返回）时，从已登记 DICOM 文件头回填。"""
    data, db = workspace
    db.create_batch("b1", STUDY, "")
    db.transition_state("b1", [STATE_PENDING], STATE_FINDING)
    db.set_manifest("b1", [(f"{STUDY}.7.0", CT, TS, "")])  # 无序列号
    db.transition_state("b1", [STATE_FINDING], STATE_MOVING)
    ds = build_ct_dataset(
        study_uid=STUDY, series_uid=SERIES_A, sop_uid=f"{STUDY}.7.0",
        patient_id="P1", patient_name="T^T", instance_number=1,
        instance_count=1,
    )
    path = storage.write_dataset("b1", ds, TS)
    assert db.register_instance("b1", f"{STUDY}.7.0", str(path), TS)
    db.transition_state("b1", [STATE_MOVING], STATE_COMPLETED, zip_path="/z")

    view = _submit(svc, _export(series=(SERIES_A,)))
    assert view["version"] == 1
    assert db.series_uids_for_batch("b1") == {SERIES_A}


# ---------------------------------------------------------------- 老库迁移
def test_migration_adds_series_column(tmp_path):
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE instances (
            batch_id TEXT NOT NULL,
            sop_instance_uid TEXT NOT NULL,
            sop_class_uid TEXT NOT NULL,
            transfer_syntax TEXT NOT NULL,
            file_path TEXT NOT NULL,
            received_at TEXT NOT NULL,
            PRIMARY KEY (batch_id, sop_instance_uid)
        );
        INSERT INTO instances VALUES ('b0', 'sop0', 'c', 't', '/f', 'now');
        """
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    cols = {r["name"] for r in
            db._conn.execute("PRAGMA table_info(instances)")}
    assert "series_instance_uid" in cols
    # 老行序列为空串，新清单可带序列号
    assert db.series_uids_for_batch("b0") == set()
    db.create_batch("b1", STUDY, "")
    db.transition_state("b1", [STATE_PENDING], STATE_FINDING)
    db.set_manifest("b1", [("sop1", CT, TS, SERIES_A)])
    assert db.series_uids_for_batch("b1") == {SERIES_A}
    db.close()
