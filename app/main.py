"""FastAPI 入口：批管理 REST API + Swagger 文档 + DICOM 服务生命周期。"""
from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ValidationError

from . import dicom, storage
from .config import settings
from .db import (
    STATE_COMPLETED,
    Database,
    set_db,
)
from .dose import DoseError, DoseService
from .pipeline import Pipeline, recover_on_startup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("api")

# 进程级单例（lifespan 中初始化）
db: Database | None = None
pipeline: Pipeline | None = None
dose_service: DoseService | None = None
_scp_server = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, pipeline, dose_service, _scp_server
    settings.ensure_dirs()
    db = Database(settings.DB_PATH)
    set_db(db)
    recover_on_startup(db)
    dose_service = DoseService(db)
    _scp_server = dicom.start_store_scp(db)
    pipeline = Pipeline(db)
    log.info("API 就绪，数据卷=%s", settings.DATA_DIR)
    try:
        yield
    finally:
        if _scp_server is not None:
            _scp_server.shutdown()
        if db is not None:
            db.close()


app = FastAPI(
    title="DICOM CT 离线取数流水线",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "提交 StudyInstanceUID → C-FIND 固定 SOP 清单 → C-MOVE 回传 → "
        "C-STORE 接收非压缩 CT → 核对收齐后打包 ZIP 下载。\n\n"
        "规则：清单外实例不入包、重复实例不重复计数、保存失败即批失败、"
        "失败批迟到回传不可恢复、重启后未完成批判失败。\n\n"
        "剂量归集：扫描仪导出的 CT 剂量 JSON 按检查归集到已完成批，"
        "按序列汇总照射事件与 DLP；同一照射事件仅在归属和剂量完全一致时"
        "计一次，冲突则整份拒绝；每次受理发布为新的版本，汇总与原件同版"
        "原子生效，失败或重启保留上次完整版本。"
    ),
)


# ------------------------------------------------------------------ schemas
class BatchCreate(BaseModel):
    study_instance_uid: str = Field(
        ..., description="源端检查的 StudyInstanceUID",
        examples=["1.2.840.10008.1.2.99.1"],
    )
    fault_inject: str = Field(
        default="",
        description="可选故障注入：save_failure=保存失败演示；留空为正常流程",
    )


class BatchView(BaseModel):
    id: str
    study_instance_uid: str
    state: str
    expected_count: int
    received_count: int
    failure_reason: str | None
    zip_path: str | None
    fault_inject: str
    created_at: str
    updated_at: str
    downloadable: bool


def _view(row: dict) -> BatchView:
    return BatchView(
        id=row["id"],
        study_instance_uid=row["study_instance_uid"],
        state=row["state"],
        expected_count=row["expected_count"],
        received_count=row["received_count"],
        failure_reason=row["failure_reason"],
        zip_path=row["zip_path"],
        fault_inject=row["fault_inject"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        downloadable=row["state"] == STATE_COMPLETED and bool(row["zip_path"]),
    )


# ------------------------------------------------------------------ routes
@app.post("/batches", response_model=BatchView, status_code=201,
          tags=["batches"], summary="提交检查，启动取数批")
def create_batch(body: BatchCreate) -> BatchView:
    if body.fault_inject not in ("", storage.FAULT_SAVE):
        raise HTTPException(400, "fault_inject 只支持 '' 或 'save_failure'")
    uid = body.study_instance_uid.strip()
    if not uid:
        raise HTTPException(400, "study_instance_uid 不能为空")

    if db.has_active_batch_for_study(uid):
        raise HTTPException(409, "该 Study 已有进行中的批，请等待其结束")

    batch_id = uuid.uuid4().hex
    db.create_batch(batch_id, uid, body.fault_inject)
    pipeline.submit(batch_id)
    return _view(db.get_batch(batch_id))


@app.get("/batches", response_model=list[BatchView],
         tags=["batches"], summary="列出全部批")
def list_batches() -> list[BatchView]:
    return [_view(r) for r in db.list_batches()]


@app.get("/batches/{batch_id}", response_model=BatchView,
         tags=["batches"], summary="查询批状态与进度")
def get_batch(batch_id: str) -> BatchView:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    return _view(row)


@app.get("/batches/{batch_id}/events",
         tags=["batches"], summary="查询批事件流水（接收/重复/拒收/状态迁移）")
def get_events(batch_id: str) -> list[dict]:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    return db.list_events(batch_id)


@app.get("/batches/{batch_id}/instances",
         tags=["batches"], summary="查询批清单及每个 SOP 的接收情况")
def get_instances(batch_id: str) -> dict:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    files = db.received_files(batch_id)
    received = {sop: path for sop, path in files}
    return {
        "batch_id": batch_id,
        "state": row["state"],
        "manifest": sorted(db.manifest_uids(batch_id)),
        "received": sorted(received),
        "missing": db.missing_uids(batch_id),
    }


@app.get("/batches/{batch_id}/download",
         tags=["batches"], summary="下载 ZIP（仅 COMPLETED 批可下载）")
def download_zip(batch_id: str) -> FileResponse:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    if row["state"] != STATE_COMPLETED or not row["zip_path"]:
        raise HTTPException(
            409,
            f"批状态 {row['state']} 不可下载；仅 C-MOVE 成功且清单收齐的批可下载",
        )
    path = Path(row["zip_path"])
    if not path.exists():
        raise HTTPException(410, "ZIP 文件缺失（数据卷可能已损坏）")
    return FileResponse(
        path, media_type="application/zip",
        filename=f"{batch_id}.zip",
    )


@app.get("/health", tags=["system"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "store_port": settings.STORE_PORT,
            "ae_title": settings.APP_AET}


# ============================================================ 剂量归集
class DoseEventIn(BaseModel):
    irradiation_event_uid: str = Field(
        ..., min_length=1, max_length=256, description="照射事件 UID",
        examples=["1.2.826.0.1.3680043.8.498.1001.dose.1"],
    )
    series_instance_uid: str = Field(
        ..., min_length=1, max_length=256, description="事件归属的 SeriesInstanceUID",
        examples=["1.2.826.0.1.3680043.8.498.2001"],
    )
    ctdi_vol_mgy: float = Field(
        ..., ge=0, allow_inf_nan=False, description="容积 CT 剂量指数（mGy）",
        examples=[12.4],
    )
    dlp_mgy_cm: float = Field(
        ..., ge=0, allow_inf_nan=False, description="剂量长度乘积（mGy·cm）",
        examples=[384.6],
    )


class DoseExportIn(BaseModel):
    """扫描仪导出的一整份 CT 剂量 JSON（整份受理或整份拒绝）。"""

    study_instance_uid: str = Field(
        ..., min_length=1, max_length=256, description="检查 StudyInstanceUID",
        examples=["1.2.826.0.1.3680043.8.498.1001"],
    )
    export_id: str = Field(
        default="", max_length=256,
        description="扫描仪侧导出标识（可选，随原件留存供溯源）",
        examples=["CT-EXPORT-20261101-001"],
    )
    events: list[DoseEventIn] = Field(
        ..., min_length=1, description="照射事件列表（同一事件重复须完全一致）",
    )


class DoseEventView(BaseModel):
    irradiation_event_uid: str
    ctdi_vol_mgy: float
    dlp_mgy_cm: float


class DoseSeriesView(BaseModel):
    series_instance_uid: str
    event_count: int
    total_dlp_mgy_cm: float
    max_ctdi_vol_mgy: float
    events: list[DoseEventView]


class DoseVersionView(BaseModel):
    study_instance_uid: str
    version: int
    batch_id: str
    export_id: str
    created_at: str
    event_count: int
    series_count: int
    total_dlp_mgy_cm: float
    series: list[DoseSeriesView]
    raw_download: str


class DoseVersionListItem(BaseModel):
    version: int
    batch_id: str
    export_id: str
    event_count: int
    series_count: int
    total_dlp_mgy_cm: float
    created_at: str
    is_current: bool


class DoseStudyItem(BaseModel):
    study_instance_uid: str
    version: int
    batch_id: str
    event_count: int
    series_count: int
    total_dlp_mgy_cm: float
    updated_at: str


def _dose_call(fn, *args):
    """把 DoseError 映射为对应 HTTP 状态。"""
    try:
        return fn(*args)
    except DoseError as exc:
        raise HTTPException(exc.status_code, exc.message) from exc


@app.post("/dose-exports", response_model=DoseVersionView, status_code=201,
          tags=["dose"],
          summary="提交扫描仪剂量导出（整份受理为新版本，冲突整份拒绝）")
async def submit_dose_export(request: Request) -> DoseVersionView:
    from starlette.concurrency import run_in_threadpool

    raw = await request.body()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(422, f"请求体不是合法 JSON：{exc}") from exc
    try:
        payload = DoseExportIn.model_validate(obj)
    except ValidationError as exc:
        raise HTTPException(422, f"剂量导出格式不符：{exc.errors()}") from exc
    # 落盘与修订锁可能阻塞，放到线程池，避免卡住事件循环
    view = await run_in_threadpool(
        _dose_call,
        dose_service.submit,
        payload.study_instance_uid,
        payload.export_id,
        [e.model_dump() for e in payload.events],
        raw,  # 原件按提交字节原样留存，供溯源下载
    )
    return DoseVersionView(**view)


@app.get("/dose-exports", response_model=list[DoseStudyItem],
         tags=["dose"], summary="列出已有剂量归集的检查及当前版本")
def list_dose_studies() -> list[DoseStudyItem]:
    return [DoseStudyItem(**r) for r in db.all_current_doses()]


@app.get("/dose-exports/{study_uid}", response_model=DoseVersionView,
         tags=["dose"], summary="查询该检查当前版本的按序列剂量汇总")
def get_current_dose(study_uid: str) -> DoseVersionView:
    return DoseVersionView(**_dose_call(dose_service.current_view, study_uid))


@app.get("/dose-exports/{study_uid}/versions",
         response_model=list[DoseVersionListItem],
         tags=["dose"], summary="列出该检查的全部剂量版本（修订历史）")
def get_dose_versions(study_uid: str) -> list[DoseVersionListItem]:
    versions = dose_service.versions(study_uid)
    if not versions:
        raise HTTPException(404, f"检查 {study_uid} 尚未归集剂量导出")
    return [DoseVersionListItem(**v) for v in versions]


@app.get("/dose-exports/{study_uid}/versions/{version}",
         response_model=DoseVersionView,
         tags=["dose"], summary="查询指定版本的按序列剂量汇总")
def get_dose_version(study_uid: str, version: int) -> DoseVersionView:
    return DoseVersionView(
        **_dose_call(dose_service.version_view, study_uid, version)
    )


@app.get("/dose-exports/{study_uid}/versions/{version}/raw",
         tags=["dose"], summary="下载指定版本的剂量导出原件（溯源）")
def get_dose_version_raw(study_uid: str, version: int) -> FileResponse:
    path = _dose_call(dose_service.raw_path, study_uid, version)
    return FileResponse(
        path, media_type="application/json",
        filename=f"dose_{study_uid}_v{version}.json",
    )


@app.get("/dose-exports/{study_uid}/raw",
         tags=["dose"], summary="下载当前版本的剂量导出原件（溯源）")
def get_current_dose_raw(study_uid: str) -> FileResponse:
    path = _dose_call(dose_service.current_raw_path, study_uid)
    return FileResponse(
        path, media_type="application/json",
        filename=f"dose_{study_uid}_{path.stem}.json",
    )


# ------------------------------------------------------------------ debug
@app.get("/internal/pacs-studies", tags=["internal"],
         summary="（演示用）STUDY/SERIES 级 C-FIND 列出模拟 PACS 中的检查与序列")
def pacs_studies() -> list[dict]:
    from pydicom.dataset import Dataset
    from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
    from pynetdicom import AE
    from pynetdicom.presentation import build_context
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

    ae = AE(ae_title=settings.APP_AET)
    ae.requested_contexts = [build_context(
        str(StudyRootQueryRetrieveInformationModelFind),
        [str(ExplicitVRLittleEndian), str(ImplicitVRLittleEndian)])]
    assoc = ae.associate(settings.PACS_HOST, settings.PACS_PORT,
                         ae_title=settings.PACS_AET)
    if not assoc.is_established:
        raise HTTPException(502, "无法连接模拟 PACS")
    try:
        q = Dataset()
        q.QueryRetrieveLevel = "STUDY"
        q.StudyInstanceUID = ""
        q.PatientID = ""
        q.PatientName = ""
        q.Modality = ""
        q.NumberOfStudyRelatedInstances = ""
        out = []
        for status, ident in assoc.send_c_find(
            q, str(StudyRootQueryRetrieveInformationModelFind)
        ):
            if ident is None:
                continue
            out.append({
                "study_instance_uid": str(ident.StudyInstanceUID),
                "patient_id": str(getattr(ident, "PatientID", "")),
                "patient_name": str(getattr(ident, "PatientName", "")),
                "modality": str(getattr(ident, "Modality", "")),
                "instances": int(getattr(ident, "NumberOfStudyRelatedInstances", 0) or 0),
                "series": [],
            })
        # SERIES 级查询补齐每个检查的序列清单（剂量演示按序列构造导出）
        for study in out:
            sq = Dataset()
            sq.QueryRetrieveLevel = "SERIES"
            sq.StudyInstanceUID = study["study_instance_uid"]
            sq.SeriesInstanceUID = ""
            sq.Modality = ""
            for status, ident in assoc.send_c_find(
                sq, str(StudyRootQueryRetrieveInformationModelFind)
            ):
                if ident is None:
                    continue
                series_uid = str(getattr(ident, "SeriesInstanceUID", "") or "")
                if series_uid:
                    study["series"].append(series_uid)
        return out
    finally:
        if assoc.is_alive:
            assoc.release()


@app.post("/internal/suicide", tags=["internal"],
          summary="（演示用）退出进程以演示重启恢复；compose 自动重启")
def suicide(tasks: BackgroundTasks) -> dict:
    def _die() -> None:
        os._exit(1)
    tasks.add_task(_die)
    return {"detail": "进程即将退出，等待 Docker 重启"}
