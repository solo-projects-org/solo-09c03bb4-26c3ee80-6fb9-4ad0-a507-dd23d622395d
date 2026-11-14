# DICOM CT 离线取数流水线

面向影像科研数据员的院内 PACS 取数工具：提交 `StudyInstanceUID`，经
**C-FIND 固定本批 SOP 清单 → C-MOVE 发起回传 → C-STORE 接收落盘 →
核对收齐 → 打包 ZIP 下载** 的完整流水线，支持多检查并行、慢回传交错、
重复投送去重、清单外实例拒收、保存失败判失败与重启恢复。

取数完成后，扫描仪导出的 **CT 剂量 JSON** 可按检查归集：**只受理已完成批**，
校验序列与已登记 DICOM 一致，**按序列汇总照射事件与 DLP**；同一照射事件
仅在归属和剂量完全一致时计一次，冲突则整份拒绝；每次受理发布为新的
**版本**，汇总与原件同版原子生效，失败或重启保留上次完整版本。

* **API 框架**：FastAPI（自带 Swagger/OpenAPI）
* **DICOM 栈**：pynetdicom 3.x（C-STORE SCP + C-FIND/C-MOVE SCU）、pydicom
* **持久化**：SQLite（WAL，全量落在数据卷）
* **模拟 PACS**：pynetdicom 实现的真实 DICOM 网络对端，内置虚构合成 CT
  （每检查双序列）与配套虚构剂量导出，可配置慢回传与重复投送；
  另带 rogue 投送器演示拒收

---

## 1. 快速开始（Docker Compose）

```bash
# 构建并启动 API + 模拟 PACS（首次会自动生成 8/6/5 个实例的三个虚构 CT 检查）
docker compose up --build -d

# Swagger 文档
open http://localhost:8000/docs

# 一键端到端演示：交错回传 / 重复投送 / 清单外拒收 / 压缩拒收 /
# 保存失败 / 失败批迟到拒收 / 重启恢复
docker compose run --rm demo
```

端口：

| 服务 | 端口 | 用途 |
|---|---|---|
| api | 8000 | REST API + Swagger |
| api | 11113 | DICOM C-STORE SCP（AE=`RETRIEVER`）|
| pacs | 11112 | 模拟 PACS（AE=`MYPACS`）|

数据卷：`api-data`（SQLite、DICOM 文件、ZIP）、`pacs-data`（虚构 CT）。

---

## 2. 业务规则与实现位置

| 规则 | 实现 |
|---|---|
| 只收**非压缩 CT** | SCP 只公布 Implicit/Explicit VR Little Endian 的 CT 存储上下文，压缩 TS 在协商阶段即被拒；`app/storage.py::validate_dataset` 二次校验 |
| 源端批内**不增删实例** | C-FIND（IMAGE 级）一次性固定清单写入 `instances` 表，`set_manifest` 只允许固定一次 |
| 回传**归入对应批** | C-STORE 按 `StudyInstanceUID` 经 `db.find_batch_for_study` 路由（按 rowid 取最新批） |
| **清单外实例不入包** | `db.is_manifest_uid` 校验，拒收并回 `0xC000`，事件 `STORE_REJECTED` |
| **重复实例不重复计数** | `(batch_id, sop_instance_uid)` 唯一约束 + `register_instance` 条件更新；重复直接回 SUCCESS，不写盘不计数，事件 `STORE_DUPLICATE` |
| **文件写完且登记成功才回 C-STORE 成功** | `handle_c_store` 严格顺序：临时文件 → fsync → rename → 目录 fsync → DB 登记；任一步失败回 `0xA700/0xC000`，登记失败会删除已写文件 |
| **C-MOVE 成功且清单收齐才可下载** | 下载接口仅允许 `COMPLETED` 且 zip_path 存在；MOVING/FAILED 返回 409 |
| 传输/保存失败、结束后缺件均**判失败** | C-MOVE 非 0x0000（含 0xA702/0xB000）失败；`missing_uids` 非空失败；写盘异常立即 `fail_batch` |
| **迟到回传不能恢复失败批** | 终态 FAILED 的批收到 C-STORE 一律拒收；DB 层 `transition_state` 禁止任何状态迁出终态 |
| 多检查**并行、慢回传互不阻塞** | 每批独立线程；C-STORE SCP 每关联一线程；C-MOVE 回传与其它批的 API/DICOM 处理无共享阻塞 |
| **重启恢复** | 启动时所有非终态批置 FAILED；COMPLETED 批保留可下载，ZIP 丢失自动按登记重建 |
| 剂量导出**只受理已完成批** | `app/dose.py::_completed_batch`：无批 404、批未完成 409 |
| 剂量**序列须与已登记 DICOM 一致** | 事件序列须在批清单的序列集合内（C-FIND 登记，缺失时从已落盘文件头回填），否则整份 422 |
| 同一照射事件**完全一致才计一次** | `app/dose.py::_dedup`：同 UID 事件的序列/CTDIvol/DLP 完全一致则去重，任一不符整份拒绝，不落任何数据 |
| **汇总与原件同版原子发布** | 先写原件文件（tmp+fsync+rename），再在单事务提交版本行+事件行+当前版本指针（`db.publish_dose_version`） |
| 修订失败/重启**保留上次完整版本** | 写盘或登记失败删除新原件、指针不动；启动时对账：孤儿原件清理、残缺版本回退指针（`DoseService.recover`） |
| **并发修订不产生混合版本** | 每检查一把修订锁串行化版本号分配与发布；旧版本不可变、永不删除，读者按指针永远读到完整一版 |

### 批状态机

```
PENDING ──C-FIND──▶ FINDING ──清单固定+故障武装──▶ MOVING
                                          │
                  收齐+C-MOVE 成功 ────────┴──▶ COMPLETED（可下载，终态）
                  任何失败/缺件 ──────────────▶ FAILED（终态，迟到回传拒收）
重启时 PENDING/FINDING/MOVING ───────────────▶ FAILED
```

---

## 3. REST API

Swagger：`GET /docs`（OpenAPI JSON：`/openapi.json`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/batches` | 提交检查，body：`{"study_instance_uid": "...", "fault_inject": ""}`，`fault_inject` 可选 `save_failure` |
| GET | `/batches` | 列出全部批 |
| GET | `/batches/{id}` | 批状态与进度（`expected_count/received_count/state/downloadable`）|
| GET | `/batches/{id}/events` | 事件流水（接收、重复、拒收、状态迁移、故障）|
| GET | `/batches/{id}/instances` | 清单、已收、缺失 SOP 列表 |
| GET | `/batches/{id}/download` | 下载 ZIP（仅 COMPLETED），ZIP 内含 `dicom/` 与 `MANIFEST.csv` |
| POST | `/dose-exports` | 提交扫描仪剂量导出（整份受理为新版本；冲突/序列不符整份拒绝）|
| GET | `/dose-exports` | 列出已有剂量归集的检查及当前版本 |
| GET | `/dose-exports/{study}` | 当前版本的按序列剂量汇总（事件、DLP 合计）|
| GET | `/dose-exports/{study}/raw` | 下载当前版本剂量导出原件（溯源）|
| GET | `/dose-exports/{study}/versions` | 版本历史（每次受理一版，旧版不可变）|
| GET | `/dose-exports/{study}/versions/{n}` | 指定版本的按序列汇总 |
| GET | `/dose-exports/{study}/versions/{n}/raw` | 下载指定版本原件 |
| GET | `/health` | 健康检查 |
| GET | `/internal/pacs-studies` | （演示）C-FIND 列出模拟 PACS 检查与序列 |
| POST | `/internal/suicide` | （演示）退出进程，由 compose 自动重启以演示恢复 |

手工调用示例：

```bash
BASE=http://localhost:8000
STUDY=$(curl -s $BASE/internal/pacs-studies | jq -r '.[0].study_instance_uid')

BID=$(curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\"}" | jq -r .id)

watch "curl -s $BASE/batches/$BID | jq"
curl -s $BASE/batches/$BID/events | jq
curl -OJ $BASE/batches/$BID/download

# 保存失败演示
curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\",\"fault_inject\":\"save_failure\"}"
```

剂量归集示例（批完成后）：

```bash
# 提交扫描仪剂量导出（虚构导出件可用模拟器生成，见 demo/run_demo.py）
curl -s -X POST $BASE/dose-exports -H 'Content-Type: application/json' -d '{
  "study_instance_uid": "'$STUDY'",
  "export_id": "CT-EXPORT-0001",
  "events": [
    {"irradiation_event_uid": "'$STUDY'.9001",
     "series_instance_uid": "<序列UID>",
     "ctdi_vol_mgy": 12.4, "dlp_mgy_cm": 384.6},
    {"irradiation_event_uid": "'$STUDY'.9002",
     "series_instance_uid": "<序列UID>",
     "ctdi_vol_mgy": 8.1, "dlp_mgy_cm": 210.0}
  ]}' | jq

curl -s $BASE/dose-exports/$STUDY | jq            # 当前版按序列汇总
curl -s $BASE/dose-exports/$STUDY/versions | jq   # 版本历史
curl -OJ $BASE/dose-exports/$STUDY/raw            # 当前版原件（溯源）
```

---

## 4. 剂量归集与版本模型

* **受理条件**：检查须有 COMPLETED 批（无批 404、批未完成 409）；
  每个照射事件的 `series_instance_uid` 必须在该批已登记的 DICOM 序列
  集合内，否则整份 422。
* **事件去重**：同一 `irradiation_event_uid` 在一份导出中重复出现时，
  序列归属与 CTDIvol/DLP 完全一致才计一次；任一字段不一致即冲突，
  **整份拒绝**，不产生任何版本。
* **版本化**：每次受理生成单调递增的新版本，整份替换旧版。版本由
  「汇总（SQLite 行）+ 原件（`dose/<study>/v<n>.json`）」组成，
  二者作为同一版原子发布：先写原件并 fsync，再在单个事务里提交
  版本行、事件行与当前版本指针。
* **失败与重启**：解析、写盘或登记任一步失败，以及发布中途重启，
  当前版本指针保持不动，读者始终看到上一次的完整版本；启动时对账
  清理未登记的孤儿原件、回退残缺版本的指针。
* **并发修订**：同一检查的并发修订由修订锁串行化，版本号不撞、
  指针不交错；旧版本不可变、永不删除，任何读者按指针取到的都是
  完整一版（绝不出现“汇总是新版、原件是旧版”的混合状态）。

---

## 5. 演示场景说明

模拟 PACS 支持环境变量（见 `docker-compose.yml`）：

* `MOVE_DELAY=1.0`：每个实例发送前停顿 1 秒，制造慢回传，便于观察多批交错
* `MOVE_DUPLICATE_EVERY=3`：每 3 个实例额外用一条新关联重发一次，模拟源端重试
  （接收端应回 SUCCESS 但不计数）

每个虚构检查包含 **2 个序列**，并在检查目录内附带 `dose_export.json`
（虚构扫描仪剂量导出，确定性生成）。`demo/run_demo.py` 会演示剂量归集
全流程：受理并按序列汇总 → 完全一致重复计一次 → 冲突重复/未登记序列
整份拒绝 → 修订发布新版本且旧版可查 → 原件逐字节回溯 → 重启后版本完好。

rogue 投送器（演示容器/API 镜像内均可运行）：

```bash
# 清单外实例：期望 0xC000 拒收
python -m simulator.rogue_sender off-list   --study <StudyUID> --host api --port 11113
# 声明 JPEG 压缩：期望关联协商被拒
python -m simulator.rogue_sender compressed --study <StudyUID> --host api --port 11113
```

保存失败演示的实现是**真实故障**而非 mock：提交时把批目录 `chmod 555`，
非 root 容器用户写入必然 `PermissionError`，可观察到：
首个实例 C-STORE 回 `0xA700` → 批立即 FAILED → 后续实例全部以“已失败”拒收
→ C-MOVE 最终 `0xA702` → 下载返回 409。

---

## 6. 目录结构

```
app/
  config.py      环境变量配置（AE、端口、数据卷路径、超时）
  db.py          SQLite schema、状态机、清单/登记去重、剂量版本表与原子发布
  storage.py     非压缩 CT 校验、原子写盘+fsync、故障注入、ZIP 打包、剂量原件落盘
  dicom.py       C-STORE SCP 处理器、C-FIND/C-MOVE SCU
  dose.py        剂量导出校验/去重/版本化发布/启动对账
  pipeline.py    每批后台线程编排与启动恢复
  main.py        FastAPI 路由、lifespan、Swagger
simulator/
  generate_data.py  虚构合成 CT 生成器（双序列、确定性模体像素，无真实患者信息）
  dose_export.py    虚构剂量导出生成器（含重复/冲突/修订变体）
  pacs.py           模拟 PACS（C-ECHO/C-FIND/C-MOVE，慢回传+重复投送）
  rogue_sender.py   清单外/压缩实例投送器
demo/
  run_demo.py       端到端演示编排（取数 + 剂量归集 + 重启恢复）
tests/
  test_db.py       状态机/去重/终态保护单元测试
  test_storage.py  校验/原子写/ZIP 单元测试
  test_dose.py     剂量校验/去重/版本/并发/恢复单元测试
  test_e2e.py      真实 pynetdicom 网络栈端到端测试（含剂量 HTTP 流程）
docker-compose.yml  Dockerfile  Dockerfile.pacs  entrypoint.sh
```

## 7. 本地测试（无需 Docker）

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```

端到端测试会在临时目录启动完整的 PACS + API 真实进程对，覆盖：
正常收齐打包、并行交错、重复去重、清单外/压缩/未知 Study 拒收、
保存失败与迟到拒收、409 冲突、重启恢复，以及剂量归集的受理/去重/
冲突拒绝/修订换版/原件溯源/重启保版（共 11 个测试）。

## 8. 接入真实 PACS 的注意事项

1. 在真实 PACS 上注册 AE Title `RETRIEVER` 及本服务主机/11113 端口作为
   C-MOVE 目标，并放通防火墙。
2. 用 `PACS_AET/PACS_HOST/PACS_PORT` 指向真实源端；确认其 C-FIND 支持
   Study Root IMAGE 级查询并返回 `TransferSyntaxUID`。
3. 流水线只接受未压缩 little-endian CT；若源端只存压缩 TS，需在源端配置
   转码，或扩展 `app/dicom.py` 的上下文与 `storage.UNCOMPRESSED_TS` 策略。
4. 数据卷需保证持久化挂载与足够 inode/容量；SQLite 与 DICOM 文件均在
   `DATA_DIR` 下，备份该目录即可完整备份批状态与文件。
