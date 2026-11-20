# Snapshot Query Service

可查询快照服务：批处理在**临时版本（staging）**中构建，通过**质量门禁**后才**原子切换**为当前版本。
读请求在单个快照内严格一致；发布失败继续提供旧版本；切换期间的并发查询与重试不会看到混合版本。
自带状态仪表盘（构建 / 门禁 / 切换 / 回滚）与端到端验证脚本（长查询 / 并行发布 / 恢复）。

零依赖，Node.js ≥ 18 即可运行。

```bash
npm start          # http://localhost:8080  (PORT、DATA_DIR、RETENTION、MIN_RECORDS 可配)
npm run verify     # 端到端验证：10 个一致性场景
```

## 架构与一致性模型

```
                build                gates              promote             switch
  source ──▶ staging/build-N ──▶ 6 项质量门禁 ──▶ snapshots/snap-N ──▶ CURRENT 指针原子替换
              (临时目录)           失败: 标记 failed,                (目录 rename 密封)
                                   当前版本不受影响
```

**读路径**：查询开始时 *pin* 当前快照（引用计数 +1，同步完成、无竞态），整个请求只读该快照目录下的
不可变文件；切换后旧快照在引用归零前不会被回收。每条记录带有版本号，响应前服务端强制校验
“所有行都属于所 pin 的快照”，违规直接 500，绝不返回混合版本。

**写路径**：

- `CURRENT` 指针文件通过 *写临时文件 + fsync + rename* 原子替换 —— 崩溃只会留下完整旧指针或完整新指针。
- 发布 / 回滚由互斥锁串行化；`expectedCurrentId` 提供乐观并发控制（过期期望 → 409）。
- 回滚 = 切换到保留窗口内（默认 10 个）的任一历史快照。

**恢复**：重启时读取 `CURRENT` 指针并校验快照完整性；指针丢失 / 损坏时回退到最新密封快照；
清理崩溃构建留下的 staging 残留。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/query?q=&limit=&slow=&snapshot=` | 查询。响应含 `snapshotId` 与 `X-Snapshot-Id` 头；`snapshot=` 可钉住版本用于重试（已回收 → 410） |
| POST | `/api/build` `{recordCount, injectBadData}` (`?wait=1` 等待完成） | 在 staging 中构建并跑门禁 |
| POST | `/api/publish` `{buildId?, expectedCurrentId?}` | 门禁通过才允许切换；409 = 未过门禁 / 期望过期 |
| POST | `/api/rollback` `{targetId?}` | 回滚到指定或上一个快照 |
| GET | `/api/status` | 当前版本、构建、门禁、事件、指标全量状态 |
| POST | `/api/admin/fault` | 故障注入：`failNextPublishBeforeRename` / `crashAfterRenameOnNextPublish` |

**重试约定**：单次请求永不混合版本；跨请求重试默认落到最新版本。需要跨重试一致时，客户端用首个
响应的 `X-Snapshot-Id` 钉住后续重试。

## 质量门禁

`row-count`（行数下限）、`parse-integrity`（JSON 可解析）、`schema-completeness`（必填字段）、
`null-ratio`（空值率 < 1%）、`uniqueness`（主键唯一）、`version-consistency`（无陈旧行）。
任一失败 → 构建标记 `failed`，当前版本不受影响，发布请求返回 409。

## 仪表盘（`http://localhost:8080`）

- 当前版本 hero 数字、记录数 / 活跃读者 / 查询量 / 切换 / 回滚 / 失败构建等磁贴
- 构建进度条、六项门禁实时结果、版本表（状态 / 读者数 / 行数 / 查询数）、事件时间线
- 查询测试器（客户端二次校验单版本一致性）、20 并发 burst、“长查询跨切换” 一键演示
- 故障注入按钮：下一次发布失败、切换中崩溃（重启后观察恢复）

## 验证场景（`npm run verify`）

| # | 场景 | 验证点 |
|---|---|---|
| S1 | 构建→门禁→发布→查询 | 全链路可用，响应单版本 |
| S2 | 脏数据构建 | 门禁拦截，当前版本不变 |
| S3 | **长查询**跨切换 | 1s 扫描全程读旧快照，新查询用新版本 |
| S4 | 切换中 30 并发查询 + 钉版本重试 | 每个响应单版本；重试落在同一快照 |
| S5 | **并行发布** | 互斥串行化，过期期望 409，最终状态一致 |
| S6 | 注入发布失败 | 旧版本继续服务，故障清除后重试成功 |
| S7 | 切换中崩溃（rename 后 SIGKILL） | **恢复**收敛到新版本 |
| S8 | 优雅重启 | 恢复同一当前版本 |
| S9 | 回滚 | 切回上一快照，事件可追溯 |
| S10 | GC 与引用计数 | 长查询钉住的快照不被回收；滚出保留窗口 → 410 |

## 目录布局

```
data/
  CURRENT                 # 当前指针（tmp+rename 原子替换）
  snapshots/snap-N/       # 密封快照：manifest.json + records.jsonl（不可变）
  staging/build-N/        # 构建中的临时版本（门禁失败也保留供排查）
  events.jsonl            # 事件归档
src/
  snapshot-manager.js     # 核心：构建/门禁/切换/回滚/GC/恢复
  gates.js  source.js  server.js
public/                   # 状态仪表盘
scripts/verify.mjs        # 端到端验证
```
