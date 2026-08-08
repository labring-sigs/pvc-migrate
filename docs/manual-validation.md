# Manual Validation Matrix

Date: 2026-08-07

Cluster: isolated Kubernetes `v1.28.15` validation environment

The manual run used only these isolated namespaces:

- application test namespace
- migration-system test namespace
- online-backup test namespace

The system namespace contained a temporary MinIO endpoint and isolated bucket for
backup tests. The storage fixtures used `openebs-hostpath` and `openebs-backup`.

## Scenario Results

| Area | Steps and evidence | Result |
| --- | --- | --- |
| WFFC | Compared a PVC consumer with `spec.nodeName` against one using `nodeSelector`. `spec.nodeName` bypassed scheduler provisioning and stayed Pending. `nodeSelector` selected the node and provisioned the WFFC claim. | Passed |
| Standalone staged online migration | Planned a two-PVC Pod, server-side dry-run reserve, real reserve, repeated idempotent reserve, two warm-copy passes, final sync, repeated final sync, activate, repeated activate, `session resume`, source-to-target worker move, SHA-256/marker checks, rollback to the source worker, second checks, finalize and session deletion. | Passed |
| Cross-namespace copy | Copied `copy-source` to `copy-destination`, verified destination SHA-256, aborted the copy session, removed temporary PVC and rollback PV, finalized source metadata and deleted the session. | Passed |
| Offline migration | Migrated `offline-source` between two workers and StorageClasses, verified SHA-256, rolled back to the original PV and worker, verified again, finalized and deleted the session. | Passed |
| Cross-namespace move | Moved `move-source` into the system namespace while retaining its test PV; verified data, rolled back namespace/name, verified again, finalized and deleted the session. | Passed |
| Online S3 backup and restore | Kept `source-writer` Running with `source-data` mounted on the source worker. Ran `live-backup --dry-run=false` against an isolated recovery-point prefix; the rclone Job completed while the source Pod remained Ready. A second invocation with the published name returned the expected immutable-recovery-point conflict. Restored the published prefix into `restore-data`. Marker SHA-256 `b4d92e106c62eb0df26c88b0afa150ce469ada7ca1a22af9d83e548d8b731ace`, nested value SHA-256 `650bc2c497f6a623d118a1e347d9f2826bf20ee157ebef1045d68d0a6d241318`, and live log SHA-256 `c1237c07d3a39d181df3160d5768c878cb7cd07dc13fabf0c6f0c2bbb2331e0f` were readable after restore. | Passed |
| Backup and restore | Ran dry-run for both commands. The object listing contained `marker.txt`, `nested.txt`, `nested/value.txt`, `payload.bin` (4 MiB), and `payload.sha256`. Source and restored SHA-256 values matched: marker `0d6ef57d66a2d1ffd33d79d9e04f1863b55f5a85534890ed2c45f571987932d0`, payload `bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8`, nested `1d3bdbc3a2ac2f030b8d839a587e41521a43f34f953d538af374675aedf4d78e`. | Passed |
| StatefulSet ordinal | Migrated ordinal 1 between two workers. Plan recorded affected ordinals 1 and 2 while migrating only the selected claim. Verified destination SHA-256 and worker, rolled back with a new Pod UID, verified again, finalized and deleted rollback PV/session. | Passed |
| KubeBlocks secondary | Initial run exposed that `HorizontalScaling` offline deletes the instance PVC/PV and makes final sync impossible. The adapter now sets component-level `componentSpecs[].stop` values, with a plan warning that the whole component shares downtime. Re-ran the secondary migration between two workers: warm copy, component stop, final sync, activation, component resume, Ready verification, rollback with distinct recovery phases, source PVC/PV restoration, and cleanup. | Passed after fix |
| KubeBlocks primary | Plan without a candidate failed with exit 3. Plan with an explicit secondary candidate passed and warned about role and component-wide downtime. Switchover, component stop, final sync, activation, component resume, Ready verification, role checks, rollback, and cleanup all passed. | Passed after fix |
| Standalone full composition | Recreated a standalone Pod with two existing PVCs, fixed a DNS-name boundary found by plan, then ran full `migrate-pod` with two PVCs. Both volumes reported `checksumVerified=true`; Pod moved to the target worker, rollback returned to the source worker and original PVs, live logs and payload hashes remained intact, cleanup passed. | Passed after fix |
| Session command surface | Listed sessions with `session status`. Dry-run cleanup produced no mutations. Deleting an active/failed session without required finalization flags returned exit 3. A failed KubeBlocks session resumed through its persisted abort phase and was cleaned safely after the source controller recreated its PVC. | Passed |
| Tagged E2E | `PVC_MIGRATE_E2E=1` with this binary and kubeconfig, source `openebs-hostpath`, destination `openebs-backup`; standalone WFFC migration, digest verification, rollback, and teardown passed in 282.47 seconds. | Passed |

## Latest Isolated-Cluster Validation

The final validation used an isolated Kubernetes `v1.28.15` environment.

| Scenario | Evidence | Result |
| --- | --- | --- |
| Standalone WFFC migration and rollback | `TestStandaloneWFFCMigrationAndRollback` passed in 306.69s. The generated namespace, session resources, helper Pods, and labeled PVs were removed after the test. | Passed |
| Offline S3 backup | Used an isolated S3-compatible Tenant endpoint and recovery-point prefix; manifest v2 contained 4 objects and 262450 bytes. | Passed |
| S3 restore integrity | Marker SHA-256 `0669edbd2edb79ba63f2054f309b3a15d25afd11cfbbb1a0363db68c09db15c3`, nested SHA-256 `6128253afd1f93ca3d606f541d3fe2a9d589fc7fe2c8dec75b50df72ea58de7c`, payload SHA-256 `f25bb2b76f220bdeced8e61c8eebf156a9971263aadeac2aaf64c0c2de2b4601`; restored hashes matched the source. | Passed |
| Online S3 backup | The source writer remained `Running` and `Ready` on the source worker, while `live.log` advanced from 56 to 58 lines during post-backup verification. The published manifest used the best-effort crash-consistent boundary and contained 5 objects. | Passed |
| Pinned-helper S3 exercise | With helper tag `v3.6.1`, online backup, restore, and offline backup all completed against the official Tenant. The source writer stayed `Running/true`; restored marker, payload, and live-log hashes were `3e8a08b6613d0453dc2660b69816094a303afa30fefeab49bbfdafa7f2c975fd`, `5647f05ec18958947d32874eeb788fa396a05d0bab7c1b71f112ceb7e9b31eee`, and `1dd89774be579906903640754e016739e795de458002b14bcff73acd6363f731`. The offline manifest was v2 with 4 objects and 2097798 bytes. | Passed |
| S3 helper cleanup | Helm Job, Pod, Secret, and ServiceAccount were removed after each backup/restore. The test namespace, its Delete-reclaim PVs, both completion prefixes, the temporary validation bucket, and the port-forward were removed and verified absent. | Passed |

## Defects Found And Fixed

1. KubeBlocks instance-level HorizontalScaling offline removed the source PVC before final sync. The adapter now sets component-level `componentSpecs[].stop` values and emits a component-wide downtime warning.
2. Completed KubeBlocks rollback reused an earlier pause operation and did not re-quiesce the workload. Operation names now include the current recovery phase; primary switchover runs only during the initial Pausing phase.
3. Aborted sessions whose controller recreated the source PVC/PV could not be finalized because recorded UIDs were stale. Cleanup skips missing pre-activation source resources while retaining identity checks for active resources.
4. Truncating a session ID at a hyphen produced an invalid destination PVC name. Name generation now trims DNS boundaries and has a regression test.
5. A manual backup fixture initially contained a literal `n` due to an unquoted shell `printf` format. The source fixture was rewritten with a quoted format and the backup/restore path was rerun with byte-level validation.
6. The online backup dry-run output now exposes the S3 object prefix, online consistency boundary, and per-file compression policy while keeping credentials out of the result.

## Cleanup Evidence

Before namespace teardown, scoped queries for the `pvc-migrate.io/session` label returned no ConfigMaps, PVs, Pods, Jobs, Services, Secrets, or ConfigMaps. Every completed session had been finalized or deleted. The online-backup namespace and its test-only workloads were then deleted. The isolated S3 recovery-point prefix and its metadata sidecar were deleted after restore verification; a final object listing returned zero entries. Cluster workloads and bucket prefixes outside these test scopes were left untouched.
