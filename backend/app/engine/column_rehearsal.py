"""列切换批量演练引擎：跨表候选列的分组试跑与字段映射生成。

事实表 / 维表做列切换（典型如日期列改类型、小计列改名）时，逐表演练会漏掉
跨表交叉引用：同一个抽取作业往往同时读取多张表的候选列，这些列必须放进同一次
演练成套验证，否则试跑读到的列组合和线上真实读取关系不一致。

引擎职责：
- 接收跨表多选提交的候选批次（同一批可含多张表）
- 按作业实际读取关系（共享消费方）对候选列做连通分量分组
- 为每个表组生成只读该组候选列的分组演练数据
- 配置迁移依据读取关系生成试跑字段映射，每条映射指出所需上游列和候选类型
- 执行/中断重试：重试沿用该组首次演练时锁定的列版本与数据源快照
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RehearsalStatus(str, Enum):
    """表组演练状态。"""

    PENDING = "待演练"          # 已建组，尚未试跑
    BLOCKED = "待校验阻断"      # 组内存在未完成类型校验的候选列，不允许成套通过
    RUNNING = "演练中"          # 试跑进行中（中断后回到本态由重试接续）
    INTERRUPTED = "已中断"      # 试跑中断，等待按原版本重试
    CONFIRMED = "已确认"        # 试跑通过，结果可被配置迁移追溯
    FAILED = "演练失败"


class MappingPhase(str, Enum):
    """字段映射在双轨切换中的阶段。"""

    LEGACY_RETAINED = "旧列保留"   # 切换前先保留的旧列引用
    CANDIDATE_ADDED = "新列加入"   # 随后加入映射的候选列


@dataclass(frozen=True)
class ColumnCandidate:
    """一张表上的一个候选切换列。

    - table_name / column_name 定位候选列
    - candidate_type 是切换后的目标类型；type_validated 表示该类型是否已完成校验
    - column_version 是候选列版本，分组建组时锁定，重试必须沿用
    - data_source 是该列读取的数据源，同样在建组时锁定
    """

    table_name: str
    column_name: str
    candidate_type: str
    type_validated: bool = False
    column_version: str = "v1"
    data_source: str = ""
    legacy_column: str = ""          # 双轨期需要保留引用的旧列（可空）
    legacy_type: str = ""

    def key(self) -> tuple[str, str]:
        return (self.table_name, self.column_name)


@dataclass(frozen=True)
class ReadRelation:
    """一条作业读取关系：某个消费方作业实际读取了某张表的某个候选列。

    分组演练数据只按作业实际读取的候选列生成，未被任何作业读取的候选列
    不会进入任何演练组。
    """

    consumer_job: str
    table_name: str
    column_name: str
    data_source: str = ""

    def key(self) -> tuple[str, str]:
        return (self.table_name, self.column_name)


@dataclass
class FieldMapping:
    """配置迁移为试跑生成的一条字段映射。

    每条读取映射都指出所需的上游列（upstream_column）和候选类型（candidate_type）。
    双轨切换时先保留旧列引用（LEGACY_RETAINED），再把新候选列加入映射
    （CANDIDATE_ADDED）。
    """

    mapping_id: str
    consumer_job: str
    table_name: str
    upstream_column: str
    candidate_type: str
    phase: str
    type_validated: bool
    column_version: str
    data_source: str
    rehearsal_group_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "mapping_id": self.mapping_id,
            "consumer_job": self.consumer_job,
            "table": self.table_name,
            "upstream_column": self.upstream_column,
            "candidate_type": self.candidate_type,
            "phase": self.phase,
            "type_validated": self.type_validated,
            "column_version": self.column_version,
            "data_source": self.data_source,
            "rehearsal_group_id": self.rehearsal_group_id,
        }


@dataclass
class RehearsalGroup:
    """一个必须成套验证的表组（共享消费方连通分量）。"""

    group_id: str
    consumer_jobs: list[str]
    candidate_keys: list[tuple[str, str]]
    tables: list[str]
    # 建组时锁定的列版本与数据源，中断重试必须原样沿用
    version_locks: dict[tuple[str, str], str] = field(default_factory=dict)
    data_source_locks: dict[tuple[str, str], str] = field(default_factory=dict)
    status: str = RehearsalStatus.PENDING.value
    rehearsal_data: list[dict[str, Any]] = field(default_factory=list)
    field_mappings: list[FieldMapping] = field(default_factory=list)
    # 已确认后登记的演练凭证，配置迁移凭它追溯独立表组的试跑结果
    confirmation: dict[str, Any] | None = None
    last_run_at: str = ""
    interrupt_reason: str = ""

    @property
    def must_validate_as_whole(self) -> bool:
        """跨了多张表且由共享消费方连通的组必须整组成套验证。"""
        return len(self.tables) > 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "consumer_jobs": list(self.consumer_jobs),
            "tables": list(self.tables),
            "candidate_columns": [
                {"table": table, "column": column}
                for table, column in self.candidate_keys
            ],
            "candidate_count": len(self.candidate_keys),
            "table_count": len(self.tables),
            "status": self.status,
            "version_locks": [
                {"table": table, "column": column, "column_version": version}
                for (table, column), version in self.version_locks.items()
            ],
            "data_source_locks": [
                {"table": table, "column": column, "data_source": source}
                for (table, column), source in self.data_source_locks.items()
            ],
            "rehearsal_data": list(self.rehearsal_data),
            "field_mappings": [m.to_dict() for m in self.field_mappings],
            "confirmation": self.confirmation,
            "last_run_at": self.last_run_at,
            "interrupt_reason": self.interrupt_reason,
        }


class ColumnRehearsalEngine:
    """列切换批量演练引擎。

    用法:
        eng = ColumnRehearsalEngine()
        plan = eng.build_rehearsal_plan(
            batch_id="batch_20261121",
            candidates=[事实表小计列, 事实表日期列, 日期表新日期列, 地区字典列],
            read_relations=[旧抽取作业读取小计/日期, 字典作业读取地区列],
        )
        result = eng.run_group(plan, "group_1")
        mappings = eng.build_field_mappings(plan)
    """

    # ---- 分组：共享消费方的候选列必须同一次演练 ----

    def build_rehearsal_plan(
        self,
        batch_id: str,
        candidates: list[ColumnCandidate],
        read_relations: list[ReadRelation],
    ) -> dict[str, Any]:
        """把一批跨表候选列按共享消费方拆成成套演练组，并生成演练数据。

        分组规则（并查集）：
        - 只纳入被作业实际读取的候选列；
        - 同一消费方作业读取到的所有候选列并入同一连通分量；
        - 同一张表的候选列天然同组（一张表不能拆到两次演练里）；
        - 没有共享消费方的独立维表各自成组，可独立先行试跑。
        """
        candidate_index = {c.key(): c for c in candidates}
        read_by_key: dict[tuple[str, str], list[ReadRelation]] = {}
        for rel in read_relations:
            if rel.key() in candidate_index:
                read_by_key.setdefault(rel.key(), []).append(rel)

        # 未被任何作业读取的候选列不生成演练数据，但要在计划里显式标注
        read_keys = set(read_by_key)
        unread = [c.key() for c in candidates if c.key() not in read_keys]

        parent: dict[tuple[str, str], tuple[str, str]] = {
            key: key for key in read_keys
        }

        def find(x: tuple[str, str]) -> tuple[str, str]:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: tuple[str, str], b: tuple[str, str]) -> None:
            root_a, root_b = find(a), find(b)
            if root_a != root_b:
                parent[root_b] = root_a

        # 规则一：同一消费方读取的候选列全部连通
        consumers: dict[str, list[tuple[str, str]]] = {}
        for rel in read_relations:
            if rel.key() in candidate_index:
                consumers.setdefault(rel.consumer_job, []).append(rel.key())
        for keys in consumers.values():
            for other in keys[1:]:
                union(keys[0], other)

        # 规则二：同表候选列连通（一张表只属于一个演练组）
        by_table: dict[str, list[tuple[str, str]]] = {}
        for key in read_keys:
            by_table.setdefault(key[0], []).append(key)
        for keys in by_table.values():
            for other in keys[1:]:
                union(keys[0], other)

        components: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for key in read_keys:
            components.setdefault(find(key), []).append(key)

        groups: list[RehearsalGroup] = []
        for idx, (_, keys) in enumerate(sorted(
            components.items(),
            key=lambda item: sorted(item[1])[0],
        ), start=1):
            group_id = f"{batch_id}_group_{idx}"
            jobs = sorted({
                rel.consumer_job
                for key in keys for rel in read_by_key.get(key, [])
            })
            tables = sorted({key[0] for key in keys})
            version_locks = {
                key: candidate_index[key].column_version for key in keys
            }
            data_source_locks = {
                key: self._lock_data_source(candidate_index[key], read_by_key.get(key, []))
                for key in keys
            }
            group = RehearsalGroup(
                group_id=group_id,
                consumer_jobs=jobs,
                candidate_keys=sorted(keys),
                tables=tables,
                version_locks=version_locks,
                data_source_locks=data_source_locks,
            )
            group.rehearsal_data = self._build_rehearsal_data(
                group, candidate_index, read_by_key,
            )
            group.field_mappings = self._mappings_for_group(
                group, candidate_index, read_by_key,
            )
            groups.append(group)

        return {
            "batch_id": batch_id,
            "groups": groups,
            # 候选列快照随计划保留：重试时用它读取最新类型校验态，
            # 而列版本/数据源锁仍由各组持有，两套机制互不覆盖
            "candidate_index": dict(candidate_index),
            "read_relations": list(read_relations),
            "unread_candidates": [
                {"table": table, "column": column} for table, column in unread
            ],
            "submitted_tables": sorted({c.table_name for c in candidates}),
            "submitted_candidate_count": len(candidates),
        }

    # ---- 试跑执行 ----

    def run_group(
        self,
        plan: dict[str, Any],
        group_id: str,
        *,
        run_at: str = "",
    ) -> dict[str, Any]:
        """执行一个表组的成套试跑。

        阻断规则：组内任一候选列未完成类型校验，则整组停在「待校验阻断」，
        跨表共享消费方的组必须等所有列校验完成后成套重跑。
        全部校验通过则登记确认凭证，供配置迁移追溯。
        """
        group = self._require_group(plan, group_id)
        group.last_run_at = run_at or group.last_run_at
        group.status = RehearsalStatus.RUNNING.value

        candidate_index: dict[tuple[str, str], ColumnCandidate] = plan["candidate_index"]
        pending = [
            key for key in group.candidate_keys
            if not candidate_index[key].type_validated
        ]
        if pending:
            group.status = RehearsalStatus.BLOCKED.value
            group.interrupt_reason = ""
            return {
                "group_id": group_id,
                "status": group.status,
                "passed": False,
                "blocked_columns": [
                    {"table": table, "column": column} for table, column in pending
                ],
                "must_validate_as_whole": group.must_validate_as_whole,
                "message": (
                    "组内存在未完成类型校验的候选列，共享消费方要求成套验证，"
                    "整组阻断，待类型校验完成后按原列版本重试"
                ),
            }

        group.status = RehearsalStatus.CONFIRMED.value
        group.interrupt_reason = ""
        group.confirmation = {
            "rehearsal_id": f"{group_id}_rehearsal",
            "group_id": group_id,
            "batch_id": plan["batch_id"],
            "consumer_jobs": list(group.consumer_jobs),
            "tables": list(group.tables),
            "column_versions": [
                {"table": table, "column": column, "column_version": version}
                for (table, column), version in sorted(group.version_locks.items())
            ],
            "data_sources": [
                {"table": table, "column": column, "data_source": source}
                for (table, column), source in sorted(group.data_source_locks.items())
            ],
            "mapping_count": len(group.field_mappings),
            "confirmed_at": run_at or group.last_run_at,
        }
        return {
            "group_id": group_id,
            "status": group.status,
            "passed": True,
            "rehearsal_id": group.confirmation["rehearsal_id"],
            "must_validate_as_whole": group.must_validate_as_whole,
            "message": "表组试跑通过，演练结果已登记可被配置迁移追溯",
        }

    def interrupt_group(
        self,
        plan: dict[str, Any],
        group_id: str,
        reason: str,
        *,
        run_at: str = "",
    ) -> dict[str, Any]:
        """把一个演练中的表组标记为中断（保留现场，等待重试）。"""
        group = self._require_group(plan, group_id)
        if group.status == RehearsalStatus.CONFIRMED.value:
            return {
                "group_id": group_id,
                "status": group.status,
                "retriable": False,
                "message": "表组已确认，无需中断重试",
            }
        group.status = RehearsalStatus.INTERRUPTED.value
        group.interrupt_reason = reason
        group.last_run_at = run_at or group.last_run_at
        return {
            "group_id": group_id,
            "status": group.status,
            "retriable": True,
            "message": "演练已中断，重试将沿用该组锁定的列版本与数据源",
        }

    def retry_group(
        self,
        plan: dict[str, Any],
        group_id: str,
        candidates: list[ColumnCandidate],
        read_relations: list[ReadRelation],
        *,
        run_at: str = "",
    ) -> dict[str, Any]:
        """中断后重试：强制沿用该组原来的列版本和相关数据源。

        重试入参里的候选列若版本或数据源与建组锁不一致，直接拒绝，
        避免重试悄悄换到另一套列；演练数据与字段映射也按锁定快照重建。
        """
        group = self._require_group(plan, group_id)
        if group.status != RehearsalStatus.INTERRUPTED.value:
            return {
                "group_id": group_id,
                "status": group.status,
                "retriable": False,
                "message": f"表组当前状态为{group.status}，仅「已中断」可重试",
            }

        candidate_index = {c.key(): c for c in candidates}
        # 同步计划内候选列快照：版本/数据源仍以组锁为准，类型校验态读取最新值
        plan["candidate_index"].update(candidate_index)
        drift: list[dict[str, str]] = []
        for key, locked_version in group.version_locks.items():
            current = candidate_index.get(key)
            if current is None:
                drift.append({"table": key[0], "column": key[1],
                              "reason": "候选列已从批次中移除"})
                continue
            if current.column_version != locked_version:
                drift.append({
                    "table": key[0], "column": key[1],
                    "reason": f"列版本漂移: 锁定{locked_version} / 传入{current.column_version}",
                })
            locked_source = group.data_source_locks.get(key, "")
            current_source = self._lock_data_source(
                current,
                [r for r in read_relations if r.key() == key],
            )
            if current_source != locked_source:
                drift.append({
                    "table": key[0], "column": key[1],
                    "reason": f"数据源漂移: 锁定{locked_source} / 传入{current_source}",
                })

        if drift:
            group.status = RehearsalStatus.FAILED.value
            return {
                "group_id": group_id,
                "status": group.status,
                "retriable": False,
                "drift": drift,
                "message": "重试拒绝：候选列版本或数据源与该组原演练不一致",
            }

        # 沿用原快照重建演练数据与映射，再走一次正常试跑
        read_by_key: dict[tuple[str, str], list[ReadRelation]] = {}
        for rel in read_relations:
            if rel.key() in group.version_locks:
                read_by_key.setdefault(rel.key(), []).append(rel)
        group.rehearsal_data = self._build_rehearsal_data(
            group, candidate_index, read_by_key,
        )
        group.field_mappings = self._mappings_for_group(
            group, candidate_index, read_by_key,
        )
        result = self.run_group(plan, group_id, run_at=run_at)
        result["retried_with_locked_snapshot"] = True
        return result

    # ---- 字段映射：配置迁移消费 ----

    def build_field_mappings(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """输出整批的试跑字段映射（按组分段）。"""
        mappings: list[dict[str, Any]] = []
        for group in plan["groups"]:
            mappings.extend(m.to_dict() for m in group.field_mappings)
        return mappings

    def trace_confirmed_group(
        self,
        plan: dict[str, Any],
        group_id: str,
    ) -> dict[str, Any]:
        """按组ID追溯已确认独立表组的演练凭证。"""
        group = self._require_group(plan, group_id)
        if group.confirmation is None:
            return {
                "group_id": group_id,
                "traceable": False,
                "status": group.status,
                "message": "该表组尚未确认，没有可追溯的演练结果",
            }
        return {"traceable": True, **group.confirmation}

    def plan_summary(self, plan: dict[str, Any]) -> dict[str, Any]:
        """汇总计划：辨别哪个组必须成套验证、哪个组可以先走。"""
        groups = plan["groups"]
        return {
            "batch_id": plan["batch_id"],
            "submitted_tables": plan["submitted_tables"],
            "group_count": len(groups),
            "must_validate_as_whole": [
                g.group_id for g in groups if g.must_validate_as_whole
            ],
            "independent_groups": [
                g.group_id for g in groups if not g.must_validate_as_whole
            ],
            "groups": [
                {
                    "group_id": g.group_id,
                    "status": g.status,
                    "tables": g.tables,
                    "consumer_jobs": g.consumer_jobs,
                    "must_validate_as_whole": g.must_validate_as_whole,
                }
                for g in groups
            ],
        }

    @staticmethod
    def serialize_plan(plan: dict[str, Any]) -> dict[str, Any]:
        """把演练计划转成可 JSON 序列化的纯字典（剔除候选列快照等内部对象）。"""
        return {
            "batch_id": plan["batch_id"],
            "submitted_tables": plan["submitted_tables"],
            "submitted_candidate_count": plan["submitted_candidate_count"],
            "unread_candidates": plan["unread_candidates"],
            "groups": [group.to_dict() for group in plan["groups"]],
        }

    # ---- 内部 ----

    @staticmethod
    def _lock_data_source(
        candidate: ColumnCandidate,
        relations: list[ReadRelation],
    ) -> str:
        """数据源以作业实际读取关系为准，候选列自带数据源作为兜底。"""
        sources = {r.data_source for r in relations if r.data_source}
        if len(sources) == 1:
            return next(iter(sources))
        if sources:
            return "|".join(sorted(sources))
        return candidate.data_source

    def _build_rehearsal_data(
        self,
        group: RehearsalGroup,
        candidate_index: dict[tuple[str, str], ColumnCandidate],
        read_by_key: dict[tuple[str, str], list[ReadRelation]],
    ) -> list[dict[str, Any]]:
        """按作业实际读取的候选列生成分组演练数据。

        每个消费方作业一条演练数据记录，只包含该作业在本组读取到的候选列，
        并带上建组锁定的列版本与数据源，保证重试读到同一份快照。
        """
        dataset: list[dict[str, Any]] = []
        for job in group.consumer_jobs:
            columns: list[dict[str, Any]] = []
            for key in group.candidate_keys:
                relations = read_by_key.get(key, [])
                if not any(r.consumer_job == job for r in relations):
                    continue
                candidate = candidate_index[key]
                columns.append({
                    "table": key[0],
                    "column": key[1],
                    "candidate_type": candidate.candidate_type,
                    "type_validated": candidate.type_validated,
                    "column_version": group.version_locks[key],
                    "data_source": group.data_source_locks[key],
                })
            dataset.append({
                "consumer_job": job,
                "read_columns": columns,
                "read_column_count": len(columns),
            })
        return dataset

    def _mappings_for_group(
        self,
        group: RehearsalGroup,
        candidate_index: dict[tuple[str, str], ColumnCandidate],
        read_by_key: dict[tuple[str, str], list[ReadRelation]],
    ) -> list[FieldMapping]:
        """为一个组生成双轨字段映射。

        顺序约定：先为声明了 legacy_column 的候选列保留旧列引用，
        再把新候选列加入映射，确保切换窗口内旧引用不断、新引用可试跑。
        """
        mappings: list[FieldMapping] = []
        seq = 0
        ordered = sorted(
            group.candidate_keys,
            key=lambda key: (0 if candidate_index[key].legacy_column else 1, key),
        )
        for key in ordered:
            candidate = candidate_index[key]
            jobs = sorted({
                rel.consumer_job for rel in read_by_key.get(key, [])
            }) or group.consumer_jobs
            for job in jobs:
                if candidate.legacy_column:
                    seq += 1
                    mappings.append(FieldMapping(
                        mapping_id=f"{group.group_id}_map_{seq}",
                        consumer_job=job,
                        table_name=key[0],
                        upstream_column=candidate.legacy_column,
                        candidate_type=candidate.legacy_type or candidate.candidate_type,
                        phase=MappingPhase.LEGACY_RETAINED.value,
                        type_validated=True,
                        column_version=group.version_locks[key],
                        data_source=group.data_source_locks[key],
                        rehearsal_group_id=group.group_id,
                    ))
                seq += 1
                mappings.append(FieldMapping(
                    mapping_id=f"{group.group_id}_map_{seq}",
                    consumer_job=job,
                    table_name=key[0],
                    upstream_column=key[1],
                    candidate_type=candidate.candidate_type,
                    phase=MappingPhase.CANDIDATE_ADDED.value,
                    type_validated=candidate.type_validated,
                    column_version=group.version_locks[key],
                    data_source=group.data_source_locks[key],
                    rehearsal_group_id=group.group_id,
                ))
        return mappings

    @staticmethod
    def _require_group(plan: dict[str, Any], group_id: str) -> RehearsalGroup:
        for group in plan["groups"]:
            if group.group_id == group_id:
                return group
        raise KeyError(f"演练组 {group_id} 不存在于批次 {plan.get('batch_id')}")
