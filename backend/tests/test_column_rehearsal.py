"""列切换批量演练引擎的场景测试。

覆盖题目场景：
- 事实表、日期表、地区字典一次提交为候选批次
- 旧抽取作业同时读取事实表小计列与日期列（日期列类型未校验）
- 独立维表（地区字典）只服务地区字典作业
- 共享消费方分组：事实表组必须成套验证并被阻断，地区字典组可先行确认
- 中断重试沿用原列版本/数据源，版本漂移被拒绝
- 字段映射先保留旧日期列引用，再加入新日期列，并带候选类型
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine.column_rehearsal import (  # noqa: E402
    ColumnCandidate,
    ColumnRehearsalEngine,
    MappingPhase,
    ReadRelation,
    RehearsalStatus,
)


def build_candidates() -> list[ColumnCandidate]:
    return [
        # 事实表：小计列已完成类型校验
        ColumnCandidate(
            table_name="dw.fact_sales",
            column_name="subtotal_amt",
            candidate_type="decimal(18,2)",
            type_validated=True,
            column_version="v3",
            data_source="src_fact_db",
        ),
        # 事实表日期列：新类型 date 尚未完成类型校验，双轨期保留旧字符串列
        ColumnCandidate(
            table_name="dw.fact_sales",
            column_name="biz_date",
            candidate_type="date",
            type_validated=False,
            column_version="v2",
            data_source="src_fact_db",
            legacy_column="biz_date_str",
            legacy_type="string",
        ),
        # 日期表新日期列（与事实表日期列被同一抽取作业读取）
        ColumnCandidate(
            table_name="dim.dim_date",
            column_name="calendar_date",
            candidate_type="date",
            type_validated=False,
            column_version="v5",
            data_source="src_date_db",
        ),
        # 独立维表：地区字典，只服务字典作业，类型已校验
        ColumnCandidate(
            table_name="dim.dim_region",
            column_name="region_name",
            candidate_type="string",
            type_validated=True,
            column_version="v1",
            data_source="src_region_db",
        ),
    ]


def build_relations() -> list[ReadRelation]:
    return [
        # 旧抽取作业同时读取事实表小计列、事实表日期列、日期表日期列
        ReadRelation("job_extract_legacy", "dw.fact_sales", "subtotal_amt", "src_fact_db"),
        ReadRelation("job_extract_legacy", "dw.fact_sales", "biz_date", "src_fact_db"),
        ReadRelation("job_extract_legacy", "dim.dim_date", "calendar_date", "src_date_db"),
        # 独立字典作业只读取地区字典
        ReadRelation("job_region_dict", "dim.dim_region", "region_name", "src_region_db"),
    ]


class ColumnRehearsalEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = ColumnRehearsalEngine()
        self.plan = self.engine.build_rehearsal_plan(
            "batch_20261121", build_candidates(), build_relations(),
        )

    def test_groups_split_by_shared_consumer(self) -> None:
        """同一消费方读取的候选列必须在同一组；独立维表单独成组。"""
        groups = self.plan["groups"]
        self.assertEqual(2, len(groups))

        fact_group = next(g for g in groups if "dw.fact_sales" in g.tables)
        region_group = next(g for g in groups if "dim.dim_region" in g.tables)

        # 事实表 + 日期表被旧抽取作业绑在一起，三列同组
        self.assertEqual(
            sorted(fact_group.tables), ["dim.dim_date", "dw.fact_sales"],
        )
        self.assertEqual(3, len(fact_group.candidate_keys))
        self.assertEqual(["job_extract_legacy"], fact_group.consumer_jobs)
        self.assertTrue(fact_group.must_validate_as_whole)

        # 地区字典独立成组，可单独走完
        self.assertEqual(["dim.dim_region"], region_group.tables)
        self.assertEqual(["job_region_dict"], region_group.consumer_jobs)
        self.assertFalse(region_group.must_validate_as_whole)

    def test_rehearsal_data_only_contains_actually_read_columns(self) -> None:
        """分组演练数据按作业实际读取的候选列生成。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        self.assertEqual(1, len(fact_group.rehearsal_data))
        record = fact_group.rehearsal_data[0]
        self.assertEqual("job_extract_legacy", record["consumer_job"])
        self.assertEqual(3, record["read_column_count"])
        read = {(c["table"], c["column"]) for c in record["read_columns"]}
        self.assertEqual(set(fact_group.candidate_keys), read)
        # 演练数据携带建组锁定的列版本与数据源
        biz_date = next(c for c in record["read_columns"] if c["column"] == "biz_date")
        self.assertEqual("v2", biz_date["column_version"])
        self.assertEqual("src_fact_db", biz_date["data_source"])

    def test_fact_group_blocked_until_date_type_validated(self) -> None:
        """日期列未完成类型校验时，共享消费方整组阻断且必须成套验证。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        result = self.engine.run_group(self.plan, fact_group.group_id)

        self.assertFalse(result["passed"])
        self.assertTrue(result["must_validate_as_whole"])
        self.assertEqual(RehearsalStatus.BLOCKED.value, result["status"])
        blocked = {(c["table"], c["column"]) for c in result["blocked_columns"]}
        self.assertIn(("dw.fact_sales", "biz_date"), blocked)
        self.assertIn(("dim.dim_date", "calendar_date"), blocked)
        # 已校验的小计列不在阻断清单里
        self.assertNotIn(("dw.fact_sales", "subtotal_amt"), blocked)

    def test_independent_region_group_can_complete_first(self) -> None:
        """独立维表组不依赖事实表组，可以先走完并留下可追溯凭证。"""
        region_group = next(g for g in self.plan["groups"] if "dim.dim_region" in g.tables)
        result = self.engine.run_group(self.plan, region_group.group_id)

        self.assertTrue(result["passed"])
        self.assertEqual(RehearsalStatus.CONFIRMED.value, result["status"])

        trace = self.engine.trace_confirmed_group(
            self.plan, region_group.group_id,
        )
        self.assertTrue(trace["traceable"])
        self.assertEqual("batch_20261121", trace["batch_id"])
        self.assertEqual(["dim.dim_region"], trace["tables"])
        self.assertEqual(
            [{"table": "dim.dim_region", "column": "region_name",
              "column_version": "v1"}],
            trace["column_versions"],
        )

    def test_retry_keeps_locked_version_and_data_source(self) -> None:
        """中断重试沿用原列版本与数据源；补完校验后整组通过。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        self.engine.run_group(self.plan, fact_group.group_id)  # 先阻断
        interrupted = self.engine.interrupt_group(
            self.plan, fact_group.group_id, "试跑集群临时不可用",
        )
        self.assertTrue(interrupted["retriable"])
        self.assertEqual(RehearsalStatus.INTERRUPTED.value, fact_group.status)

        # 日期列补完类型校验，但列版本/数据源保持不变
        refreshed = [
            ColumnCandidate(
                table_name=c.table_name, column_name=c.column_name,
                candidate_type=c.candidate_type,
                type_validated=True,  # 校验完成
                column_version=c.column_version,
                data_source=c.data_source,
                legacy_column=c.legacy_column, legacy_type=c.legacy_type,
            )
            for c in build_candidates()
        ]
        retry = self.engine.retry_group(
            self.plan, fact_group.group_id, refreshed, build_relations(),
        )
        self.assertTrue(retry["retried_with_locked_snapshot"])
        self.assertTrue(retry["passed"])
        self.assertEqual(RehearsalStatus.CONFIRMED.value, fact_group.status)
        # 锁定快照未变
        self.assertEqual("v2", fact_group.version_locks[("dw.fact_sales", "biz_date")])
        self.assertEqual(
            "src_fact_db",
            fact_group.data_source_locks[("dw.fact_sales", "biz_date")],
        )

    def test_retry_rejects_version_drift(self) -> None:
        """重试时若列版本被换掉，必须拒绝而不是悄悄换列试跑。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        self.engine.interrupt_group(
            self.plan, fact_group.group_id, "模拟中断",
        )
        drifted = []
        for c in build_candidates():
            if c.key() == ("dw.fact_sales", "biz_date"):
                drifted.append(ColumnCandidate(
                    table_name=c.table_name, column_name=c.column_name,
                    candidate_type=c.candidate_type, type_validated=True,
                    column_version="v9",  # 版本漂移
                    data_source=c.data_source,
                ))
            else:
                drifted.append(c)
        retry = self.engine.retry_group(
            self.plan, fact_group.group_id, drifted, build_relations(),
        )
        self.assertFalse(retry["retriable"])
        self.assertEqual(RehearsalStatus.FAILED.value, fact_group.status)
        self.assertTrue(any("版本漂移" in item["reason"] for item in retry["drift"]))

    def test_retry_rejects_data_source_drift(self) -> None:
        """重试时作业读取的数据源漂移同样拒绝。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        self.engine.interrupt_group(self.plan, fact_group.group_id, "模拟中断")
        # 数据源以作业实际读取关系为准：日期表改从另一个源读取
        drifted_relations = [
            ReadRelation(
                consumer_job=r.consumer_job,
                table_name=r.table_name,
                column_name=r.column_name,
                data_source=(
                    "src_other_db"
                    if r.key() == ("dim.dim_date", "calendar_date")
                    else r.data_source
                ),
            )
            for r in build_relations()
        ]
        retry = self.engine.retry_group(
            self.plan, fact_group.group_id, build_candidates(), drifted_relations,
        )
        self.assertFalse(retry["retriable"])
        self.assertTrue(any("数据源漂移" in item["reason"] for item in retry["drift"]))

    def test_field_mappings_keep_legacy_then_add_candidate(self) -> None:
        """字段映射先保留旧日期列引用，再加入新日期列，且标明上游列与候选类型。"""
        fact_group = next(g for g in self.plan["groups"] if "dw.fact_sales" in g.tables)
        mappings = fact_group.field_mappings

        legacy = [
            m for m in mappings
            if m.phase == MappingPhase.LEGACY_RETAINED.value
            and m.upstream_column == "biz_date_str"
        ]
        self.assertEqual(1, len(legacy))
        self.assertEqual("string", legacy[0].candidate_type)
        self.assertEqual("job_extract_legacy", legacy[0].consumer_job)

        new_date = [
            m for m in mappings
            if m.phase == MappingPhase.CANDIDATE_ADDED.value
            and m.upstream_column == "biz_date"
        ]
        self.assertEqual(1, len(new_date))
        self.assertEqual("date", new_date[0].candidate_type)
        self.assertFalse(new_date[0].type_validated)
        self.assertEqual(fact_group.group_id, new_date[0].rehearsal_group_id)

        # 顺序约束：旧列保留映射排在同列新列加入映射之前
        legacy_idx = mappings.index(legacy[0])
        new_idx = mappings.index(new_date[0])
        self.assertLess(legacy_idx, new_idx)

        # 每条映射都必须指出所需上游列和候选类型
        for mapping in mappings:
            self.assertTrue(mapping.upstream_column)
            self.assertTrue(mapping.candidate_type)

    def test_summary_distinguishes_whole_set_and_independent_groups(self) -> None:
        """计划汇总能辨别必须成套验证的组与可以先走的独立组。"""
        summary = self.engine.plan_summary(self.plan)
        self.assertEqual(1, len(summary["must_validate_as_whole"]))
        self.assertEqual(1, len(summary["independent_groups"]))
        whole = self.plan["groups"][0] if self.plan["groups"][0].must_validate_as_whole \
            else self.plan["groups"][1]
        self.assertIn(whole.group_id, summary["must_validate_as_whole"])

    def test_unread_candidate_is_flagged_not_grouped(self) -> None:
        """没有被任何作业读取的候选列不进演练组，但要显式标注。"""
        candidates = build_candidates() + [
            ColumnCandidate(
                table_name="dw.fact_sales",
                column_name="unused_flag",
                candidate_type="boolean",
                type_validated=True,
            )
        ]
        plan = self.engine.build_rehearsal_plan(
            "batch_with_unread", candidates, build_relations(),
        )
        self.assertEqual(
            [{"table": "dw.fact_sales", "column": "unused_flag"}],
            plan["unread_candidates"],
        )
        grouped = {key for g in plan["groups"] for key in g.candidate_keys}
        self.assertNotIn(("dw.fact_sales", "unused_flag"), grouped)


if __name__ == "__main__":
    unittest.main(verbosity=2)
