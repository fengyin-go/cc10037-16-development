"""业务编排API：跨模块的ETL编排、质量门禁、回刷、Schema变更等操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.engine.alert_matcher import AlertLevel, AlertRule
from app.engine.backfill_plan import BackfillEngine
from app.engine.column_rehearsal import (
    ColumnCandidate, ColumnRehearsalEngine, ReadRelation,
)
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord, WatermarkStatus
from app.orchestrator import ETLOrchestrator

router = APIRouter(prefix="/api/business", tags=["业务编排"])

_orchestrator = ETLOrchestrator()
_rehearsal_engine = ColumnRehearsalEngine()
# 内存态演练批次登记表：batch_id -> 演练计划（含分组、版本锁、试跑凭证）
_rehearsal_plans: dict[str, dict[str, Any]] = {}


# ---- 管线编排 ----

@router.post("/pipelines/orchestrate")
def orchestrate_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    """为管线执行完整的DAG拓扑验证和血缘登记。

    body:
      pipeline_id, pipeline_name, source_table, target_table,
      stages: [{"name": "extract", "nodes": [{"id": "n1", "name": "读取"}]}, ...]
    """
    try:
        return _orchestrator.create_pipeline_with_full_orchestration(
            pipeline_id=body["pipeline_id"],
            pipeline_name=body["pipeline_name"],
            source_table=body["source_table"],
            target_table=body["target_table"],
            stages=body.get("stages", []),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {e}")


# ---- 质量门禁 ----

@router.post("/quality/gate")
def run_quality_gate(body: dict[str, Any]) -> dict[str, Any]:
    """对管线数据执行质量门禁。

    body:
      pipeline_id, rules: [{"rule_id", "rule_name", "target_table", "check_field",
                             "check_type", "threshold", "level", "extra": {}}],
      sample_data: [dict]
    """
    pipeline_id = body["pipeline_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
        )
        for r in body.get("rules", [])
    ]
    result = _orchestrator.run_quality_gate(
        pipeline_id, rules, body.get("sample_data", []),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message}
            for a in result.triggered_alerts
        ],
    }


# ---- 水位与回刷 ----

@router.post("/watermark/check")
def check_watermark(body: dict[str, Any]) -> dict[str, Any]:
    """检查水位并判断是否需要回刷。

    body:
      pipeline_id, partition_field, current_watermark, new_watermark,
      scheduled_time (ISO), backfill_threshold (可选)
    """
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=body["pipeline_id"],
        partition_field=body["partition_field"],
        current_watermark=body["current_watermark"],
        scheduled_time=datetime.fromisoformat(body["scheduled_time"]),
        status=WatermarkStatus(body.get("status", "正常")),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    return _orchestrator.check_watermark_and_backfill(
        record,
        body["new_watermark"],
        backfill_threshold=body.get("backfill_threshold", 3),
    )


# ---- 回刷计划 ----

@router.post("/backfill/plan")
def generate_backfill_plan(body: dict[str, Any]) -> dict[str, Any]:
    """生成回刷补偿方案。

    body:
      pipeline_id, start_partition, end_partition, parallelism, reason
    """
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id=body["pipeline_id"],
        start_partition=body["start_partition"],
        end_partition=body["end_partition"],
        parallelism=int(body.get("parallelism", 4)),
        reason=body.get("reason", ""),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    # 分配资源
    alloc = _orchestrator.allocate_for_pipeline(
        body["pipeline_id"],
        node_count=plan.total_partitions,
        estimated_memory_gb=plan.total_partitions * 0.5,
    )

    return {
        "plan": {
            "pipeline_id": plan.pipeline_id,
            "task_name": plan.task_name,
            "reason": plan.reason,
            "total_partitions": plan.total_partitions,
            "batch_count": len(plan.batches),
            "parallelism": plan.parallelism,
            "estimated_completion": plan.estimated_completion,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "partitions": b.range.partitions,
                    "count": b.range.count,
                }
                for b in plan.batches
            ],
        },
        "resource_allocation": alloc["allocation"],
    }


# ---- Schema变更评估 ----

@router.post("/schema/evaluate")
def evaluate_schema_change(body: dict[str, Any]) -> dict[str, Any]:
    """评估Schema变更的兼容性和影响。

    body:
      change_id, table_name,
      before_columns: [{"name", "data_type", "nullable", "is_partition"}],
      after_columns: [{"name", "data_type", "nullable", "is_partition"}],
      pipeline_deps: {table_name: [pipeline_ids]}
    """
    from app.engine.schema_evolution import SchemaColumn, SchemaDefinition

    before = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("before_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("before_columns", []) if c.get("is_partition")
        ],
    )
    after = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("after_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("after_columns", []) if c.get("is_partition")
        ],
    )
    result = _orchestrator.evaluate_schema_change(
        change_id=body.get("change_id", "auto"),
        before=before,
        after=after,
        pipeline_deps=body.get("pipeline_deps", {}),
    )
    return {
        "change_id": result.change.change_id,
        "table": result.change.target_table,
        "compatibility": result.change.compatibility.value,
        "changes": result.change.detail.get("changes", []),
        "affected_pipelines": result.change.affected_pipelines,
        "rollback_plan": result.change.rollback_plan,
        "migration_recommendation": result.migration_recommendation,
        "walk_pipelines": result.affected_pipelines,
    }


# ---- 血缘影响分析 ----

@router.get("/lineage/impact/{node_id}")
def analyze_lineage_impact(node_id: str) -> dict[str, Any]:
    """分析数据血缘影响：上游溯源 + 下游影响。"""
    result = _orchestrator.lineage.analyze_impact(node_id)
    return {
        "target_node": result.target_node,
        "upstream_count": len(result.upstream_nodes),
        "downstream_count": len(result.downstream_nodes),
        "affected_pipelines": result.affected_pipelines,
        "lineage_depth": result.lineage_depth,
        "has_cycle": result.has_cycle,
        "cycle_path": result.cycle_path,
    }


# ---- 资源健康 ----

@router.get("/resources/health")
def cluster_health() -> dict[str, Any]:
    """计算资源集群健康状态。"""
    return _orchestrator.balancer.cluster_health()


# ---- 告警批量评估 ----

@router.post("/alerts/evaluate")
def evaluate_alerts(body: dict[str, Any]) -> dict[str, Any]:
    """批量评估告警规则。

    body:
      rules: [{"rule_id", "rule_name", "monitor_target", "condition", "level"}],
      metrics: dict
    """
    rules = [
        AlertRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            monitor_target=r["monitor_target"],
            condition=r["condition"],
            level=AlertLevel(r.get("level", "WARN")),
            channels=r.get("channels", ["email"]),
        )
        for r in body.get("rules", [])
    ]
    matcher = _orchestrator.alert
    results = matcher.match_batch(rules, body.get("metrics", {}))

    return {
        "total_rules": len(rules),
        "triggered": sum(1 for r in results if r.matched),
        "results": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "matched": r.matched,
                "level": r.level.value,
                "message": r.message,
            }
            for r in results
        ],
    }


# ---- DAG拓扑分析 ----

@router.post("/dag/analyze")
def analyze_dag(body: dict[str, Any]) -> dict[str, Any]:
    """分析DAG拓扑结构。

    body:
      nodes: [{"id", "name", "weight"}],
      edges: [{"from", "to"}]
    """
    from app.engine.dag_topology import DagNode, DagTopology

    topo = DagTopology()
    for n in body.get("nodes", []):
        topo.add_node(DagNode(
            node_id=n["id"], name=n.get("name", n["id"]),
            weight=float(n.get("weight", 1.0)),
        ))
    for e in body.get("edges", []):
        topo.add_edge(e["from"], e["to"])

    result = topo.solve()
    return {
        "node_count": topo.node_count,
        "has_cycle": result.has_cycle,
        "cycle_nodes": result.cycle_nodes,
        "topology_order": result.order,
        "critical_path": result.critical_path,
        "critical_length": result.critical_length,
        "node_depth": result.node_depth,
    }


# ---- 综合管线运行报告 ----

@router.get("/pipelines/{pipeline_id}/run-report")
def pipeline_run_report(pipeline_id: str) -> dict[str, Any]:
    """获取管线的综合运行报告。"""
    # 模拟生成运行报告
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=pipeline_id,
        partition_field="dt",
        current_watermark=datetime.now().strftime("%Y-%m-%d"),
        scheduled_time=datetime.now(),
        delay_minutes=0,
        status=WatermarkStatus.NORMAL,
    )

    # 模拟质量结果
    quality = _orchestrator.run_quality_gate(
        pipeline_id,
        [
            QualityRule(
                rule_id="r_default",
                rule_name="默认非空检查",
                target_table=f"dw.{pipeline_id}",
                check_field="id",
                check_type=CheckType.NOT_NULL,
                threshold=0.01,
            ),
        ],
        [{"id": "001"}, {"id": None}],
    )

    return _orchestrator.pipeline_run_report(pipeline_id, record, quality)


# ---- 列切换批量演练（跨表多选 / 共享消费方分组 / 试跑映射） ----

def _parse_candidates(raw: list[dict[str, Any]]) -> list[ColumnCandidate]:
    candidates: list[ColumnCandidate] = []
    for item in raw:
        try:
            candidates.append(ColumnCandidate(
                table_name=item["table_name"],
                column_name=item["column_name"],
                candidate_type=item.get("candidate_type", "string"),
                type_validated=bool(item.get("type_validated", False)),
                column_version=str(item.get("column_version", "v1")),
                data_source=item.get("data_source", ""),
                legacy_column=item.get("legacy_column", ""),
                legacy_type=item.get("legacy_type", ""),
            ))
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"候选列缺少必填参数: {exc}")
    return candidates


def _parse_relations(raw: list[dict[str, Any]]) -> list[ReadRelation]:
    relations: list[ReadRelation] = []
    for item in raw:
        try:
            relations.append(ReadRelation(
                consumer_job=item["consumer_job"],
                table_name=item["table_name"],
                column_name=item["column_name"],
                data_source=item.get("data_source", ""),
            ))
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"读取关系缺少必填参数: {exc}")
    return relations


def _require_plan(batch_id: str) -> dict[str, Any]:
    plan = _rehearsal_plans.get(batch_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"演练批次 {batch_id} 不存在")
    return plan


@router.post("/column-rehearsal/batches")
def create_rehearsal_batch(body: dict[str, Any]) -> dict[str, Any]:
    """提交跨表多选的候选批次，按共享消费方生成分组演练计划。

    body:
      batch_id,
      candidates: [{"table_name", "column_name", "candidate_type", "type_validated",
                    "column_version", "data_source", "legacy_column", "legacy_type"}],
      read_relations: [{"consumer_job", "table_name", "column_name", "data_source"}]
    """
    batch_id = body.get("batch_id", "").strip()
    if not batch_id:
        raise HTTPException(status_code=400, detail="缺少必填参数: batch_id")
    candidates = _parse_candidates(body.get("candidates", []))
    if not candidates:
        raise HTTPException(status_code=400, detail="候选批次至少包含一个候选列")
    relations = _parse_relations(body.get("read_relations", []))

    plan = _rehearsal_engine.build_rehearsal_plan(batch_id, candidates, relations)
    _rehearsal_plans[batch_id] = plan
    return {
        "summary": _rehearsal_engine.plan_summary(plan),
        "plan": _rehearsal_engine.serialize_plan(plan),
        "field_mappings": _rehearsal_engine.build_field_mappings(plan),
    }


@router.get("/column-rehearsal/batches/{batch_id}")
def get_rehearsal_batch(batch_id: str) -> dict[str, Any]:
    """查看批次的分组演练数据、字段映射和各组最新状态。"""
    plan = _require_plan(batch_id)
    return {
        "summary": _rehearsal_engine.plan_summary(plan),
        "plan": _rehearsal_engine.serialize_plan(plan),
        "field_mappings": _rehearsal_engine.build_field_mappings(plan),
    }


@router.post("/column-rehearsal/batches/{batch_id}/groups/{group_id}/run")
def run_rehearsal_group(batch_id: str, group_id: str) -> dict[str, Any]:
    """对单个表组执行成套试跑（独立维表组可先行，共享消费方组整组阻断/通过）。"""
    plan = _require_plan(batch_id)
    try:
        result = _rehearsal_engine.run_group(plan, group_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return result


@router.post("/column-rehearsal/batches/{batch_id}/groups/{group_id}/interrupt")
def interrupt_rehearsal_group(batch_id: str, group_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """标记演练中断，保留该组锁定的列版本与数据源。"""
    plan = _require_plan(batch_id)
    try:
        return _rehearsal_engine.interrupt_group(
            plan, group_id, body.get("reason", "未说明的中断原因"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/column-rehearsal/batches/{batch_id}/groups/{group_id}/retry")
def retry_rehearsal_group(batch_id: str, group_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """中断后重试：沿用该组原来的列版本和数据源，漂移则拒绝。

    body 可携带最新 candidates（例如日期列已补完类型校验），
    不传则沿用批次内已登记的候选列快照。
    """
    plan = _require_plan(batch_id)
    candidates = (
        _parse_candidates(body["candidates"])
        if body.get("candidates")
        else list(plan["candidate_index"].values())
    )
    relations = (
        _parse_relations(body["read_relations"])
        if body.get("read_relations")
        else list(plan["read_relations"])
    )
    try:
        return _rehearsal_engine.retry_group(plan, group_id, candidates, relations)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/column-rehearsal/batches/{batch_id}/groups/{group_id}/trace")
def trace_rehearsal_group(batch_id: str, group_id: str) -> dict[str, Any]:
    """追溯已确认表组（含独立维表组）的演练凭证与锁定快照。"""
    plan = _require_plan(batch_id)
    try:
        return _rehearsal_engine.trace_confirmed_group(plan, group_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))