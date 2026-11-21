/**
 * 列切换批量演练前端域：跨表候选批次的分组试跑客户端与视图模型。
 *
 * 与后端 app/engine/column_rehearsal.py 的结构保持一致：
 * - 一次提交多张表（事实表 / 日期表 / 地区字典）的候选列
 * - 共享消费方读取的候选列被后端并查集分到同一演练组，必须成套验证
 * - 字段映射先保留旧列引用，再加入新候选列
 * - 中断重试沿用组内锁定的列版本与数据源
 */
import { request } from '@/api/client'

export type RehearsalStatus =
  | '待演练'
  | '待校验阻断'
  | '演练中'
  | '已中断'
  | '已确认'
  | '演练失败'

export type MappingPhase = '旧列保留' | '新列加入'

export type ColumnCandidateInput = {
  table_name: string
  column_name: string
  candidate_type: string
  type_validated: boolean
  column_version: string
  data_source: string
  legacy_column?: string
  legacy_type?: string
}

export type ReadRelationInput = {
  consumer_job: string
  table_name: string
  column_name: string
  data_source?: string
}

export type VersionLock = {
  table: string
  column: string
  column_version: string
}

export type DataSourceLock = {
  table: string
  column: string
  data_source: string
}

export type RehearsalColumn = {
  table: string
  column: string
  candidate_type: string
  type_validated: boolean
  column_version: string
  data_source: string
}

export type RehearsalDataRecord = {
  consumer_job: string
  read_columns: RehearsalColumn[]
  read_column_count: number
}

export type FieldMapping = {
  mapping_id: string
  consumer_job: string
  table: string
  upstream_column: string
  candidate_type: string
  phase: MappingPhase
  type_validated: boolean
  column_version: string
  data_source: string
  rehearsal_group_id: string
}

export type RehearsalGroup = {
  group_id: string
  consumer_jobs: string[]
  tables: string[]
  candidate_columns: { table: string; column: string }[]
  candidate_count: number
  table_count: number
  status: RehearsalStatus
  version_locks: VersionLock[]
  data_source_locks: DataSourceLock[]
  rehearsal_data: RehearsalDataRecord[]
  field_mappings: FieldMapping[]
  confirmation: {
    rehearsal_id: string
    group_id: string
    batch_id: string
    consumer_jobs: string[]
    tables: string[]
    column_versions: VersionLock[]
    data_sources: DataSourceLock[]
    mapping_count: number
    confirmed_at: string
  } | null
  last_run_at: string
  interrupt_reason: string
  must_validate_as_whole: boolean
}

export type RehearsalBatchPlan = {
  batch_id: string
  submitted_tables: string[]
  submitted_candidate_count: number
  unread_candidates: { table: string; column: string }[]
  groups: RehearsalGroup[]
}

export type RehearsalSummary = {
  batch_id: string
  submitted_tables: string[]
  group_count: number
  must_validate_as_whole: string[]
  independent_groups: string[]
  groups: {
    group_id: string
    status: RehearsalStatus
    tables: string[]
    consumer_jobs: string[]
    must_validate_as_whole: boolean
  }[]
}

export type BatchResponse = {
  summary: RehearsalSummary
  plan: RehearsalBatchPlan
  field_mappings: FieldMapping[]
}

export type GroupRunResult = {
  group_id: string
  status: RehearsalStatus
  passed: boolean
  must_validate_as_whole?: boolean
  rehearsal_id?: string
  blocked_columns?: { table: string; column: string }[]
  retriable?: boolean
  retried_with_locked_snapshot?: boolean
  drift?: { table: string; column: string; reason: string }[]
  message: string
}

async function postJson<T>(path: string, body: unknown = {}): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = (await response.json().catch(() => null))?.detail ?? `接口返回 ${response.status}`
    throw new Error(detail)
  }
  return (await response.json()) as T
}

/** 提交跨表多选的候选批次，返回分组演练计划与试跑字段映射。 */
export async function createRehearsalBatch(
  batchId: string,
  candidates: ColumnCandidateInput[],
  readRelations: ReadRelationInput[],
): Promise<BatchResponse> {
  return postJson<BatchResponse>('/api/business/column-rehearsal/batches', {
    batch_id: batchId,
    candidates,
    read_relations: readRelations,
  })
}

/** 读取批次的分组演练数据、字段映射和各组最新状态。 */
export async function loadRehearsalBatch(batchId: string): Promise<BatchResponse> {
  const response = await request(`/api/business/column-rehearsal/batches/${batchId}`)
  if (!response.ok) throw new Error(`演练批次 ${batchId} 读取失败`)
  return (await response.json()) as BatchResponse
}

/** 对单个表组执行成套试跑。 */
export async function runRehearsalGroup(batchId: string, groupId: string): Promise<GroupRunResult> {
  return postJson<GroupRunResult>(
    `/api/business/column-rehearsal/batches/${batchId}/groups/${groupId}/run`,
  )
}

/** 标记演练中断，保留该组锁定的列版本与数据源。 */
export async function interruptRehearsalGroup(
  batchId: string,
  groupId: string,
  reason: string,
): Promise<GroupRunResult> {
  return postJson<GroupRunResult>(
    `/api/business/column-rehearsal/batches/${batchId}/groups/${groupId}/interrupt`,
    { reason },
  )
}

/**
 * 中断后重试。candidates 可携带最新类型校验态（例如日期列补完校验），
 * 但列版本 / 数据源必须与组锁一致，否则后端拒绝。
 */
export async function retryRehearsalGroup(
  batchId: string,
  groupId: string,
  candidates: ColumnCandidateInput[],
  readRelations?: ReadRelationInput[],
): Promise<GroupRunResult> {
  return postJson<GroupRunResult>(
    `/api/business/column-rehearsal/batches/${batchId}/groups/${groupId}/retry`,
    { candidates, ...(readRelations ? { read_relations: readRelations } : {}) },
  )
}

/** 追溯已确认表组（含独立维表组）的演练凭证。 */
export async function traceRehearsalGroup(
  batchId: string,
  groupId: string,
): Promise<{ traceable: boolean } & Record<string, unknown>> {
  const response = await request(
    `/api/business/column-rehearsal/batches/${batchId}/groups/${groupId}/trace`,
  )
  if (!response.ok) throw new Error('演练凭证追溯失败')
  return (await response.json()) as { traceable: boolean } & Record<string, unknown>
}

// ---- 视图模型辅助：承载前端展示侧的业务差异 ----

/** 阻断态的组需要提示用户先补齐类型校验，且不能逐列放行。 */
export function isGroupBlocked(group: RehearsalGroup): boolean {
  return group.status === '待校验阻断'
}

/** 独立维表组（不与其他表共享消费方）可以先于事实表组走完。 */
export function canRunIndependently(group: RehearsalGroup): boolean {
  return !group.must_validate_as_whole && group.status !== '已确认'
}

/**
 * 检查字段映射的双轨顺序：同一候选列若声明了旧列保留映射，
 * 它必须排在新列加入映射之前，保证切换窗口内旧引用不断。
 */
export function findOutOfOrderMappings(mappings: FieldMapping[]): FieldMapping[] {
  const outOfOrder: FieldMapping[] = []
  const byTable = new Map<string, FieldMapping[]>()
  for (const mapping of mappings) {
    const list = byTable.get(mapping.table) ?? []
    list.push(mapping)
    byTable.set(mapping.table, list)
  }
  for (const tableMappings of byTable.values()) {
    const legacyIdx = tableMappings.findIndex((m) => m.phase === '旧列保留')
    if (legacyIdx === -1) continue
    const firstCandidateIdx = tableMappings.findIndex((m) => m.phase === '新列加入')
    if (firstCandidateIdx !== -1 && firstCandidateIdx < legacyIdx) {
      outOfOrder.push(tableMappings[firstCandidateIdx])
    }
  }
  return outOfOrder
}

/** 汇总映射里尚未完成类型校验的新列，提示这些组会被成套阻断。 */
export function unvalidatedCandidateMappings(mappings: FieldMapping[]): FieldMapping[] {
  return mappings.filter((m) => m.phase === '新列加入' && !m.type_validated)
}
