package postgres

// 本文件是模型 API 请求记录（模型网关设计 2026-10-11 §4、§7.7 第 8 步；G3）：model_requests 与 model_request_tries 在
// 请求的 hold 结算之后一个事务写入。两表按 created_at 的 UTC 月分区：写入某月之前先确保该月与下月的分区存在
// （agentbox_model_request_partitions，单独的短事务；每个进程每月只做一次）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/modelapi"
)

var _ modelapi.Store = (*ModelRequestStore)(nil)

// ModelRequestStore 实现 modelapi.Store。
type ModelRequestStore struct {
	s *Store

	mu    sync.Mutex
	parts map[string]bool // 已确认存在分区的月份（"YYYY-MM"）
}

// ModelRequests 以 modelapi.Store 接口返回模型请求记录用例（装配以接口持有，不导入本包）。
func (s *Store) ModelRequests() modelapi.Store { return &ModelRequestStore{s: s, parts: map[string]bool{}} }

// RecordModelRequest 实现 modelapi.Store：同一 ID 重复写入为空操作（提交结果未知时重跑）。OrgID 为 0（运维）时取保留
// org operator。
func (m *ModelRequestStore) RecordModelRequest(ctx context.Context, r modelapi.Record) error {
	if err := m.ensurePartition(ctx, r.CreatedAt); err != nil {
		return err
	}
	return m.s.run(ctx, "RecordModelRequest", r.ID, func(ctx context.Context, tx pgx.Tx) error {
		return insertModelRequestTx(ctx, tx, r)
	})
}

func insertModelRequestTx(ctx context.Context, tx pgx.Tx, r modelapi.Record) error {
	var ttfb *int64
	if r.TTFB != nil {
		v := r.TTFB.Milliseconds()
		ttfb = &v
	}
	dropped := r.Dropped
	if dropped == nil {
		dropped = []string{}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO model_requests (id, org_id, user_id, key_id, client_protocol, model, stream, provider,
			provider_protocol, converted, status, error_code, prompt_tokens, completion_tokens, cache_read_tokens, cache_write_tokens,
			usage_known, cost_micro, ttfb_ms, latency_ms, stream_complete, dropped_fields, hold_id, created_at)
		VALUES ($1, COALESCE(NULLIF($2::bigint, 0), (SELECT id FROM orgs WHERE name = 'operator')), NULLIF($3::bigint, 0),
			NULLIF($4, ''), $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), $10, $11, NULLIF($12, ''), $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24)
		ON CONFLICT DO NOTHING`,
		r.ID, r.OrgID, r.UserID, r.KeyID, string(r.ClientProtocol), r.Model, r.Stream, r.Provider, string(r.ProviderProtocol),
		r.Converted, r.Status, r.ErrorCode, r.Usage.PromptTotal(), r.Usage.OutputTokens, r.Usage.CacheReadTokens,
		r.Usage.CacheWriteTokens, r.UsageKnown, r.CostMicro, ttfb, r.Latency.Milliseconds(), r.StreamComplete, dropped,
		string(r.HoldID), r.CreatedAt)
	if err != nil || tag.RowsAffected() == 0 || len(r.Tries) == 0 {
		return err // 已写入（重跑）时不再写 try
	}
	n := len(r.Tries)
	nos, providers, outcomes, skipped := make([]int32, n), make([]string, n), make([]string, n), make([]string, n)
	statuses, latencies, costs := make([]int32, n), make([]int64, n), make([]int64, n)
	for i, t := range r.Tries {
		nos[i], providers[i], outcomes[i], skipped[i] = int32(t.No), t.Provider, t.Outcome, t.Skipped
		statuses[i], latencies[i], costs[i] = int32(t.Status), t.Latency.Milliseconds(), t.CostMicro
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_request_tries (request_id, created_at, try_no, provider, outcome, status, latency_ms,
			cost_micro, skipped_reason)
		SELECT $1, $2, * FROM unnest($3::int[], $4::text[], $5::text[], $6::int[], $7::bigint[], $8::bigint[], $9::text[])`,
		r.ID, r.CreatedAt, nos, providers, outcomes, statuses, latencies, costs, skipped)
	return err
}

// ensurePartition 确保 t 所在月份与下一个月的分区存在（每个进程每月一次）。并发创建同一分区时，落败的一方得到
// "已存在" 类错误，视为成功。
func (m *ModelRequestStore) ensurePartition(ctx context.Context, t time.Time) error {
	month := t.UTC().Format("2006-01")
	m.mu.Lock()
	done := m.parts[month]
	m.mu.Unlock()
	if done {
		return nil
	}
	err := m.s.run(ctx, "ModelRequestPartitions", month, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT agentbox_model_request_partitions($1), agentbox_model_request_partitions($1 + interval '1 month')`, t)
		return err
	})
	var pgErr *pgconn.PgError
	if err != nil && !(errors.As(err, &pgErr) && (pgErr.Code == "42P07" || pgErr.Code == "23505")) {
		return fmt.Errorf("postgres: 模型请求分区 %s: %w", month, err)
	}
	m.mu.Lock()
	m.parts[month] = true
	m.mu.Unlock()
	return nil
}
