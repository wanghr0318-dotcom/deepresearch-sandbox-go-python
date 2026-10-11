package app

import (
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/modelapi"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
)

// modelRequestStorer 是 Store 的可选能力：模型 API 请求记录（internal/persistence/postgres 实现）。
type modelRequestStorer interface {
	ModelRequests() modelapi.Store
}

// modelRoutes 是模型 API 使用的 Gateway 模型路由：Gateway 装配完成后设置一次（API 可能先于执行以诊断模式启动，
// 此前模型 API 回答 503）。
type modelRoutes struct {
	p atomic.Pointer[call.ModelRouting]
}

func (m *modelRoutes) get() *call.ModelRouting { return m.p.Load() }

// modelAPIHandler 构造模型 API（--model-api，模型网关 G3）：需要多租户（账号）、预算存储与模型请求记录；与运行时 API
// 共用限流器。没有打开时返回 nil；打开了但缺少这些能力时返回错误（不悄悄关闭，复审 n5）。
func (s *server) modelAPIHandler(q quota.Store, limiter ratelimit.Limiter) (*modelapi.Handler, error) {
	if !s.cfg.ModelAPI {
		return nil, nil
	}
	rs, ok := s.store.(modelRequestStorer)
	if q == nil || !ok {
		return nil, errors.New("app: --model-api 需要启用账号（--model-base-url）与 PostgreSQL 的预算与模型请求记录")
	}
	models := []string{s.cfg.Model.Name}
	for _, m := range s.cfg.Model.Models {
		if !slices.Contains(models, m) {
			models = append(models, m)
		}
	}
	maxDur := s.cfg.ModelAPIMaxDuration
	if maxDur <= 0 {
		maxDur = modelapi.DefaultMaxDuration
	}
	s.log.Info("模型 API 已启用", "paths", "/v1/chat/completions,/v1/messages,/v1/models", "max_duration", maxDur.String())
	return modelapi.New(modelapi.Config{Routing: s.modelRoutes.get, Models: models, Holder: q, Limits: q, Limiter: limiter,
		Store: rs.ModelRequests(), MaxDuration: maxDur, MaxBody: s.cfg.ModelAPIMaxBody, Logger: s.log, Now: time.Now}), nil
}
