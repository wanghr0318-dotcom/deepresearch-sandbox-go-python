package app

import (
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

// modelAPIHandler 构造模型 API（--model-api，模型网关 G3）：需要多租户（账号）与预算存储；与运行时 API 共用限流器。
// 不满足时返回 nil（validate 已在启动前拒绝 --model-api 而没有模型上游的配置）。
func (s *server) modelAPIHandler(q quota.Store, limiter ratelimit.Limiter) *modelapi.Handler {
	if !s.cfg.ModelAPI || q == nil {
		return nil
	}
	rs, ok := s.store.(modelRequestStorer)
	if !ok {
		s.log.Error("模型 API 未启用：存储不支持模型请求记录")
		return nil
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
		Store: rs.ModelRequests(), MaxDuration: maxDur, Logger: s.log, Now: time.Now})
}
