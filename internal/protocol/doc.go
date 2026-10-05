// Package protocol 实现 Worker 协议 v1（task 模式）的消息类型、编解码、校验与事件流顺序检查。
//
// 规则来源：v0.2 规格 §5；Python 侧的 agentbox_worker.protocol 实现同一组规则，
// 二者共用 protocol/fixtures/v1 下的 fixtures。本包只依赖标准库。
package protocol
