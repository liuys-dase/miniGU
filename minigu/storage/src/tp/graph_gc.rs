use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use minigu_common::types::{EdgeId, VertexId};
use minigu_transaction::global_timestamp_generator;

use super::memory_graph::MemoryGraph;
use super::transaction::{MemTxnManager, UndoEntry};
use crate::common::model::edge::{Edge, Neighbor};
use crate::common::transaction::DeltaOp;
use crate::error::StorageResult;

/// GC 监控信息
#[derive(Debug, Clone)]
pub struct GcInfo {
    /// 当前的 watermark（最小活跃事务的开始时间戳）
    pub watermark: u64,
    /// 已提交事务的数量
    pub committed_txns_count: usize,
    /// 上次 GC 的时间戳
    pub last_gc_timestamp: u64,
}

/// GC 配置参数
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// 检查间隔（毫秒）
    pub check_interval_ms: u64,
    /// watermark 变化阈值，超过此值触发 GC
    pub watermark_threshold: u64,
    /// 已提交事务数量阈值，超过此值触发 GC
    pub committed_txns_threshold: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,       // 1秒检查一次
            watermark_threshold: 50,       // 使用之前的阈值
            committed_txns_threshold: 100, // 100个已提交事务
        }
    }
}

/// GC 监控 trait
/// 提供对事务管理器状态的只读访问
pub trait GcMonitor {
    /// 判断是否应该触发 GC
    fn should_trigger_gc(&self) -> bool;
    /// 获取当前的 GC 相关信息
    fn get_gc_info(&self) -> GcInfo;
    /// 获取需要 GC 的过期事务信息
    fn get_expired_entries(&self) -> (Vec<Arc<UndoEntry>>, u64);
    /// 更新上次 GC 时间戳
    fn update_last_gc_timestamp(&self, timestamp: u64);
}

/// 图垃圾回收请求
#[derive(Debug)]
pub struct GcRequest {
    /// 需要回收的过期事务的undo条目
    pub expired_undo_entries: Vec<Arc<UndoEntry>>,
    /// 最小读时间戳，用于确定哪些版本可以安全删除
    pub min_read_timestamp: u64,
}

/// 图数据垃圾回收器
/// 专门负责处理图数据的垃圾回收，与事务管理器解耦
pub struct GraphGarbageCollector {
    /// 图的引用（用于实际执行垃圾回收）
    graph: Arc<MemoryGraph>,
    /// GC线程的句柄
    gc_thread_handle: Option<thread::JoinHandle<()>>,
    /// 停止信号的发送端
    stop_sender: Option<Sender<()>>,
}

impl GraphGarbageCollector {
    /// 创建新的图垃圾回收器
    pub fn new(graph: Arc<MemoryGraph>, _txn_manager: Weak<MemTxnManager>) -> Self {
        Self::new_with_config(graph, GcConfig::default())
    }

    /// 创建新的图垃圾回收器，使用自定义配置
    pub fn new_with_config(graph: Arc<MemoryGraph>, config: GcConfig) -> Self {
        Self::new_with_thread_control(graph, config, true)
    }

    /// 创建新的图垃圾回收器，可控制是否启动线程（主要用于测试）
    pub fn new_with_thread_control(
        graph: Arc<MemoryGraph>,
        config: GcConfig,
        start_thread: bool,
    ) -> Self {
        let (stop_sender, stop_receiver) = channel::<()>();

        let handle = if start_thread {
            let graph_clone = graph.clone();
            let config_clone = config.clone();

            Some(thread::spawn(move || {
                Self::gc_monitor_loop(graph_clone, Weak::new(), config_clone, stop_receiver);
            }))
        } else {
            None
        };

        Self {
            graph,
            gc_thread_handle: handle,
            stop_sender: Some(stop_sender),
        }
    }

    /// 停止垃圾回收线程
    pub fn stop_gc_thread(&mut self) {
        // 发送停止信号
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }

        // 等待线程结束
        if let Some(handle) = self.gc_thread_handle.take() {
            let _ = handle.join();
        }
    }

    /// 同步处理垃圾回收请求（主要用于测试）
    pub fn process_gc_request_sync(graph: &MemoryGraph, request: GcRequest) -> StorageResult<()> {
        Self::process_gc_request(graph, request)
    }

    /// 手动触发一次垃圾回收（主要用于测试）
    pub fn trigger_gc_sync(&self) -> StorageResult<()> {
        let txn_manager = &self.graph.txn_manager;
        let (expired_entries, min_read_ts) = txn_manager.get_expired_entries();
        if !expired_entries.is_empty() {
            let request = GcRequest {
                expired_undo_entries: expired_entries,
                min_read_timestamp: min_read_ts,
            };
            Self::process_gc_request(&self.graph, request)?;

            // 更新事务管理器中的上次 GC 时间戳
            let current_ts = global_timestamp_generator().current();
            txn_manager.update_last_gc_timestamp(current_ts.0);
        }
        Ok(())
    }

    /// 新的 GC 监控循环，被动监控事务管理器状态
    fn gc_monitor_loop(
        graph: Arc<MemoryGraph>,
        _txn_manager: Weak<MemTxnManager>,
        config: GcConfig,
        stop_receiver: Receiver<()>,
    ) {
        let check_interval = Duration::from_millis(config.check_interval_ms);

        loop {
            // 检查是否收到停止信号
            if stop_receiver.try_recv().is_ok() {
                break;
            }

            // 直接从 graph 中获取 txn_manager 引用
            let txn_manager = &graph.txn_manager;

            // 检查是否需要触发 GC
            if txn_manager.should_trigger_gc() {
                let (expired_entries, min_read_ts) = txn_manager.get_expired_entries();

                if !expired_entries.is_empty() {
                    let request = GcRequest {
                        expired_undo_entries: expired_entries,
                        min_read_timestamp: min_read_ts,
                    };

                    if let Err(e) = Self::process_gc_request(&graph, request) {
                        eprintln!("Graph garbage collection failed: {:?}", e);
                    } else {
                        // 更新上次 GC 时间戳
                        let current_ts = global_timestamp_generator().current();
                        txn_manager.update_last_gc_timestamp(current_ts.0);
                    }
                }
            }

            // 等待一段时间再进行下次检查
            thread::sleep(check_interval);
        }
    }

    /// 处理单个垃圾回收请求
    fn process_gc_request(graph: &MemoryGraph, request: GcRequest) -> StorageResult<()> {
        let mut expired_edges: HashMap<EdgeId, Edge> = HashMap::new();

        // 分析过期的undo条目，收集需要回收的边
        for undo_entry in request.expired_undo_entries {
            match undo_entry.delta() {
                // DeltaOp::CreateEdge 意味着边在这个事务中被删除了
                DeltaOp::CreateEdge(edge) => {
                    expired_edges.insert(edge.eid(), edge.without_properties());
                }
                DeltaOp::DelEdge(eid) => {
                    expired_edges.remove(eid);
                }
                _ => {}
            }
        }

        // 处理过期的边，清理图数据
        for (_, edge) in expired_edges {
            Self::cleanup_expired_edge(graph, &edge)?;
        }

        Ok(())
    }

    /// 清理单个过期的边及相关数据
    fn cleanup_expired_edge(graph: &MemoryGraph, edge: &Edge) -> StorageResult<()> {
        // 检查实体是否标记为墓碑
        macro_rules! check_tombstone {
            ($graph:expr, $collection:ident, $id_method:expr) => {
                $graph
                    .$collection
                    .get($id_method)
                    .map(|v| Some(v.value().chain.current.read().unwrap().data.is_tombstone))
                    .unwrap_or(None)
            };
        }

        let src_tombstone = check_tombstone!(graph, vertices, &edge.src_id());
        let dst_tombstone = check_tombstone!(graph, vertices, &edge.dst_id());
        let edge_tombstone = check_tombstone!(graph, edges, &edge.eid());

        // 如果源顶点是墓碑，清理它和相关的邻接关系
        if let Some(true) = src_tombstone {
            Self::remove_vertex_and_adjacencies(graph, edge.src_id());
        }

        // 如果目标顶点是墓碑，清理它和相关的邻接关系
        if let Some(true) = dst_tombstone {
            Self::remove_vertex_and_adjacencies(graph, edge.dst_id());
        }

        // 如果边是墓碑，清理边和相关的邻接关系
        if let Some(true) = edge_tombstone {
            graph.edges.remove(&edge.eid());
            graph
                .adjacency_list
                .entry(edge.src_id())
                .and_modify(|adj_container| {
                    adj_container.outgoing().remove(&Neighbor::new(
                        edge.label_id(),
                        edge.dst_id(),
                        edge.eid(),
                    ));
                });
            graph
                .adjacency_list
                .entry(edge.dst_id())
                .and_modify(|adj_container| {
                    adj_container.incoming().remove(&Neighbor::new(
                        edge.label_id(),
                        edge.src_id(),
                        edge.eid(),
                    ));
                });
        }

        Ok(())
    }

    /// 移除顶点及其所有邻接关系
    fn remove_vertex_and_adjacencies(graph: &MemoryGraph, vid: VertexId) {
        graph.vertices.remove(&vid);
        let mut incoming_to_remove = Vec::new();
        let mut outgoing_to_remove = Vec::new();

        if let Some(adj_container) = graph.adjacency_list.get(&vid) {
            for adj in adj_container.incoming().iter() {
                outgoing_to_remove.push((
                    adj.neighbor_id(),
                    Neighbor::new(adj.label_id(), vid, adj.eid()),
                ));
            }
            for adj in adj_container.outgoing().iter() {
                incoming_to_remove.push((
                    adj.neighbor_id(),
                    Neighbor::new(adj.label_id(), vid, adj.eid()),
                ));
            }
        }

        for (other_vid, euid) in incoming_to_remove {
            graph.adjacency_list.entry(other_vid).and_modify(|l| {
                l.incoming().remove(&euid);
            });
        }

        for (other_vid, euid) in outgoing_to_remove {
            graph.adjacency_list.entry(other_vid).and_modify(|l| {
                l.outgoing().remove(&euid);
            });
        }

        // 移除顶点的邻接表
        graph.adjacency_list.remove(&vid);
    }
}

impl Drop for GraphGarbageCollector {
    fn drop(&mut self) {
        self.stop_gc_thread();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tp::memory_graph;

    #[test]
    fn test_graph_gc_creation() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();
        // 测试创建但不启动线程
        let gc = GraphGarbageCollector::new_with_thread_control(graph, GcConfig::default(), false);
        assert!(gc.gc_thread_handle.is_none());
    }

    #[test]
    fn test_gc_process_request_sync() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();

        // 创建一个空的GC请求
        let request = GcRequest {
            expired_undo_entries: Vec::new(),
            min_read_timestamp: 0,
        };

        // 测试同步处理功能
        let result = GraphGarbageCollector::process_gc_request_sync(&graph, request);
        assert!(result.is_ok());
    }
}
