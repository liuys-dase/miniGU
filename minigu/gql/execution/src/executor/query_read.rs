use minigu_common::data_chunk::DataChunk;
use minigu_context::graph::GraphReadSession;

use super::{BoxedExecutor, Executor};
use crate::error::{ExecutionError, ExecutionResult};

pub struct QueryReadExecutor {
    inner: BoxedExecutor,
    read_session: Option<GraphReadSession>,
    finished: bool,
}

impl QueryReadExecutor {
    pub fn new(inner: BoxedExecutor, read_session: Option<GraphReadSession>) -> Self {
        Self {
            inner,
            read_session,
            finished: false,
        }
    }

    fn finish_success(&mut self) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Some(read_session) = self.read_session.take() {
            read_session.commit().map_err(ExecutionError::from)?;
        }
        Ok(())
    }

    fn finish_error(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(read_session) = self.read_session.take() {
            let _ = read_session.abort();
        }
    }
}

impl Executor for QueryReadExecutor {
    fn next_chunk(&mut self) -> Option<ExecutionResult<DataChunk>> {
        match self.inner.next_chunk() {
            Some(Ok(chunk)) => Some(Ok(chunk)),
            Some(Err(err)) => {
                self.finish_error();
                Some(Err(err))
            }
            None => match self.finish_success() {
                Ok(()) => None,
                Err(err) => Some(Err(err)),
            },
        }
    }
}

impl Drop for QueryReadExecutor {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_error();
        }
    }
}
