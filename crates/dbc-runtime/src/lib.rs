//! Dedicated asynchronous runtime used by database drivers.

use std::{fmt, future::Future, sync::Arc};

use thiserror::Error;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    sync::Semaphore,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_concurrent_tasks: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: 4,
            max_concurrent_tasks: 8,
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RuntimeConfigError {
    #[error("worker thread count must be greater than zero")]
    ZeroWorkerThreads,
    #[error("maximum concurrency must be greater than zero")]
    ZeroConcurrency,
    #[error("failed to create Tokio runtime: {0}")]
    Build(String),
}

/// Runtime task failure that keeps cancellation, operation, and join errors distinct.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskError<E> {
    #[error("task was cancelled")]
    Cancelled,
    #[error("task failed")]
    Operation(E),
    #[error("task join failed: {0}")]
    Join(String),
}

/// Handle for cooperative cancellation and completion.
pub struct TaskHandle<T, E> {
    cancellation: CancellationToken,
    join: JoinHandle<Result<T, TaskError<E>>>,
}

impl<T, E> TaskHandle<T, E> {
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Wait for task completion.
    ///
    /// # Errors
    ///
    /// Returns cancellation, operation, or runtime join failures without panicking.
    pub async fn wait(self) -> Result<T, TaskError<E>> {
        self.join
            .await
            .map_err(|error| TaskError::Join(error.to_string()))?
    }
}

/// A dedicated Tokio runtime with one global task semaphore.
pub struct TaskRuntime {
    runtime: Option<Runtime>,
    handle: Handle,
    semaphore: Arc<Semaphore>,
}

impl fmt::Debug for TaskRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskRuntime")
            .field("available_permits", &self.semaphore.available_permits())
            .finish_non_exhaustive()
    }
}

impl TaskRuntime {
    /// Create a driver runtime.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits and reports runtime construction failures.
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeConfigError> {
        if config.worker_threads == 0 {
            return Err(RuntimeConfigError::ZeroWorkerThreads);
        }
        if config.max_concurrent_tasks == 0 {
            return Err(RuntimeConfigError::ZeroConcurrency);
        }

        let runtime = Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .thread_name("dbc-driver")
            .enable_all()
            .build()
            .map_err(|error| RuntimeConfigError::Build(error.to_string()))?;
        let handle = runtime.handle().clone();

        Ok(Self {
            runtime: Some(runtime),
            handle,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_tasks)),
        })
    }

    #[must_use]
    pub fn spawn<F, Fut, T, E>(&self, operation: F) -> TaskHandle<T, E>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let semaphore = Arc::clone(&self.semaphore);
        let join = self.handle.spawn(async move {
            let permit = tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return Err(TaskError::Cancelled),
                permit = semaphore.acquire_owned() => {
                    permit.map_err(|_| TaskError::Cancelled)?
                }
            };

            let result = tokio::select! {
                biased;
                () = task_cancellation.cancelled() => Err(TaskError::Cancelled),
                result = operation(task_cancellation.clone()) => {
                    if task_cancellation.is_cancelled() {
                        Err(TaskError::Cancelled)
                    } else {
                        result.map_err(TaskError::Operation)
                    }
                }
            };
            drop(permit);
            result
        });

        TaskHandle { cancellation, join }
    }

    /// Spawn an operation and report its completion from the runtime thread.
    ///
    /// The callback is invoked exactly once after the operation succeeds, fails,
    /// or observes cancellation. The returned token can be used to cancel the
    /// operation without requiring an async executor on the caller's thread.
    #[must_use]
    pub fn spawn_reported<F, Fut, T, E, C>(&self, operation: F, on_complete: C) -> CancellationToken
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        C: FnOnce(Result<T, TaskError<E>>) + Send + 'static,
    {
        let task = self.spawn(operation);
        let cancellation = task.cancellation_token();
        let _completion = self.handle.spawn(async move {
            on_complete(task.wait().await);
        });
        cancellation
    }
}

impl Drop for TaskRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
