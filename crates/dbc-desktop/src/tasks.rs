//! Dedicated asynchronous runtime used by database drivers.
//!
//! Database work never runs on the egui UI thread: callers hand a future to
//! [`TaskRuntime::spawn_reported`] and get a cancellation token back synchronously.

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

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use super::{RuntimeConfig, RuntimeConfigError, TaskError, TaskRuntime};

    #[test]
    fn runtime_rejects_zero_workers_and_zero_concurrency() {
        assert_eq!(
            TaskRuntime::new(RuntimeConfig {
                worker_threads: 0,
                max_concurrent_tasks: 8,
            })
            .expect_err("zero worker threads should fail"),
            RuntimeConfigError::ZeroWorkerThreads
        );
        assert_eq!(
            TaskRuntime::new(RuntimeConfig {
                worker_threads: 2,
                max_concurrent_tasks: 0,
            })
            .expect_err("zero concurrency should fail"),
            RuntimeConfigError::ZeroConcurrency
        );
    }

    #[tokio::test]
    async fn runtime_enforces_the_global_concurrency_limit() {
        let runtime = TaskRuntime::new(RuntimeConfig {
            worker_threads: 2,
            max_concurrent_tasks: 2,
        })
        .expect("runtime should build");
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..6 {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            handles.push(runtime.spawn(move |_| async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, Infallible>(())
            }));
        }

        for handle in handles {
            handle.wait().await.expect("task should finish");
        }

        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cooperative_cancellation_is_reported_to_the_caller() {
        let runtime = TaskRuntime::new(RuntimeConfig::default()).expect("runtime should build");
        let handle = runtime.spawn(|cancellation| async move {
            cancellation.cancelled().await;
            Ok::<_, Infallible>(())
        });
        let cancellation = handle.cancellation_token();

        handle.cancellation_token().cancel();
        assert!(cancellation.is_cancelled());
        assert_eq!(handle.wait().await, Err(TaskError::Cancelled));
    }

    #[test]
    fn reported_task_delivers_success_once() {
        let runtime = TaskRuntime::new(RuntimeConfig::default()).expect("runtime should build");
        let (sender, receiver) = mpsc::sync_channel(1);
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_for_task = Arc::clone(&callback_count);

        let _cancellation = runtime.spawn_reported(
            |_| async { Ok::<_, Infallible>(42) },
            move |result| {
                callback_count_for_task.fetch_add(1, Ordering::SeqCst);
                sender
                    .send(result)
                    .expect("receiver should remain available");
            },
        );

        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("completion should be reported"),
            Ok(42)
        );
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reported_task_delivers_operation_error() {
        let runtime = TaskRuntime::new(RuntimeConfig::default()).expect("runtime should build");
        let (sender, receiver) = mpsc::sync_channel(1);

        let _cancellation = runtime.spawn_reported(
            |_| async { Err::<(), _>("operation failed") },
            move |result| {
                sender
                    .send(result)
                    .expect("receiver should remain available");
            },
        );

        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("completion should be reported"),
            Err(TaskError::Operation("operation failed"))
        );
    }

    #[test]
    fn reported_task_delivers_cancellation() {
        let runtime = TaskRuntime::new(RuntimeConfig::default()).expect("runtime should build");
        let (sender, receiver) = mpsc::sync_channel(1);

        let cancellation = runtime.spawn_reported(
            |task_cancellation| async move {
                task_cancellation.cancelled().await;
                Ok::<_, Infallible>(())
            },
            move |result| {
                sender
                    .send(result)
                    .expect("receiver should remain available");
            },
        );
        cancellation.cancel();

        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("completion should be reported"),
            Err(TaskError::Cancelled)
        );
    }
}
