use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use dbc_runtime::{RuntimeConfig, RuntimeConfigError, TaskError, TaskRuntime};

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

    handle.cancel();
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
