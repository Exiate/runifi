use std::sync::Arc;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::connection::flow_connection::FlowConnection;
use runifi_core::id::IdGenerator;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;
use runifi_core::session::process_session::CoreProcessSession;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{FlowFile, REL_SUCCESS};

fn make_flowfile(id: u64, size: u64) -> FlowFile {
    FlowFile {
        id,
        attributes: Vec::new(),
        content_claim: None,
        size,
        created_at_nanos: 0,
        lineage_start_id: id,
        penalized_until_nanos: 0,
    }
}

fn bench_session_create_transfer_commit(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());

    c.bench_function("session_create_transfer_commit", |b| {
        b.iter(|| {
            let mut session =
                CoreProcessSession::new(content_repo.clone(), id_gen.clone(), vec![], 1000, 30_000);
            let ff = session.create();
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
            black_box(session.is_committed());
        });
    });
}

fn bench_session_write_content_5kb(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());
    let data = Bytes::from(vec![0u8; 5120]);

    c.bench_function("session_write_content_5KB", |b| {
        b.iter(|| {
            let mut session =
                CoreProcessSession::new(content_repo.clone(), id_gen.clone(), vec![], 1000, 30_000);
            let ff = session.create();
            let ff = session.write_content(ff, data.clone()).unwrap();
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
        });
    });
}

fn bench_end_to_end_pipeline_step(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());
    let data = Bytes::from(vec![0u8; 5120]); // 5 KB micro profile

    c.bench_function("pipeline_step_5KB", |b| {
        b.iter(|| {
            // Simulate a single pipeline step:
            // 1. Create FlowFile
            // 2. Write 5KB content
            // 3. Transfer to success
            // 4. Commit
            let mut session =
                CoreProcessSession::new(content_repo.clone(), id_gen.clone(), vec![], 1000, 30_000);
            let ff = session.create();
            let ff = session.write_content(ff, data.clone()).unwrap();
            let _ = session.read_content(&ff).unwrap();
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
        });
    });
}

fn bench_pipeline_with_connection(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());
    let data = Bytes::from(vec![0u8; 5120]);

    c.bench_function("pipeline_conn_send_recv_5KB", |b| {
        b.iter(|| {
            let conn = Arc::new(FlowConnection::new("bench", BackPressureConfig::default()));

            // Producer: create FF, write content, send to connection.
            let claim = content_repo.create(data.clone()).unwrap();
            let mut ff = make_flowfile(id_gen.next_id(), 5120);
            ff.content_claim = Some(claim);
            conn.try_send(ff).unwrap();

            // Consumer: receive from connection, read content.
            let received = conn.try_recv().unwrap();
            let content = content_repo
                .read(received.content_claim.as_ref().unwrap())
                .unwrap();
            black_box(content);

            // Cleanup.
            content_repo
                .decrement_ref(received.content_claim.unwrap().resource_id)
                .unwrap();
        });
    });
}

fn bench_batch_processing(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());

    c.bench_function("batch_create_100_flowfiles", |b| {
        b.iter(|| {
            let mut session =
                CoreProcessSession::new(content_repo.clone(), id_gen.clone(), vec![], 1000, 30_000);
            for _ in 0..100 {
                let ff = session.create();
                session.transfer(ff, &REL_SUCCESS);
            }
            session.commit();
            black_box(session.is_committed());
        });
    });
}

fn bench_session_rollback(c: &mut Criterion) {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let id_gen = Arc::new(IdGenerator::new());
    let data = Bytes::from(vec![0u8; 1024]);

    c.bench_function("session_rollback_with_content", |b| {
        b.iter(|| {
            let mut session =
                CoreProcessSession::new(content_repo.clone(), id_gen.clone(), vec![], 1000, 30_000);
            let ff = session.create();
            let _ff = session.write_content(ff, data.clone()).unwrap();
            session.rollback();
        });
    });
}

criterion_group!(
    benches,
    bench_session_create_transfer_commit,
    bench_session_write_content_5kb,
    bench_end_to_end_pipeline_step,
    bench_pipeline_with_connection,
    bench_batch_processing,
    bench_session_rollback,
);
criterion_main!(benches);
