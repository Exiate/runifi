use criterion::{Criterion, black_box, criterion_group, criterion_main};
use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::connection::flow_connection::FlowConnection;
use runifi_plugin_api::FlowFile;

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

fn bench_send_recv(c: &mut Criterion) {
    let conn = FlowConnection::new("bench", BackPressureConfig::default());

    c.bench_function("connection_send_recv_cycle", |b| {
        let mut id = 0u64;
        b.iter(|| {
            id += 1;
            let ff = make_flowfile(id, 100);
            conn.try_send(ff).unwrap();
            let received = conn.try_recv().unwrap();
            black_box(received);
        });
    });
}

fn bench_send_throughput(c: &mut Criterion) {
    c.bench_function("connection_send_1000", |b| {
        b.iter(|| {
            let conn = FlowConnection::new("bench", BackPressureConfig::default());
            for i in 0..1000 {
                conn.try_send(make_flowfile(i, 100)).unwrap();
            }
            black_box(conn.count());
        });
    });
}

fn bench_recv_throughput(c: &mut Criterion) {
    c.bench_function("connection_recv_1000", |b| {
        b.iter_with_setup(
            || {
                let conn = FlowConnection::new("bench", BackPressureConfig::default());
                for i in 0..1000 {
                    conn.try_send(make_flowfile(i, 100)).unwrap();
                }
                conn
            },
            |conn| {
                for _ in 0..1000 {
                    black_box(conn.try_recv().unwrap());
                }
            },
        );
    });
}

fn bench_batch_recv(c: &mut Criterion) {
    c.bench_function("connection_batch_recv_100", |b| {
        b.iter_with_setup(
            || {
                let conn = FlowConnection::new("bench", BackPressureConfig::default());
                for i in 0..100 {
                    conn.try_send(make_flowfile(i, 100)).unwrap();
                }
                conn
            },
            |conn| {
                let batch = conn.try_recv_batch(100);
                black_box(batch);
            },
        );
    });
}

fn bench_back_pressure_check(c: &mut Criterion) {
    let conn = FlowConnection::new("bench", BackPressureConfig::new(10000, u64::MAX));
    for i in 0..5000 {
        conn.try_send(make_flowfile(i, 100)).unwrap();
    }

    c.bench_function("connection_back_pressure_check", |b| {
        b.iter(|| {
            black_box(conn.is_back_pressured());
        });
    });
}

fn bench_queue_snapshot(c: &mut Criterion) {
    let conn = FlowConnection::new("bench", BackPressureConfig::default());
    for i in 0..100 {
        conn.try_send(make_flowfile(i, 100)).unwrap();
    }

    c.bench_function("connection_queue_snapshot_100", |b| {
        b.iter(|| {
            let snapshot = conn.queue_snapshot(0, 100);
            black_box(snapshot);
        });
    });
}

criterion_group!(
    benches,
    bench_send_recv,
    bench_send_throughput,
    bench_recv_throughput,
    bench_batch_recv,
    bench_back_pressure_check,
    bench_queue_snapshot,
);
criterion_main!(benches);
