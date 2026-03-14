use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;

fn bench_create_small(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 64]); // 64 bytes — micro profile

    c.bench_function("content_create_64B", |b| {
        b.iter(|| {
            black_box(repo.create(data.clone()).unwrap());
        });
    });
}

fn bench_create_5kb(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 5120]); // 5 KB — micro profile

    c.bench_function("content_create_5KB", |b| {
        b.iter(|| {
            black_box(repo.create(data.clone()).unwrap());
        });
    });
}

fn bench_read_small(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 64]);
    let claim = repo.create(data).unwrap();

    c.bench_function("content_read_64B", |b| {
        b.iter(|| {
            black_box(repo.read(&claim).unwrap());
        });
    });
}

fn bench_read_5kb(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 5120]);
    let claim = repo.create(data).unwrap();

    c.bench_function("content_read_5KB", |b| {
        b.iter(|| {
            black_box(repo.read(&claim).unwrap());
        });
    });
}

fn bench_create_and_read_cycle(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 1024]);

    c.bench_function("content_create_read_cycle_1KB", |b| {
        b.iter(|| {
            let claim = repo.create(data.clone()).unwrap();
            let read_back = repo.read(&claim).unwrap();
            black_box(read_back);
            repo.decrement_ref(claim.resource_id).unwrap();
        });
    });
}

fn bench_ref_counting(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0u8; 64]);
    let claim = repo.create(data).unwrap();

    c.bench_function("content_increment_ref", |b| {
        b.iter(|| {
            repo.increment_ref(claim.resource_id).unwrap();
        });
    });
}

fn bench_zero_copy_slice(c: &mut Criterion) {
    let repo = InMemoryContentRepository::new();
    let data = Bytes::from(vec![0xABu8; 1024 * 1024]); // 1 MB
    let claim = repo.create(data).unwrap();

    let slice_claim = runifi_plugin_api::ContentClaim {
        resource_id: claim.resource_id,
        offset: 512 * 1024, // Middle of the buffer.
        length: 1024,
    };

    c.bench_function("content_zero_copy_slice_1KB_from_1MB", |b| {
        b.iter(|| {
            black_box(repo.read(&slice_claim).unwrap());
        });
    });
}

criterion_group!(
    benches,
    bench_create_small,
    bench_create_5kb,
    bench_read_small,
    bench_read_5kb,
    bench_create_and_read_cycle,
    bench_ref_counting,
    bench_zero_copy_slice,
);
criterion_main!(benches);
