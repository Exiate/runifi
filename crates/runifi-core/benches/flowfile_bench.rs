use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use runifi_plugin_api::flowfile::{ContentClaim, FlowFile};

fn make_flowfile(id: u64) -> FlowFile {
    FlowFile {
        id,
        attributes: Vec::new(),
        content_claim: None,
        size: 0,
        created_at_nanos: 0,
        lineage_start_id: id,
        penalized_until_nanos: 0,
    }
}

fn bench_flowfile_creation(c: &mut Criterion) {
    c.bench_function("flowfile_creation", |b| {
        let mut id = 0u64;
        b.iter(|| {
            id += 1;
            black_box(make_flowfile(id));
        });
    });
}

fn bench_flowfile_creation_with_attributes(c: &mut Criterion) {
    c.bench_function("flowfile_creation_with_attrs", |b| {
        let mut id = 0u64;
        let key1: Arc<str> = Arc::from("filename");
        let key2: Arc<str> = Arc::from("mime.type");
        let key3: Arc<str> = Arc::from("path");
        b.iter(|| {
            id += 1;
            let mut ff = make_flowfile(id);
            ff.set_attribute(key1.clone(), Arc::from("test.txt"));
            ff.set_attribute(key2.clone(), Arc::from("text/plain"));
            ff.set_attribute(key3.clone(), Arc::from("/data"));
            black_box(ff);
        });
    });
}

fn bench_attribute_lookup(c: &mut Criterion) {
    let mut ff = make_flowfile(1);
    for i in 0..16 {
        ff.set_attribute(
            Arc::from(format!("attr-{}", i).as_str()),
            Arc::from(format!("value-{}", i).as_str()),
        );
    }

    c.bench_function("attribute_lookup_16_attrs", |b| {
        b.iter(|| {
            black_box(ff.get_attribute("attr-15"));
        });
    });
}

fn bench_attribute_set(c: &mut Criterion) {
    c.bench_function("attribute_set", |b| {
        let key: Arc<str> = Arc::from("filename");
        let value: Arc<str> = Arc::from("test.txt");
        let mut ff = make_flowfile(1);
        b.iter(|| {
            ff.set_attribute(key.clone(), value.clone());
            black_box(&ff);
        });
    });
}

fn bench_flowfile_clone(c: &mut Criterion) {
    let mut ff = make_flowfile(1);
    ff.size = 1024;
    ff.content_claim = Some(ContentClaim {
        resource_id: 1,
        offset: 0,
        length: 1024,
    });
    for i in 0..8 {
        ff.set_attribute(
            Arc::from(format!("attr-{}", i).as_str()),
            Arc::from(format!("value-{}", i).as_str()),
        );
    }

    c.bench_function("flowfile_clone_8_attrs", |b| {
        b.iter(|| {
            black_box(ff.clone());
        });
    });
}

fn bench_id_generator_throughput(c: &mut Criterion) {
    use runifi_core::id::IdGenerator;

    let id_gen = IdGenerator::new();
    c.bench_function("id_generator", |b| {
        b.iter(|| {
            black_box(id_gen.next_id());
        });
    });
}

criterion_group!(
    benches,
    bench_flowfile_creation,
    bench_flowfile_creation_with_attributes,
    bench_attribute_lookup,
    bench_attribute_set,
    bench_flowfile_clone,
    bench_id_generator_throughput,
);
criterion_main!(benches);
