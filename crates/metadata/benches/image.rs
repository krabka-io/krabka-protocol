//! `CodSpeed` microbenchmarks for `krabka-metadata`.
//!
//! Covers the hot paths for the Raft state machine and for request handlers:
//!
//! - `MetadataImage::apply` for the common record kinds.
//! - `MetadataImage::partitions_of` and `all_partitions`: per-topic lookups
//!   and the reconcile-loop scan, at many-topic cluster scale.
//! - `MetadataImage::matching_acls`, called once per `authorize()`.
//! - `MetadataRecord` serialize and deserialize with wincode.

use std::{collections::BTreeMap, env};

use criterion::{BatchSize, Criterion, abs_file, black_box, criterion_main};
use krabka_metadata::{
    AclEntry, AclOperation, BrokerEndpoint, BrokerRegistrationRecord, DeleteTopicRecord,
    LeaderEpoch, LeaderRecoveryState, MetadataImage, MetadataRecord, NodeId, PartitionElrRecord,
    PartitionOffsetAdvanceRecord, PartitionRecord, PartitionRecoveryRecord, PatternType,
    PermissionType, ResourceType, TopicRecord,
};
use krabka_security::ListenerProtocol;
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::{Deserialize as _, Serialize as _, WriteResult};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn topic_record(name: &str, parts: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.to_owned(),
        topic_id: Uuid::nil(),
        partitions: parts,
        replication_factor: 3,
    })
}

fn partition_record(topic: &str, p: i32) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_owned(),
        partition: p,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

fn broker_record(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        node_id: NodeId(node_id),
        broker_epoch: 0,
        incarnation_id: Uuid::nil(),
        host: format!("broker-{node_id}.example.com"),
        port: 9092,
        rack: Some("us-east-1a".to_owned()),
        log_dirs: vec![],
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        endpoints: vec![BrokerEndpoint {
            name: "PLAINTEXT".to_owned(),
            host: format!("broker-{node_id}"),
            port: 9092,
            protocol: ListenerProtocol::Plaintext,
        }],
        features: BTreeMap::new(),
    })
}

fn acl_entry(name: &str, prefixed: bool) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: name.to_owned(),
        pattern_type: if prefixed {
            PatternType::Prefixed
        } else {
            PatternType::Literal
        },
        principal: "User:alice".to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }
}

fn image_with_acls(num_literal: usize, num_prefixed: usize) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for i in 0..num_literal {
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_entry(
            &format!("topic-{i:04}"),
            false,
        )));
    }
    for i in 0..num_prefixed {
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_entry(
            &format!("prefix-{i:02}-"),
            true,
        )));
    }
    img
}

// ---------------------------------------------------------------------------
// MetadataImage::apply — bulk replay of records.
// ---------------------------------------------------------------------------

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/apply");

    let topics: Vec<MetadataRecord> = (0..100)
        .map(|i| topic_record(&format!("t-{i}"), 8))
        .collect();
    let partitions: Vec<MetadataRecord> = (0..100)
        .flat_map(|t| (0..8).map(move |p| partition_record(&format!("t-{t}"), p)))
        .collect();
    let brokers: Vec<MetadataRecord> = (0..16).map(broker_record).collect();

    group.bench_function("replay_100_topics", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&topics) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    group.bench_function("replay_100topics_800partitions", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&topics) {
                img.apply(r);
            }
            for r in black_box(&partitions) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    group.bench_function("replay_16_brokers", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&brokers) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    let single_topic = topic_record("t-0", 8);
    group.bench_function("single_topic", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            img.apply(black_box(&single_topic));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// partitions_of / all_partitions — the per-topic and reconcile hot paths.
//
// Pins the scaling contract for many-topic clusters (the Gres per-tenant-topic
// model): per-topic lookups must stay flat as the topic count grows (the
// 1k-topic and 10k-topic `partitions_of` numbers must match — a regression to
// a full-map filter scan makes the 10k case ~10× slower), and one
// reconcile-shaped pass over the whole image must stay O(P), not O(topics×P).
// ---------------------------------------------------------------------------

fn image_with_single_partition_topics(n: usize) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for i in 0..n {
        let name = format!("tenant-topic-{i:05}");
        img.apply(&topic_record(&name, 1));
        img.apply(&partition_record(&name, 0));
    }
    img
}

fn bench_partitions_by_topic(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/partitions_by_topic");

    for n in [1_000usize, 10_000] {
        let img = image_with_single_partition_topics(n);
        let probe = format!("tenant-topic-{:05}", n / 2);

        group.bench_function(format!("partitions_of/{n}_topics"), |b| {
            b.iter(|| img.partitions_of(black_box(&probe)).count());
        });
        group.bench_function(format!("topic_partition_count/{n}_topics"), |b| {
            b.iter(|| black_box(&img).topic_partition_count(black_box(&probe)));
        });
        // The replication supervisor's desired-follower-set shape: one pass
        // over every partition per metadata-image change.
        group.bench_function(format!("reconcile_scan/{n}_topics"), |b| {
            b.iter(|| {
                black_box(&img)
                    .all_partitions()
                    .filter(|p| p.replicas.contains(&NodeId(2)) && p.leader != NodeId(2))
                    .count()
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// matching_acls — the authorize() hot path.
// ---------------------------------------------------------------------------

fn bench_matching_acls(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/matching_acls");

    let img_small = image_with_acls(10, 4);
    let img_large = image_with_acls(1000, 50);

    group.bench_function("small_literal_hit", |b| {
        b.iter(|| {
            let count = img_small
                .matching_acls(ResourceType::Topic, black_box("topic-0005"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("small_no_match", |b| {
        b.iter(|| {
            let count = img_small
                .matching_acls(ResourceType::Topic, black_box("nope"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("large_literal_hit", |b| {
        b.iter(|| {
            let count = img_large
                .matching_acls(ResourceType::Topic, black_box("topic-0500"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("large_prefix_hit", |b| {
        b.iter(|| {
            let count = img_large
                .matching_acls(ResourceType::Topic, black_box("prefix-25-xyz"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("all_acls_iter_large", |b| {
        b.iter(|| black_box(&img_large).all_acls().count());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Serialization (wincode via serde-wincode).
// ---------------------------------------------------------------------------

/// Encodes the three serde fixture records, in topic, partition, broker order.
fn encode_fixtures(records: [&MetadataRecord; 3]) -> WriteResult<[Vec<u8>; 3]> {
    let [topic, partition, broker] = records;
    Ok([
        <SerdeCompat<MetadataRecord>>::serialize(topic)?,
        <SerdeCompat<MetadataRecord>>::serialize(partition)?,
        <SerdeCompat<MetadataRecord>>::serialize(broker)?,
    ])
}

fn bench_record_serde(c: &mut Criterion) {
    let topic = topic_record("benchmarks", 32);
    let partition = partition_record("benchmarks", 5);
    let broker = broker_record(7);

    let [topic_bytes, partition_bytes, broker_bytes] =
        match encode_fixtures([&topic, &partition, &broker]) {
            Ok(encoded) => encoded,
            Err(error) => {
                eprintln!("metadata_record/serde: encoding a fixture record failed: {error}");
                return;
            }
        };

    let mut group = c.benchmark_group("metadata_record/serde");
    group.bench_function("serialize_topic", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&topic)));
    });
    group.bench_function("serialize_partition", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&partition)));
    });
    group.bench_function("serialize_broker", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&broker)));
    });
    group.bench_function("deserialize_topic", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&topic_bytes)));
    });
    group.bench_function("deserialize_partition", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&partition_bytes)));
    });
    group.bench_function("deserialize_broker", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&broker_bytes)));
    });

    group.finish();
}

fn bench_delete_topic_auxiliary_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_topic_auxiliary_state");
    for topics in [100, 1_000, 10_000] {
        let mut image = MetadataImage::new(Uuid::nil());
        let mut deletes = Vec::new();
        for topic in 0..topics {
            let name = format!("topic-{topic}");
            deletes.push(MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: name.clone(),
            }));
            for partition in 0..10 {
                image.apply(&MetadataRecord::V1PartitionOffsetAdvance(
                    PartitionOffsetAdvanceRecord {
                        topic: name.clone(),
                        partition,
                        count: 7,
                    },
                ));
                image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
                    topic: name.clone(),
                    partition,
                    eligible_leader_replicas: vec![NodeId(1)],
                    last_known_elr: vec![NodeId(2)],
                }));
                image.apply(&MetadataRecord::V1PartitionRecovery(
                    PartitionRecoveryRecord {
                        topic: name.clone(),
                        partition,
                        state: LeaderRecoveryState::Recovering,
                    },
                ));
            }
        }
        group.bench_function(format!("{topics}_topics"), |b| {
            b.iter_batched(
                || image.clone(),
                |mut image| {
                    for delete in &deletes {
                        image.apply(black_box(delete));
                    }
                    black_box(image);
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (&'static str, fn(&mut Criterion));

/// Every benchmark target, in run order.
const TARGETS: [Target; 5] = [
    ("bench_apply", bench_apply),
    (
        "bench_delete_topic_auxiliary_state",
        bench_delete_topic_auxiliary_state,
    ),
    ("bench_partitions_by_topic", bench_partitions_by_topic),
    ("bench_matching_acls", bench_matching_acls),
    ("bench_record_serde", bench_record_serde),
];

/// Runs every target in [`TARGETS`].
fn run_targets(criterion: &mut Criterion) {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion);
    }
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it, so that the `Criterion` it configures drops as soon as the
/// targets have run.
fn benches() {
    run_targets(&mut Criterion::default().configure_from_args());
}

criterion_main!(benches);
