use super::records::{decode_record, encode_record};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ExampleRecord {
    value: u64,
}

#[test]
fn durable_records_reject_unknown_schema_versions() {
    let bytes = encode_record(&ExampleRecord { value: 42 }).unwrap();
    assert_eq!(bytes[0], 1);
    assert_eq!(
        decode_record::<ExampleRecord>(&bytes).unwrap(),
        ExampleRecord { value: 42 }
    );

    let mut superseded = bytes.clone();
    superseded[0] = 0;
    assert!(decode_record::<ExampleRecord>(&superseded).is_err());

    let mut future = bytes;
    future[0] = 2;
    assert!(decode_record::<ExampleRecord>(&future).is_err());
}
