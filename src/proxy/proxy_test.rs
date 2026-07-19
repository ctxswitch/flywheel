use super::RequestFlight;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn last_paired_request_removes_the_flight_entry() {
    let requests = DashMap::new();
    let key = "00000000000000000000000000:proxy:test".to_owned();
    requests.insert(key.clone(), Arc::new(Mutex::new(())));

    let lock = Arc::clone(requests.get(&key).unwrap().value());
    let flight = RequestFlight {
        requests: &requests,
        key: key.clone(),
    };
    let guard = lock.lock().await;

    drop(guard);
    drop(flight);

    assert!(!requests.contains_key(&key));
    drop(lock);
}

#[tokio::test]
async fn flight_entry_stays_while_another_paired_waiter_exists() {
    let requests = DashMap::new();
    let key = "00000000000000000000000000:proxy:test".to_owned();
    requests.insert(key.clone(), Arc::new(Mutex::new(())));

    let first_lock = Arc::clone(requests.get(&key).unwrap().value());
    let first_flight = RequestFlight {
        requests: &requests,
        key: key.clone(),
    };
    let first_guard = first_lock.lock().await;

    // The second request owns both parts of the invariant while it waits: an Arc
    // clone and a flight that will retry removal when this request finishes.
    let second_lock = Arc::clone(requests.get(&key).unwrap().value());
    let second_flight = RequestFlight {
        requests: &requests,
        key: key.clone(),
    };

    drop(first_guard);
    drop(first_flight);
    assert!(requests.contains_key(&key));
    drop(first_lock);

    let second_guard = second_lock.lock().await;
    drop(second_guard);
    drop(second_flight);
    assert!(!requests.contains_key(&key));
    drop(second_lock);
}
