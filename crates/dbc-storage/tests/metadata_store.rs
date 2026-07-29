use dbc_core::{
    driver::ConnectionProfile,
    policy::{AccessMode, Actor, SecurityPolicy},
};
use dbc_storage::{MemorySecretStore, MetadataStore, SecretStore, StoredConnection};

fn profile(id: &str, name: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: id.to_owned(),
        driver_id: "postgres".to_owned(),
        display_name: name.to_owned(),
        endpoint: "postgres://localhost:5432".to_owned(),
        database: Some("app".to_owned()),
        user: Some("developer".to_owned()),
        secret_id: Some(format!("connection:{id}")),
    }
}

#[test]
fn connection_metadata_round_trips_without_secret_material() {
    let store = MetadataStore::open_in_memory().expect("store should open");
    let policy =
        SecurityPolicy::default().with_mode(Actor::Desktop, AccessMode::SafeWrite);
    let connection = StoredConnection {
        profile: profile("primary", "Primary"),
        policy,
    };

    store
        .upsert_connection(&connection)
        .expect("connection should save");
    let loaded = store
        .list_connections()
        .expect("connections should load");

    assert_eq!(loaded, vec![connection]);
    let raw = store
        .raw_connection_payload("primary")
        .expect("raw payload should load")
        .expect("connection should exist");
    assert!(!raw.contains("super-secret-password"));
    assert!(!raw.contains("\"password\""));
}

#[test]
fn upsert_updates_metadata_and_delete_is_idempotent() {
    let store = MetadataStore::open_in_memory().expect("store should open");
    let original = StoredConnection {
        profile: profile("primary", "Old name"),
        policy: SecurityPolicy::default(),
    };
    store
        .upsert_connection(&original)
        .expect("connection should save");

    let updated = StoredConnection {
        profile: profile("primary", "New name"),
        policy: SecurityPolicy::default(),
    };
    store
        .upsert_connection(&updated)
        .expect("connection should update");
    assert_eq!(
        store
            .list_connections()
            .expect("connections should load"),
        vec![updated]
    );

    assert!(store.delete_connection("primary").expect("delete should work"));
    assert!(!store.delete_connection("primary").expect("repeat should work"));
}

#[test]
fn memory_secret_store_has_keychain_semantics() {
    let secrets = MemorySecretStore::new();

    assert_eq!(
        secrets.get("connection:missing").expect("lookup should work"),
        None
    );
    secrets
        .set("connection:primary", "super-secret-password")
        .expect("secret should save");
    assert_eq!(
        secrets
            .get("connection:primary")
            .expect("secret should load")
            .as_deref(),
        Some("super-secret-password")
    );
    assert!(secrets.delete("connection:primary").expect("delete should work"));
    assert!(!secrets.delete("connection:primary").expect("repeat should work"));
}
