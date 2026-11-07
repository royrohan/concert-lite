//! The coordinator's workflows. Their type names, updates, queries and inputs are the Java contract.

pub mod ingest;
pub mod key_lock;

pub use ingest::IngestSupervisor;
pub use key_lock::KeyLock;
