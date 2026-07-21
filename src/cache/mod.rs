mod recent_use;
mod service;
mod space;
mod stripes;

#[cfg(test)]
mod recent_use_test;
#[cfg(test)]
mod space_test;

pub use crate::artifact::StoredEncoding;
pub use crate::storage::{
    local::LocalError,
    metadata::{Durability, MetadataError, ReferenceRecord},
    records::RecordError,
};
pub(crate) use service::CacheDependencies;
pub use service::{
    Admission, CacheError, CacheService, LocatedArtifact, Publication, PublicationOutcome,
    PublicationTarget, PublishRequest,
};
pub(crate) use space::Mode;
pub use space::{FreeSpace, SpaceLedger, SpacePolicy, SpaceSnapshot, StatvfsFreeSpace};
