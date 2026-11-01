//! The head each attached volume's writer last published on this node.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use swarmy_core::{ManifestId, VolumeId};

/// Shared between volume metadata and the tool path.
#[derive(Clone, Default)]
pub(crate) struct Heads(Arc<Mutex<BTreeMap<VolumeId, ManifestId>>>);
impl Heads {
    pub(crate) fn set(&self, volume: VolumeId, head: ManifestId) {
        self.0
            .lock()
            .expect("heads lock poisoned")
            .insert(volume, head);
    }
    pub(crate) fn get(&self, volume: VolumeId) -> Option<ManifestId> {
        self.0
            .lock()
            .expect("heads lock poisoned")
            .get(&volume)
            .copied()
    }
    pub(crate) fn forget(&self, volume: VolumeId) {
        self.0.lock().expect("heads lock poisoned").remove(&volume);
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn heads_follow_set_and_forget() {
        let heads = Heads::default();
        let volume = VolumeId::from_ulid(ulid::Ulid::generate());
        let head = ManifestId::from_ulid(ulid::Ulid::generate());
        assert_eq!(heads.get(volume), None);
        heads.set(volume, head);
        let shared = heads.clone();
        assert_eq!(shared.get(volume), Some(head));
        heads.forget(volume);
        assert_eq!(heads.get(volume), None);
    }
}
