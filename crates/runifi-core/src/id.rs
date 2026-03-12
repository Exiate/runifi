use std::sync::atomic::{AtomicU64, Ordering};

/// A thread-safe monotonic ID generator.
///
/// Uses a simple atomic counter for zero-contention ID allocation.
/// IDs start at 1 (0 is reserved as "no ID").
pub struct IdGenerator {
    next: AtomicU64,
}

impl IdGenerator {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next unique ID.
    pub fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_monotonic() {
        let id_gen = IdGenerator::new();
        assert_eq!(id_gen.next_id(), 1);
        assert_eq!(id_gen.next_id(), 2);
        assert_eq!(id_gen.next_id(), 3);
    }

    #[test]
    fn ids_are_unique_across_threads() {
        use std::sync::Arc;

        let id_gen = Arc::new(IdGenerator::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let id_gen = id_gen.clone();
            handles.push(std::thread::spawn(move || {
                (0..1000).map(|_| id_gen.next_id()).collect::<Vec<_>>()
            }));
        }

        let mut all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), 4000);
    }
}
