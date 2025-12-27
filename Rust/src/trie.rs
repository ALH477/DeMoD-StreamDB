//! Reverse Trie implementation for efficient suffix searches.
//!
//! This module provides a persistent (immutable) trie data structure optimized for
//! suffix-based lookups. Keys are stored in reverse order, allowing O(m + k) suffix
//! searches where m is the suffix length and k is the number of matches.
//!
//! The implementation uses `im::OrdMap` for structural sharing, making updates
//! efficient by only copying the path from root to the modified node.

use im::OrdMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A node in the reverse trie.
///
/// Uses `im::OrdMap` for efficient persistent updates with structural sharing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trie {
    /// Child nodes indexed by byte value
    children: OrdMap<u8, Trie>,
    
    /// Document ID if this node represents a complete key
    value: Option<Uuid>,
    
    /// Number of keys in this subtree (for efficient len())
    count: usize,
}

impl Default for Trie {
    fn default() -> Self {
        Self::new()
    }
}

impl Trie {
    /// Create a new empty trie.
    pub fn new() -> Self {
        Self {
            children: OrdMap::new(),
            value: None,
            count: 0,
        }
    }
    
    /// Insert a key-value pair into the trie.
    ///
    /// Keys are stored in reverse order for suffix search optimization.
    /// Returns a new trie with the key inserted (the original is unchanged).
    ///
    /// # Arguments
    ///
    /// * `key` - The key bytes (will be reversed internally)
    /// * `id` - The document ID to associate with this key
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::Trie;
    /// use uuid::Uuid;
    ///
    /// let trie = Trie::new();
    /// let id = Uuid::new_v4();
    /// let trie = trie.insert(b"hello", id);
    /// assert_eq!(trie.get(b"hello"), Some(id));
    /// ```
    pub fn insert(&self, key: &[u8], id: Uuid) -> Self {
        self.insert_reversed(&reverse(key), id)
    }
    
    /// Internal recursive insert with already-reversed key.
    fn insert_reversed(&self, reversed: &[u8], id: Uuid) -> Self {
        match reversed.split_first() {
            None => {
                // End of key - store value here
                let count_delta = if self.value.is_some() { 0 } else { 1 };
                Self {
                    children: self.children.clone(),
                    value: Some(id),
                    count: self.count + count_delta,
                }
            }
            Some((&byte, rest)) => {
                // Get or create child, then recursively insert
                let child = self.children.get(&byte).cloned().unwrap_or_default();
                let new_child = child.insert_reversed(rest, id);
                let count_delta = new_child.count - child.count;
                
                Self {
                    children: self.children.update(byte, new_child),
                    value: self.value,
                    count: self.count + count_delta,
                }
            }
        }
    }
    
    /// Get the document ID for a key.
    ///
    /// Returns `None` if the key doesn't exist.
    pub fn get(&self, key: &[u8]) -> Option<Uuid> {
        self.get_reversed(&reverse(key))
    }
    
    /// Internal get with already-reversed key.
    fn get_reversed(&self, reversed: &[u8]) -> Option<Uuid> {
        match reversed.split_first() {
            None => self.value,
            Some((&byte, rest)) => {
                self.children.get(&byte)?.get_reversed(rest)
            }
        }
    }
    
    /// Remove a key from the trie.
    ///
    /// Returns `Some(new_trie)` if the key was found and removed,
    /// or `None` if the key didn't exist.
    pub fn remove(&self, key: &[u8]) -> Option<Self> {
        self.remove_reversed(&reverse(key))
    }
    
    /// Internal recursive remove with already-reversed key.
    fn remove_reversed(&self, reversed: &[u8]) -> Option<Self> {
        match reversed.split_first() {
            None => {
                // End of key - remove value
                if self.value.is_some() {
                    Some(Self {
                        children: self.children.clone(),
                        value: None,
                        count: self.count - 1,
                    })
                } else {
                    None // Key didn't exist
                }
            }
            Some((&byte, rest)) => {
                // Recursively remove from child
                let child = self.children.get(&byte)?;
                let new_child = child.remove_reversed(rest)?;
                
                // If child is now empty, remove it entirely
                let new_children = if new_child.is_node_empty() {
                    self.children.without(&byte)
                } else {
                    self.children.update(byte, new_child)
                };
                
                Some(Self {
                    children: new_children,
                    value: self.value,
                    count: self.count - 1,
                })
            }
        }
    }
    
    /// Search for all keys ending with the given suffix.
    ///
    /// Returns a vector of (key, id) pairs for all matching keys.
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::Trie;
    /// use uuid::Uuid;
    ///
    /// let trie = Trie::new()
    ///     .insert(b"user:alice", Uuid::new_v4())
    ///     .insert(b"admin:alice", Uuid::new_v4())
    ///     .insert(b"user:bob", Uuid::new_v4());
    ///
    /// let results = trie.suffix_search(b"alice");
    /// assert_eq!(results.len(), 2);
    /// ```
    pub fn suffix_search(&self, suffix: &[u8]) -> Vec<(Vec<u8>, Uuid)> {
        let reversed_suffix = reverse(suffix);
        let mut results = Vec::new();
        
        // Navigate to the node matching the reversed suffix
        if let Some(node) = self.navigate(&reversed_suffix) {
            // Collect all keys in this subtree
            let mut path = reversed_suffix.clone();
            node.collect_all(&mut path, &mut results);
        }
        
        results
    }
    
    /// Navigate to a node following the given path.
    fn navigate(&self, path: &[u8]) -> Option<&Self> {
        match path.split_first() {
            None => Some(self),
            Some((&byte, rest)) => {
                self.children.get(&byte)?.navigate(rest)
            }
        }
    }
    
    /// Collect all key-value pairs in this subtree.
    fn collect_all(&self, current_path: &mut Vec<u8>, results: &mut Vec<(Vec<u8>, Uuid)>) {
        // If this node has a value, add it to results
        if let Some(id) = self.value {
            // Reverse the path back to get the original key
            let key: Vec<u8> = current_path.iter().rev().copied().collect();
            results.push((key, id));
        }
        
        // Recurse into children
        for (&byte, child) in &self.children {
            current_path.push(byte);
            child.collect_all(current_path, results);
            current_path.pop();
        }
    }
    
    /// Iterate over all key-value pairs.
    ///
    /// The callback receives (key, id) for each entry. Return `true` to continue,
    /// `false` to stop early.
    pub fn for_each<F>(&self, callback: &mut F)
    where
        F: FnMut(&[u8], Uuid) -> bool,
    {
        let mut path = Vec::new();
        self.for_each_internal(&mut path, callback, &mut true);
    }
    
    fn for_each_internal<F>(&self, path: &mut Vec<u8>, callback: &mut F, continue_flag: &mut bool)
    where
        F: FnMut(&[u8], Uuid) -> bool,
    {
        if !*continue_flag {
            return;
        }
        
        if let Some(id) = self.value {
            let key: Vec<u8> = path.iter().rev().copied().collect();
            *continue_flag = callback(&key, id);
        }
        
        if *continue_flag {
            for (&byte, child) in &self.children {
                path.push(byte);
                child.for_each_internal(path, callback, continue_flag);
                path.pop();
                
                if !*continue_flag {
                    break;
                }
            }
        }
    }
    
    /// Get the number of keys in the trie.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }
    
    /// Check if the trie is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    
    /// Check if this node is empty (no value, no children).
    #[inline]
    fn is_node_empty(&self) -> bool {
        self.value.is_none() && self.children.is_empty()
    }
}

/// Reverse a byte slice.
#[inline]
fn reverse(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().rev().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_insert_and_get() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"hello", id1)
            .insert(b"world", id2);
        
        assert_eq!(trie.get(b"hello"), Some(id1));
        assert_eq!(trie.get(b"world"), Some(id2));
        assert_eq!(trie.get(b"other"), None);
        assert_eq!(trie.len(), 2);
    }
    
    #[test]
    fn test_update() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"key", id1)
            .insert(b"key", id2);
        
        assert_eq!(trie.get(b"key"), Some(id2));
        assert_eq!(trie.len(), 1); // Count shouldn't increase on update
    }
    
    #[test]
    fn test_remove() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"hello", id1)
            .insert(b"world", id2);
        
        // Remove existing key
        let trie = trie.remove(b"hello").unwrap();
        assert_eq!(trie.get(b"hello"), None);
        assert_eq!(trie.get(b"world"), Some(id2));
        assert_eq!(trie.len(), 1);
        
        // Remove non-existing key
        assert!(trie.remove(b"nonexistent").is_none());
    }
    
    #[test]
    fn test_suffix_search() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();
        let id4 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"user:alice", id1)
            .insert(b"admin:alice", id2)
            .insert(b"user:bob", id3)
            .insert(b"guest:charlie", id4);
        
        // Search for "alice"
        let results = trie.suffix_search(b"alice");
        assert_eq!(results.len(), 2);
        
        let ids: Vec<_> = results.iter().map(|(_, id)| *id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
        
        // Search for "bob"
        let results = trie.suffix_search(b"bob");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, id3);
        
        // Search for non-existing suffix
        let results = trie.suffix_search(b"xyz");
        assert!(results.is_empty());
    }
    
    #[test]
    fn test_suffix_search_partial() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"car", id1)
            .insert(b"cart", id2);
        
        // Search for "ar" should match both
        let results = trie.suffix_search(b"ar");
        assert_eq!(results.len(), 1); // Only "car" ends with "ar", "cart" ends with "art"
        
        // Search for "art" should match only "cart"
        let results = trie.suffix_search(b"art");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"cart");
    }
    
    #[test]
    fn test_binary_keys() {
        let id = Uuid::new_v4();
        let key = vec![0x00, 0x01, 0xFF, 0xFE, 0x00];
        
        let trie = Trie::new().insert(&key, id);
        
        assert_eq!(trie.get(&key), Some(id));
        assert_eq!(trie.len(), 1);
    }
    
    #[test]
    fn test_for_each() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"a", id1)
            .insert(b"b", id2)
            .insert(b"c", id3);
        
        let mut count = 0;
        trie.for_each(&mut |_, _| {
            count += 1;
            true
        });
        assert_eq!(count, 3);
        
        // Test early termination
        count = 0;
        trie.for_each(&mut |_, _| {
            count += 1;
            count < 2 // Stop after 2
        });
        assert_eq!(count, 2);
    }
    
    #[test]
    fn test_structural_sharing() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        
        let trie1 = Trie::new().insert(b"hello", id1);
        let trie2 = trie1.insert(b"world", id2);
        
        // Original trie should still work
        assert_eq!(trie1.get(b"hello"), Some(id1));
        assert_eq!(trie1.get(b"world"), None);
        assert_eq!(trie1.len(), 1);
        
        // New trie should have both
        assert_eq!(trie2.get(b"hello"), Some(id1));
        assert_eq!(trie2.get(b"world"), Some(id2));
        assert_eq!(trie2.len(), 2);
    }
    
    #[test]
    fn test_empty_trie() {
        let trie = Trie::new();
        
        assert!(trie.is_empty());
        assert_eq!(trie.len(), 0);
        assert_eq!(trie.get(b"anything"), None);
        assert!(trie.remove(b"anything").is_none());
        assert!(trie.suffix_search(b"anything").is_empty());
    }
    
    #[test]
    fn test_prefix_keys() {
        // Test keys that are prefixes of each other
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();
        
        let trie = Trie::new()
            .insert(b"a", id1)
            .insert(b"ab", id2)
            .insert(b"abc", id3);
        
        assert_eq!(trie.get(b"a"), Some(id1));
        assert_eq!(trie.get(b"ab"), Some(id2));
        assert_eq!(trie.get(b"abc"), Some(id3));
        assert_eq!(trie.len(), 3);
        
        // Remove middle key
        let trie = trie.remove(b"ab").unwrap();
        assert_eq!(trie.get(b"a"), Some(id1));
        assert_eq!(trie.get(b"ab"), None);
        assert_eq!(trie.get(b"abc"), Some(id3));
        assert_eq!(trie.len(), 2);
    }
}
