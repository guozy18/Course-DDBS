use std::collections::HashMap;

#[derive(Default, Debug)]
/// The `SymbolTable` contains two (redundant) data structures:
/// - `symbols` is a `Vec`, recording the insertion order;
/// - `lookup` is a `HashMap`, supporting fast name resolution.
pub struct SymbolTable {
    /// The symbols.
    symbols: Vec<(String, String)>,
    /// Maps the identifier names to the positions in the `symbols` vector.
    lookup: HashMap<String, usize>,
}

impl SymbolTable {
    /// Create a new, empty symbol table.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    pub fn len(&self) -> usize {
        self.symbols.len()
    }

    /// Return an iterator of all symbols in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &(String, String)> {
        self.symbols.iter()
    }

    /// Insert a new symbol. If the symbol already exists, return `Some(old)`;
    pub fn insert(&mut self, name: String, alias: String) -> Option<String> {
        let pos = self.symbols.len();
        self.symbols.push((name.clone(), alias));
        self.lookup
            .insert(name, pos)
            .map(|old| self.symbols[old].1.clone())
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, name: &str) -> Option<(String, String)> {
        if let Some(pos) = self.lookup.remove(name) {
            let res = self.symbols.remove(pos);
            for (_, position) in self.lookup.iter_mut() {
                if *position > pos {
                    *position -= 1;
                }
            }
            Some(res)
        } else {
            None
        }
    }

    /// Find a symbol.
    pub fn get(&self, name: &str) -> Option<String> {
        self.lookup
            .get(name)
            .map(|pos| self.symbols[*pos].1.clone())
    }

    pub fn get_index(&self, index: usize) -> Option<String> {
        self.symbols.get(index).map(|x| x.clone().0)
    }

    /// Clear the symbol table.
    pub fn clear(&mut self) {
        self.symbols.clear();
        self.lookup.clear();
    }

    /// Compare two symbol tables.
    pub fn compare_symbols(&self, others: &[(String, String)]) -> bool {
        if self.symbols.len() != others.len() {
            return false;
        }
        for ((name1, _), (name2, _)) in self.iter().zip(others.iter()) {
            if name1 != name2 {
                return false;
            }
        }
        true
    }

    /// Clear the symbol table with default value and return the original one.
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Try to merge two symbol tables.
    /// Return 'None' if no conflict, otherwise return the conflicted symbol name.
    pub fn merge(&mut self, other: SymbolTable) -> Option<String> {
        for (name, var) in other.iter() {
            if self.insert(name.clone(), var.clone()).is_some() {
                return Some(name.clone());
            }
        }
        None
    }
}
