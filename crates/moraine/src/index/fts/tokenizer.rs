//! Tokenizers of the full-text kind, referenced by name from index specs (cases
//! F3 and F11).
//!
//! A spec stores a tokenizer's name, and the name is part of the spec hash, so a name must mean
//! the same tokenisation in every client. The built-in names are fixed and cannot be
//! registered again; a custom tokenizer is registered on the [`crate::DatabaseBuilder`] under
//! the same name by every client that builds or searches the index. A name no client knows is
//! [`crate::Error::TokenizerNotRegistered`], never a silent fallback.
//!
//! | Name | Tokenisation |
//! |---|---|
//! | `default` | Words (runs of letters and digits), longer than 40 bytes dropped, lowercased |
//! | `raw` | The whole value as one token |
//! | `whitespace` | Split on whitespace, lowercased |
//! | `en_stem`, `de_stem`, ... | `default`, then the Snowball stemmer of the language |
//! | `ngram2`, `ngram3` | Lowercased character bigrams or trigrams (languages without spaces) |

use std::collections::HashMap;
use std::sync::RwLock;

use tantivy::tokenizer::{
    Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer,
    TextAnalyzer, TokenizerManager, WhitespaceTokenizer,
};

use crate::error::{Error, Result};

/// The stemming tokenizers: name and Snowball language.
const STEMMERS: &[(&str, Language)] = &[
    ("ar_stem", Language::Arabic),
    ("da_stem", Language::Danish),
    ("de_stem", Language::German),
    ("el_stem", Language::Greek),
    ("en_stem", Language::English),
    ("es_stem", Language::Spanish),
    ("fi_stem", Language::Finnish),
    ("fr_stem", Language::French),
    ("hu_stem", Language::Hungarian),
    ("it_stem", Language::Italian),
    ("nl_stem", Language::Dutch),
    ("no_stem", Language::Norwegian),
    ("pt_stem", Language::Portuguese),
    ("ro_stem", Language::Romanian),
    ("ru_stem", Language::Russian),
    ("sv_stem", Language::Swedish),
    ("ta_stem", Language::Tamil),
    ("tr_stem", Language::Turkish),
];

/// Names of the other built-in tokenizers.
const PLAIN: &[&str] = &["default", "raw", "whitespace", "ngram2", "ngram3"];

/// Whether `name` is a built-in tokenizer.
pub fn is_builtin(name: &str) -> bool {
    PLAIN.contains(&name) || STEMMERS.iter().any(|(n, _)| *n == name)
}

/// The names of every built-in tokenizer.
pub fn builtin_names() -> Vec<&'static str> {
    PLAIN
        .iter()
        .copied()
        .chain(STEMMERS.iter().map(|(n, _)| *n))
        .collect()
}

fn builtin(name: &str) -> Option<TextAnalyzer> {
    match name {
        "default" => Some(
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .build(),
        ),
        "raw" => Some(TextAnalyzer::from(RawTokenizer::default())),
        "whitespace" => Some(
            TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build(),
        ),
        "ngram2" | "ngram3" => {
            let n = if name == "ngram2" { 2 } else { 3 };
            let t = NgramTokenizer::new(n, n, false).ok()?;
            Some(TextAnalyzer::builder(t).filter(LowerCaser).build())
        }
        _ => STEMMERS.iter().find(|(n, _)| *n == name).map(|(_, lang)| {
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(*lang))
                .build()
        }),
    }
}

/// The tokenizers one database knows: the built-ins and the ones registered on its builder.
#[derive(Default)]
pub struct Tokenizers {
    custom: RwLock<HashMap<String, TextAnalyzer>>,
}

impl std::fmt::Debug for Tokenizers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<String> = self
            .custom
            .read()
            .map(|g| g.keys().cloned().collect())
            .unwrap_or_default();
        f.debug_struct("Tokenizers")
            .field("custom", &names)
            .finish()
    }
}

impl Tokenizers {
    /// Registers `analyzer` under `name`. Built-in names are refused: their meaning is fixed.
    pub fn register(&self, name: &str, analyzer: TextAnalyzer) -> Result<()> {
        if name.is_empty() || name.len() > 64 || !name.bytes().all(|c| c.is_ascii_graphic()) {
            return Err(Error::InvalidConfig(format!(
                "tokenizer name {name:?} must be 1 to 64 printable ASCII bytes"
            )));
        }
        if is_builtin(name) {
            return Err(Error::InvalidConfig(format!(
                "tokenizer {name:?} is built in and cannot be registered again"
            )));
        }
        let mut g = self.custom.write().unwrap_or_else(|p| p.into_inner());
        g.insert(name.to_string(), analyzer);
        Ok(())
    }

    /// The tokenizer `name`, or [`Error::TokenizerNotRegistered`].
    pub fn get(&self, name: &str) -> Result<TextAnalyzer> {
        if let Some(t) = builtin(name) {
            return Ok(t);
        }
        self.custom
            .read()
            .ok()
            .and_then(|g| g.get(name).cloned())
            .ok_or_else(|| Error::TokenizerNotRegistered(self.not_found(name)))
    }

    /// A not-found error for `name` that lists the registered tokenizers.
    pub(crate) fn not_found(&self, name: &str) -> crate::names::NotFound {
        let mut names: Vec<String> = builtin_names().into_iter().map(String::from).collect();
        if let Ok(g) = self.custom.read() {
            names.extend(g.keys().cloned());
        }
        crate::names::NotFound::among(name, names)
    }

    /// Whether `name` is known.
    pub fn knows(&self, name: &str) -> bool {
        is_builtin(name)
            || self
                .custom
                .read()
                .map(|g| g.contains_key(name))
                .unwrap_or(false)
    }

    /// A tantivy tokenizer manager holding `names`, and `default`, which tantivy looks up for
    /// fields that are not text.
    pub fn manager<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<TokenizerManager> {
        let m = TokenizerManager::new();
        m.register("default", self.get("default")?);
        for n in names {
            m.register(n, self.get(n)?);
        }
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(t: &Tokenizers, name: &str, text: &str) -> Vec<String> {
        let mut a = t.get(name).unwrap();
        let mut s = a.token_stream(text);
        let mut out = Vec::new();
        while let Some(tok) = s.next() {
            out.push(tok.text.clone());
        }
        out
    }

    #[test]
    fn builtins() {
        let t = Tokenizers::default();
        assert_eq!(
            tokens(&t, "default", "Object-Storage DBs"),
            ["object", "storage", "dbs"]
        );
        assert_eq!(tokens(&t, "raw", "Object Storage"), ["Object Storage"]);
        assert_eq!(tokens(&t, "en_stem", "running writers"), ["run", "writer"]);
        assert_eq!(tokens(&t, "de_stem", "Häuser"), ["haus"]);
        assert_eq!(tokens(&t, "ngram2", "東京都"), ["東京", "京都"]);
        assert_eq!(tokens(&t, "whitespace", "A-b C"), ["a-b", "c"]);
        for n in builtin_names() {
            assert!(t.get(n).is_ok(), "{n}");
        }
    }

    #[test]
    fn registry() {
        let t = Tokenizers::default();
        assert!(matches!(
            t.get("mine"),
            Err(Error::TokenizerNotRegistered(_))
        ));
        assert!(
            t.register("default", TextAnalyzer::from(RawTokenizer::default()))
                .is_err()
        );
        assert!(
            t.register("", TextAnalyzer::from(RawTokenizer::default()))
                .is_err()
        );
        t.register("mine", TextAnalyzer::from(RawTokenizer::default()))
            .unwrap();
        assert!(t.knows("mine"));
        assert_eq!(tokens(&t, "mine", "A B"), ["A B"]);
        assert!(t.manager(["mine", "default"]).is_ok());
        assert!(t.manager(["nope"]).is_err());
    }
}
