//! Text processing operations for Synor.
//!
//! This crate provides text processing functionality including:
//! - Text splitting by separators
//! - Recursive text chunking with syntax awareness
//!
//! Language detection / the tree-sitter registry and the shared
//! [`CodeSource`](synor_code_ast::CodeSource) input type live in the
//! `synor_code_ast` crate.

pub mod pattern_matcher;
pub mod split;
