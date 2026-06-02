// Allow pre-existing clippy lints that are not introduced by the current feature.
#![allow(clippy::ptr_arg)]
#![allow(clippy::print_literal)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::while_let_on_iterator)]
#![allow(clippy::manual_ignore_case_cmp)]
#![allow(clippy::manual_contains)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::never_loop)]
#![allow(clippy::new_without_default)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::field_reassign_with_default)]
#![allow(noop_method_call)]

pub mod cli;
pub mod config;
pub mod daemon;
pub mod exit_codes;
pub mod logging;
pub mod output;
pub mod protocol;
pub mod types;
