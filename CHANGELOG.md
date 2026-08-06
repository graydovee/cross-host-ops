# Changelog

<!-- Maintenance rules: before every commit, prepend one line to the "Entries" section below.
     Format: yyyy-MM-dd [tag] brief content
     tag values: feat | bug | refactor | docs
     Classify by the substance of the change, not the commit prefix:
       new feature or capability (incl. independently added tests/tools) -> [feat]
       bug fix -> [bug]
       behavior-preserving refactor, internal cleanup, dependency upgrade, CI/build adjustment -> [refactor]
       documentation change -> [docs]
     Tests/formatting/minor cleanup bundled with a fix/feat are NOT logged separately; fold them into that entry. -->

## Entries

<!-- Prepend new entries at the very top -->
2026-08-06 [bug] list_servers skips gateways without LIST capability (e.g. Jumpserver bastions); no longer returns Unsupported sources to clients
