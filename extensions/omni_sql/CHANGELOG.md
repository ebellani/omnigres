# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2024-06-19

### Fixed

* Query validation might not work in presence of a post_parse_analyze hook
  [#582](https://github.com/omnigres/omnigres/pull/582)

## [0.2.1] - 2024-04-14

### Fixed

* `raw_statements` parser would incorrectly calculate the length of a statement
  if it is preceeded by whitespace [#561](https://github.com/omnigres/omnigres/pull/561)

## [0.2.0] - 2024-04-10

### Added

* Raw statement parser that preserves statement syntax and
  location [#554](https://github.com/omnigres/omnigres/pull/554)
* Statement type function [#555](https://github.com/omnigres/omnigres/pull/555)

## [0.1.0] - 2024-03-05

Initial release following a few months of iterative development.

[Unreleased]: https://github.com/omnigres/omnigres/commits/next/omni_sql

[0.1.0]: [https://github.com/omnigres/omnigres/pull/511]

[0.2.0]: [https://github.com/omnigres/omnigres/pull/553]

[0.2.1]: [https://github.com/omnigres/omnigres/pull/561]

[0.2.2]: [https://github.com/omnigres/omnigres/pull/581]

