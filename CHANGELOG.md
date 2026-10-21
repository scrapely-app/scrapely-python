# Changelog

All notable changes to this project will be documented in this file.

## [0.1.32] - 2026-10-20

- Maintenance release.

## [0.1.31] - 2026-10-20

- The storage client percent-encodes request ids and key-value store record keys as one URL path segment and refuses `''`, `.` and `..`. A request id such as `..` read from a queue used to send the call to another resource.

## [0.1.30] - 2026-10-20

- Maintenance release.

## [0.1.29] - 2026-10-19

- Initial public release.
