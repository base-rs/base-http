# Releases

## Unreleased

- Parse HTTP/1 server request heads with less work. The server classifies Host and hop-by-hop fields once while reading the head instead of storing and then removing them, accepts the common `reg-name[:port]` authority in a single table-driven pass, and uses cheaper character-class and CRLF checks. Accepted and rejected requests are unchanged; a request with several defects may report a different first error. On Linux at 64 connections, user instructions per request fall from 7,915 to 6,419.
