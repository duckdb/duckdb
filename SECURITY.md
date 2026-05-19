# DuckDB Security Vulnerability Response Policy

Please see the "[Securing DuckDB](https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview)" page first. To quote:
> DuckDB is a powerful analytical database engine. It can read and write files, access the network, load extensions, and use system resources. Like any powerful tool, these capabilities require appropriate configuration when working with sensitive data or in shared environments.

## Supported Versions

Please see our [release calendar](https://duckdb.org/release_calendar) or DuckDB's [`endoflife.date`](https://endoflife.date/duckdb) page to see which versions are currently supported.

## Reporting a Vulnerability
Please use GitHub's [security reporting tool here](https://github.com/duckdb/duckdb/security/advisories/new) to report a potential security issue. 
Our team will investigate and get back to you. There are three major outcomes of this investigation:

1. The issue is an actual security issue. For [example](https://github.com/duckdb/duckdb/security/advisories/GHSA-vmp8-hg63-v2hp), a return value of a cryptographic function is not checked, and that could lead to failure to encrypt.
1. The issue is a bug, but not really security related. For example, triggering an (internal) exception is generally not considered a security issue.
1. The issue is neither.

If the issue is determined to be an actual security issue, we will request a CVE. For the second case, our [general support policy](https://duckdblabs.com/community_support_policy/) will be followed.


## Public Disclosure
We would prefer to only disclose the issue once a DuckDB release is published with a fix. We generally publish a new DuckDB release every few weeks.
Again, see the [release calendar](https://duckdb.org/release_calendar) for the planned release dates. 
