# Test secrets
DuckDB only allows persistent secrets with the x00 permission (e.g. 600 or 700). Therefore to use these 
secrets, the permissions need to be set before running any tests that uses them.

The recommended way to add tests that touch these persistent secret files is to put them behind a
```shell
require-env TEST_PERSISTENT_SECRETS_AVAILABLE
```
statement, which ensures the tests only run in CI jobs where the permissions are set correctly.

