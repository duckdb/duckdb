#### Build the ODBC client (from within the main DuckDB repository)

```bash
BUILD_ODBC=1 DISABLE_SANITIZER=1 make debug -j
```

#### Run the ODBC client tests

```bash
# run all tests
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace

# run an individual test
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace --test result-conversions-test
```

#### Fix the ODBC Client Tests
First verify that the new output is correct. Then run the test using the `run_psqlodbc_test.py` script as usual, but use the `--fix` parameter.

```bash
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace --test result-conversions-test --fix
```

This overwrites the stored output. You can run the test without the fix flag, and it should now pass.

Afterwards, we need to commit the changes to the psqlodbc repository:

```bash
cd psqlodbc
git add -p
git commit -m "Fix output"
git push
```

Then we need to update the git hash in our PR that broke the psqlodbc tests. Use `git log` to get the hash of the previous commit and the current commit.

```
git log
# old: d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e
# new: 4acd0c615b690d526fd3cf7f2e666eee4ff9cbdb
```

Then search in the CI folder for the old hash, and replace it within the new hash:

```bash
grep -R .github 4acd0c615b690d526fd3cf7f2e666eee4ff9cbdb
.github/workflows/Windows.yml:        (cd psqlodbc && git checkout d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e && make release)
.github/workflows/Main.yml:        (cd psqlodbc && git checkout d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e && make debug)
```
