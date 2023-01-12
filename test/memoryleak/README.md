## Usage

Run all memory leak tests:

```bash
python3 test/memoryleak/test_memory_leaks.py
```

Run a specific test:

```bash
python3 test/memoryleak/test_memory_leaks.py --test="Test temporary table leaks (#5501)"
```

Running tests in the debugger requires passing the `--memory-leak-tests` flag:

```bash
build/debug/test/unittest "Test temporary table leaks (#5501)" --memory-leak-tests
```

## Description

The memory leak folder contains memory leak tests. These are tests that run forever (i.e. they contain a `while true` loop). The tests are disabled unless the `--memory-leak-tests` flag is passed to the test runner.

The core idea of the tests is that they perform operations in a loop that should not increase memory of the system (e.g. they might create tables and then drop them again, or create connections and then destroy them).

A separate Python script is used to run these tests (`test/memoryleak/test_memory_leaks.py`). The Python script measures the resident set size of the unittest using the `ps` system call.

* The script measures memory usage of the test - if the memory usage does not stabilize within the timeout the test is considered a failure.
* Stabilized memory usage means that the trend of memory usage has not been going up in the past 10 seconds
* The exact threshold of what "going up" means is determined by `--threshold-percentage` and `--threshold-absolute`
* The timeout is determined by `--timeout`