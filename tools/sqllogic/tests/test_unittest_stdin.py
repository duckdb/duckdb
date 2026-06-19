import subprocess


def test_unittest_stdin_separate_writes(unittest_binary):
    proc = subprocess.Popen(
        [unittest_binary, "--stdin"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        proc.stdin.write("statement ok\nCREATE TABLE t(i INTEGER);\n\n")
        proc.stdin.flush()

        proc.stdin.write("statement ok\nINSERT INTO t VALUES (42);\n\n")
        proc.stdin.write("query I\nSELECT i FROM t\n----\n42")
        proc.stdin.close()

        stdout = proc.stdout.read()
        print(stdout)
        assert 'All tests passed' in stdout
        stderr = proc.stderr.read()
        print(stderr)
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    assert proc.returncode == 0, stderr
    assert "All tests passed" in stdout
    assert "<stdin>" in stdout
