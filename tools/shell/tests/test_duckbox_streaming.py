# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest


def test_analyze_truncation(shell):
    # if we find a long string after a bunch of short strings with a low analyze count, the string will get truncated
    test = (
        ShellTest(shell)
        .statement(".maxrows -1 1000")
        .statement("select i::varchar from range(10_000) t(i) union all select 'this is a very long string that will get truncated'")
    )
    result = test.run()
    result.check_stdout('9999')
    result.check_stdout('…')

def test_analyze_streaming_wrap(shell):
    # we can wrap also while streaming, if our max width is set
    test = (
        ShellTest(shell)
        .statement(".maxrows -1 100")
        .statement(".maxwidth 100")
        .statement("select concat('this is a long string that will wrap given the current maximum width that is set to only 100 which is not very wide (#', i, ')') from range(10000) t(i);")
    )
    result = test.run()
    result.check_stdout('#9999')


# fmt: on
