# fmt: off

from conftest import ShellTest

def test_get_env(shell):
    test = (
        ShellTest(shell)
            .statement('.null NULL')
            .statement("SET default_null_order=getenv('DEFAULT_NULL_ORDER');")
            .statement("SELECT * FROM (VALUES (42), (NULL)) ORDER BY 1 LIMIT 1;")
    )
    test.environment['DEFAULT_NULL_ORDER'] = 'NULLS_FIRST'
    result = test.run()
    result.check_stdout('NULL')

    test.environment['DEFAULT_NULL_ORDER'] = 'NULLS_LAST'
    result = test.run()
    result.check_stdout('42')

# fmt: on
