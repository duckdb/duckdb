# fmt: off

from conftest import ShellTest


def test_many_schemas(shell):
    test = (
        ShellTest(shell)
        .statement('''
attach ':memory:' as my_db1;
attach ':memory:' as my_db2;
create schema my_db1.db1_s1;
create schema my_db1.db1_s2;
create schema my_db2.db2_s1;
create schema my_db2.db2_s2;
create schema memory.memory_s1;
create schema memory.memory_s2;''')
        .statement('show schemas')
    )

    result = test.run()
    result.check_stdout("memory")
    result.check_stdout("my_db1")
    result.check_stdout("my_db2")
    result.check_stdout("main")
    result.check_stdout("memory_s1")
    result.check_stdout("memory_s2")
    result.check_stdout("db1_s1")
    result.check_stdout("db1_s2")
    result.check_stdout("db2_s1")
    result.check_stdout("db2_s2")


def test_detaching_default_db(shell):
    test = (
        ShellTest(shell)
        .statement("attach ':memory:' as new_db")
        .statement("use new_db")
        .statement("detach memory")
        .statement("show schemas")
    )

    result = test.run()
    result.check_stdout("new_db")
    result.check_stdout("main")
    result.check_stdout("true")
    result.check_not_exist("memory")
