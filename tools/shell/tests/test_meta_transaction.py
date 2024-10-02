# fmt: off

from conftest import ShellTest

def test_temp_directory(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE SEQUENCE id_seq;")
        .statement("""
            CREATE TABLE my_table (
            id INTEGER DEFAULT nextval('id_seq'),
            a INTEGER
        );""")
        .statement("ATTACH ':memory:' AS s1;")
        .statement("CREATE TABLE s1.tbl AS FROM range(2000000);")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
        .statement("INSERT INTO my_table (a) SELECT * FROM s1.tbl;")
    )
    test = test.statement("select count(*) from my_table")
    result = test.run()
    result.check_stdout("20000000")

# fmt: on
