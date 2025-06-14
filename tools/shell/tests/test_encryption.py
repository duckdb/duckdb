import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

# the correct keys used are:
# 'masterkey' for encrypted_master_key.db
# 'userkey' for encrypted_key.db
# 'attachkey' for encrypted_attach.db


def test_no_key(shell):
    """
    Encrypted database created withouth a -key or -master_key
    throws an error
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_mkey.db'])

    result = test.run()
    result.check_stderr(
        'Error: unable to open database "test/storage/encryption/encrypted_mkey.db": Catalog Error: Cannot open encrypted database "test/storage/encryption/encrypted_mkey.db" without a key'
    )


def test_unencrypted_database_key(shell):
    """
    Unencrypted database opened with -key
    throws an error
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/plaintext.db', '-key', 'userkey'])

    result = test.run()
    result.check_stderr(
        'Error: unable to open database "test/storage/encryption/plaintext.db": Catalog Error: A key is explicitly specified, but database "test/storage/encryption/plaintext.db" is not encrypted'
    )


def test_unencrypted_database_master_key(shell):
    """
    Unencrypted database opened with -master_key
    throws an error
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/plaintext.db', '-master_key', 'masterkey'])

    result = test.run()
    result.check_stderr(
        'Error: unable to open database "test/storage/encryption/plaintext.db": Catalog Error: A master key is found, but database "test/storage/encryption/plaintext.db" is not encrypted'
    )


def test_correct_master_key(shell):
    """
    Encrypted database created with -master_key in the command line
    Opened with 'encrypted_key.db' -master_key masterkey
    """

    test = (
        ShellTest(shell, arguments=['-master_key', 'masterkey'])
        .statement("ATTACH 'test/storage/encryption/encrypted_mkey.db' as enc")
        .statement("USE enc")
        .statement("select * from tbl;")
    )

    result = test.run()
    result.check_stdout('1')


def test_wrong_master_key(shell):
    """
    Encrypted database created with -master_key in the command line
    Opened with 'encrypted_key.db' -master_key masterkey
    Incorrect master key is given as input
    """

    test = ShellTest(shell, arguments=['-master_key', 'xxx']).statement(
        "ATTACH 'test/storage/encryption/encrypted_mkey.db' as enc"
    )

    result = test.run()
    result.check_stderr(
        'IO Error: Master key found in cache, but wrong encryption key used to open the database file. Try to explicitly define an ENCRYPTION_KEY with ATTACH'
    )


def test_correct_key(shell):
    """
    Encrypted database created with -key in the command line
    Opened with 'encrypted_key.db' -key userkey
    """
    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_key.db', '-key', 'userkey']).statement(
        "select * from tbl;"
    )

    result = test.run()
    result.check_stdout('1')


def test_wrong_key(shell):
    """
    Encrypted database created with -key in the command line
    Opened with 'encrypted_key.db' -key
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_key.db', '-key', 'xxx'])

    result = test.run()
    result.check_stderr('IO Error: Wrong encryption key used to open the database file')


def test_correct_key_attach(shell):
    """
    Encrypted database created with attach
    Opened with 'encrypted_attach.db' -key
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_attach.db', '-key', 'attachkey']).statement(
        "select * from tbl;"
    )

    result = test.run()
    result.check_stdout('1')


def test_wrong_key_attach(shell):
    """
    Encrypted database created with attach
    Opened with 'encrypted_attach.db' -key
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_attach.db', '-key', 'xxx'])

    result = test.run()
    result.check_stderr('IO Error: Wrong encryption key used to open the database file')


def test_wrong_master_key_no_explicit_attach_key(shell):
    """
    Encrypted database created with attach
    -master_key given as input, but this key differs from the encryption key
    so it throws an error
    """

    test = ShellTest(shell, arguments=['-master_key', 'masterkey']).statement(
        "ATTACH 'test/storage/encryption/encrypted_attach.db' as enc"
    )

    result = test.run()
    result.check_stderr(
        'IO Error: Master key found in cache, but wrong encryption key used to open the database file. Try to explicitly define an ENCRYPTION_KEY with ATTACH'
    )


def test_wrong_master_key_correct_attach(shell):
    """
    Encrypted database created with attach
    Opened with 'encrypted_attach.db' -master_key
    but explicitly uses another key on attach
    """

    test = (
        ShellTest(shell, arguments=['-master_key', 'masterkey'])
        .statement("ATTACH 'test/storage/encryption/encrypted_attach.db' as enc (ENCRYPTION_KEY attachkey)")
        .statement("USE enc")
        .statement("select * from tbl;")
    )

    result = test.run()
    result.check_stdout('1')


@pytest.mark.skipif(os.name == 'nt', reason="Windows highlighting does not use shell escapes")
def test_explicit_key_and_master_key(shell):
    """
    Encrypted database created with key
    Opened with 'encrypted_key.db' -master_key masterkey -key userkey
    """

    test = ShellTest(
        shell,
        arguments=['-master_key', 'masterkey', '-key', 'userkey', 'test/storage/encryption/encrypted_key.db'],
    ).statement("select * from tbl;")

    result = test.run()
    result.check_stderr(
        'Cannot specify both -key and -master_key.\nError: unable to open database "test/storage/encryption/encrypted_key.db'
    )


def test_key_no_database(shell):
    """
    Opened with -key userkey
    but no database is found as input
    """

    test = ShellTest(shell, arguments=['-key', 'userkey'])

    result = test.run()
    result.check_stderr('Error: key specified but no database found')


def test_long_base64_key(shell):
    """
    Opened with a long -key
    """

    test = ShellTest(
        shell,
        arguments=[
            'test/storage/encryption/encrypted_long_key.db',
            '-key',
            'test_key_U2FsdGVkX18+0aLSpbJ9V0hVTVV1oYlJbVhGZ0NMTkpqaWp4R3hNR2VaUkVHaVRqR1htR3V1QXZmc0NtbUxSaUlwWVdoVkNrT0RUR2pqUGxkUVdtSmtOUldHWlJRa05JUWlVSkZaVk10aEZXR2FRa0Z6eEx0VlZIVG9LR0UwRkdNZ0xXaEpKWkNKRW5BaXlLSnNWTkdWTkpHZ0ZR',
        ],
    ).statement("select * from tbl;")

    result = test.run()
    result.check_stdout('1')


def test_long_base64_master_key(shell):
    """
    Opened with a long -master_key
    """

    test = (
        ShellTest(
            shell,
            arguments=[
                '-master_key',
                'test_key_U2FsdGVkX18+0aLSpbJ9V0hVTVV1oYlJbVhGZ0NMTkpqaWp4R3hNR2VaUkVHaVRqR1htR3V1QXZmc0NtbUxSaUlwWVdoVkNrT0RUR2pqUGxkUVdtSmtOUldHWlJRa05JUWlVSkZaVk10aEZXR2FRa0Z6eEx0VlZIVG9LR0UwRkdNZ0xXaEpKWkNKRW5BaXlLSnNWTkdWTkpHZ0ZR',
            ],
        )
        .statement("ATTACH 'test/storage/encryption/encrypted_long_key.db' as enc")
        .statement("USE enc")
        .statement("select * from tbl;")
    )

    result = test.run()
    result.check_stdout('1')
