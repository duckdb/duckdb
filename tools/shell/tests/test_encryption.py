import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

# the correct keys used are:
# 'userkey' for encrypted_key.db
# 'attachkey' for encrypted_attach.db


def test_no_key(shell):
    """
    Encrypted database created withouth a -key
    throws an error
    """

    test = ShellTest(shell, arguments=['test/storage/encryption/encrypted_key.db'])

    result = test.run()
    result.check_stderr(
        'Error: unable to open database "test/storage/encryption/encrypted_key.db": Catalog Error: Cannot open encrypted database "test/storage/encryption/encrypted_key.db" without a key'
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
