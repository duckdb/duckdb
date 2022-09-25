import gzip
import io
import json
import os
import unittest
from contextlib import contextmanager
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from itertools import chain
from os.path import expanduser
from pathlib import Path
from shutil import rmtree
from subprocess import check_output, PIPE
from threading import Thread
from typing import Type, Generator, BinaryIO, Union, Tuple, Any, List
from unittest import TestCase, main
from urllib.parse import urlparse

binary_root = Path(os.environ.get('DUCKDB_BINARY_ROOT', "../build/debug"))
duckdb_path = binary_root / "duckdb"
assert duckdb_path.exists(), duckdb_path
ext_path = binary_root / "extension/httpfs/httpfs.duckdb_extension"
assert duckdb_path.exists()

EXPECTED_REQUESTS = [
    ("HEAD", "/fake.csv"),
    ("GET", "/fake.csv"),
] * 2  # TODO: why are the requests repeated twice?
CSV_RESPONSE = "hello,world\nhello,world\n".encode()
CSV_JSON = [
    {"column0": "hello", "column1": "world"},
    {"column0": "hello", "column1": "world"},
]
EXTENSION_RESPONSE = gzip.compress(b"this isn't a real extension", compresslevel=9)


def make_handler(
    expected_host: str, response: bytes
) -> Tuple[List[Tuple[str, str]], Type[SimpleHTTPRequestHandler]]:
    requests = []

    class FakeProxyHandler(SimpleHTTPRequestHandler):
        def send_head(self) -> Union[io.BytesIO, BinaryIO, None]:
            parse_result = urlparse(self.path)
            assert parse_result.hostname == expected_host
            path = parse_result.path
            requests.append((self.command, path))

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", self.guess_type(path))
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Last-Modified", self.date_time_string())
            self.end_headers()
            return io.BytesIO(response)

    return requests, FakeProxyHandler


@contextmanager
def make_proxy(
    expected_host: str, response: bytes
) -> Generator[None, Tuple[str, Tuple[str, str]], None]:
    hostname = "localhost"
    requests, handler = make_handler(expected_host, response)
    server = ThreadingHTTPServer((hostname, 0), handler)
    Thread(target=server.serve_forever).start()
    proxy_url = f"http://{hostname}:{server.server_port}"
    try:
        yield proxy_url, requests
    finally:
        server.server_close()
        server.shutdown()


def call_duckdb(*sql: str) -> Any:
    return json.loads(
        check_output(
            [
                duckdb_path,
                "-json",
                "-unsigned",
                *chain.from_iterable(("-c", line) for line in sql),
            ],
            stderr=PIPE,
            text=True,
        )
    )


class HttpProxyTest(TestCase):
    version: str

    def setUp(self) -> None:
        self.version = call_duckdb("pragma version")[0]["source_id"]
        rmtree(expanduser(f"~/.duckdb/extensions/{self.version}"), ignore_errors=True)

    def test_csv_via_httpfs(self):
        with make_proxy("bucket.s3.amazonaws.com", CSV_RESPONSE) as (
            proxy_url,
            requests,
        ):
            stdout = call_duckdb(
                f"set http_proxy='{proxy_url}'",
                "set s3_use_ssl=false",
                f"install '{self.ext_path()}'",
                "load httpfs",
                "select * from 's3://bucket/fake.csv'",
            )

            self.assertEqual(stdout, CSV_JSON)
            self.assertEqual(EXPECTED_REQUESTS, requests)

    def ext_path(self):
        ext_path = binary_root / "extension/httpfs/httpfs.duckdb_extension"
        if not ext_path.exists():
            raise unittest.SkipTest('missing httpfs extension')
        return ext_path

    def test_csv_via_http(self):
        with make_proxy("bucket.s3.amazonaws.com", CSV_RESPONSE) as (
            proxy_url,
            requests,
        ):
            stdout = call_duckdb(
                f"set http_proxy='{proxy_url}'",
                f"install '{self.ext_path()}'",
                "load httpfs",
                "select * from 'http://bucket.s3.amazonaws.com/fake.csv'",
            )

            self.assertEqual(stdout, CSV_JSON)
            self.assertEqual(EXPECTED_REQUESTS, requests)

    def test_extension_install(self):
        with make_proxy("extensions.duckdb.org", EXTENSION_RESPONSE) as (
            proxy_url,
            requests,
        ):
            res = call_duckdb(
                f"set http_proxy='{proxy_url}'",
                "set s3_use_ssl=false",
                "install fake_extension",
                "select extension_name from duckdb_extensions() where extension_name = 'fake_extension'",
            )

            self.assertEqual([{"extension_name": "fake_extension"}], res)
            self.assertEqual(
                [
                    (
                        "GET",
                        f"/{self.version}/linux_amd64/fake_extension.duckdb_extension.gz",
                    )
                ],
                requests,
            )


if __name__ == "__main__":
    main()
