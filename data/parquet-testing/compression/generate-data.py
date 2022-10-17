#! /usr/bin/env python3

import itertools
import numpy as np
from numpy.random import Generator, PCG64
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


"""Generate data to test parquet data page decompression."""


COMPRESSION_CODECS = [
    "NONE",
    "SNAPPY",
    "GZIP",
    # Brotli is currently not supported by duckdb
    "BROTLI",
    # This generates the new LZ4_RAW parquet compression, which duckdb does not
    # support
    "LZ4",
    "ZSTD",
]


DATA_PAGE_VERSIONS = [
    "1.0",
    "2.0",
]


def build_table():
    # Init rng in a reproducible way
    rng = Generator(PCG64(12345))


    # Plain table.
    N = 30  # column count
    p = .2  # NULL probability

    columns = {}

    # Integer columns, no nesting, no NULL, no repetition
    columns["plain"] = pa.array(np.arange(N))
    columns["plain_random"] = pa.array(rng.choice(N, N))

    # Mixed dtype struct column, NULLs exist at all levels
    x = pa.array(
        rng.choice(["foo", "bar", "baz"], N),
        mask=rng.choice([True, False], N, p=[p, 1 - p]),
    )
    y = pa.array(
        rng.choice(42, N),
        mask=rng.choice([True, False], N, p=[p, 1 - p]),
    )
    z = pa.StructArray.from_arrays(
        (x, y), names=("string", "int"),
        mask=pa.array(rng.choice([True, False], N, p=[p, 1 - p])),
    )
    columns["nested_nulls"] = z

    # Integer list with variable list length and NULLs
    values = list(range(42)) + [None]
    columns["list"] = pa.array(
        [rng.choice(values, count) for count in rng.choice(20, N)],
        mask=pa.array(rng.choice([True, False], N, p=[p, 1 - p])),
    )

    return pa.Table.from_pydict(columns)


table = build_table()

root = Path("generated")
root.mkdir(exist_ok=True)

for compression, data_page_version in itertools.product(COMPRESSION_CODECS, DATA_PAGE_VERSIONS):
    pq_args = { 
        "data_page_version": data_page_version,
        "compression": compression,
    }

    pq.write_table(
        table,
        (root / "_".join([
            f"data_page={data_page_version[0]}",
            f"{compression}",
        ])).with_suffix(".parquet"),
        **pq_args
    )
