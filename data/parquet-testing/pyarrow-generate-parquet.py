import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path


def generate_parquet(data_dir: Path):
    generate_silly_names(data_dir / 'silly-names.parquet')
    generate_byte_stream_split(data_dir / 'byte_stream_split.parquet')


def generate_silly_names(path: Path):
    df = pd.DataFrame({'önë': [1, 2, 3],
                       '': ['foo', 'bar', 'baz'],
                       '🦆': [True, False, True]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)


def generate_byte_stream_split(path: Path):
    num_rows = 100
    rng = np.random.default_rng(0)

    floats = pa.array(rng.uniform(-100.0, 100.0, num_rows), type=pa.float32())
    doubles = pa.array(rng.uniform(-100.0, 100.0, num_rows), type=pa.float64())

    null_mask = np.ones(num_rows, dtype=np.bool_)
    null_mask[num_rows // 10:] = False
    rng.shuffle(null_mask)
    nullable_floats = pa.array(
            rng.uniform(-100.0, 100.0, num_rows), type=pa.float32(), mask=null_mask)

    table = pa.Table.from_arrays(
            [floats, doubles, nullable_floats],
            ["floats", "doubles", "nullable_floats"])

    with pq.ParquetWriter(
            path,
            table.schema,
            use_dictionary=False,
            use_byte_stream_split=True) as writer:
        writer.write_table(table)

    csv_path = path.with_suffix('.csv')
    options = csv.WriteOptions(include_header=True, delimiter='|')
    csv.write_csv(table, csv_path, options)
    fix_csv_nulls(csv_path)


def fix_csv_nulls(path: Path):
    """ Replace empty values with 'NULL' """
    with open(path, 'r') as f:
        lines = f.readlines()

    with open(path, 'w') as f:
        for line in lines:
            split_line = ["NULL" if val == "" else val for val in line.strip().split("|")]
            f.write("|".join(split_line) + "\n")


if __name__ == '__main__':
    data_dir = Path(__file__).parent
    generate_parquet(data_dir)
