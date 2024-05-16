import os
import sys
import duckdb
import pandas as pd
import pyarrow as pa
import time
import argparse
from typing import Dict, List, Any
import numpy as np

TPCH_QUERIES = []
res = duckdb.execute(
    """
    select query from tpch_queries()
"""
).fetchall()
for x in res:
    TPCH_QUERIES.append(x[0])

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", action="store_true", help="Enable verbose mode", default=False)
parser.add_argument("--threads", type=int, help="Number of threads", default=None)
parser.add_argument("--nruns", type=int, help="Number of runs", default=10)
parser.add_argument("--out-file", type=str, help="Output file path", default=None)
parser.add_argument("--scale-factor", type=float, help="Set the scale factor TPCH is generated at", default=1.0)
args, unknown_args = parser.parse_known_args()

verbose = args.verbose
threads = args.threads
nruns = args.nruns
out_file = args.out_file
scale_factor = args.scale_factor

if unknown_args:
    parser.error(f"Unrecognized parameter(s): {', '.join(unknown_args)}")


def print_msg(message: str):
    if not verbose:
        return
    print(message)


def write_result(benchmark_name, nrun, t):
    bench_result = f"{benchmark_name}\t{nrun}\t{t}"
    if out_file is not None:
        if not hasattr(write_result, 'file'):
            write_result.file = open(out_file, 'w+')
        write_result.file.write(bench_result)
        write_result.file.write('\n')
    else:
        print_msg(bench_result)


def close_result():
    if not hasattr(write_result, 'file'):
        return
    write_result.file.close()


class BenchmarkResult:
    def __init__(self, name):
        self.name = name
        self.runs: List[float] = []

    def add(self, duration: float):
        self.runs.append(duration)

    def write(self):
        for i, run in enumerate(self.runs):
            write_result(self.name, i, run)


class TPCHData:
    TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    def __init__(self, scale_factor):
        self.conn = duckdb.connect()
        self.conn.execute(f'CALL dbgen(sf={scale_factor})')

    def get_tables(self, convertor) -> Dict[str, Any]:
        res = {}
        for table in self.TABLES:
            res[table] = convertor(self.conn, table)
        return res

    def load_lineitem(self, collector, benchmark_name) -> BenchmarkResult:
        query = 'SELECT * FROM lineitem'
        result = BenchmarkResult(benchmark_name)
        for _ in range(nruns):
            duration = 0.0
            start = time.time()
            rel = self.conn.sql(query)
            res = collector(rel)
            end = time.time()
            duration = float(end - start)
            del res
            padding = " " * len(str(nruns))
            print_msg(f"T{padding}: {duration}s")
            result.add(duration)
        return result


class TPCHBenchmarker:
    def __init__(self, name: str):
        self.initialize_connection()
        self.name = name

    def initialize_connection(self):
        self.con = duckdb.connect()
        if not threads:
            return
        print_msg(f'Limiting threads to {threads}')
        self.con.execute(f"SET threads={threads}")

    def register_tables(self, tables: Dict[str, Any]):
        for name, table in tables.items():
            self.con.register(name, table)

    def run_tpch(self, collector, benchmark_name) -> BenchmarkResult:
        print_msg("")
        print_msg(TPCH_QUERIES)
        result = BenchmarkResult(benchmark_name)
        for _ in range(nruns):
            duration = 0.0
            # Execute all queries
            for i, query in enumerate(TPCH_QUERIES):
                start = time.time()
                rel = self.con.sql(query)
                if rel:
                    res = collector(rel)
                    del res
                else:
                    print_msg(f"Query '{query}' did not produce output")
                end = time.time()
                query_time = float(end - start)
                print_msg(f"Q{str(i).ljust(len(str(nruns)), ' ')}: {query_time}")
                duration += float(end - start)
                padding = " " * len(str(nruns))
                print_msg(f"T{padding}: {duration}s")
            result.add(duration)
        return result


def test_tpch():
    print_msg(f"Generating TPCH (sf={scale_factor})")
    tpch = TPCHData(scale_factor)

    ## -------- Benchmark converting LineItem to different formats ---------

    def fetch_native(rel: duckdb.DuckDBPyRelation):
        return rel.fetchall()

    def fetch_pandas(rel: duckdb.DuckDBPyRelation):
        return rel.df()

    def fetch_arrow(rel: duckdb.DuckDBPyRelation):
        return rel.arrow()

    COLLECTORS = {'native': fetch_native, 'pandas': fetch_pandas, 'arrow': fetch_arrow}
    # For every collector, load lineitem 'nrun' times
    for collector in COLLECTORS:
        result: BenchmarkResult = tpch.load_lineitem(COLLECTORS[collector], collector + "_load_lineitem")
        print_msg(result.name)
        print_msg(collector)
        result.write()

    ## ------- Benchmark running TPCH queries on top of different formats --------

    def convert_pandas(conn: duckdb.DuckDBPyConnection, table_name: str):
        return conn.execute(f"SELECT * FROM {table_name}").df()

    def convert_arrow(conn: duckdb.DuckDBPyConnection, table_name: str):
        df = convert_pandas(conn, table_name)
        return pa.Table.from_pandas(df)

    CONVERTORS = {'pandas': convert_pandas, 'arrow': convert_arrow}
    # Convert TPCH data to the right format, then run TPCH queries on that data
    for convertor in CONVERTORS:
        tables = tpch.get_tables(CONVERTORS[convertor])
        tester = TPCHBenchmarker(convertor)
        tester.register_tables(tables)
        collector = COLLECTORS[convertor]
        result: BenchmarkResult = tester.run_tpch(collector, f"{convertor}tpch")
        result.write()


def generate_string(seed: int):
    output = ''
    for _ in range(10):
        output += chr(ord('A') + int(seed % 26))
        seed /= 26
    return output


class ArrowDictionary:
    def __init__(self, unique_values):
        self.size = unique_values
        self.dict = [generate_string(x) for x in range(unique_values)]


class ArrowDictionaryBenchmark:
    def __init__(self, unique_values, values, arrow_dict: ArrowDictionary):
        assert unique_values <= arrow_dict.size
        self.initialize_connection()
        self.generate(unique_values, values, arrow_dict)

    def initialize_connection(self):
        self.con = duckdb.connect()
        if not threads:
            return
        print_msg(f'Limiting threads to {threads}')
        self.con.execute(f"SET threads={threads}")

    def generate(self, unique_values, values, arrow_dict: ArrowDictionary):
        self.input = []
        self.expected = []
        for x in range(values):
            value = arrow_dict.dict[x % unique_values]
            self.input.append(value)
            self.expected.append((value,))

        array = pa.array(
            self.input,
            type=pa.dictionary(pa.int64(), pa.string()),
        )
        self.table = pa.table([array], names=["x"])

    def benchmark(self, benchmark_name) -> BenchmarkResult:
        self.con.register('arrow_table', self.table)
        result = BenchmarkResult(benchmark_name)
        for _ in range(nruns):
            duration = 0.0
            start = time.time()
            res = self.con.execute(
                """
                select * from arrow_table
            """
            ).fetchall()
            end = time.time()
            duration = float(end - start)
            assert self.expected == res
            del res
            padding = " " * len(str(nruns))
            print_msg(f"T{padding}: {duration}s")
            result.add(duration)
        return result


class SelectAndCallBenchmark:
    def __init__(self):
        """
        SELECT statements become QueryRelations, any other statement type becomes a MaterializedRelation.
        We use SELECT and CALL here because their execution plans are identical
        """
        self.initialize_connection()

    def initialize_connection(self):
        self.con = duckdb.connect()
        if not threads:
            return
        print_msg(f'Limiting threads to {threads}')
        self.con.execute(f"SET threads={threads}")

    def benchmark(self, name, query) -> List[BenchmarkResult]:
        results: List[BenchmarkResult] = []
        methods = {'select': 'select * from ', 'call': 'call '}
        for key, value in methods.items():
            for rowcount in [2048, 50000, 2500000]:
                result = BenchmarkResult(f'{key}_{name}_{rowcount}')
                query_string = query.format(rows=rowcount)
                query_string = value + query_string
                rel = self.con.sql(query_string)
                print_msg(rel.type)
                for _ in range(nruns):
                    duration = 0.0
                    start = time.time()
                    rel.fetchall()
                    end = time.time()
                    duration = float(end - start)
                    padding = " " * len(str(nruns))
                    print_msg(f"T{padding}: {duration}s")
                    result.add(duration)
                results.append(result)
        return results


class PandasDFLoadBenchmark:
    def __init__(self):
        self.initialize_connection()
        self.generate()

    def initialize_connection(self):
        self.con = duckdb.connect()
        if not threads:
            return
        print_msg(f'Limiting threads to {threads}')
        self.con.execute(f"SET threads={threads}")

    def generate(self):
        self.con.execute("call dbgen(sf=0.1)")
        new_table = "*, " + ", ".join(["l_shipdate"] * 300)
        self.con.execute(f"create table wide as select {new_table} from lineitem limit 500")
        self.con.execute(f"copy wide to 'wide_table.csv' (FORMAT CSV)")

    def benchmark(self, benchmark_name) -> BenchmarkResult:
        result = BenchmarkResult(benchmark_name)
        for _ in range(nruns):
            duration = 0.0
            pandas_df = pd.read_csv('wide_table.csv')
            start = time.time()
            for _ in range(30):
                res = self.con.execute("""select * from pandas_df""").df()
            end = time.time()
            duration = float(end - start)
            del res
            result.add(duration)
        return result


class PandasAnalyzerBenchmark:
    def __init__(self):
        self.initialize_connection()
        self.generate()

    def initialize_connection(self):
        self.con = duckdb.connect()
        if not threads:
            return
        print_msg(f'Limiting threads to {threads}')
        self.con.execute(f"SET threads={threads}")

    def generate(self):
        return

    def benchmark(self, benchmark_name) -> BenchmarkResult:
        result = BenchmarkResult(benchmark_name)
        data = [None] * 9999999 + [1]  # Last element is 1, others are None

        # Create the DataFrame with the specified data and column type as object
        pandas_df = pd.DataFrame(data, columns=['Column'], dtype=object)
        for _ in range(nruns):
            duration = 0.0
            start = time.time()
            for _ in range(30):
                res = self.con.execute("""select * from pandas_df""").df()
            end = time.time()
            duration = float(end - start)
            del res
            result.add(duration)
        return result


def test_arrow_dictionaries_scan():
    DICT_SIZE = 26 * 1000
    print_msg(f"Generating a unique dictionary of size {DICT_SIZE}")
    arrow_dict = ArrowDictionary(DICT_SIZE)
    DATASET_SIZE = 10000000
    for unique_values in [2, 1000, DICT_SIZE]:
        test = ArrowDictionaryBenchmark(unique_values, DATASET_SIZE, arrow_dict)
        benchmark_name = f"arrow_dict_unique_{unique_values}_total_{DATASET_SIZE}"
        result = test.benchmark(benchmark_name)
        result.write()


def test_loading_pandas_df_many_times():
    test = PandasDFLoadBenchmark()
    benchmark_name = f"load_pandas_df_many_times"
    result = test.benchmark(benchmark_name)
    result.write()


def test_pandas_analyze():
    test = PandasAnalyzerBenchmark()
    benchmark_name = f"pandas_analyze"
    result = test.benchmark(benchmark_name)
    result.write()


def test_call_and_select_statements():
    test = SelectAndCallBenchmark()
    queries = {
        'repeat_row': "repeat_row(42, 'test', True, 'this is a long string', num_rows={rows})",
    }
    for key, value in queries.items():
        results = test.benchmark(key, value)
        for res in results:
            res.write()


def main():
    test_tpch()
    test_arrow_dictionaries_scan()
    test_loading_pandas_df_many_times()
    test_pandas_analyze()
    test_call_and_select_statements()

    close_result()


if __name__ == '__main__':
    main()
