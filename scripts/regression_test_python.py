import os
import sys
import duckdb
import pandas as pd
import pyarrow as pa
import time
import argparse
from typing import Dict, List, Any

TPCH_NQUERIES = 22
TPCH_QUERIES = []
for i in range(1, TPCH_NQUERIES + 1):
    TPCH_QUERIES.append(f'PRAGMA tpch({i})')


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
    def __init__(self, duration, run_number):
        self.duration = duration
        self.run_number = run_number


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

    def load_lineitem(self, collector) -> List[BenchmarkResult]:
        query = 'SELECT * FROM lineitem'
        results = []
        for nrun in range(nruns):
            duration = 0.0
            start = time.time()
            rel = self.conn.sql(query)
            res = collector(rel)
            end = time.time()
            duration = float(end - start)
            del res
            padding = " " * len(str(nruns))
            print_msg(f"T{padding}: {duration}s")
            results.append(BenchmarkResult(duration, nrun))
        return results


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

    def run_tpch(self, collector) -> List[BenchmarkResult]:
        print_msg("")
        print_msg(TPCH_QUERIES)
        results = []
        for nrun in range(nruns):
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
            results.append(BenchmarkResult(duration, nrun))
        return results


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
        results: List[BenchmarkResult] = tpch.load_lineitem(COLLECTORS[collector])
        benchmark_name = collector + "_load_lineitem"
        print_msg(benchmark_name)
        print_msg(collector)
        for res in results:
            run_number = res.run_number
            duration = res.duration
            write_result(benchmark_name, run_number, duration)

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
        results: List[BenchmarkResult] = tester.run_tpch(collector)
        benchmark_name = f"{convertor}tpch"
        for res in results:
            run_number = res.run_number
            duration = res.duration
            write_result(benchmark_name, run_number, duration)


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

    def benchmark(self) -> List[BenchmarkResult]:
        self.con.register('arrow_table', self.table)
        results = []
        for nrun in range(nruns):
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
            results.append(BenchmarkResult(duration, nrun))
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

    def benchmark(self) -> List[BenchmarkResult]:
        results = []
        for nrun in range(nruns):
            duration = 0.0
            pandas_df = pd.read_csv('wide_table.csv')
            start = time.time()
            for amplification in range(30):
                res = self.con.execute("""select * from pandas_df""").df()
            end = time.time()
            duration = float(end - start)
            del res
            results.append(BenchmarkResult(duration, nrun))
        return results


def test_arrow_dictionaries_scan():
    DICT_SIZE = 26 * 1000
    print_msg(f"Generating a unique dictionary of size {DICT_SIZE}")
    arrow_dict = ArrowDictionary(DICT_SIZE)
    DATASET_SIZE = 10000000
    for unique_values in [2, 1000, DICT_SIZE]:
        test = ArrowDictionaryBenchmark(unique_values, DATASET_SIZE, arrow_dict)
        results = test.benchmark()
        benchmark_name = f"arrow_dict_unique_{unique_values}_total_{DATASET_SIZE}"
        for res in results:
            run_number = res.run_number
            duration = res.duration
            write_result(benchmark_name, run_number, duration)


def test_loading_pandas_df_many_times():
    test = PandasDFLoadBenchmark()
    results = test.benchmark()
    benchmark_name = f"load_pandas_df_many_times"
    for res in results:
        run_number = res.run_number
        duration = res.duration
        write_result(benchmark_name, run_number, duration)


def main():
    test_tpch()
    test_arrow_dictionaries_scan()
    test_loading_pandas_df_many_times()

    close_result()


if __name__ == '__main__':
    main()
