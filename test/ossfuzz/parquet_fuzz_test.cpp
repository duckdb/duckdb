// Fuzzer for DuckDB's Parquet reader.
//
// Rationale
// ---------
// The existing OSS-Fuzz harness (parse_fuzz_test.cpp) feeds raw SQL strings to
// an in-memory connection. It exercises the SQL parser/planner/executor but
// NEVER reaches the Parquet stack, because that requires reading an actual
// Parquet file from disk via read_parquet(). As a result the entire Parquet
// binary-deserialization surface is at ~0% coverage in production:
//   * third_party/parquet/parquet_types.cpp  (Thrift-generated metadata structs)
//   * third_party/thrift                       (compact-protocol footer decode)
//   * extension/parquet/*                      (column readers, page/dictionary/
//                                               RLE/bit-packing decoders)
//   * third_party/{zstd,brotli,miniz} decompress (column-chunk decompression)
//
// Parquet is a complex binary columnar format routinely read from untrusted
// sources, and the metadata footer is Thrift-deserialized before any sanity
// checks on the data. This is exactly the high-value, memory-unsafe attack
// surface fuzzing is meant to cover, so a tiny harness here unlocks tens of
// thousands of lines of unreached parser code.
//
// Design
// ------
// libFuzzer drives one input at a time in a single process/thread, so we can
// safely write the candidate bytes to a fixed temp path and ask DuckDB to read
// it back as a Parquet file. The footer (Thrift metadata) is parsed at bind
// time and the column chunks are decoded/decompressed when the result is
// materialized by con.Query(), so a full SELECT exercises the whole pipeline.
// Malformed inputs throw DuckDB exceptions (expected) which we swallow; only
// memory-safety faults / sanitizer reports are real findings.

#include "duckdb.hpp"

#include <cstdint>
#include <cstdio>
#include <string>

static const char *kTmpFile = "/tmp/duckdb_parquet_fuzz.parquet";

// The Parquet reader is stateless with respect to the database, so a single
// in-memory instance is reused across iterations. Constructing a fresh DuckDB
// per input dominated runtime (~5 exec/s); reusing it keeps the fuzzer focused
// on the parser and runs an order of magnitude faster.
static duckdb::DuckDB *g_db = nullptr;
static duckdb::Connection *g_con = nullptr;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	// A valid Parquet file is at minimum "PAR1" + footer + "PAR1"; anything
	// smaller cannot reach meaningful parsing, so skip to save cycles.
	if (size < 8) {
		return 0;
	}

	if (!g_db) {
		g_db = new duckdb::DuckDB(nullptr);
		g_con = new duckdb::Connection(*g_db);
		// Single-threaded + no progress bar for determinism and speed; these
		// never fail and do not affect the parser paths under test.
		g_con->Query("SET threads=1");
		g_con->Query("PRAGMA enable_progress_bar=false");
	}

	FILE *f = fopen(kTmpFile, "wb");
	if (!f) {
		return 0;
	}
	if (fwrite(data, 1, size, f) != size) {
		fclose(f);
		remove(kTmpFile);
		return 0;
	}
	fclose(f);

	duckdb::Connection &con = *g_con;
	const std::string path(kTmpFile);
	try {
		// Full materialization forces the footer (Thrift) parse plus column
		// reader / decompression paths for every row group and column chunk.
		auto result = con.Query("SELECT * FROM read_parquet('" + path + "')");
		(void)result;
	} catch (const std::exception &e) {
		// Malformed Parquet -> DuckDB exception. Expected, not a bug.
	}

	try {
		// parquet_schema() / parquet_metadata() reach metadata-only code paths
		// (key/value metadata, schema element decoding, statistics) that a data
		// read can short-circuit past, for very little extra cost.
		auto meta = con.Query("SELECT * FROM parquet_schema('" + path + "')");
		(void)meta;
	} catch (const std::exception &e) {
		// Expected for malformed metadata.
	}

	remove(kTmpFile);
	return 0;
}
