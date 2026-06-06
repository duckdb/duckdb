#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

//////////////
// INSERT //
//////////////
#define APPEND_BENCHMARK_INSERT(CREATE_STATEMENT, AUTO_COMMIT)                                                         \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		if (!AUTO_COMMIT)                                                                                              \
			state->conn.Query("BEGIN TRANSACTION");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			state->conn.Query("INSERT INTO integers VALUES (" + std::to_string(i) + ")");                              \
		}                                                                                                              \
		if (!AUTO_COMMIT)                                                                                              \
			state->conn.Query("COMMIT");                                                                               \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using a series of INSERT INTO statements";                      \
	}

DUCKDB_BENCHMARK(Append100KIntegersINSERT, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
FINISH_BENCHMARK(Append100KIntegersINSERT)

DUCKDB_BENCHMARK(Append100KIntegersINSERTDisk, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersINSERTDisk)

DUCKDB_BENCHMARK(Append100KIntegersINSERTPrimary, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER PRIMARY KEY)", false)
FINISH_BENCHMARK(Append100KIntegersINSERTPrimary)

DUCKDB_BENCHMARK(Append100KIntegersINSERTAutoCommit, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", true)
FINISH_BENCHMARK(Append100KIntegersINSERTAutoCommit)

//////////////
// PREPARED //
//////////////
struct DuckDBPreparedState : public DuckDBBenchmarkState {
	duckdb::unique_ptr<PreparedStatement> prepared;

	DuckDBPreparedState(string path) : DuckDBBenchmarkState(path) {
	}
	virtual ~DuckDBPreparedState() {
	}
};

#define APPEND_BENCHMARK_PREPARED(CREATE_STATEMENT)                                                                    \
	duckdb::unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                         \
		auto result = make_uniq<DuckDBPreparedState>(GetDatabasePath());                                               \
		return std::move(result);                                                                                      \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state_p) override {                                                                \
		auto state = (DuckDBPreparedState *)state_p;                                                                   \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
		state->prepared = state->conn.Prepare("INSERT INTO integers VALUES ($1)");                                     \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state_p) override {                                                        \
		auto state = (DuckDBPreparedState *)state_p;                                                                   \
		state->conn.Query("BEGIN TRANSACTION");                                                                        \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			state->prepared->Execute(i);                                                                               \
		}                                                                                                              \
		state->conn.Query("COMMIT");                                                                                   \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using a series of prepared INSERT INTO statements";             \
	}

DUCKDB_BENCHMARK(Append100KIntegersPREPARED, "[append]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersPREPARED)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDDisk, "[append]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersPREPAREDDisk)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDPrimary, "[append]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersPREPAREDPrimary)

//////////////
// APPENDER //
//////////////
#define APPEND_BENCHMARK_APPENDER(CREATE_STATEMENT)                                                                    \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		state->conn.Query("BEGIN TRANSACTION");                                                                        \
		Appender appender(state->conn, "integers");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.Append<int32_t>(i);                                                                               \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
		state->conn.Query("COMMIT");                                                                                   \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using an Appender";                                             \
	}

DUCKDB_BENCHMARK(Append100KIntegersAPPENDER, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersAPPENDER)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERDisk, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersAPPENDERDisk)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERPrimary, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersAPPENDERPrimary)

///////////////
// COPY INTO //
///////////////
#define APPEND_BENCHMARK_COPY(CREATE_STATEMENT)                                                                        \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query("CREATE TABLE integers(i INTEGER)");                                                         \
		Appender appender(state->conn, "integers");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.Append<int32_t>(i);                                                                               \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
		state->conn.Query("COPY integers TO 'integers.csv' DELIMITER '|'");                                            \
		state->conn.Query("DROP TABLE integers");                                                                      \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	string GetQuery() override {                                                                                       \
		return "COPY integers FROM 'integers.csv' DELIMITER '|'";                                                      \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using the COPY INTO statement";                                 \
	}

DUCKDB_BENCHMARK(Append100KIntegersCOPY, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersCOPY)

DUCKDB_BENCHMARK(Append100KIntegersCOPYDisk, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersCOPYDisk)

DUCKDB_BENCHMARK(Append100KIntegersCOPYPrimary, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersCOPYPrimary)

DUCKDB_BENCHMARK(Write100KIntegers, "[append]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
	Appender appender(state->conn, "integers");
	for (int32_t i = 0; i < 100000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
}
string GetQuery() override {
	return "COPY integers TO 'integers.csv' DELIMITER '|' HEADER";
}
string VerifyResult(QueryResult *result) override {
	if (result->HasError()) {
		return result->GetError();
	}
	return string();
}
string BenchmarkInfo() override {
	return "Write 100K 4-byte integers to CSV";
}
FINISH_BENCHMARK(Write100KIntegers)

namespace {

static int32_t GenerateI32InputValue(idx_t row_idx, idx_t chunk_idx) {
	const auto raw = static_cast<int64_t>(((row_idx + 1) * 8191 + (chunk_idx + 1) * 131) % 200000) - 100000;
	return static_cast<int32_t>(raw);
}

template <class T>
struct AppendDataChunkBenchmarkState : public DuckDBBenchmarkState {
	AppendDataChunkBenchmarkState(string path) : DuckDBBenchmarkState(std::move(path)) {
	}

	vector<unique_ptr<DataChunk>> chunks;
};

template <class T>
static void FillInputChunk(DataChunk &chunk, idx_t chunk_idx, T (*generate_value)(idx_t, idx_t), bool with_nulls,
                           idx_t cardinality) {
	auto &input_vector = chunk.data[0];
	auto data = FlatVector::GetDataMutable<T>(input_vector);
	for (idx_t row_idx = 0; row_idx < cardinality; row_idx++) {
		data[row_idx] = generate_value(row_idx, chunk_idx);
		if (with_nulls && ((row_idx + chunk_idx * 13) % 97) == 0) {
			FlatVector::SetNull(input_vector, row_idx, true);
		}
	}
	chunk.SetCardinality(cardinality);
}

template <class T, idx_t ROWS_PER_CHUNK>
static void PrepareAppendDataChunkBenchmarkState(DuckDBBenchmarkState *state_p, const LogicalType &logical_type,
                                                 const string &sql_type, T (*generate_value)(idx_t, idx_t),
                                                 bool with_nulls) {
	auto state = static_cast<AppendDataChunkBenchmarkState<T> *>(state_p);
	auto result = state->conn.Query("CREATE TABLE append_data_chunk_numeric(value " + sql_type + ")");
	if (result->HasError()) {
		throw InternalException(result->GetError());
	}

	state->chunks.clear();
	vector<LogicalType> types {logical_type};
	for (idx_t row_offset = 0; row_offset < 100000; row_offset += ROWS_PER_CHUNK) {
		const auto remaining = 100000 - row_offset;
		const auto cardinality = remaining < ROWS_PER_CHUNK ? remaining : ROWS_PER_CHUNK;
		const auto chunk_idx = row_offset / ROWS_PER_CHUNK;
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(*state->conn.context, types);
		FillInputChunk<T>(*chunk, chunk_idx, generate_value, with_nulls, cardinality);
		state->chunks.push_back(std::move(chunk));
	}
}

template <class T, bool FLUSH_PER_CHUNK>
static void RunAppendDataChunkLoop(DuckDBBenchmarkState *state_p) {
	auto state = static_cast<AppendDataChunkBenchmarkState<T> *>(state_p);
	state->conn.Query("BEGIN TRANSACTION");
	Appender appender(state->conn, "append_data_chunk_numeric");
	for (auto &chunk : state->chunks) {
		appender.AppendDataChunk(*chunk);
		if (FLUSH_PER_CHUNK) {
			appender.Flush();
		}
	}
	appender.Close();
	state->conn.Query("COMMIT");
}

template <class T, idx_t ROWS_PER_CHUNK>
static void ResetAppendDataChunkBenchmarkState(DuckDBBenchmarkState *state_p, const LogicalType &logical_type,
                                               const string &sql_type, T (*generate_value)(idx_t, idx_t),
                                               bool with_nulls) {
	auto result = state_p->conn.Query("DROP TABLE append_data_chunk_numeric");
	if (result->HasError()) {
		throw InternalException(result->GetError());
	}
	PrepareAppendDataChunkBenchmarkState<T, ROWS_PER_CHUNK>(state_p, logical_type, sql_type, generate_value,
	                                                        with_nulls);
}

} // namespace

///////////////////////////////////
// APPEND DATA CHUNK - 1 COLUMN //
///////////////////////////////////

#define DEFINE_APPEND_DATA_CHUNK_NUMERIC_BENCHMARK(NAME, GROUP, CPP_TYPE, LOGICAL_TYPE, SQL_TYPE, GENERATE_VALUE,      \
                                                   WITH_NULLS, ROWS_PER_CHUNK, FLUSH_PER_CHUNK, DESCRIPTION)           \
	DUCKDB_BENCHMARK(NAME, GROUP)                                                                                      \
	duckdb::unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                         \
		return make_uniq<AppendDataChunkBenchmarkState<CPP_TYPE>>(GetDatabasePath());                                  \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		PrepareAppendDataChunkBenchmarkState<CPP_TYPE, ROWS_PER_CHUNK>(state, LOGICAL_TYPE, SQL_TYPE, GENERATE_VALUE,  \
		                                                               WITH_NULLS);                                    \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		RunAppendDataChunkLoop<CPP_TYPE, FLUSH_PER_CHUNK>(state);                                                      \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		ResetAppendDataChunkBenchmarkState<CPP_TYPE, ROWS_PER_CHUNK>(state, LOGICAL_TYPE, SQL_TYPE, GENERATE_VALUE,    \
		                                                             WITH_NULLS);                                      \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K values from prebuilt, 1-column DataChunks of " + string(SQL_TYPE) + " " +                  \
		       string(DESCRIPTION);                                                                                    \
	}                                                                                                                  \
	FINISH_BENCHMARK(NAME)

DEFINE_APPEND_DATA_CHUNK_NUMERIC_BENCHMARK(AppendDataChunkI32AllValid1ColFlushEvery128Rows,
                                           "[append][append_data_chunk][flush]", int32_t, LogicalType::INTEGER,
                                           "INTEGER", GenerateI32InputValue, false, 128, true,
                                           "(all valid, 128 rows per DataChunk, flushing after every 128 rows)")
