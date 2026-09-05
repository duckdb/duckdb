#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

#include <algorithm>

using namespace duckdb;

namespace {

struct ParserBenchmarkState : public DuckDBBenchmarkState {
	explicit ParserBenchmarkState(string path) : DuckDBBenchmarkState(std::move(path)) {
	}

	vector<string> queries;
};

string ReadFile(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	string result(handle->GetFileSize(), '\0');
	handle->Read(&result[0], result.size());
	return result;
}

// reads every .sql file in the directory, sorted by name so the corpus order is stable across runs
vector<string> ReadQueryDirectory(const string &directory) {
	auto fs = FileSystem::CreateLocal();
	vector<string> files;
	fs->ListFiles(directory, [&](const string &name, bool is_directory) {
		if (!is_directory && StringUtil::EndsWith(name, ".sql")) {
			files.push_back(fs->JoinPath(directory, name));
		}
	});
	if (files.empty()) {
		throw IOException("Parser benchmark found no .sql files in \"%s\"", directory);
	}
	std::sort(files.begin(), files.end());
	vector<string> queries;
	for (auto &file : files) {
		queries.push_back(ReadFile(*fs, file));
	}
	return queries;
}

void ParseCorpus(ParserBenchmarkState &state, idx_t iterations) {
	// the connection's options carry the compiled grammar, so the grammar is shared across parses
	auto options = state.conn.context->GetParserOptions();
	for (idx_t i = 0; i < iterations; i++) {
		for (auto &query : state.queries) {
			Parser parser(options);
			parser.ParseQuery(query);
		}
	}
}

// a single SELECT with a wide expression list, nested parentheses, a long IN list, CASE, chained function calls and
// window functions
string BuildExpressionQuery() {
	string sql = "SELECT ";
	for (idx_t i = 0; i < 40; i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += StringUtil::Format("(a%d + b%d * 3 - c%d / 2) %% 7 AS x%d", i, i, i, i);
	}
	sql += ", CASE WHEN a0 > 10 AND b0 < 20 THEN 'low' WHEN a0 BETWEEN 20 AND 30 OR c0 IS NULL THEN 'mid' ELSE "
	       "'high' END AS bucket";
	sql += ", ((((((((((a1 + 1) * 2) - 3) / 4) + 5) * 6) - 7) / 8) + 9) * 10) AS nested";
	sql += ", upper(trim(concat(cast(a2 AS VARCHAR), '-', lower(coalesce(s2, 'none'))))) AS chained";
	sql += ", row_number() OVER (PARTITION BY a3, b3 ORDER BY c3 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT "
	       "ROW) AS rn";
	sql += ", sum(a4) FILTER (WHERE b4 > 0) OVER (ORDER BY c4 RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS ws";
	sql += " FROM t WHERE a0 IN (";
	for (idx_t i = 0; i < 200; i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += std::to_string(i);
	}
	sql += ") AND (b0 LIKE 'abc%' OR b0 NOT SIMILAR TO 'x.*') AND c0::INTEGER = 1 AND d0 = DATE '2024-01-01' AND "
	       "e0 IS NOT DISTINCT FROM f0 AND EXISTS (SELECT 1 FROM u WHERE u.id = t.id AND u.v > (SELECT avg(v) FROM u))";
	sql += " ORDER BY 1, 2 DESC NULLS LAST LIMIT 100 OFFSET 10;";
	return sql;
}

// DDL and DML statements with many columns, constraints and rows
vector<string> BuildDDLQueries() {
	vector<string> queries;
	string create = "CREATE TABLE IF NOT EXISTS main.wide_table (";
	for (idx_t i = 0; i < 200; i++) {
		if (i > 0) {
			create += ", ";
		}
		switch (i % 5) {
		case 0:
			create += StringUtil::Format("col%d INTEGER NOT NULL DEFAULT 0", i);
			break;
		case 1:
			create += StringUtil::Format("col%d VARCHAR UNIQUE", i);
			break;
		case 2:
			create += StringUtil::Format("col%d DECIMAL(18, 3) CHECK (col%d >= 0)", i, i);
			break;
		case 3:
			create += StringUtil::Format("col%d TIMESTAMP WITH TIME ZONE", i);
			break;
		default:
			create += StringUtil::Format("col%d STRUCT(a INTEGER, b MAP(VARCHAR, DOUBLE[]))", i);
			break;
		}
	}
	create += ", PRIMARY KEY (col0, col5), FOREIGN KEY (col10) REFERENCES other_table (id));";
	queries.push_back(create);

	string insert = "INSERT INTO wide_table (col0, col1, col2, col3) VALUES ";
	for (idx_t i = 0; i < 500; i++) {
		if (i > 0) {
			insert += ", ";
		}
		insert += StringUtil::Format("(%d, 'value_%d', %d.5, '2024-01-01 00:00:00+00')", i, i, i);
	}
	insert += ";";
	queries.push_back(insert);

	queries.push_back("CREATE OR REPLACE VIEW v AS SELECT col0, col1, count(*) AS cnt FROM wide_table GROUP BY ALL "
	                  "HAVING count(*) > 1 ORDER BY cnt DESC;");
	queries.push_back("ALTER TABLE wide_table ADD COLUMN IF NOT EXISTS extra BIGINT DEFAULT 42;");
	queries.push_back("ALTER TABLE wide_table ALTER COLUMN col1 SET DATA TYPE TEXT;");
	queries.push_back("COPY wide_table TO 'out.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);");
	queries.push_back("CREATE INDEX idx ON wide_table USING ART (col0, col1) WITH (leaf_size = 4);");
	queries.push_back("UPDATE wide_table SET col0 = col0 + 1, col1 = 'x' WHERE col2 > 10 AND col3 IS NOT NULL;");
	queries.push_back("DELETE FROM wide_table USING other_table WHERE wide_table.col10 = other_table.id;");
	queries.push_back("WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 100) SELECT * "
	                  "FROM cte;");
	queries.push_back("PIVOT wide_table ON col1 USING sum(col0) GROUP BY col2;");
	queries.push_back("SELECT * FROM read_csv('file.csv', header = true, delim = '|', columns = {'a': 'INTEGER', "
	                  "'b': 'VARCHAR'});");
	return queries;
}

} // namespace

#define PARSER_BENCHMARK_BODY(LOAD, INFO)                                                                              \
	duckdb::unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                         \
		return make_uniq<ParserBenchmarkState>(GetDatabasePath());                                                     \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state_p) override {                                                                \
		auto &state = static_cast<ParserBenchmarkState &>(*state_p);                                                   \
		LOAD;                                                                                                          \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return INFO;                                                                                                   \
	}

DUCKDB_BENCHMARK(ParserTPCH, "[parser]")
PARSER_BENCHMARK_BODY(state.queries = ReadQueryDirectory("extension/tpch/dbgen/queries"),
                      "Parse the 22 TPC-H queries 200 times")
void RunBenchmark(DuckDBBenchmarkState *state_p) override {
	ParseCorpus(static_cast<ParserBenchmarkState &>(*state_p), 200);
}
FINISH_BENCHMARK(ParserTPCH)

DUCKDB_BENCHMARK(ParserTPCDS, "[parser]")
PARSER_BENCHMARK_BODY(state.queries = ReadQueryDirectory("extension/tpcds/dsdgen/queries"),
                      "Parse the 99 TPC-DS queries 50 times")
void RunBenchmark(DuckDBBenchmarkState *state_p) override {
	ParseCorpus(static_cast<ParserBenchmarkState &>(*state_p), 50);
}
FINISH_BENCHMARK(ParserTPCDS)

DUCKDB_BENCHMARK(ParserExpressions, "[parser]")
PARSER_BENCHMARK_BODY(state.queries.push_back(BuildExpressionQuery()), "Parse an expression-heavy SELECT 500 times")
void RunBenchmark(DuckDBBenchmarkState *state_p) override {
	ParseCorpus(static_cast<ParserBenchmarkState &>(*state_p), 500);
}
FINISH_BENCHMARK(ParserExpressions)

DUCKDB_BENCHMARK(ParserDDL, "[parser]")
PARSER_BENCHMARK_BODY(state.queries = BuildDDLQueries(), "Parse a set of wide DDL and DML statements 200 times")
void RunBenchmark(DuckDBBenchmarkState *state_p) override {
	ParseCorpus(static_cast<ParserBenchmarkState &>(*state_p), 200);
}
FINISH_BENCHMARK(ParserDDL)

DUCKDB_BENCHMARK(TokenizerTPCDS, "[parser]")
PARSER_BENCHMARK_BODY(state.queries = ReadQueryDirectory("extension/tpcds/dsdgen/queries"),
                      "Tokenize the 99 TPC-DS queries 200 times")
void RunBenchmark(DuckDBBenchmarkState *state_p) override {
	auto &state = static_cast<ParserBenchmarkState &>(*state_p);
	for (idx_t i = 0; i < 200; i++) {
		for (auto &query : state.queries) {
			Parser::Tokenize(query);
		}
	}
}
FINISH_BENCHMARK(TokenizerTPCDS)
