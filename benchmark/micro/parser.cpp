#include "benchmark_runner.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"

#include <sstream>

namespace duckdb {

enum class ParserWorkload : uint8_t {
	SIMPLE_SELECT,
	KEYWORD_IDENTIFIERS,
	WIDE_SELECT,
	NESTED_EXPRESSIONS,
	CTES,
	STATEMENTS,
	TPCH,
	TPCDS,
	FLUMMI
};

struct ParserBenchmarkState : public BenchmarkState {
	ParserOptions options;
	vector<string> queries;
	idx_t statements_parsed = 0;
	bool valid_statement_counts = true;
	atomic<bool> interrupted {false};
};

class ParserMicroBenchmark : public Benchmark {
public:
	ParserMicroBenchmark(const string &name, ParserWorkload workload_p, idx_t iterations_p, idx_t statement_count_p = 1)
	    : Benchmark(true, name, "[parser]"), workload(workload_p), iterations(iterations_p),
	      statement_count(statement_count_p) {
	}

	unique_ptr<BenchmarkState> Initialize(BenchmarkConfiguration &config) override {
		auto state = make_uniq<ParserBenchmarkState>();
		state->queries = LoadQueries();
		state->options.compiled_grammar = CompiledGrammar::Create();
		for (idx_t i = 0; i < state->queries.size(); i++) {
			Parser parser(state->options);
			parser.ParseQuery(state->queries[i]);
			if (parser.statements.size() != statement_count) {
				throw InvalidInputException("Parser benchmark '%s' input %llu expected %llu statements, got %llu", name,
				                            i + 1, statement_count, parser.statements.size());
			}
		}
		return std::move(state);
	}

	void Run(BenchmarkState *state_p) override {
		auto &state = static_cast<ParserBenchmarkState &>(*state_p);
		state.statements_parsed = 0;
		state.valid_statement_counts = true;
		for (idx_t i = 0; i < iterations; i++) {
			for (auto &query : state.queries) {
				if (state.interrupted.load()) {
					return;
				}
				Parser parser(state.options);
				parser.ParseQuery(query);
				state.statements_parsed += parser.statements.size();
				state.valid_statement_counts &= parser.statements.size() == statement_count;
			}
		}
	}

	void Cleanup(BenchmarkState *state_p) override {
		auto &state = static_cast<ParserBenchmarkState &>(*state_p);
		state.interrupted.store(false);
	}

	string Verify(BenchmarkState *state_p) override {
		auto &state = static_cast<ParserBenchmarkState &>(*state_p);
		if (!state.valid_statement_counts || state.statements_parsed != iterations * QueryCount() * statement_count) {
			return "Unexpected number of parsed statements";
		}
		return string();
	}

	void Interrupt(BenchmarkState *state_p) override {
		auto &state = static_cast<ParserBenchmarkState &>(*state_p);
		state.interrupted.store(true);
	}

	string GetLogOutput(BenchmarkState *state) override {
		return string();
	}

	string DisplayName() override {
		return StringUtil::Format("%s (%llu inputs x %llu repetitions, %llu statements/input)", name, QueryCount(),
		                          iterations, statement_count);
	}

	string BenchmarkInfo() override {
		return StringUtil::Format("Parser::ParseQuery, %llu calls/run, %llu statements/call; "
		                          "default parser options, reused compiled grammar, no query execution",
		                          iterations * QueryCount(), statement_count);
	}

	string GetQuery() override {
		switch (workload) {
		case ParserWorkload::SIMPLE_SELECT:
			return "SELECT 1";
		case ParserWorkload::KEYWORD_IDENTIFIERS:
			return "SeLeCt abort, action, comment, database, first, last FROM source_table "
			       "WHERE action IS NOT NULL AND comment <> 'value' ORDER BY first, last";
		case ParserWorkload::WIDE_SELECT: {
			string query = "SELECT ";
			for (idx_t i = 0; i < 128; i++) {
				if (i > 0) {
					query += ", ";
				}
				query += "column_" + to_string(i) + " + " + to_string(i) + " AS alias_" + to_string(i);
			}
			return query + " FROM source_table";
		}
		case ParserWorkload::NESTED_EXPRESSIONS: {
			string expression = "value_column";
			for (idx_t i = 0; i < 32; i++) {
				expression = "coalesce(" + expression + ", " + to_string(i) + ")";
			}
			return "SELECT " + expression + " FROM source_table";
		}
		case ParserWorkload::CTES: {
			string query = "WITH cte_0 AS (SELECT 1 AS value_column)";
			for (idx_t i = 1; i < 16; i++) {
				query += ", cte_" + to_string(i) + " AS (SELECT value_column + 1 AS value_column FROM cte_" +
				         to_string(i - 1) + " WHERE value_column < 100)";
			}
			return query + " SELECT * FROM cte_15";
		}
		case ParserWorkload::STATEMENTS: {
			string query;
			for (idx_t i = 0; i < 32; i++) {
				query += "SELECT " + to_string(i) + " AS \"quoted column\"; -- statement boundary\n";
			}
			return query;
		}
		case ParserWorkload::TPCH:
		case ParserWorkload::TPCDS:
		case ParserWorkload::FLUMMI:
			return StringUtil::Join(LoadQueries(), "\n");
		default:
			throw InternalException("Unknown parser benchmark workload");
		}
	}

private:
	idx_t QueryCount() const {
		switch (workload) {
		case ParserWorkload::TPCH:
			return 22;
		case ParserWorkload::TPCDS:
			return 99;
		default:
			return 1;
		}
	}

	static string ReadQuery(const string &path) {
		ifstream input(path, ios::binary);
		if (!input.is_open()) {
			throw IOException("Cannot open parser benchmark input '%s'; provide the query files under --root-dir",
			                  path);
		}
		std::ostringstream buffer;
		buffer << input.rdbuf();
		if (input.bad() || buffer.bad()) {
			throw IOException("Cannot read parser benchmark input '%s'", path);
		}
		auto query = buffer.str();
		if (query.empty()) {
			throw InvalidInputException("Parser benchmark input '%s' is empty", path);
		}
		return query;
	}

	vector<string> LoadQueries() {
		if (workload == ParserWorkload::FLUMMI) {
			return {ReadQuery("benchmark/recursive_cte/queries/performance/flummi_ray.sql")};
		}
		if (workload != ParserWorkload::TPCH && workload != ParserWorkload::TPCDS) {
			return {GetQuery()};
		}
		const string prefix =
		    workload == ParserWorkload::TPCH ? "extension/tpch/dbgen/queries/q" : "extension/tpcds/dsdgen/queries/";
		vector<string> queries;
		for (idx_t i = 1; i <= QueryCount(); i++) {
			queries.push_back(ReadQuery(prefix + (i < 10 ? "0" : "") + to_string(i) + ".sql"));
		}
		return queries;
	}

private:
	ParserWorkload workload;
	idx_t iterations;
	idx_t statement_count;
};

ParserMicroBenchmark parser_simple_select("ParserSimpleSelect", ParserWorkload::SIMPLE_SELECT, 10000);
ParserMicroBenchmark parser_keyword_identifiers("ParserKeywordIdentifiers", ParserWorkload::KEYWORD_IDENTIFIERS, 2000);
ParserMicroBenchmark parser_wide_select("ParserWideSelect", ParserWorkload::WIDE_SELECT, 500);
ParserMicroBenchmark parser_nested_expressions("ParserNestedExpressions", ParserWorkload::NESTED_EXPRESSIONS, 1000);
ParserMicroBenchmark parser_ctes("ParserCTEs", ParserWorkload::CTES, 500);
ParserMicroBenchmark parser_statements("ParserStatements", ParserWorkload::STATEMENTS, 1000, 32);
ParserMicroBenchmark parser_tpch("ParserTPCH", ParserWorkload::TPCH, 50);
ParserMicroBenchmark parser_tpcds("ParserTPCDS", ParserWorkload::TPCDS, 10);
ParserMicroBenchmark parser_flummi("ParserFlummi", ParserWorkload::FLUMMI, 5);

} // namespace duckdb
