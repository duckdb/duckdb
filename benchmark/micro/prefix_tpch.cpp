#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

//----------------------------- PREFIX1 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem)

DUCKDB_BENCHMARK(PrefixLineitemInlined, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined)

DUCKDB_BENCHMARK(PrefixLineitemPointer, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer)

//----------------------------- PREFIX2 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem222, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix2(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem222)

DUCKDB_BENCHMARK(PrefixLineitemInlined222, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix2(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined222)

DUCKDB_BENCHMARK(PrefixLineitemPointer222, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix2(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer222)

//----------------------------- PREFIX3 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem333, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix3(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem333)

DUCKDB_BENCHMARK(PrefixLineitemInlined333, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix3(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined333)

DUCKDB_BENCHMARK(PrefixLineitemPointer333, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix3(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer333)

//----------------------------- PREFIX4 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem444, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix4(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem444)

DUCKDB_BENCHMARK(PrefixLineitemInlined444, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix4(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined444)

DUCKDB_BENCHMARK(PrefixLineitemPointer444, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix4(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer444)


//----------------------------- PREFIX5 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem555, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix5(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem555)

DUCKDB_BENCHMARK(PrefixLineitemInlined555, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix5(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined555)

DUCKDB_BENCHMARK(PrefixLineitemPointer555, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix5(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer555)

//----------------------------- PREFIX6 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem666, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix6(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem666)

DUCKDB_BENCHMARK(PrefixLineitemInlined666, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix6(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined666)

DUCKDB_BENCHMARK(PrefixLineitemPointer666, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix6(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer666)


//----------------------------- PREFIX7 ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem777, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix7(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem777)

DUCKDB_BENCHMARK(PrefixLineitemInlined777, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix7(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined777)

DUCKDB_BENCHMARK(PrefixLineitemPointer777, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE prefix7(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer777)

//-------------------------------- LIKE ---------------------------------------
DUCKDB_BENCHMARK(PrefixLineitemLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'NONE%'";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix early out LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemLike)

DUCKDB_BENCHMARK(PrefixLineitemInlinedLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'COLLECT%'";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemInlinedLike)

DUCKDB_BENCHMARK(PrefixLineitemPointerLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override {
    return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'DELIVER IN PERS%'";
}
string VerifyResult(QueryResult *result) override {
    if (!result->success) {
        return result->error;
    }
    return string();
}
string BenchmarkInfo() override {
    return "Prefix inlined LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemPointerLike)
