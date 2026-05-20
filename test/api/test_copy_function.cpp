#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"

using namespace duckdb;

namespace {

struct FinishCopyBindData : public FunctionData {
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<FinishCopyBindData>();
	}

	bool Equals(const FunctionData &) const override {
		return true;
	}
};

struct FinishCopyLocalState : public LocalFunctionData {
};

struct FinishCopyGlobalState : public GlobalFunctionData {
	idx_t chunks_seen = 0;
};

static unique_ptr<FunctionData> FinishCopyBind(ClientContext &, CopyFunctionBindInput &, const vector<string> &,
                                               const vector<LogicalType> &) {
	return make_uniq<FinishCopyBindData>();
}

static unique_ptr<LocalFunctionData> FinishCopyInitializeLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<FinishCopyLocalState>();
}

static unique_ptr<GlobalFunctionData> FinishCopyInitializeGlobal(ClientContext &, FunctionData &, const string &) {
	return make_uniq<FinishCopyGlobalState>();
}

static SinkResultType FinishCopySink(ExecutionContext &, FunctionData &, GlobalFunctionData &gstate,
                                     LocalFunctionData &, DataChunk &) {
	auto &state = gstate.Cast<FinishCopyGlobalState>();
	state.chunks_seen++;
	return SinkResultType::FINISHED;
}

static void FinishCopyCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void FinishCopyFinalize(ClientContext &, FunctionData &, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<FinishCopyGlobalState>();
	REQUIRE(state.chunks_seen == 1);
}

static void RegisterFinishCopyFunction(DuckDB &db) {
	CopyFunction function("finish_copy");
	function.copy_to_bind = FinishCopyBind;
	function.copy_to_initialize_local = FinishCopyInitializeLocal;
	function.copy_to_initialize_global = FinishCopyInitializeGlobal;
	function.copy_to_sink_with_result = FinishCopySink;
	function.copy_to_combine = FinishCopyCombine;
	function.copy_to_finalize = FinishCopyFinalize;

	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "finish_copy_extension", ""};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(std::move(function));
}

} // namespace

TEST_CASE("COPY TO supports sink result callbacks", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterFinishCopyFunction(db);

	auto result =
	    con.Query("COPY (SELECT i FROM range(8192) tbl(i)) TO 'unused' (FORMAT finish_copy, USE_TMP_FILE false)");
	REQUIRE_NO_FAIL(*result);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	auto copied_rows = chunk->GetValue(0, 0).GetValue<int64_t>();
	REQUIRE(copied_rows > 0);
	REQUIRE(copied_rows < 8192);
}
