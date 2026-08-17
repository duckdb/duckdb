#include "catch.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct CopyAbortTestInfo : CopyFunctionInfo {
	CopyAbortTestInfo(bool fail_sink_p, bool fail_finalize_p = false)
	    : fail_sink(fail_sink_p), fail_finalize(fail_finalize_p) {
	}

	bool fail_sink;
	bool fail_finalize;
	atomic<idx_t> initialize_count {0};
	atomic<idx_t> finalize_count {0};
	atomic<idx_t> abort_count {0};
};

struct CopyAbortBindData : FunctionData {
	explicit CopyAbortBindData(shared_ptr<CopyFunctionInfo> info_p) : info(std::move(info_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CopyAbortBindData>(info);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CopyAbortBindData>();
		return info == other.info;
	}

	CopyAbortTestInfo &GetInfo() {
		return info->Cast<CopyAbortTestInfo>();
	}

	shared_ptr<CopyFunctionInfo> info;
};

struct CopyAbortLocalData : LocalFunctionData {};

struct CopyAbortGlobalData : GlobalFunctionData {};

static unique_ptr<FunctionData> CopyAbortBind(ClientContext &, CopyFunctionBindInput &input, const vector<Identifier> &,
                                              const vector<LogicalType> &) {
	return make_uniq<CopyAbortBindData>(input.function_info);
}

static unique_ptr<LocalFunctionData> CopyAbortInitializeLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<CopyAbortLocalData>();
}

static unique_ptr<GlobalFunctionData> CopyAbortInitializeGlobal(ClientContext &, FunctionData &bind_data,
                                                                const string &) {
	++bind_data.Cast<CopyAbortBindData>().GetInfo().initialize_count;
	return make_uniq<CopyAbortGlobalData>();
}

static void CopyAbortSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &, LocalFunctionData &,
                          DataChunk &) {
	if (bind_data.Cast<CopyAbortBindData>().GetInfo().fail_sink) {
		throw IOException("Injected COPY sink failure");
	}
}

static void CopyAbortCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void CopyAbortFinalize(ClientContext &, FunctionData &bind_data, GlobalFunctionData &) {
	auto &info = bind_data.Cast<CopyAbortBindData>().GetInfo();
	++info.finalize_count;
	if (info.fail_finalize) {
		throw IOException("Injected COPY finalize failure");
	}
}

static void CopyAbortAbort(ClientContext &, FunctionData &bind_data, GlobalFunctionData &) {
	++bind_data.Cast<CopyAbortBindData>().GetInfo().abort_count;
}

static CopyFunction CreateCopyAbortFunction(const string &name, const shared_ptr<CopyAbortTestInfo> &info) {
	CopyFunction function {Identifier(name)};
	function.copy_to_bind = CopyAbortBind;
	function.copy_to_initialize_local = CopyAbortInitializeLocal;
	function.copy_to_initialize_global = CopyAbortInitializeGlobal;
	function.copy_to_sink = CopyAbortSink;
	function.copy_to_combine = CopyAbortCombine;
	function.copy_to_finalize = CopyAbortFinalize;
	function.copy_to_abort = CopyAbortAbort;
	function.function_info = info;
	return function;
}

} // namespace

TEST_CASE("COPY dispatches abort only for unfinished file state", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));

	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "copy_abort_test", ""};
	ExtensionLoader loader {load_info};

	auto failure_info = make_shared_ptr<CopyAbortTestInfo>(true);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_failure", failure_info));
	REQUIRE_FAIL(connection.Query(
	    "COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_failure.test' (FORMAT copy_abort_failure)"));
	REQUIRE(failure_info->initialize_count == 1);
	REQUIRE(failure_info->finalize_count == 0);
	REQUIRE(failure_info->abort_count == 1);

	auto finalize_failure_info = make_shared_ptr<CopyAbortTestInfo>(false, true);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_finalize_failure", finalize_failure_info));
	REQUIRE_FAIL(connection.Query("COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_finalize_failure.test' "
	                              "(FORMAT copy_abort_finalize_failure)"));
	REQUIRE(finalize_failure_info->initialize_count == 1);
	REQUIRE(finalize_failure_info->finalize_count == 1);
	REQUIRE(finalize_failure_info->abort_count == 1);

	auto success_info = make_shared_ptr<CopyAbortTestInfo>(false);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_success", success_info));
	REQUIRE_NO_FAIL(connection.Query(
	    "COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_success.test' (FORMAT copy_abort_success)"));
	REQUIRE(success_info->initialize_count == 1);
	REQUIRE(success_info->finalize_count == 1);
	REQUIRE(success_info->abort_count == 0);
}
