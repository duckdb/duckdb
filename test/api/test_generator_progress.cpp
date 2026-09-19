#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include <cmath>

using namespace duckdb;

static void CheckGenerator(const string &name, vector<Value> arguments, idx_t expected_count, bool exact_count = true) {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context, QualifiedName(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), Identifier(name)));
	vector<LogicalType> argument_types;
	for (auto &argument : arguments) {
		argument_types.push_back(argument.type());
	}
	auto function = *entry.functions.GetFunctionByArguments(context, argument_types);
	REQUIRE(function.table_scan_progress);
	REQUIRE(function.init_global);
	named_parameter_map_t named;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(arguments, named, input_types, input_names, nullptr, nullptr, function, ref);
	vector<LogicalType> return_types;
	vector<Identifier> return_names;
	auto bind_data = function.bind(context, bind_input, return_types, return_names);
	vector<column_t> columns {0};
	TableFunctionInitInput init_input(bind_data.get(), columns, {}, nullptr);
	REQUIRE_FALSE(function.init_global(context, init_input));
	PhysicalPlan plan(Allocator::Get(context));
	PhysicalTableInOutFunction in_out(plan, return_types, function, nullptr, {ColumnIndex(0)}, {}, nullptr,
	                                  expected_count, {});
	auto in_out_state = in_out.GetGlobalOperatorState(context);
	REQUIRE(in_out_state->MaxThreads(48) == 48);
	PhysicalTableScan scan(plan, return_types, function, nullptr, return_types, {ColumnIndex(0)}, {}, {}, nullptr,
	                       expected_count, {}, arguments, {});
	init_input.op = scan;
	auto global = function.init_global(context, init_input);
	REQUIRE(global);
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	auto local = function.init_local(execution, init_input, global.get());
	TableFunctionInput function_input(bind_data.get(), local.get(), global.get());
	DataChunk input, output;
	input.Initialize(Allocator::Get(context), argument_types);
	output.Initialize(Allocator::Get(context), return_types);
	for (idx_t i = 0; i < arguments.size(); i++) {
		input.data[i].Append(arguments[i]);
	}
	input.SetChildCardinality(1);
	auto previous = function.table_scan_progress(context, bind_data.get(), global.get());
	REQUIRE(previous == 0);
	idx_t rows = 0;
	bool advanced = false;
	bool finished = false;
	for (idx_t chunk = 0; chunk < expected_count / STANDARD_VECTOR_SIZE + 3; chunk++) {
		output.Reset();
		auto result = function.in_out_function(execution, function_input, input, output);
		rows += output.size();
		auto progress = function.table_scan_progress(context, bind_data.get(), global.get());
		INFO(name << " rows=" << rows << " progress=" << progress);
		REQUIRE(std::isfinite(progress));
		REQUIRE(progress >= previous);
		REQUIRE(progress >= 0);
		REQUIRE(progress <= 100);
		if (progress > 0 && progress < 100) {
			advanced = true;
		}
		if (exact_count && expected_count > 0 && output.size()) {
			REQUIRE(progress == Approx(100.0 * double(rows) / expected_count).margin(1e-12));
		}
		previous = progress;
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			finished = true;
			break;
		}
	}
	REQUIRE(finished);
	REQUIRE(previous == 100);
	if (exact_count) {
		REQUIRE(rows == expected_count);
	}
	if (expected_count > STANDARD_VECTOR_SIZE) {
		REQUIRE(advanced);
	}
	con.Rollback();
}

TEST_CASE("Integer generators report exact chunk progress", "[progress-bar][generator-progress]") {
	const int64_t count = 2 * STANDARD_VECTOR_SIZE + 1;
	for (auto name : {"range", "generate_series"}) {
		const bool inclusive = string(name) == "generate_series";
		SECTION(name) {
			CheckGenerator(name, {Value::BIGINT(count)}, count + inclusive);
			CheckGenerator(name, {Value::BIGINT(5), Value::BIGINT(5 + 3 * count), Value::BIGINT(3)}, count + inclusive);
			CheckGenerator(name, {Value::BIGINT(5 + 3 * count), Value::BIGINT(5), Value::BIGINT(-3)},
			               count + inclusive);
			CheckGenerator(name, {Value::BIGINT(5), Value::BIGINT(6 + 3 * count), Value::BIGINT(3)}, count + 1);
			CheckGenerator(name, {Value::BIGINT(6 + 3 * count), Value::BIGINT(5), Value::BIGINT(-3)}, count + 1);
		}
	}
}

TEST_CASE("Empty and singleton generators finish progress", "[progress-bar][generator-progress]") {
	for (auto name : {"range", "generate_series"}) {
		SECTION(name) {
			CheckGenerator(name, {Value::BIGINT(5), Value::BIGINT(5)}, string(name) == "range" ? 0 : 1);
			CheckGenerator(name, {Value::BIGINT(5), Value::BIGINT(0), Value::BIGINT(1)}, 0);
			CheckGenerator(name, {Value::BIGINT(0), Value::BIGINT(5), Value::BIGINT(-1)}, 0);
			CheckGenerator(name, {Value(LogicalType::BIGINT)}, 0);
		}
	}
}

TEST_CASE("Generator progress handles ranges near integer bounds", "[progress-bar][generator-progress]") {
	const int64_t count = 2 * STANDARD_VECTOR_SIZE + 1;
	for (auto name : {"range", "generate_series"}) {
		const bool inclusive = string(name) == "generate_series";
		SECTION(name) {
			CheckGenerator(name, {Value::BIGINT(INT64_MIN), Value::BIGINT(INT64_MIN + count)}, count + inclusive);
			CheckGenerator(name, {Value::BIGINT(INT64_MAX - count), Value::BIGINT(INT64_MAX)}, count + inclusive);
			CheckGenerator(name, {Value::BIGINT(INT64_MAX), Value::BIGINT(INT64_MAX - count), Value::BIGINT(-1)},
			               count + inclusive);
			CheckGenerator(name, {Value::BIGINT(INT64_MIN + count), Value::BIGINT(INT64_MIN), Value::BIGINT(-1)},
			               count + inclusive);
		}
	}
}

TEST_CASE("Timestamp generators report bounded advancing progress", "[progress-bar][generator-progress]") {
	const int64_t days = 2 * STANDARD_VECTOR_SIZE + 1;
	const auto start = Timestamp::FromString("2000-01-01", false);
	const timestamp_t end(start.value + days * Interval::MICROS_PER_DAY);
	const idx_t years = STANDARD_VECTOR_SIZE / 6 + 1;
	const auto month_start = Timestamp::FromString("2000-01-31", false);
	const auto month_end = Timestamp::FromString(std::to_string(2000 + years) + "-01-31", false);
	for (auto name : {"range", "generate_series"}) {
		SECTION(name) {
			CheckGenerator(name, {Value::TIMESTAMP(start), Value::TIMESTAMP(end), Value::INTERVAL(0, 1, 0)}, days + 1,
			               false);
			CheckGenerator(name, {Value::TIMESTAMP(end), Value::TIMESTAMP(start), Value::INTERVAL(0, -1, 0)}, days + 1,
			               false);
			CheckGenerator(name, {Value::TIMESTAMP(month_start), Value::TIMESTAMP(month_end), Value::INTERVAL(1, 0, 0)},
			               years * 12 + 1, false);
			CheckGenerator(name,
			               {Value::TIMESTAMP(month_end), Value::TIMESTAMP(month_start), Value::INTERVAL(-1, 0, 0)},
			               years * 12 + 1, false);
		}
	}
}

TEST_CASE("Generator progress advances through a mixed UNION", "[progress-bar][generator-progress]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("SET threads=1")->HasError());
	REQUIRE_FALSE(con.Query("SET enable_progress_bar=true")->HasError());
	REQUIRE_FALSE(con.Query("SET enable_progress_bar_print=false")->HasError());
	REQUIRE_FALSE(con.Query("SET progress_bar_time=0")->HasError());
	const idx_t native_count = 2 * STANDARD_VECTOR_SIZE;
	const idx_t generator_count = 1024 * STANDARD_VECTOR_SIZE;
	REQUIRE_FALSE(
	    con.Query("CREATE TABLE progress_input AS SELECT i FROM range(" + std::to_string(native_count) + ") t(i)")
	        ->HasError());
	auto pending = con.PendingQuery("SELECT sum(i) FROM (SELECT i FROM progress_input UNION "
	                                "ALL SELECT i FROM range(" +
	                                std::to_string(generator_count) + ") t(i))");
	REQUIRE_FALSE(pending->HasError());
	double previous = 0;
	bool advanced_past_native_input = false;
	bool ready = false;
	for (idx_t task = 0; task < generator_count / STANDARD_VECTOR_SIZE + 100; task++) {
		auto status = pending->ExecuteTask();
		REQUIRE(status != PendingExecutionResult::EXECUTION_ERROR);
		if (PendingQueryResult::IsResultReady(status)) {
			ready = true;
			break;
		}
		const auto progress = con.context->GetQueryProgress().GetPercentage();
		REQUIRE(std::isfinite(progress));
		REQUIRE(progress >= previous);
		REQUIRE(progress <= 100);
		if (progress > 10 && progress < 100) {
			advanced_past_native_input = true;
		}
		previous = progress;
	}
	REQUIRE(ready);
	REQUIRE(advanced_past_native_input);
	auto result = pending->Execute();
	REQUIRE_FALSE(result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	const int64_t expected =
	    int64_t(native_count) * (native_count - 1) / 2 + int64_t(generator_count) * (generator_count - 1) / 2;
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == expected);
}
