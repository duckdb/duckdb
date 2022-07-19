#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow_test_factory.hpp"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include <parquet/arrow/reader.h>
#include "arrow/io/file.h"
#include <arrow/type_traits.h>
#include "arrow/table.h"
#include "arrow/c/bridge.h"
#include <memory>
#include "parquet/exception.h"

std::unique_ptr<duckdb::QueryResult> FromArrow(int32_t month, int64_t day_time_ms, bool null = false) {
	auto schema =
	    arrow::schema({arrow::field("a", arrow::day_time_interval()), arrow::field("b", arrow::month_interval())});
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	arrow::TypeTraits<arrow::Int32Type>::BuilderType month_builder;
	arrow::TypeTraits<arrow::Int64Type>::BuilderType days_builder;
	if (null) {
		month_builder.AppendNull();
		days_builder.AppendNull();
	} else {
		month_builder.Append(month).ok();
		days_builder.Append(day_time_ms).ok();
	}

	auto months_res = month_builder.Finish();
	auto days_res = days_builder.Finish();
	batches.push_back(arrow::RecordBatch::Make(schema, 1, {days_res.MoveValueUnsafe(), months_res.MoveValueUnsafe()}));
	SimpleFactory factory {batches, schema};
	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	duckdb::vector<duckdb::Value> params;
	params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
	params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
	params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::GetSchema));
	params.push_back(duckdb::Value::UBIGINT(1000000));
	auto result = conn.TableFunction("arrow_scan", params)->Execute();
	return result;
}
TEST_CASE("Test Interval", "[arrow]") {
	//! Test Conversion
	auto result = FromArrow(1, 1000);
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {"00:00:01"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1 month"}));

	//! Test Nulls
	result = FromArrow(0, 0, true);
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {nullptr}));
	REQUIRE(CHECK_COLUMN(result, 1, {nullptr}));
}
