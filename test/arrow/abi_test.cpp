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

static arrow::Result<std::shared_ptr<arrow::Array>> GenI32Seq(int32_t n, int32_t next) {
	arrow::TypeTraits<arrow::Int32Type>::BuilderType builder;
	for (int32_t i = 0; i < n; ++i) {
		builder.Append(next++).ok();
	}
	REQUIRE(builder.length() == n);
	return builder.Finish();
}

TEST_CASE("Test random integers", "[arrow]") {

	for (size_t n : std::vector<size_t> {100, 1000, 10000, 100000}) {
		auto schema = arrow::schema({
		    arrow::field("a", arrow::int32()),
		    arrow::field("b", arrow::int32()),
		    arrow::field("c", arrow::int32()),
		});
		int32_t start_a = 0, start_b = 0xFFFF, start_c = 0xFFFFF;

		DYNAMIC_SECTION("N=" << n) {
			int32_t next_a = start_a, next_b = start_b, next_c = start_c;

			std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
			while (n > 0) {
				auto here = std::min<size_t>(STANDARD_VECTOR_SIZE, n);
				REQUIRE_RESULT(auto a, GenI32Seq(here, next_a));
				REQUIRE_RESULT(auto b, GenI32Seq(here, next_b));
				REQUIRE_RESULT(auto c, GenI32Seq(here, next_c));
				next_a += here;
				next_b += here;
				next_c += here;
				batches.push_back(arrow::RecordBatch::Make(schema, here, {a, b, c}));
				n -= here;
			}

			SECTION("Batch reader") {
				REQUIRE_RESULT(auto reader, arrow::RecordBatchReader::Make(batches, schema));

				int32_t expected_a = start_a, expected_b = start_b, expected_c = start_c;

				for (auto next = reader->Next(); next.ok() && next.ValueUnsafe(); next = reader->Next()) {
					auto &batch = *next.ValueUnsafe();
					REQUIRE(batch.num_columns() == 3);

					auto a_array = batch.column(0);
					auto b_array = batch.column(1);
					auto c_array = batch.column(2);
					REQUIRE(a_array->type_id() == arrow::Type::INT32);
					REQUIRE(b_array->type_id() == arrow::Type::INT32);
					REQUIRE(c_array->type_id() == arrow::Type::INT32);

					for (int64_t i = 0; i < batch.num_rows(); ++i) {
						INFO(a_array->ToString());
						REQUIRE(reinterpret_cast<const arrow::Int32Array *>(a_array.get())->Value(i) == expected_a++);
						REQUIRE(reinterpret_cast<const arrow::Int32Array *>(b_array.get())->Value(i) == expected_b++);
						REQUIRE(reinterpret_cast<const arrow::Int32Array *>(c_array.get())->Value(i) == expected_c++);
					}
				}
			}

			SECTION("DuckDB scan") {
				SimpleFactory factory {batches, schema};
				duckdb::DuckDB db;
				duckdb::Connection conn {db};

				duckdb::vector<duckdb::Value> params;
				params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
				params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
				params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::GetSchema));
				params.push_back(duckdb::Value::UBIGINT(1000000));
				auto result = conn.TableFunction("arrow_scan", params)->Execute();
				REQUIRE(result->ColumnCount() == 3);
				int32_t expected_a = start_a, expected_b = start_b, expected_c = start_c;
				auto col_chunk = result->Fetch();
				while (col_chunk) {
					for (size_t i = 0; i < col_chunk->size(); i++) {
						REQUIRE(duckdb::IntegerValue::Get(col_chunk->GetValue(0, i)) == expected_a++);
						REQUIRE(duckdb::IntegerValue::Get(col_chunk->GetValue(1, i)) == expected_b++);
						REQUIRE(duckdb::IntegerValue::Get(col_chunk->GetValue(2, i)) == expected_c++);
					}
					col_chunk = result->Fetch();
				}
			}
		}
	}
}
