#include <arrow/type_traits.h>
#define CATCH_CONFIG_MAIN
#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "catch.hpp"

static arrow::Result<std::shared_ptr<arrow::Array>> GenI32Seq(int32_t n, int32_t& next) {
	arrow::TypeTraits<arrow::Int32Type>::BuilderType builder;
	for (int32_t i = 0; i < n; ++i) {
		builder.Append(next++).ok();
	}
	return builder.Finish();
}
constexpr size_t ARRAY_SIZE = 1024;

#define REQUIRE_RESULT(OUT, IN) REQUIRE(IN.ok()); OUT = IN.ValueUnsafe();

TEST_CASE("Test random integers") {

	for (size_t n : std::vector<size_t> {100, 1000, 10000, 100000}) {
		auto schema = arrow::schema({
		    arrow::field("a", arrow::int32()),
		    arrow::field("b", arrow::int32()),
		    arrow::field("c", arrow::int32()),
		});
        int32_t start_a = 0, start_b = 0xFFFF, start_c = 0xFFFFF;
        int32_t next_a = start_a, next_b = start_b, next_c = start_c;

		DYNAMIC_SECTION("N=" << n) {
			std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
			for (size_t ofs = 0; ofs < n;) {
				auto here = std::min<size_t>(ARRAY_SIZE, n - ofs);
                REQUIRE_RESULT(auto a, GenI32Seq(ofs, next_a));
                REQUIRE_RESULT(auto b, GenI32Seq(ofs, next_b));
                REQUIRE_RESULT(auto c, GenI32Seq(ofs, next_c));
				batches.push_back(
				    arrow::RecordBatch::Make(schema, here, {a, b, c}));
                ofs += ARRAY_SIZE;
			}
            REQUIRE_RESULT(auto maybe_reader, arrow::RecordBatchReader::Make(std::move(batches), schema));
		}
	}
}