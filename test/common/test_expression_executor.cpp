#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Table filter selection preserves both partitions with mapped input", "[expression_executor]") {
	constexpr idx_t COUNT = 17;
	DuckDB db(nullptr);
	Connection con(db);
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BOOLEAN}, COUNT);

	for (idx_t pattern = 0; pattern < 3; pattern++) {
		{
			auto writer = FlatVector::Writer<bool>(input.data[0], COUNT);
			for (idx_t i = 0; i < COUNT; i++) {
				if (pattern == 2 && i % 3 == 2) {
					writer.WriteNull();
				} else {
					writer.WriteValue(pattern == 1 || (pattern == 2 && i % 3 == 0));
				}
			}
		}
		input.CheckCardinality(COUNT);
		auto values = input.data[0].Values<bool>();
		for (idx_t count : {0, 1, 2, 17}) {
			DataChunk prefix;
			prefix.InitializeEmpty(input.GetTypes());
			prefix.Slice(input, 0, count);
			for (bool mapped : {false, true}) {
				for (idx_t outputs = 1; outputs <= 3; outputs++) {
					for (idx_t alias = 0; alias <= 2; alias++) {
						if (alias && (!mapped || !(outputs & alias))) {
							continue;
						}
						INFO("pattern=" << pattern << " count=" << count << " mapped=" << mapped
						                << " outputs=" << outputs << " alias=" << alias);
						SelectionVector mapping(COUNT), true_sel(COUNT), false_sel(COUNT);
						vector<sel_t> expected_true, expected_false;
						for (idx_t i = 0; i < count; i++) {
							auto row = mapped ? (i * 7 + 3) % 11 : i;
							mapping.set_index(i, row);
							auto entry = values[row];
							auto &expected = entry.IsValid() && entry.GetValue() ? expected_true : expected_false;
							expected.push_back(NumericCast<sel_t>(row));
						}
						auto true_output = outputs & 1 ? (alias == 1 ? &mapping : &true_sel) : nullptr;
						auto false_output = outputs & 2 ? (alias == 2 ? &mapping : &false_sel) : nullptr;
						auto expression = CreateSelectivityOptionalFilterExpression(
						    make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 0), LogicalType::BOOLEAN, 1.0f,
						    9);
						ExpressionExecutor executor(*con.context, *expression);
						auto true_count = executor.SelectExpression(mapped ? input : prefix, true_output, false_output,
						                                            mapped ? &mapping : nullptr, count);
						REQUIRE(true_count == expected_true.size());
						if (true_output) {
							for (idx_t i = 0; i < expected_true.size(); i++) {
								REQUIRE(true_output->get_index(i) == expected_true[i]);
							}
						}
						if (false_output) {
							for (idx_t i = 0; i < expected_false.size(); i++) {
								REQUIRE(false_output->get_index(i) == expected_false[i]);
							}
						}
					}
				}
			}
		}
	}
}
