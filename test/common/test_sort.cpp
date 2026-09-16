#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Sort decodes keys with default order modifiers", "[sort]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	ThreadContext thread(*con.context);
	ExecutionContext context(*con.context, thread, nullptr);
	for (const auto &type : {LogicalType::INTEGER, LogicalType::VARCHAR}) {
		for (const auto direction : {OrderType::ASCENDING, OrderType::DESCENDING, OrderType::ORDER_DEFAULT}) {
			for (const auto null_order :
			     {OrderByNullType::NULLS_FIRST, OrderByNullType::NULLS_LAST, OrderByNullType::ORDER_DEFAULT}) {
				CAPTURE(type, direction, null_order);
				duckdb::vector<LogicalType> types {type};
				duckdb::vector<BoundOrderByNode> orders;
				orders.emplace_back(direction, null_order, make_uniq<BoundReferenceExpression>(type, 0));
				Sort sort(*con.context, orders, types, {});
				auto global_sink = sort.GetGlobalSinkState(*con.context);
				auto local_sink = sort.GetLocalSinkState(context);
				InterruptState interrupt;
				OperatorSinkInput sink {*global_sink, *local_sink, interrupt};
				DataChunk input;
				input.Initialize(*con.context, types);
				const auto low = type == LogicalType::INTEGER ? Value::INTEGER(-7) : Value("a");
				const auto high = type == LogicalType::INTEGER ? Value::INTEGER(42) : Value("z");
				duckdb::vector<Value> values {high, Value(type), low, high};
				for (idx_t offset = 0; offset < values.size(); offset += STANDARD_VECTOR_SIZE) {
					input.Reset();
					const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, values.size() - offset);
					for (idx_t row = 0; row < count; row++) {
						input.data[0].Append(values[offset + row]);
					}
					input.CheckCardinality(count);
					sort.Sink(context, input, sink);
				}
				OperatorSinkCombineInput combine {*global_sink, *local_sink, interrupt};
				sort.Combine(context, combine);
				OperatorSinkFinalizeInput finalize {*global_sink, interrupt};
				sort.Finalize(*con.context, finalize);
				auto global_source = sort.GetGlobalSourceState(*con.context, *global_sink);
				auto local_source = sort.GetLocalSourceState(context, *global_source);
				OperatorSourceInput source {*global_source, *local_source, interrupt};

				// BoundOrderByNode renders default direction as DESC and default NULL ordering as LAST.
				duckdb::vector<Value> expected = direction == OrderType::ASCENDING
				                                     ? duckdb::vector<Value> {low, high, high}
				                                     : duckdb::vector<Value> {high, high, low};
				if (null_order == OrderByNullType::NULLS_FIRST) {
					expected.insert(expected.begin(), Value(type));
				} else {
					expected.emplace_back(type);
				}
				DataChunk output;
				output.Initialize(*con.context, types);
				idx_t count = 0;
				SourceResultType status;
				do {
					output.Reset();
					status = sort.GetData(context, output, source);
					for (idx_t row = 0; row < output.size(); row++) {
						REQUIRE(count < expected.size());
						CHECK(Value::NotDistinctFrom(output.GetValue(0, row), expected[count++]));
					}
				} while (status != SourceResultType::FINISHED);
				CHECK(count == expected.size());
			}
		}
	}
	con.Rollback();
}
