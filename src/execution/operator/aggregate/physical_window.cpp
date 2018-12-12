#include "execution/operator/aggregate/physical_window.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/window_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalWindow::PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                               PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {

	// TODO: check we have at least one window aggr in the select list otherwise this is pointless
}

// TODO what if we have no PARTITION BY/ORDER?
// this implements a sorted window functions variant

void PhysicalWindow::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalWindowOperatorState *>(state_);
	// we kind of need to materialize the intermediate here.
	ChunkCollection &big_data = state->tuples;

	if (state->position == 0) {
		// materialize intermediate
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			if (select_list[expr_idx]->GetExpressionClass() != ExpressionClass::WINDOW) {
				continue;
			}

			// sort by partition and order clause in window def
			auto wexpr = reinterpret_cast<WindowExpression *>(select_list[expr_idx].get());
			vector<TypeId> sort_types;
			vector<Expression *> exprs;
			OrderByDescription odesc;

			for (size_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
				auto &pexpr = wexpr->partitions[prt_idx];
				sort_types.push_back(pexpr->return_type);
				exprs.push_back(pexpr.get());
				odesc.orders.push_back(OrderByNode(
				    OrderType::ASCENDING,
				    make_unique_base<Expression, ColumnRefExpression>(pexpr->return_type, exprs.size() - 1)));
			}
			// create this here before the order keys are added
			auto serializer = TupleSerializer(sort_types);

			for (size_t ord_idx = 0; ord_idx < wexpr->ordering.orders.size(); ord_idx++) {
				auto &oexpr = wexpr->ordering.orders[ord_idx].expression;
				sort_types.push_back(oexpr->return_type);
				exprs.push_back(oexpr.get());
				odesc.orders.push_back(OrderByNode(
				    wexpr->ordering.orders[ord_idx].type,
				    make_unique_base<Expression, ColumnRefExpression>(oexpr->return_type, exprs.size() - 1)));
			}
			assert(sort_types.size() > 0);

			// create a chunkcollection for the results of the expressions in the window definitions
			ChunkCollection sort_collection;
			for (size_t i = 0; i < big_data.chunks.size(); i++) {
				DataChunk sort_chunk;
				sort_chunk.Initialize(sort_types);

				ExpressionExecutor executor(*big_data.chunks[i], context);
				executor.Execute(sort_chunk, [&](size_t i) { return exprs[i]; }, exprs.size());
				sort_chunk.Verify();
				sort_collection.Append(sort_chunk);
			}

			assert(sort_collection.count == big_data.count);
			// sort by the window def using the expression result collection
			auto sorted_vector = new uint64_t[big_data.count];
			sort_collection.Sort(odesc, sorted_vector);

			auto sorted_vector2 = new uint64_t[big_data.count];
			memcpy(sorted_vector2, sorted_vector, sizeof(uint64_t) * big_data.count);

			// inplace reorder of big_data according to sorted_vector
			big_data.Reorder(sorted_vector);
			sorted_vector = nullptr;

			sort_collection.Reorder(sorted_vector2);
			sorted_vector2 = nullptr;

			// serialize partitions
			// FIXME: this is again lots of intermediate data, we can do this comparision in the loop below too
			// we need to fetch this from sort_collection though, so keep sorted_vector around?
			// FIXME how do we serialize single values? Chunk-Wise?

			uint8_t partition_data[sort_collection.count][serializer.TupleSize()];
			uint8_t *partition_elements[sort_collection.count];
			for (size_t prt_idx = 0; prt_idx < sort_collection.count; prt_idx++) {
				partition_elements[prt_idx] = partition_data[prt_idx];
			}
			size_t partition_offset = 0;
			for (size_t i = 0; i < sort_collection.chunks.size(); i++) {

				// serialize the partition columns into bytes so we can compute partition ids below efficiently
				size_t ser_offset = 0;
				for (size_t ser_col = 0; ser_col < serializer.TypeSize(); ser_col++) {
					serializer.SerializeColumn(*sort_collection.chunks[i], &partition_elements[partition_offset],
					                           ser_col, ser_offset);
				}
				partition_offset += STANDARD_VECTOR_SIZE;
			}

			// evaluate inner expressions of window functions, could be more complex

			// FIXME assert this?
			ChunkCollection payload_collection;
			assert(wexpr->children.size() == 1);
			vector<TypeId> inner_types = {wexpr->children[0]->return_type};
			for (size_t i = 0; i < big_data.chunks.size(); i++) {
				DataChunk payload_chunk;
				payload_chunk.Initialize(inner_types);

				ExpressionExecutor executor(*big_data.chunks[i], context);
				executor.ExecuteExpression(wexpr->children[0].get(), payload_chunk.data[0]);

				payload_chunk.Verify();
				payload_collection.Append(payload_chunk);
			}

			// actual computation of windows, naively
			// FIXME: implement TUM method here (!)
			// build tree based on payload collection
			size_t window_start = 0;
			size_t window_end = 0;
			uint8_t *prev = partition_elements[0];

			for (size_t row_idx = 0; row_idx < big_data.count; row_idx++) {
				// FIXME handle other window types

				if (serializer.Compare(prev, partition_elements[row_idx]) != 0) {
					window_start = row_idx;
				}
				window_end = row_idx + 1;

				// TODO suuper ugly
				switch (wexpr->type) {
				case ExpressionType::AGGREGATE_SUM: {
					Value sum = Value::BIGINT(0).CastAs(wexpr->return_type);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						sum = sum + payload_collection.GetValue(0, row_idx_w);
					}
					big_data.SetValue(expr_idx, row_idx, sum);
					break;
				}
				default:
					throw NotImplementedException("Window aggregate type %s",
					                              ExpressionTypeToString(wexpr->type).c_str());
				}

				prev = partition_elements[row_idx];
			}
		}
	}

	if (state->position >= big_data.count) {
		return;
	}

	// just return what was computed before
	auto &ret_ch = big_data.GetChunk(state->position);
	ret_ch.Copy(chunk);
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalWindowOperatorState>(children[0].get(), parent);
}
