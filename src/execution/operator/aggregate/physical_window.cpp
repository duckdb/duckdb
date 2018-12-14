#include "execution/operator/aggregate/physical_window.hpp"

#include "common/types/chunk_collection.hpp"
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
	ChunkCollection &big_data = state->tuples;
	ChunkCollection &window_results = state->window_results;

	if (state->position == 0) {
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		vector<TypeId> window_types;
		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
				window_types.push_back(select_list[expr_idx]->return_type);
			}
		}

		for (size_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk window_chunk;
			window_chunk.Initialize(window_types);
			for (size_t col_idx = 0; col_idx < window_chunk.column_count; col_idx++) {
				window_chunk.data[col_idx].count = big_data.chunks[i]->size();
			}
			window_chunk.Verify();
			window_results.Append(window_chunk);
		}

		size_t window_output_idx = 0;
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
			// inplace reorder of big_data according to sorted_vector
			big_data.Reorder(sorted_vector);
			sort_collection.Reorder(sorted_vector);

			// evaluate inner expressions of window functions, could be more complex
			ChunkCollection payload_collection;
			if (wexpr->children.size() > 0) {
				vector<TypeId> inner_types = {wexpr->children[0]->return_type};
				for (size_t i = 0; i < big_data.chunks.size(); i++) {
					DataChunk payload_chunk;
					payload_chunk.Initialize(inner_types);

					ExpressionExecutor executor(*big_data.chunks[i], context);
					executor.ExecuteExpression(wexpr->children[0].get(), payload_chunk.data[0]);

					payload_chunk.Verify();
					payload_collection.Append(payload_chunk);
				}
			}

			// actual computation of windows, naively
			// FIXME: implement TUM method here (!)
			// build tree based on payload collection
			size_t window_start = 0;
			size_t window_end = 0;
			size_t rank = 0;

			// initialize current partition to first row
			vector<Value> prev;
			prev.resize(sort_types.size());
			for (size_t p_idx = 0; p_idx < prev.size(); p_idx++) {
				prev[p_idx] = sort_collection.GetValue(p_idx, 0);
			}

			for (size_t row_idx = 0; row_idx < big_data.count; row_idx++) {
				// FIXME handle other window types
				for (size_t p_idx = 0; p_idx < prev.size(); p_idx++) {
					Value s_val = sort_collection.GetValue(p_idx, row_idx);
					if (prev[p_idx] != s_val && p_idx < wexpr->partitions.size()) {
						window_start = row_idx;
						rank = 0;
					}
					// TODO: only do this check with RANK functions
					if (prev[p_idx] != s_val) {
						rank++;
					}
					// always overwrite because whatever
					prev[p_idx] = s_val;
				}
				window_end = row_idx + 1;

				switch (wexpr->type) {
				case ExpressionType::WINDOW_SUM: {
					Value sum = Value::Numeric(wexpr->return_type, 0);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						sum = sum + payload_collection.GetValue(0, row_idx_w);
					}
					window_results.SetValue(window_output_idx, row_idx, sum);
					break;
				}
				case ExpressionType::WINDOW_ROW_NUMBER: {
					Value rowno = Value::Numeric(wexpr->return_type, 0);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						rowno = rowno + 1;
					}
					window_results.SetValue(window_output_idx, row_idx, rowno);
					break;
				}
				case ExpressionType::WINDOW_RANK: {
					Value rank_val = Value::Numeric(wexpr->return_type, rank);
					window_results.SetValue(window_output_idx, row_idx, rank_val);
					break;
				}
				case ExpressionType::WINDOW_FIRST_VALUE: {
					window_results.SetValue(window_output_idx, row_idx, payload_collection.GetValue(0, window_start));
					break;
				}
				default:
					throw NotImplementedException("Window aggregate type %s",
					                              ExpressionTypeToString(wexpr->type).c_str());
				}
			}
			window_output_idx++;
		}
	}

	if (state->position >= big_data.count) {
		return;
	}

	// just return what was computed before
	auto &proj_ch = big_data.GetChunk(state->position);
	auto &wind_ch = window_results.GetChunk(state->position);

	size_t window_output_idx = 0;
	for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
			chunk.data[expr_idx].Reference(wind_ch.data[window_output_idx]);
			window_output_idx++;
		} else {
			ExpressionExecutor executor(proj_ch, context);
			executor.ExecuteExpression(select_list[expr_idx].get(), chunk.data[expr_idx]);
		}
	}

	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalWindowOperatorState>(children[0].get(), parent);
}
