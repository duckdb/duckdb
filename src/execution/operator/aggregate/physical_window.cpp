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

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                               PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {
}

static void MaterializeExpression(ClientContext &context, Expression *expr, ChunkCollection &input,
                                  ChunkCollection &output) {
	ChunkCollection boundary_start_collection;
	vector<TypeId> types = {expr->return_type};
	for (size_t i = 0; i < input.chunks.size(); i++) {
		DataChunk chunk;
		chunk.Initialize(types);

		ExpressionExecutor executor(*input.chunks[i], context);
		executor.ExecuteExpression(expr, chunk.data[0]);

		chunk.Verify();
		output.Append(chunk);
	}
}

static bool EqualsSubset(vector<Value> &a, vector<Value> &b, size_t start, size_t end) {
	assert(start <= end);
	for (size_t i = start; i < end; i++) {
		if (a[i] != b[i]) {
			return false;
		}
	}
	return true;
}

void PhysicalWindow::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalWindowOperatorState *>(state_);
	ChunkCollection &big_data = state->tuples;
	ChunkCollection &window_results = state->window_results;

	// this is a blocking operator, so compute complete result on first invocation
	if (state->position == 0) {
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		if (big_data.count == 0) {
			return;
		}

		vector<TypeId> window_types;
		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			window_types.push_back(select_list[expr_idx]->return_type);
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
			assert(select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW);
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
			ChunkCollection sort_collection;
			// if we have no sort nor order by we don't have to sort and the window is everything
			if (sort_types.size() > 0) {
				assert(sort_types.size() > 0);

				// create a chunkcollection for the results of the expressions in the window definitions
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
				auto sorted_vector = unique_ptr<uint64_t[]>(new uint64_t[big_data.count]);
				sort_collection.Sort(odesc, sorted_vector.get());
				// inplace reorder of big_data according to sorted_vector
				big_data.Reorder(sorted_vector.get());
				sort_collection.Reorder(sorted_vector.get());
			}

			// evaluate inner expressions of window functions, could be more complex
			ChunkCollection payload_collection;
			if (wexpr->children.size() > 0) {
				MaterializeExpression(context, wexpr->children[0].get(), big_data, payload_collection);
			}

			// evaluate boundaries if present.
			// TODO optimization if those are scalars we can just use those
			ChunkCollection boundary_start_collection;
			if (wexpr->start_expr &&
			    (wexpr->start == WindowBoundary::EXPR_PRECEDING || wexpr->start == WindowBoundary::EXPR_FOLLOWING)) {
				MaterializeExpression(context, wexpr->start_expr.get(), big_data, boundary_start_collection);
			}
			ChunkCollection boundary_end_collection;
			if (wexpr->end_expr &&
			    (wexpr->end == WindowBoundary::EXPR_PRECEDING || wexpr->end == WindowBoundary::EXPR_FOLLOWING)) {
				MaterializeExpression(context, wexpr->end_expr.get(), big_data, boundary_end_collection);
			}

			// actual computation of windows, naively
			// FIXME: implement TUM method here instead
			ssize_t window_start = 0;
			ssize_t window_end = 0;

			size_t partition_start = 0;
			size_t partition_end = 0;

			size_t peer_start = 0;
			size_t peer_end = 0;

			// FIXME
			size_t dense_rank = 1;
			size_t rank_equal = 0;
			size_t rank = 1;
			size_t row_no = 1;

			vector<Value> row_prev = sort_collection.GetRow(0);

			// this is the main loop, go through all sorted rows and compute window function result
			for (size_t row_idx = 0; row_idx < big_data.count; row_idx++) {
				vector<Value> row_cur = sort_collection.GetRow(row_idx);

				// determine partition and peer group boundaries to ultimately figure out window size
				bool is_same_partition = EqualsSubset(row_prev, row_cur, 0, wexpr->partitions.size());
				bool is_peer = EqualsSubset(row_prev, row_cur, wexpr->partitions.size(), sort_types.size());

				if (!is_same_partition || row_idx == 0) {
					partition_start = row_idx;

					dense_rank = 1;
					rank = 1;
					row_no = 1;
					rank_equal = 0;
					peer_start = row_idx;

					// find end of partition
					// TODO: use binary search here
					for (size_t row_idx_e = partition_start; row_idx_e < big_data.count; row_idx_e++) {
						auto next_row = sort_collection.GetRow(row_idx_e);
						if (!EqualsSubset(next_row, row_cur, 0, wexpr->partitions.size())) {
							break;
						}
						partition_end = row_idx_e + 1;
					}

				} else if (!is_peer) {
					peer_start = row_idx;

					dense_rank++;
					rank += rank_equal;
					rank_equal = 0;
				}

				// TODO: we can use binary search here!
				if (wexpr->end == WindowBoundary::CURRENT_ROW_RANGE) {
					for (size_t row_idx_e = row_idx; row_idx_e < partition_end; row_idx_e++) {
						auto next_row = sort_collection.GetRow(row_idx_e);
						if (!EqualsSubset(next_row, row_cur, wexpr->partitions.size(), sort_types.size())) {
							break;
						}
						peer_end = row_idx_e + 1;
					}
				}

				row_prev = row_cur;

				window_start = -1;
				window_end = -1;

				switch (wexpr->start) {
				case WindowBoundary::UNBOUNDED_PRECEDING:
					window_start = partition_start;
					break;
				case WindowBoundary::CURRENT_ROW_ROWS:
					window_start = row_idx;
					break;
				case WindowBoundary::CURRENT_ROW_RANGE:
					window_start = peer_start;
					break;
				case WindowBoundary::UNBOUNDED_FOLLOWING:
					assert(0); // disallowed
					break;
				case WindowBoundary::EXPR_PRECEDING: {
					assert(boundary_start_collection.column_count() > 0);
					window_start = (ssize_t)row_idx - boundary_start_collection.GetValue(0, row_idx).GetNumericValue();
					break;
				}
				case WindowBoundary::EXPR_FOLLOWING: {
					assert(boundary_start_collection.column_count() > 0);
					window_start = row_idx + boundary_start_collection.GetValue(0, row_idx).GetNumericValue();
					break;
				}

				default:
					throw NotImplementedException("Unsupported boundary");
				}

				switch (wexpr->end) {
				case WindowBoundary::UNBOUNDED_PRECEDING:
					assert(0); // disallowed
					break;
				case WindowBoundary::CURRENT_ROW_ROWS:
					window_end = row_idx + 1;
					break;
				case WindowBoundary::CURRENT_ROW_RANGE:
					window_end = peer_end;
					break;
				case WindowBoundary::UNBOUNDED_FOLLOWING:
					window_end = partition_end;
					break;
				case WindowBoundary::EXPR_PRECEDING:
					assert(boundary_end_collection.column_count() > 0);
					window_end = (ssize_t)row_idx - boundary_end_collection.GetValue(0, row_idx).GetNumericValue() + 1;
					break;
				case WindowBoundary::EXPR_FOLLOWING:
					assert(boundary_end_collection.column_count() > 0);
					window_end = row_idx + boundary_end_collection.GetValue(0, row_idx).GetNumericValue() + 1;

					break;
				default:
					throw NotImplementedException("Unsupported boundary");
				}

				// clamp windows to partitions if they should exceed
				if (window_start < (ssize_t)partition_start) {
					window_start = partition_start;
				}
				if (window_end > partition_end) {
					window_end = partition_end;
				}

				if (window_start < 0 || window_end < 0) {
					throw Exception("Failed to compute window boundaries");
				}

				// now we have window start and end indices and can compute the actual aggregation

				// if no values are read for window, result is NULL
				if (window_start >= window_end) {
					window_results.SetValue(window_output_idx, row_idx, Value());
					row_no++;
					continue;
				}

				switch (wexpr->type) {
				case ExpressionType::WINDOW_SUM: {
					Value sum = Value::Numeric(wexpr->return_type, 0);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						sum = sum + payload_collection.GetValue(0, row_idx_w);
					}
					window_results.SetValue(window_output_idx, row_idx, sum);
					break;
				}
				case ExpressionType::WINDOW_MIN: {
					Value min = Value::MaximumValue(wexpr->return_type);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						auto val = payload_collection.GetValue(0, row_idx_w);
						if (val < min) {
							min = val;
						}
					}
					window_results.SetValue(window_output_idx, row_idx, min);
					break;
				}
				case ExpressionType::WINDOW_MAX: {
					Value max = Value::MinimumValue(wexpr->return_type);
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						auto val = payload_collection.GetValue(0, row_idx_w);
						if (val > max) {
							max = val;
						}
					}
					window_results.SetValue(window_output_idx, row_idx, max);
					break;
				}
				case ExpressionType::WINDOW_AVG: {
					double sum = 0;
					for (size_t row_idx_w = window_start; row_idx_w < window_end; row_idx_w++) {
						sum += payload_collection.GetValue(0, row_idx_w).GetNumericValue();
					}
					sum = sum / (window_end - window_start);
					window_results.SetValue(window_output_idx, row_idx, Value(sum));
					break;
				}
				case ExpressionType::WINDOW_COUNT_STAR: {
					window_results.SetValue(window_output_idx, row_idx,
					                        Value::Numeric(wexpr->return_type, window_end - window_start));
					break;
				}
				case ExpressionType::WINDOW_ROW_NUMBER: {
					window_results.SetValue(window_output_idx, row_idx, Value::Numeric(wexpr->return_type, row_no));
					break;
				}
				case ExpressionType::WINDOW_RANK_DENSE: {
					window_results.SetValue(window_output_idx, row_idx, Value::Numeric(wexpr->return_type, dense_rank));
					break;
				}
				case ExpressionType::WINDOW_RANK: {
					window_results.SetValue(window_output_idx, row_idx, Value::Numeric(wexpr->return_type, rank));
					rank_equal++;
					break;
				}
				case ExpressionType::WINDOW_FIRST_VALUE: {
					window_results.SetValue(window_output_idx, row_idx, payload_collection.GetValue(0, window_start));
					break;
				}
				case ExpressionType::WINDOW_LAST_VALUE: {

					window_results.SetValue(window_output_idx, row_idx, payload_collection.GetValue(0, window_end - 1));
					break;
				}
				default:
					throw NotImplementedException("Window aggregate type %s",
					                              ExpressionTypeToString(wexpr->type).c_str());
				}
				row_no++;
			}
			window_output_idx++;
		}
	}

	if (state->position >= big_data.count) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunk(state->position);
	auto &wind_ch = window_results.GetChunk(state->position);

	size_t out_idx = 0;
	for (size_t col_idx = 0; col_idx < proj_ch.column_count; col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (size_t col_idx = 0; col_idx < wind_ch.column_count; col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalWindowOperatorState>(children[0].get(), parent);
}
