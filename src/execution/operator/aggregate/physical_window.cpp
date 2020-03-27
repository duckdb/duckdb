#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <cmath>

using namespace duckdb;
using namespace std;

//! The operator state of the window
class PhysicalWindowOperatorState : public PhysicalOperatorState {
public:
	PhysicalWindowOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	idx_t position;
	ChunkCollection tuples;
	ChunkCollection window_results;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                               PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {
}

static bool EqualsSubset(vector<Value> &a, vector<Value> &b, idx_t start, idx_t end) {
	assert(start <= end);
	for (idx_t i = start; i < end; i++) {
		if (a[i] != b[i]) {
			return false;
		}
	}
	return true;
}

static idx_t BinarySearchRightmost(ChunkCollection &input, vector<Value> row, idx_t l, idx_t r, idx_t comp_cols) {
	if (comp_cols == 0) {
		return r - 1;
	}
	while (l < r) {
		idx_t m = floor((l + r) / 2);
		bool less_than_equals = true;
		for (idx_t i = 0; i < comp_cols; i++) {
			if (input.GetRow(m)[i] > row[i]) {
				less_than_equals = false;
				break;
			}
		}
		if (less_than_equals) {
			l = m + 1;
		} else {
			r = m;
		}
	}
	return l - 1;
}

static void MaterializeExpressions(ClientContext &context, Expression **exprs, idx_t expr_count, ChunkCollection &input,
                                   ChunkCollection &output, bool scalar = false) {
	if (expr_count == 0) {
		return;
	}

	vector<TypeId> types;
	ExpressionExecutor executor;
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	for (idx_t i = 0; i < input.chunks.size(); i++) {
		DataChunk chunk;
		chunk.Initialize(types);

		executor.Execute(*input.chunks[i], chunk);

		chunk.Verify();
		output.Append(chunk);

		if (scalar) {
			break;
		}
	}
}

static void MaterializeExpression(ClientContext &context, Expression *expr, ChunkCollection &input,
                                  ChunkCollection &output, bool scalar = false) {
	MaterializeExpressions(context, &expr, 1, input, output, scalar);
}

static void SortCollectionForWindow(ClientContext &context, BoundWindowExpression *wexpr, ChunkCollection &input,
                                    ChunkCollection &output, ChunkCollection &sort_collection) {
	vector<TypeId> sort_types;
	vector<OrderType> orders;
	ExpressionExecutor executor;

	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		auto &pexpr = wexpr->partitions[prt_idx];
		sort_types.push_back(pexpr->return_type);
		orders.push_back(OrderType::ASCENDING);
		executor.AddExpression(*pexpr);
	}

	for (idx_t ord_idx = 0; ord_idx < wexpr->orders.size(); ord_idx++) {
		auto &oexpr = wexpr->orders[ord_idx].expression;
		sort_types.push_back(oexpr->return_type);
		orders.push_back(wexpr->orders[ord_idx].type);
		executor.AddExpression(*oexpr);
	}

	assert(sort_types.size() > 0);

	// create a chunkcollection for the results of the expressions in the window definitions
	for (idx_t i = 0; i < input.chunks.size(); i++) {
		DataChunk sort_chunk;
		sort_chunk.Initialize(sort_types);

		executor.Execute(*input.chunks[i], sort_chunk);

		sort_chunk.Verify();
		sort_collection.Append(sort_chunk);
	}

	assert(input.count == sort_collection.count);

	auto sorted_vector = unique_ptr<idx_t[]>(new idx_t[input.count]);
	sort_collection.Sort(orders, sorted_vector.get());

	input.Reorder(sorted_vector.get());
	output.Reorder(sorted_vector.get());
	sort_collection.Reorder(sorted_vector.get());
}

struct WindowBoundariesState {
	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	bool is_same_partition = false;
	bool is_peer = false;
	vector<Value> row_prev;
};

static bool WindowNeedsRank(BoundWindowExpression *wexpr) {
	return wexpr->type == ExpressionType::WINDOW_PERCENT_RANK || wexpr->type == ExpressionType::WINDOW_RANK ||
	       wexpr->type == ExpressionType::WINDOW_RANK_DENSE || wexpr->type == ExpressionType::WINDOW_CUME_DIST;
}

static void UpdateWindowBoundaries(BoundWindowExpression *wexpr, ChunkCollection &input, idx_t input_size,
                                   idx_t row_idx, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, WindowBoundariesState &bounds) {

	if (input.column_count() > 0) {
		vector<Value> row_cur = input.GetRow(row_idx);
		idx_t sort_col_count = wexpr->partitions.size() + wexpr->orders.size();

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = EqualsSubset(bounds.row_prev, row_cur, 0, wexpr->partitions.size());
		bounds.is_peer = bounds.is_same_partition &&
		                 EqualsSubset(bounds.row_prev, row_cur, wexpr->partitions.size(), sort_col_count);
		bounds.row_prev = row_cur;

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition || row_idx == 0) { // special case for first row, need to init
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end =
			    BinarySearchRightmost(input, row_cur, bounds.partition_start, input.count, wexpr->partitions.size()) +
			    1;

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (wexpr->end == WindowBoundary::CURRENT_ROW_RANGE || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
			bounds.peer_end = BinarySearchRightmost(input, row_cur, row_idx, bounds.partition_end, sort_col_count) + 1;
		}
	} else {
		bounds.is_same_partition = 0;
		bounds.is_peer = true;
		bounds.partition_end = input_size;
		bounds.peer_end = bounds.partition_end;
	}

	// determine window boundaries depending on the type of expression
	bounds.window_start = -1;
	bounds.window_end = -1;

	switch (wexpr->start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		bounds.window_start = bounds.partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_start = bounds.peer_start;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		assert(0); // disallowed
		break;
	case WindowBoundary::EXPR_PRECEDING: {
		assert(boundary_start_collection.column_count() > 0);
		bounds.window_start =
		    (int64_t)row_idx -
		    boundary_start_collection.GetValue(0, wexpr->start_expr->IsScalar() ? 0 : row_idx).GetValue<int64_t>();
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING: {
		assert(boundary_start_collection.column_count() > 0);
		bounds.window_start =
		    row_idx +
		    boundary_start_collection.GetValue(0, wexpr->start_expr->IsScalar() ? 0 : row_idx).GetValue<int64_t>();
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
		bounds.window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_end = bounds.peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.window_end = bounds.partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING:
		assert(boundary_end_collection.column_count() > 0);
		bounds.window_end =
		    (int64_t)row_idx -
		    boundary_end_collection.GetValue(0, wexpr->end_expr->IsScalar() ? 0 : row_idx).GetValue<int64_t>() + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING:
		assert(boundary_end_collection.column_count() > 0);
		bounds.window_end =
		    row_idx +
		    boundary_end_collection.GetValue(0, wexpr->end_expr->IsScalar() ? 0 : row_idx).GetValue<int64_t>() + 1;

		break;
	default:
		throw NotImplementedException("Unsupported boundary");
	}

	// clamp windows to partitions if they should exceed
	if (bounds.window_start < (int64_t)bounds.partition_start) {
		bounds.window_start = bounds.partition_start;
	}
	if ((idx_t)bounds.window_end > bounds.partition_end) {
		bounds.window_end = bounds.partition_end;
	}

	if (bounds.window_start < 0 || bounds.window_end < 0) {
		throw Exception("Failed to compute window boundaries");
	}
}

static void ComputeWindowExpression(ClientContext &context, BoundWindowExpression *wexpr, ChunkCollection &input,
                                    ChunkCollection &output, idx_t output_idx) {

	ChunkCollection sort_collection;
	bool needs_sorting = wexpr->partitions.size() + wexpr->orders.size() > 0;
	if (needs_sorting) {
		SortCollectionForWindow(context, wexpr, input, output, sort_collection);
	}

	// evaluate inner expressions of window functions, could be more complex
	ChunkCollection payload_collection;
	vector<Expression *> exprs;
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	// TODO: child may be a scalar, don't need to materialize the whole collection then
	MaterializeExpressions(context, exprs.data(), exprs.size(), input, payload_collection);

	ChunkCollection leadlag_offset_collection;
	ChunkCollection leadlag_default_collection;
	if (wexpr->type == ExpressionType::WINDOW_LEAD || wexpr->type == ExpressionType::WINDOW_LAG) {
		if (wexpr->offset_expr) {
			MaterializeExpression(context, wexpr->offset_expr.get(), input, leadlag_offset_collection,
			                      wexpr->offset_expr->IsScalar());
		}
		if (wexpr->default_expr) {
			MaterializeExpression(context, wexpr->default_expr.get(), input, leadlag_default_collection,
			                      wexpr->default_expr->IsScalar());
		}
	}

	// evaluate boundaries if present.
	ChunkCollection boundary_start_collection;
	if (wexpr->start_expr &&
	    (wexpr->start == WindowBoundary::EXPR_PRECEDING || wexpr->start == WindowBoundary::EXPR_FOLLOWING)) {
		MaterializeExpression(context, wexpr->start_expr.get(), input, boundary_start_collection,
		                      wexpr->start_expr->IsScalar());
	}
	ChunkCollection boundary_end_collection;
	if (wexpr->end_expr &&
	    (wexpr->end == WindowBoundary::EXPR_PRECEDING || wexpr->end == WindowBoundary::EXPR_FOLLOWING)) {
		MaterializeExpression(context, wexpr->end_expr.get(), input, boundary_end_collection,
		                      wexpr->end_expr->IsScalar());
	}

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->return_type, &payload_collection);
	}

	WindowBoundariesState bounds;
	uint64_t dense_rank = 1, rank_equal = 0, rank = 1;

	if (needs_sorting) {
		bounds.row_prev = sort_collection.GetRow(0);
	}

	// this is the main loop, go through all sorted rows and compute window function result
	for (idx_t row_idx = 0; row_idx < input.count; row_idx++) {
		// special case, OVER (), aggregate over everything
		UpdateWindowBoundaries(wexpr, sort_collection, input.count, row_idx, boundary_start_collection,
		                       boundary_end_collection, bounds);
		if (WindowNeedsRank(wexpr)) {
			if (!bounds.is_same_partition || row_idx == 0) { // special case for first row, need to init
				dense_rank = 1;
				rank = 1;
				rank_equal = 0;
			} else if (!bounds.is_peer) {
				dense_rank++;
				rank += rank_equal;
				rank_equal = 0;
			}
			rank_equal++;
		}

		auto res = Value();

		// if no values are read for window, result is NULL
		if (bounds.window_start >= bounds.window_end) {
			output.SetValue(output_idx, row_idx, res);
			continue;
		}

		switch (wexpr->type) {
		case ExpressionType::WINDOW_AGGREGATE: {
			res = segment_tree->Compute(bounds.window_start, bounds.window_end);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			res = Value::Numeric(wexpr->return_type, row_idx - bounds.partition_start + 1);
			break;
		}
		case ExpressionType::WINDOW_RANK_DENSE: {
			res = Value::Numeric(wexpr->return_type, dense_rank);
			break;
		}
		case ExpressionType::WINDOW_RANK: {
			res = Value::Numeric(wexpr->return_type, rank);
			break;
		}
		case ExpressionType::WINDOW_PERCENT_RANK: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start - 1;
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			res = Value(percent_rank);
			break;
		}
		case ExpressionType::WINDOW_CUME_DIST: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start;
			double cume_dist = denom > 0 ? ((double)(bounds.peer_end - bounds.partition_start)) / denom : 0;
			res = Value(cume_dist);
			break;
		}
		case ExpressionType::WINDOW_NTILE: {
			if (payload_collection.column_count() != 1) {
				throw Exception("NTILE needs a parameter");
			}
			auto n_param = payload_collection.GetValue(0, row_idx).GetValue<int64_t>();
			// With thanks from SQLite's ntileValueFunc()
			int64_t n_total = bounds.partition_end - bounds.partition_start;
			int64_t n_size = (n_total / n_param);
			if (n_size > 0) {
				int64_t n_large = n_total - n_param * n_size;
				int64_t i_small = n_large * (n_size + 1);

				assert((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

				if (row_idx < (idx_t)i_small) {
					res = Value::Numeric(wexpr->return_type, 1 + row_idx / (n_size + 1));
				} else {
					res = Value::Numeric(wexpr->return_type, 1 + n_large + (row_idx - i_small) / n_size);
				}
			}
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			Value def_val = Value(wexpr->return_type);
			idx_t offset = 1;
			if (wexpr->offset_expr) {
				offset = leadlag_offset_collection.GetValue(0, wexpr->offset_expr->IsScalar() ? 0 : row_idx)
				             .GetValue<int64_t>();
			}
			if (wexpr->default_expr) {
				def_val = leadlag_default_collection.GetValue(0, wexpr->default_expr->IsScalar() ? 0 : row_idx);
			}
			if (wexpr->type == ExpressionType::WINDOW_LEAD) {
				auto lead_idx = row_idx + 1;
				if (lead_idx < bounds.partition_end) {
					res = payload_collection.GetValue(0, lead_idx);
				} else {
					res = def_val;
				}
			} else {
				int64_t lag_idx = (int64_t)row_idx - offset;
				if (lag_idx >= 0 && (idx_t)lag_idx >= bounds.partition_start) {
					res = payload_collection.GetValue(0, lag_idx);
				} else {
					res = def_val;
				}
			}

			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			res = payload_collection.GetValue(0, bounds.window_start);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			res = payload_collection.GetValue(0, bounds.window_end - 1);
			break;
		}
		default:
			throw NotImplementedException("Window aggregate type %s", ExpressionTypeToString(wexpr->type).c_str());
		}

		output.SetValue(output_idx, row_idx, res);
	}
}

void PhysicalWindow::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
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
		for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			window_types.push_back(select_list[expr_idx]->return_type);
		}

		for (idx_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk window_chunk;
			window_chunk.Initialize(window_types);
			window_chunk.SetCardinality(big_data.chunks[i]->size());
			for (idx_t col_idx = 0; col_idx < window_chunk.column_count(); col_idx++) {
				window_chunk.data[col_idx].vector_type = VectorType::CONSTANT_VECTOR;
				ConstantVector::SetNull(window_chunk.data[col_idx], true);
			}

			window_chunk.Verify();
			window_results.Append(window_chunk);
		}

		assert(window_results.column_count() == select_list.size());
		idx_t window_output_idx = 0;
		// we can have multiple window functions
		for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			assert(select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			// sort by partition and order clause in window def
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(select_list[expr_idx].get());
			ComputeWindowExpression(context, wexpr, big_data, window_results, window_output_idx++);
		}
	}

	if (state->position >= big_data.count) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunk(state->position);
	auto &wind_ch = window_results.GetChunk(state->position);

	idx_t out_idx = 0;
	assert(proj_ch.size() == wind_ch.size());
	chunk.SetCardinality(proj_ch);
	for (idx_t col_idx = 0; col_idx < proj_ch.column_count(); col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < wind_ch.column_count(); col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState() {
	return make_unique<PhysicalWindowOperatorState>(children[0].get());
}
