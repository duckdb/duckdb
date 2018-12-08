#include "execution/operator/aggregate/physical_window.hpp"

#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/constant_expression.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "parser/expression/window_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalWindow::PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                                     PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {

	// TODO: check we have at least one window aggr in the select list otherwise this is pointless
}

// TODO what if we have no PARTITION BY/ORDER?

// multiple sorts may be required
void PhysicalWindow::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalWindowOperatorState *>(state_);
	// we kind of need to materialize the intermediate here.
	ChunkCollection &big_data = state->tuples;
	if (state->position == 0) {
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);


		// materialize all expressions that are relevant for the windows in the selection list
		// we will use them in separate sorting passes below
		// TODO: perhaps its simpler to do this separately?
		// FIXME: types are not bound yet?!
		vector<TypeId> sort_types;
		vector<Expression *> exprs;

		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
				auto wexpr = reinterpret_cast<WindowExpression *>(select_list[expr_idx].get());

				// TODO: remember where we put those for constructing orderdesc below?
				for (size_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
					auto &pexpr = wexpr->partitions[prt_idx];
					sort_types.push_back(pexpr->return_type);
					exprs.push_back(pexpr.get());
				}

				for (size_t ord_idx = 0; ord_idx < wexpr->ordering.orders.size(); ord_idx++) {
					auto &oexpr = wexpr->ordering.orders[ord_idx].expression;
					sort_types.push_back(oexpr->return_type);
					exprs.push_back(oexpr.get());

				}
			}
		}

		assert(sort_types.size() > 0);

		ChunkCollection sort_collection;
		for (size_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk sort_chunk;
			sort_chunk.Initialize(sort_types);

			ExpressionExecutor executor(*big_data.chunks[i], context);
			executor.Execute(sort_chunk, [&](size_t i) {
				return exprs[i];
			},exprs.size());
		sort_chunk.Verify();
			sort_collection.Append(sort_chunk);
		}

		sort_collection.Print();
//
//		if (sort_collection.count != big_data.count) {
//			throw Exception("Cardinalities of ORDER BY columns and input "
//			                "columns don't match [?]");
//		}
//
//		// now perform the actual sort
//		state->sorted_vector = unique_ptr<uint64_t[]>(new uint64_t[sort_collection.count]);
//		sort_collection.Sort(description, state->sorted_vector.get());
	}



	if (state->position >= big_data.count) {
		return;
	}

	auto& ret_ch = big_data.GetChunk(state->position);
	ret_ch.Copy(chunk);

	for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
			VectorOperations::Set(chunk.data[expr_idx], Value());
		}
	}


//	size_t remaining_data = min((size_t)STANDARD_VECTOR_SIZE, big_data.count - state->position);
//	big_data.
	state->position += STANDARD_VECTOR_SIZE;
}


unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalWindowOperatorState>(children[0].get(), parent);

}



//state->sorted_vector = unique_ptr<uint64_t[]>(new uint64_t[sort_collection.count]);
//sort_collection.Sort(description, state->sorted_vector.get());
