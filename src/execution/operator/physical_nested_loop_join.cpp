
#include "execution/operator/physical_nested_loop_join.hpp"
#include "common/types/vector_operations.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(
    std::unique_ptr<PhysicalOperator> left,
    std::unique_ptr<PhysicalOperator> right,
    std::unique_ptr<AbstractExpression> cond, JoinType join_type)
    : PhysicalOperator(PhysicalOperatorType::NESTED_LOOP_JOIN),
      cross_product(PhysicalCrossProduct(move(left), move(right))),
      condition(move(cond)), type(join_type) {}

vector<TypeId> PhysicalNestedLoopJoin::GetTypes() {
	return cross_product.GetTypes();
}

void PhysicalNestedLoopJoin::GetChunk(DataChunk &chunk,
                                      PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalCrossProductOperatorState *>(state_);
	chunk.Reset();

	if (type != JoinType::INNER) {
		throw Exception("Only inner joins supported for now!");
	}
	do {
		// generate the cross product for this chunk
		cross_product.GetChunk(chunk, state_);
		if (chunk.count == 0) {
			return;
		}
		// resolve the join condition on the chunk
		Vector result(TypeId::BOOLEAN, chunk.count);
		ExpressionExecutor exec(chunk, state->parent);
		exec.Execute(condition.get(), result);
		assert(result.count == chunk.count);
		// now generate the selection vector
		bool *matches = (bool *)result.data;
		auto sel_vector = unique_ptr<sel_t[]>(new sel_t[chunk.count]);
		size_t match_count = 0;
		for (size_t i = 0; i < result.count; i++) {
			if (matches[i]) {
				sel_vector[match_count++] = i;
			}
		}
		if (match_count == 0) {
			chunk.count = 0;
			continue;
		}
		if (match_count == result.count) {
			// everything matches! don't need a selection vector!
			sel_vector.reset();
		}
		chunk.SetSelVector(move(sel_vector), match_count);
	} while (chunk.count == 0);
}

std::unique_ptr<PhysicalOperatorState>
PhysicalNestedLoopJoin::GetOperatorState(ExpressionExecutor *parent_executor) {
	return cross_product.GetOperatorState(parent_executor);
}
