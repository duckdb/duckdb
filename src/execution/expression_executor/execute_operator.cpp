#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundOperatorExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}

		Vector left(expr.children[0]->return_type);
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), sel, count, left);

		// init result to false
		Vector intermediate(LogicalType::BOOLEAN);
		Value false_val = Value::BOOLEAN(false);
		intermediate.Reference(false_val);

		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (idx_t child = 1; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Vector comp_res(LogicalType::BOOLEAN);

			Execute(*expr.children[child], state->child_states[child].get(), sel, count, vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res, count);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(LogicalType::BOOLEAN, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result, count);
				intermediate.Reference(new_result);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result, count);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
	} else if (expr.type == ExpressionType::OPERATOR_COALESCE) {
		SelectionVector next_sel(count);
		SelectionVector result_sel(count);
		SelectionVector remaining_sel;
		idx_t remaining_count = count;
		idx_t next_count;
		for(idx_t child = 0; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Execute(*expr.children[child], state->child_states[child].get(), sel, count, vector_to_check);

			VectorData vdata;
			vector_to_check.Orrify(count, vdata);

			idx_t result_count = 0;
			next_count = 0;
			for(idx_t i = 0; i < remaining_count; i++) {
				auto base_idx = remaining_sel.get_index(i);
				auto idx = vdata.sel->get_index(base_idx);
				if (vdata.validity.RowIsValid(idx)) {
					result_sel.set_index(result_count++, base_idx);
				} else {
					next_sel.set_index(next_count++, base_idx);
				}
			}
			if (result_count > 0) {
				vector_to_check.Slice(result_sel, result_count);
				FillSwitch(vector_to_check, result, result_sel, result_count);
			}
			remaining_sel.Initialize(next_sel);
			remaining_count = next_count;
			if (next_count == 0) {
				break;
			}
		}
		if (remaining_count > 0) {
			auto &result_mask = FlatVector::Validity(result);
			for (idx_t i = 0; i < remaining_count; i++) {
				result_mask.SetInvalid(remaining_sel.get_index(i));
			}
		}
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expr.children.size() == 1) {
		state->intermediate_chunk.Reset();
		auto &child = state->intermediate_chunk.data[0];

		Execute(*expr.children[0], state->child_states[0].get(), sel, count, child);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result, count);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

} // namespace duckdb
