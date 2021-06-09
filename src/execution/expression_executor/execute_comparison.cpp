#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.left.get());
	result->AddChild(expr.right.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// resolve the children
	Vector left, right;
	left.Reference(state->intermediate_chunk.data[0]);
	right.Reference(state->intermediate_chunk.data[1]);

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		VectorOperations::DistinctFrom(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		VectorOperations::NotDistinctFrom(left, right, result, count);
		break;
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

template <typename OP>
static idx_t StructSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel);

template <class OP>
static idx_t TemplatedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                      SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::POINTER:
		return BinaryExecutor::Select<uintptr_t, uintptr_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return BinaryExecutor::Select<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return BinaryExecutor::Select<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::STRUCT:
		return StructSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::LIST:
		return BinaryExecutor::Select<Vector, Vector, OP>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InvalidTypeException(left.GetType(), "Invalid type for comparison");
	}
}

struct PositionComparator {

	// Select the rows that definitely match.
	// Sometimes this cannot be determined without looking at the next position.
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector &false_sel) {
		return 0;
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector &true_sel, SelectionVector *false_sel) {
		return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, &true_sel, false_sel);
	}

	// Select the matching rows for the final position.
	// This is typically more restrictive than Possible and less restrictive than Definite.
	// The default is for struct equality.
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                   SelectionVector *false_sel) {
		return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	}
};

static inline void GetStructChildren(Vector &v, const idx_t vcount, vector<Vector> &struct_vectors) {
	if (v.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(v);
		auto &dict_sel = DictionaryVector::SelVector(v);
		auto &children = StructVector::GetEntries(child);
		for (auto &struct_child : children) {
			Vector struct_vector;
			struct_vector.Slice(*struct_child, dict_sel, vcount);
			struct_vectors.push_back(move(struct_vector));
		}
	} else {
		auto &children = StructVector::GetEntries(v);
		for (auto &struct_child : children) {
			Vector struct_vector;
			struct_vector.Reference(*struct_child);
			struct_vectors.push_back(move(struct_vector));
		}
	}
}

template <typename OP>
static idx_t StructSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t result = 0;

	// Incrementally fill in successes and failures as we discover them
	SelectionVector true_vec;
	if (true_sel) {
		true_vec.Initialize(true_sel->data());
		true_sel = &true_vec;
	}

	SelectionVector false_vec;
	if (false_sel) {
		false_vec.Initialize(false_sel->data());
		false_sel = &false_vec;
	}

	//	We need multiple, real selections
	SelectionVector maybe_vec(count);
	if (!sel) {
		sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	}

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	VectorData ldata, rdata;
	left.Orrify(count, ldata);
	right.Orrify(count, rdata);
	if (!ldata.validity.AllValid() || !rdata.validity.AllValid()) {
		idx_t true_count = 0;
		idx_t false_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel->get_index(i);
			if (!ldata.validity.RowIsValid(idx) || !rdata.validity.RowIsValid(idx)) {
				if (false_sel) {
					false_sel->set_index(false_count, idx);
				}
				++false_count;
			} else {
				maybe_vec.set_index(true_count++, idx);
			}
		}
		sel = &maybe_vec;
		count = true_count;
	}

	vector<Vector> lchildren;
	GetStructChildren(left, count, lchildren);

	vector<Vector> rchildren;
	GetStructChildren(right, count, rchildren);

	D_ASSERT(lchildren.size() == rchildren.size());

	idx_t col_no = 0;
	for (; col_no < lchildren.size() - 1; ++col_no) {
		auto &lchild = lchildren[col_no];
		auto &rchild = rchildren[col_no];

		// Find everything that definitely matches
		auto definite = PositionComparator::Definite<OP>(lchild, rchild, sel, count, true_sel, maybe_vec);

		// Advance the true selection
		if (true_sel) {
			true_sel->Initialize(true_sel->data() + definite);
		}
		count -= definite;
		result += definite;

		// Find what might match on the next position
		idx_t possible = 0;
		if (definite > 0) {
			possible = PositionComparator::Possible<OP>(lchild, rchild, &maybe_vec, count, maybe_vec, false_sel);
		} else {
			// If there were no definite matches, then for speed,
			// matched may not have been filled in.
			possible = PositionComparator::Possible<OP>(lchild, rchild, sel, count, maybe_vec, false_sel);
		}

		// Advance the false selection
		if (false_sel) {
			false_sel->Initialize(false_sel->data() + (count - possible));
		}
		sel = &maybe_vec;
		count = possible;
	}

	//	Find everything that matches the last column exactly
	result += PositionComparator::Final<OP>(lchildren[col_no], rchildren[col_no], sel, count, true_sel, false_sel);

	return result;
}

idx_t VectorOperations::Equals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	Vector left, right;
	left.Reference(state->intermediate_chunk.data[0]);
	right.Reference(state->intermediate_chunk.data[1]);

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::SelectDistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

} // namespace duckdb
