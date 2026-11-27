#include "duckdb/common/vector/list_vector.hpp"
#include "core_functions/scalar/list_functions.hpp"
#include "core_functions/array_kernels.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Generic "fold" function
//------------------------------------------------------------------------------
// Given two lists of the same size, combine and reduce their elements into a
// single scalar value.

template <class TYPE, class OP>
static void ListGenericFold(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lstate = state.Cast<ExecuteFunctionState>();
	const auto &expr = lstate.expr.Cast<BoundFunctionExpression>();
	const auto &func_name = expr.Function().GetName();

	auto &lhs_vec = args.data[0];
	auto &rhs_vec = args.data[1];

	auto &lhs_child = ListVector::GetChildMutable(lhs_vec);
	auto &rhs_child = ListVector::GetChildMutable(rhs_vec);

	lhs_child.Flatten();
	rhs_child.Flatten();

	D_ASSERT(lhs_child.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(rhs_child.GetVectorType() == VectorType::FLAT_VECTOR);

	// reject lists whose elements contain NULLs, but only the elements that belong to non-NULL
	// parent rows: a fully-NULL list occupies no elements and yields a NULL result below. The
	// previous whole-child-vector check (CheckAllValid over the full element count) wrongly
	// rejected a NULL list read from Parquet, where the NULL list entry still occupies a child
	// slot with an invalid validity entry.
	UnifiedVectorFormat lhs_format;
	lhs_vec.ToUnifiedFormat(lhs_format);
	UnifiedVectorFormat rhs_format;
	rhs_vec.ToUnifiedFormat(rhs_format);
	UnifiedVectorFormat lhs_child_format;
	lhs_child.ToUnifiedFormat(lhs_child_format);
	UnifiedVectorFormat rhs_child_format;
	rhs_child.ToUnifiedFormat(rhs_child_format);
	for (idx_t i = 0; i < args.size(); i++) {
		idx_t li = lhs_format.sel ? lhs_format.sel->get_index(i) : i;
		if (!lhs_format.validity.RowIsValid(li)) {
			continue;
		}
		auto lhs_entry = reinterpret_cast<const list_entry_t *>(lhs_format.data)[li];
		if (!lhs_child_format.validity.CheckAllValid(lhs_entry.offset + lhs_entry.length, lhs_entry.offset)) {
			throw InvalidInputException("%s: left argument can not contain NULL values", SQLIdentifier(func_name));
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		idx_t ri = rhs_format.sel ? rhs_format.sel->get_index(i) : i;
		if (!rhs_format.validity.RowIsValid(ri)) {
			continue;
		}
		auto rhs_entry = reinterpret_cast<const list_entry_t *>(rhs_format.data)[ri];
		if (!rhs_child_format.validity.CheckAllValid(rhs_entry.offset + rhs_entry.length, rhs_entry.offset)) {
			throw InvalidInputException("%s: right argument can not contain NULL values", SQLIdentifier(func_name));
		}
	}

	auto lhs_data = FlatVector::GetData<TYPE>(lhs_child);
	auto rhs_data = FlatVector::GetData<TYPE>(rhs_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, TYPE>(
	    lhs_vec, rhs_vec, result, [&](const list_entry_t &left, const list_entry_t &right) -> optional<TYPE> {
		    if (left.length != right.length) {
			    throw InvalidInputException(
			        "%s: list dimensions must be equal, got left length '%d' and right length '%d'",
			        SQLIdentifier(func_name), left.length, right.length);
		    }

		    if (!OP::ALLOW_EMPTY && left.length == 0) {
			    return nullopt;
		    }

		    return OP::Operation(lhs_data + left.offset, rhs_data + right.offset, left.length);
	    });
}

//-------------------------------------------------------------------------
// Function Registration
//-------------------------------------------------------------------------

template <class OP>
static void AddListFoldFunction(ScalarFunctionSet &set, const LogicalType &type) {
	const auto list = LogicalType::LIST(type);
	scalar_function_t function;
	if (type.id() == LogicalTypeId::FLOAT) {
		function = ListGenericFold<float, OP>;
	} else if (type.id() == LogicalTypeId::DOUBLE) {
		function = ListGenericFold<double, OP>;
	} else {
		throw NotImplementedException("List function not implemented for type %s", type.ToString());
	}
	ScalarFunction fun({}, type, function);
	fun.GetSignature().AddParameter("list1", list).AddParameter("list2", list);
	set.AddFunction(fun);
}

ScalarFunctionSet ListDistanceFun::GetFunctions() {
	ScalarFunctionSet set("list_distance");
	for (auto &type : LogicalType::Real()) {
		AddListFoldFunction<DistanceOp>(set, type);
	}
	set.SetFallible();
	return set;
}

ScalarFunctionSet ListInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("list_inner_product");
	for (auto &type : LogicalType::Real()) {
		AddListFoldFunction<InnerProductOp>(set, type);
	}
	set.SetFallible();
	return set;
}

ScalarFunctionSet ListNegativeInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("list_negative_inner_product");
	for (auto &type : LogicalType::Real()) {
		AddListFoldFunction<NegativeInnerProductOp>(set, type);
	}
	set.SetFallible();
	return set;
}

ScalarFunctionSet ListCosineSimilarityFun::GetFunctions() {
	ScalarFunctionSet set("list_cosine_similarity");
	for (auto &type : LogicalType::Real()) {
		AddListFoldFunction<CosineSimilarityOp>(set, type);
	}
	set.SetFallible();
	return set;
}

ScalarFunctionSet ListCosineDistanceFun::GetFunctions() {
	ScalarFunctionSet set("list_cosine_distance");
	for (auto &type : LogicalType::Real()) {
		AddListFoldFunction<CosineDistanceOp>(set, type);
	}
	set.SetFallible();
	return set;
}

} // namespace duckdb
