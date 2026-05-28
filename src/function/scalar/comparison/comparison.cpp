#include "duckdb/function/scalar/comparison_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

struct ComparisonFunctionData : public FunctionData {
	explicit ComparisonFunctionData(ExpressionType expression_type_p) : expression_type(expression_type_p) {
	}

	ExpressionType expression_type;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ComparisonFunctionData>(expression_type);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ComparisonFunctionData>();
		return expression_type == other.expression_type;
	}
};

template <ExpressionType TYPE>
void ComparisonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)state;
	const auto &left = args.data[0];
	const auto &right = args.data[1];

	if constexpr (TYPE == ExpressionType::COMPARE_EQUAL) {
		VectorOperations::Equals(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_NOTEQUAL) {
		VectorOperations::NotEquals(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_LESSTHAN) {
		VectorOperations::LessThan(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_GREATERTHAN) {
		VectorOperations::GreaterThan(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		VectorOperations::LessThanEquals(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		VectorOperations::GreaterThanEquals(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_DISTINCT_FROM) {
		VectorOperations::DistinctFrom(left, right, result);
	} else if constexpr (TYPE == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		VectorOperations::NotDistinctFrom(left, right, result);
	} else {
		throw InternalException("Unknown comparison type!");
	}
}

#ifndef DUCKDB_SMALLER_BINARY
template <ExpressionType TYPE>
idx_t ComparisonSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                       optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	(void)state;
	const auto &left = args.data[0];
	const auto &right = args.data[1];
	auto count = args.size();

	if constexpr (TYPE == ExpressionType::COMPARE_EQUAL) {
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_NOTEQUAL) {
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_LESSTHAN) {
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_GREATERTHAN) {
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_DISTINCT_FROM) {
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	} else if constexpr (TYPE == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	} else {
		throw InternalException("Unknown comparison type!");
	}
}
#endif

template <ExpressionType TYPE>
ExpressionType ComparisonGetExpressionType(FunctionToStringInput &input) {
	(void)input;
	return TYPE;
}

template <ExpressionType TYPE>
static ScalarFunction GetComparisonFunctionInternal(const string &name) {
	ScalarFunction comparison_fun(name, {LogicalType::ANY, LogicalType::ANY}, LogicalType::BOOLEAN,
	                              ComparisonFunction<TYPE>);
	comparison_fun.SetGetExpressionTypeCallback(ComparisonGetExpressionType<TYPE>);
	if constexpr (TYPE == ExpressionType::COMPARE_DISTINCT_FROM || TYPE == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		comparison_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	}
#ifndef DUCKDB_SMALLER_BINARY
	comparison_fun.SetSelectCallback(ComparisonSelect<TYPE>);
#endif
	return comparison_fun;
}

static ScalarFunction GetComparisonFunction(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return OperatorEqualFun::GetFunction();
	case ExpressionType::COMPARE_NOTEQUAL:
		return OperatorNotEqualFun::GetFunction();
	case ExpressionType::COMPARE_LESSTHAN:
		return OperatorLessThanFun::GetFunction();
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return OperatorLessThanEqualsFun::GetFunction();
	case ExpressionType::COMPARE_GREATERTHAN:
		return OperatorGreaterThanFun::GetFunction();
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return OperatorGreaterThanEqualsFun::GetFunction();
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return IsDistinctFromFun::GetFunction();
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return IsNotDistinctFromFun::GetFunction();
	default:
		throw InternalException("Unknown comparison type!");
	}
}

ScalarFunction OperatorEqualFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_EQUAL>(OperatorEqualFun::Name);
}

ScalarFunction OperatorNotEqualFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_NOTEQUAL>(OperatorNotEqualFun::Name);
}

ScalarFunction OperatorLessThanFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_LESSTHAN>(OperatorLessThanFun::Name);
}

ScalarFunction OperatorLessThanEqualsFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_LESSTHANOREQUALTO>(OperatorLessThanEqualsFun::Name);
}

ScalarFunction OperatorGreaterThanFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_GREATERTHAN>(OperatorGreaterThanFun::Name);
}

ScalarFunction OperatorGreaterThanEqualsFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_GREATERTHANOREQUALTO>(
	    OperatorGreaterThanEqualsFun::Name);
}

ScalarFunction IsDistinctFromFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_DISTINCT_FROM>(IsDistinctFromFun::Name);
}

ScalarFunction IsNotDistinctFromFun::GetFunction() {
	return GetComparisonFunctionInternal<ExpressionType::COMPARE_NOT_DISTINCT_FROM>(IsNotDistinctFromFun::Name);
}

unique_ptr<FunctionData> BindComparisonFun(BindScalarFunctionInput &input) {
	throw InvalidInputException("Comparison function cannot be called directly");
}

static unique_ptr<Expression> ComparisonBindExpression(FunctionBindExpressionInput &input) {
	auto &comparison_data = input.bind_data->Cast<ComparisonFunctionData>();
	return BoundComparisonExpression::Create(comparison_data.expression_type, std::move(input.children[0]),
	                                         std::move(input.children[1]));
}

void ComparisonFunctionSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                 const BoundScalarFunction &function) {
	auto &bind_data = bind_data_p->Cast<ComparisonFunctionData>();
	serializer.WriteProperty(100, "comparison_type", bind_data.expression_type);
}

unique_ptr<FunctionData> ComparisonFunctionDeserialize(Deserializer &deserializer, BoundScalarFunction &function) {
	auto expression_type = deserializer.ReadProperty<ExpressionType>(100, "comparison_type");
	return make_uniq<ComparisonFunctionData>(expression_type);
}

ScalarFunction ComparisonFun::GetFunction() {
	ScalarFunction comparison_fun(ComparisonFun::Name, {LogicalType::ANY, LogicalType::ANY}, LogicalType::BOOLEAN,
	                              nullptr, BindComparisonFun);
	comparison_fun.SetBindExpressionCallback(ComparisonBindExpression);
	comparison_fun.SetSerializeCallback(ComparisonFunctionSerialize);
	comparison_fun.SetDeserializeCallback(ComparisonFunctionDeserialize);
	return comparison_fun;
}

//===--------------------------------------------------------------------===//
// BoundComparisonExpression
//===--------------------------------------------------------------------===//
unique_ptr<Expression> BoundComparisonExpression::Create(ExpressionType type, unique_ptr<Expression> left,
                                                         unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));

	auto result = make_uniq<BoundFunctionExpression>(BoundScalarFunction(GetComparisonFunction(type)),
	                                                 std::move(children), nullptr, true);
	return std::move(result);
}

bool BoundComparisonExpression::IsComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

bool BoundComparisonExpression::IsComparison(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	return IsComparison(expr.GetExpressionType());
}

const Expression &BoundComparisonExpression::Left(const BoundFunctionExpression &comparison_expr) {
	return *comparison_expr.children[0];
}

const Expression &BoundComparisonExpression::Right(const BoundFunctionExpression &comparison_expr) {
	return *comparison_expr.children[1];
}

unique_ptr<Expression> &BoundComparisonExpression::LeftMutable(BoundFunctionExpression &comparison_expr) {
	return comparison_expr.children[0];
}

unique_ptr<Expression> &BoundComparisonExpression::RightMutable(BoundFunctionExpression &comparison_expr) {
	return comparison_expr.children[1];
}

void BoundComparisonExpression::SetType(BoundFunctionExpression &comparison_expr, ExpressionType new_type) {
	auto arguments = comparison_expr.function.GetArguments();
	auto original_arguments = comparison_expr.function.GetOriginalArguments();

	comparison_expr.SetExpressionTypeUnsafe(new_type);
	comparison_expr.function = BoundScalarFunction(GetComparisonFunction(new_type));
	comparison_expr.function.GetArguments() = std::move(arguments);
	comparison_expr.function.GetOriginalArguments() = std::move(original_arguments);
	comparison_expr.bind_info.reset();
	comparison_expr.is_operator = true;
}

void BoundComparisonExpression::FlipType(BoundFunctionExpression &comparison_expr) {
	SetType(comparison_expr, FlipComparisonExpression(comparison_expr.GetExpressionType()));
}

} // namespace duckdb
