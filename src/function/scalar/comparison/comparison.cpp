#include "duckdb/function/scalar/comparison_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

struct ComparisonFunctionData : public FunctionData {
	explicit ComparisonFunctionData(ExpressionType expression_type) : expression_type(expression_type) {
	}

	ExpressionType expression_type;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ComparisonFunctionData>(expression_type);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ComparisonFunctionData>();
		return expression_type == other.expression_type;
	}
};

void ComparisonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto expr_type = state.expr.GetExpressionType();
	const auto &left = args.data[0];
	const auto &right = args.data[1];

	switch (expr_type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		VectorOperations::DistinctFrom(left, right, result);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		VectorOperations::NotDistinctFrom(left, right, result);
		break;
	default:
		throw InternalException("Unknown comparison type!");
	}
}

idx_t ComparisonSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                       optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	auto expr_type = state.expr.GetExpressionType();
	const auto &left = args.data[0];
	const auto &right = args.data[1];
	auto count = args.size();

	switch (expr_type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unknown comparison type!");
	}
}

unique_ptr<FunctionData> BindComparisonFun(BindScalarFunctionInput &input) {
	throw InvalidInputException("Comparison function cannot be called directly");
}

string ComparisonToString(FunctionToStringInput &input) {
	auto &comparison_data = input.bind_data->Cast<ComparisonFunctionData>();
	return ComparisonExpression::ToString(comparison_data.expression_type, *input.children[0], *input.children[1]);
}

ExpressionType ComparisonGetExpressionType(FunctionToStringInput &input) {
	auto &comparison_data = input.bind_data->Cast<ComparisonFunctionData>();
	return comparison_data.expression_type;
}

unique_ptr<Expression> ComparisonLegacySerializeCallback(FunctionToStringInput &input) {
	auto &comparison_data = input.bind_data->Cast<ComparisonFunctionData>();
	return make_uniq<LegacyBoundComparisonExpression>(comparison_data.expression_type, input.GetChild(0).Copy(),
	                                                  input.GetChild(1).Copy());
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
	ScalarFunction Comparison_fun("__comparison", {LogicalType::ANY, LogicalType::ANY}, LogicalType::BOOLEAN,
	                              ComparisonFunction, BindComparisonFun);
	Comparison_fun.SetToStringCallback(ComparisonToString);
	Comparison_fun.SetGetExpressionTypeCallback(ComparisonGetExpressionType);
	Comparison_fun.SetLegacySerializeCallback(ComparisonLegacySerializeCallback);
	Comparison_fun.SetSerializeCallback(ComparisonFunctionSerialize);
	Comparison_fun.SetDeserializeCallback(ComparisonFunctionDeserialize);
	// DISTINCT FROM / NOT DISTINCT FROM handle NULLs specially - they must not be short-circuited on NULL input
	Comparison_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
#ifndef DUCKDB_SMALLER_BINARY
	Comparison_fun.SetSelectCallback(ComparisonSelect);
#endif
	return Comparison_fun;
}

//===--------------------------------------------------------------------===//
// BoundComparisonExpression
//===--------------------------------------------------------------------===//
unique_ptr<Expression> BoundComparisonExpression::Create(ExpressionType type, unique_ptr<Expression> left,
                                                         unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));

	auto function_data = make_uniq<ComparisonFunctionData>(type);

	auto result = make_uniq<BoundFunctionExpression>(BoundScalarFunction(ComparisonFun::GetFunction()),
	                                                 std::move(children), std::move(function_data), false);
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
	comparison_expr.SetExpressionTypeUnsafe(new_type);
	comparison_expr.bind_info->Cast<ComparisonFunctionData>().expression_type = new_type;
}

void BoundComparisonExpression::FlipType(BoundFunctionExpression &comparison_expr) {
	SetType(comparison_expr, FlipComparisonExpression(comparison_expr.GetExpressionType()));
}

} // namespace duckdb
