#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar/comparison_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_between_expression.hpp"

namespace duckdb {

struct BetweenFunctionData : public FunctionData {
	BetweenFunctionData(bool lower_inclusive, bool upper_inclusive, unique_ptr<Expression> comparison)
	    : lower_inclusive(lower_inclusive), upper_inclusive(upper_inclusive), comparison(std::move(comparison)) {
	}

	bool lower_inclusive;
	bool upper_inclusive;
	//! "input <op> lower AND input <op> upper" over the (input, lower, upper) arguments
	unique_ptr<Expression> comparison;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BetweenFunctionData>(lower_inclusive, upper_inclusive, comparison->Copy());
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BetweenFunctionData>();
		return lower_inclusive == other.lower_inclusive && upper_inclusive == other.upper_inclusive &&
		       comparison->Equals(*other.comparison);
	}
};

struct BetweenLocalState : public FunctionLocalState {
	BetweenLocalState(Context &context, const BetweenFunctionData &data) : executor(context, *data.comparison) {
	}

	ExpressionExecutor executor;
};

static unique_ptr<FunctionLocalState> BetweenInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            optional_ptr<const FunctionData> bind_data) {
	return make_uniq<BetweenLocalState>(state.GetContext(), bind_data->Cast<BetweenFunctionData>());
}

void BetweenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<BetweenLocalState>();
	lstate.executor.ExecuteExpression(args, result);
}

#if !DUCKDB_SMALLER_BINARY(between_select)
struct BothInclusiveBetweenOperator {
	static constexpr bool LOWER_INCLUSIVE = true;
	static constexpr bool UPPER_INCLUSIVE = true;

	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct LowerInclusiveBetweenOperator {
	static constexpr bool LOWER_INCLUSIVE = true;
	static constexpr bool UPPER_INCLUSIVE = false;

	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

struct UpperInclusiveBetweenOperator {
	static constexpr bool LOWER_INCLUSIVE = false;
	static constexpr bool UPPER_INCLUSIVE = true;

	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct ExclusiveBetweenOperator {
	static constexpr bool LOWER_INCLUSIVE = false;
	static constexpr bool UPPER_INCLUSIVE = false;

	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

template <class OP>
static idx_t BetweenLoopTypeSwitch(BetweenLocalState &lstate, DataChunk &args, optional_ptr<const SelectionVector> sel,
                                   idx_t count, optional_ptr<SelectionVector> true_sel,
                                   optional_ptr<SelectionVector> false_sel) {
	const auto &input = args.data[0];
	const auto &lower = args.data[1];
	const auto &upper = args.data[2];
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TernaryExecutor::Select<int8_t, int8_t, int8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::INT16:
		return TernaryExecutor::Select<int16_t, int16_t, int16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT32:
		return TernaryExecutor::Select<int32_t, int32_t, int32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT64:
		return TernaryExecutor::Select<int64_t, int64_t, int64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT128:
		return TernaryExecutor::Select<hugeint_t, hugeint_t, hugeint_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                    false_sel);
	case PhysicalType::UINT8:
		return TernaryExecutor::Select<uint8_t, uint8_t, uint8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::UINT16:
		return TernaryExecutor::Select<uint16_t, uint16_t, uint16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT32:
		return TernaryExecutor::Select<uint32_t, uint32_t, uint32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT64:
		return TernaryExecutor::Select<uint64_t, uint64_t, uint64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT128:
		return TernaryExecutor::Select<uhugeint_t, uhugeint_t, uhugeint_t, OP>(input, lower, upper, sel, count,
		                                                                       true_sel, false_sel);
	case PhysicalType::FLOAT:
		return TernaryExecutor::Select<float, float, float, OP>(input, lower, upper, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return TernaryExecutor::Select<double, double, double, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::VARCHAR:
		return TernaryExecutor::Select<string_t, string_t, string_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::INTERVAL:
		return TernaryExecutor::Select<interval_t, interval_t, interval_t, OP>(input, lower, upper, sel, count,
		                                                                       true_sel, false_sel);
	default: {
		// types without a templated select path (e.g. nested types): the arguments are dense, 'sel' maps their rows
		Vector result(LogicalType::BOOLEAN);
		lstate.executor.ExecuteExpression(args, result);
		return UnaryExecutor::Select<bool>(
		    result, sel.get(), count, [](bool value) { return value; }, true_sel.get(), false_sel.get());
	}
	}
}

idx_t BetweenSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	auto &between_expr = state.expr.Cast<BoundFunctionExpression>();
	bool upper_inclusive = BoundBetweenExpression::UpperInclusive(between_expr);
	bool lower_inclusive = BoundBetweenExpression::LowerInclusive(between_expr);

	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<BetweenLocalState>();
	auto count = args.size();

	if (upper_inclusive && lower_inclusive) {
		return BetweenLoopTypeSwitch<BothInclusiveBetweenOperator>(lstate, args, sel, count, true_sel, false_sel);
	} else if (lower_inclusive) {
		return BetweenLoopTypeSwitch<LowerInclusiveBetweenOperator>(lstate, args, sel, count, true_sel, false_sel);
	} else if (upper_inclusive) {
		return BetweenLoopTypeSwitch<UpperInclusiveBetweenOperator>(lstate, args, sel, count, true_sel, false_sel);
	} else {
		return BetweenLoopTypeSwitch<ExclusiveBetweenOperator>(lstate, args, sel, count, true_sel, false_sel);
	}
}
#endif

unique_ptr<Expression> BoundBetweenExpression::Create(ClientContext &context, unique_ptr<Expression> input,
                                                      unique_ptr<Expression> lower, unique_ptr<Expression> upper,
                                                      bool lower_inclusive, bool upper_inclusive) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(input));
	children.push_back(std::move(lower));
	children.push_back(std::move(upper));

	auto &input_type = children[0]->GetReturnType();
	auto lower_comparison = BoundComparisonExpression::CreateOnColumns(
	    context, BoundBetweenExpression::LowerComparisonType(lower_inclusive), input_type, 0,
	    children[1]->GetReturnType(), 1);
	auto upper_comparison = BoundComparisonExpression::CreateOnColumns(
	    context, BoundBetweenExpression::UpperComparisonType(upper_inclusive), input_type, 0,
	    children[2]->GetReturnType(), 2);
	auto comparison = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                        std::move(lower_comparison), std::move(upper_comparison));
	auto function_data = make_uniq<BetweenFunctionData>(lower_inclusive, upper_inclusive, std::move(comparison));

	auto result = make_uniq<BoundFunctionExpression>(BoundScalarFunction(BetweenFun::GetFunction()),
	                                                 std::move(children), std::move(function_data), false);
	return std::move(result);
}

unique_ptr<FunctionData> BindBetweenFun(BindScalarFunctionInput &input) {
	throw InvalidInputException("Between function cannot be called directly");
}

string BetweenToString(FunctionToStringInput &input) {
	auto &between_info = input.bind_data->Cast<BetweenFunctionData>();
	auto &between_input = input.GetChild(0);
	auto &lower = input.GetChild(1);
	auto &upper = input.GetChild(2);
	if (between_info.lower_inclusive && between_info.upper_inclusive) {
		return BetweenExpression::ToString(between_input, lower, upper);
	}
	auto lower_type = BoundBetweenExpression::LowerComparisonType(between_info.lower_inclusive);
	auto upper_type = BoundBetweenExpression::UpperComparisonType(between_info.upper_inclusive);
	return "(" + ComparisonExpression::ToString(lower_type, between_input, lower) + " AND " +
	       ComparisonExpression::ToString(upper_type, between_input, upper) + ")";
}

ExpressionType BetweenGetExpressionType(FunctionToStringInput &input) {
	return ExpressionType::COMPARE_BETWEEN;
}

unique_ptr<Expression> BetweenLegacySerializeCallback(FunctionToStringInput &input) {
	auto &between_info = input.bind_data->Cast<BetweenFunctionData>();
	return make_uniq<LegacyBoundBetweenExpression>(input.GetChild(0).Copy(), input.GetChild(1).Copy(),
	                                               input.GetChild(2).Copy(), between_info.lower_inclusive,
	                                               between_info.upper_inclusive);
}

void BetweenFunctionSerialize(Serializer &serializer, const optional_ptr<const FunctionData> bind_data_p,
                              const BoundScalarFunction &function) {
	auto &bind_data = bind_data_p->Cast<BetweenFunctionData>();
	serializer.WriteProperty(100, "lower_inclusive", bind_data.lower_inclusive);
	serializer.WriteProperty(101, "upper_inclusive", bind_data.upper_inclusive);
	serializer.WriteProperty(102, "comparison", bind_data.comparison);
}

unique_ptr<FunctionData> BetweenFunctionDeserialize(Deserializer &deserializer, BoundScalarFunction &function) {
	auto lower_inclusive = deserializer.ReadProperty<bool>(100, "lower_inclusive");
	auto upper_inclusive = deserializer.ReadProperty<bool>(101, "upper_inclusive");
	auto comparison = deserializer.ReadProperty<unique_ptr<Expression>>(102, "comparison");
	return make_uniq<BetweenFunctionData>(lower_inclusive, upper_inclusive, std::move(comparison));
}

ScalarFunction BetweenFun::GetFunction() {
	ScalarFunction between_fun("__between", {}, LogicalType::BOOLEAN, BetweenFunction, BindBetweenFun);
	between_fun.GetSignature()
	    .AddParameter("input", LogicalType::ANY)
	    .AddParameter("lower", LogicalType::ANY)
	    .AddParameter("upper", LogicalType::ANY);
	between_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	between_fun.SetToStringCallback(BetweenToString);
	between_fun.SetGetExpressionTypeCallback(BetweenGetExpressionType);
	between_fun.SetLegacySerializeCallback(BetweenLegacySerializeCallback);
	between_fun.SetInitStateCallback(BetweenInitLocalState);
	between_fun.SetSerializeCallback(BetweenFunctionSerialize);
	between_fun.SetDeserializeCallback(BetweenFunctionDeserialize);
#if !DUCKDB_SMALLER_BINARY(between_select)
	between_fun.SetSelectCallback(BetweenSelect);
#endif
	return between_fun;
}

//===--------------------------------------------------------------------===//
// BoundBetweenExpression
//===--------------------------------------------------------------------===//
bool BoundBetweenExpression::LowerInclusive(const BoundFunctionExpression &between_expr) {
	auto &data = between_expr.BindInfo()->Cast<BetweenFunctionData>();
	return data.lower_inclusive;
}

bool BoundBetweenExpression::UpperInclusive(const BoundFunctionExpression &between_expr) {
	auto &data = between_expr.BindInfo()->Cast<BetweenFunctionData>();
	return data.upper_inclusive;
}

bool BoundBetweenExpression::HasValidBindData(const BoundFunctionExpression &between_expr) {
	return between_expr.BindInfo() != nullptr;
}

const Expression &BoundBetweenExpression::Input(const BoundFunctionExpression &between_expr) {
	return *between_expr.GetChildren()[0];
}

const Expression &BoundBetweenExpression::LowerBound(const BoundFunctionExpression &between_expr) {
	return *between_expr.GetChildren()[1];
}

const Expression &BoundBetweenExpression::UpperBound(const BoundFunctionExpression &between_expr) {
	return *between_expr.GetChildren()[2];
}

unique_ptr<Expression> &BoundBetweenExpression::InputMutable(BoundFunctionExpression &between_expr) {
	return between_expr.GetChildrenMutable()[0];
}

unique_ptr<Expression> &BoundBetweenExpression::LowerBoundMutable(BoundFunctionExpression &between_expr) {
	return between_expr.GetChildrenMutable()[1];
}

unique_ptr<Expression> &BoundBetweenExpression::UpperBoundMutable(BoundFunctionExpression &between_expr) {
	return between_expr.GetChildrenMutable()[2];
}

} // namespace duckdb
