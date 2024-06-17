#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ConstantReturnBindData : public FunctionData {
	Value val;

	explicit ConstantReturnBindData(Value val_p) : val(std::move(val_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ConstantReturnBindData>(val);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ConstantReturnBindData>();
		return val == other.val;
	}
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                      const ScalarFunction &function) {
		auto &info = bind_data->Cast<ConstantReturnBindData>();
		serializer.WriteProperty(100, "constant_value", info.val);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &bound_function) {
		auto value = deserializer.ReadProperty<Value>(100, "constant_value");
		return make_uniq<ConstantReturnBindData>(std::move(value));
	}
};

static void TypeOfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		Value v(args.data[0].GetType().ToString());
		result.Reference(v);
	} else {
		auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ConstantReturnBindData>();
		result.Reference(bind_data.val);
	}
}

unique_ptr<FunctionData> BindTypeOfFunction(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	Value return_value(arguments[0]->return_type.ToString());
	arguments.clear();
	return make_uniq<ConstantReturnBindData>(return_value);
}

ScalarFunction TypeOfFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, TypeOfFunction, BindTypeOfFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ConstantReturnBindData::Serialize;
	fun.deserialize = ConstantReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb
