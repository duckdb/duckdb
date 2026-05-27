#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

unique_ptr<Expression> LegacyBFTableFilter::ToExpression(const Expression &column) const {
	auto function = BloomFilterScalarFun::GetFunction(column.GetReturnType());
	auto bind_data =
	    make_uniq<BloomFilterFunctionData>(filter, filters_null_values, key_column_name, key_type, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          std::move(bind_data));
}

void LegacyBFTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
}

unique_ptr<TableFilter> LegacyBFTableFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	auto result = make_uniq<LegacyBFTableFilter>(nullptr, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
