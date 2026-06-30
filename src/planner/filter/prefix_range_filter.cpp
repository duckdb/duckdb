#include "duckdb/planner/filter/prefix_range_filter.hpp"

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

LegacyPrefixRangeTableFilter::LegacyPrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p,
                                                           const string &key_column_name_p,
                                                           const LogicalType &key_type_p)
    : TableFilter(TYPE), filter(filter_p), key_column_name(key_column_name_p), key_type(key_type_p) {
}

unique_ptr<Expression> LegacyPrefixRangeTableFilter::ToExpression(const Expression &column) const {
	auto function = PrefixRangeScalarFun::GetFunction(column.GetReturnType());
	auto bind_data = make_uniq<PrefixRangeFunctionData>(filter, key_column_name, key_type, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          std::move(bind_data));
}

void LegacyPrefixRangeTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(201, "key_type", key_type);
}

unique_ptr<TableFilter> LegacyPrefixRangeTableFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(201, "key_type");

	auto result = make_uniq<LegacyPrefixRangeTableFilter>(nullptr, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
