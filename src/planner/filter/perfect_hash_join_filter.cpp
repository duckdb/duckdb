#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

LegacyPerfectHashJoinFilter::LegacyPerfectHashJoinFilter(
    optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor_p, const string &key_column_name_p,
    const LogicalType &key_type_p)
    : TableFilter(TYPE), perfect_join_executor(perfect_join_executor_p), key_column_name(key_column_name_p),
      key_type(key_type_p) {
}

unique_ptr<Expression> LegacyPerfectHashJoinFilter::ToExpression(const Expression &column) const {
	auto function = PerfectHashJoinScalarFun::GetFunction(column.return_type);
	auto bind_data = make_uniq<PerfectHashJoinFunctionData>(perfect_join_executor, key_column_name, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(function), std::move(arguments),
	                                          std::move(bind_data));
}

void LegacyPerfectHashJoinFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(201, "key_type", key_type);
}

unique_ptr<TableFilter> LegacyPerfectHashJoinFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(201, "key_type");
	return make_uniq<LegacyPerfectHashJoinFilter>(nullptr, key_column_name, key_type);
}

} // namespace duckdb
