#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"

#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

PerfectHashJoinFilter::PerfectHashJoinFilter(const string &key_column_name_p)
    : TableFilter(TYPE), key_column_name(key_column_name_p) {
}

unique_ptr<Expression> PerfectHashJoinFilter::ToExpression(const Expression &column) const {
	auto func = PerfectHashJoinScalarFun::GetFunction(column.return_type);
	auto bind_data = make_uniq<PerfectHashJoinFunctionData>(nullptr, key_column_name, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

void PerfectHashJoinFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
}

unique_ptr<TableFilter> PerfectHashJoinFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	return make_uniq<PerfectHashJoinFilter>(key_column_name);
}

} // namespace duckdb
