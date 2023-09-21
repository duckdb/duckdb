#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

BoundParameterMap::BoundParameterMap(case_insensitive_map_t<BoundParameterData> &parameter_data)
    : parameter_data(parameter_data) {
}

LogicalType BoundParameterMap::GetReturnType(const string &identifier) {
	D_ASSERT(!identifier.empty());
	auto it = parameter_data.find(identifier);
	if (it == parameter_data.end()) {
		return LogicalTypeId::UNKNOWN;
	}
	return it->second.return_type;
}

bound_parameter_map_t *BoundParameterMap::GetParametersPtr() {
	return &parameters;
}

const bound_parameter_map_t &BoundParameterMap::GetParameters() {
	return parameters;
}

const case_insensitive_map_t<BoundParameterData> &BoundParameterMap::GetParameterData() {
	return parameter_data;
}

shared_ptr<BoundParameterData> BoundParameterMap::CreateOrGetData(const string &identifier) {
	auto entry = parameters.find(identifier);
	if (entry == parameters.end()) {
		// no entry yet: create a new one
		auto data = make_shared<BoundParameterData>();
		data->return_type = GetReturnType(identifier);

		CreateNewParameter(identifier, data);
		return data;
	}
	return entry->second;
}

unique_ptr<BoundParameterExpression> BoundParameterMap::BindParameterExpression(ParameterExpression &expr) {
	auto &identifier = expr.identifier;
	auto return_type = GetReturnType(identifier);

	D_ASSERT(!parameter_data.count(identifier));

	// No value has been supplied yet,
	// We return a shared pointer to an object that will get populated wtih a Value later
	// When the BoundParameterExpression get executed, this will be used to get the corresponding value
	auto param_data = CreateOrGetData(identifier);
	auto bound_expr = make_uniq<BoundParameterExpression>(identifier);
	bound_expr->parameter_data = param_data;
	bound_expr->return_type = return_type;
	bound_expr->alias = expr.alias;
	return bound_expr;
}

void BoundParameterMap::CreateNewParameter(const string &id, const shared_ptr<BoundParameterData> &param_data) {
	D_ASSERT(!parameters.count(id));
	parameters.emplace(std::make_pair(id, param_data));
}

} // namespace duckdb
