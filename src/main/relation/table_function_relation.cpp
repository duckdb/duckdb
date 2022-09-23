#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

TableFunctionRelation::TableFunctionRelation(const std::shared_ptr<ClientContext> &context, string name_p,
                                             vector<Value> parameters_p, named_parameter_map_t named_parameters,
                                             shared_ptr<Relation> input_relation_p)
    : Relation(context, RelationType::TABLE_FUNCTION_RELATION), name(move(name_p)), parameters(move(parameters_p)),
      named_parameters(move(named_parameters)), input_relation(move(input_relation_p)) {
	context->TryBindRelation(*this, this->columns);
}
TableFunctionRelation::TableFunctionRelation(const std::shared_ptr<ClientContext> &context, string name_p,
                                             vector<Value> parameters_p,

                                             shared_ptr<Relation> input_relation_p)
    : Relation(context, RelationType::TABLE_FUNCTION_RELATION), name(move(name_p)), parameters(move(parameters_p)),
      input_relation(move(input_relation_p)) {
	context->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> TableFunctionRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return move(result);
}

unique_ptr<TableRef> TableFunctionRelation::GetTableRef() {
	vector<unique_ptr<ParsedExpression>> children;
	if (input_relation) { // input relation becomes first parameter if present, always
		auto subquery = make_unique<SubqueryExpression>();
		subquery->subquery = make_unique<SelectStatement>();
		subquery->subquery->node = input_relation->GetQueryNode();
		subquery->subquery_type = SubqueryType::SCALAR;
		children.push_back(move(subquery));
	}
	for (auto &parameter : parameters) {
		children.push_back(make_unique<ConstantExpression>(parameter));
	}

	for (auto &parameter : named_parameters) {
		// Hackity-hack some comparisons with column refs
		// This is all but pretty, basically the named parameter is the column, the table is empty because that's what
		// the function binder likes
		auto column_ref = make_unique<ColumnRefExpression>(parameter.first);
		auto constant_value = make_unique<ConstantExpression>(parameter.second);
		auto comparison =
		    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(column_ref), move(constant_value));
		children.push_back(move(comparison));
	}

	auto table_function = make_unique<TableFunctionRef>();
	auto function = make_unique<FunctionExpression>(name, move(children));
	table_function->function = move(function);
	return move(table_function);
}

string TableFunctionRelation::GetAlias() {
	return name;
}

const vector<ColumnDefinition> &TableFunctionRelation::Columns() {
	return columns;
}

string TableFunctionRelation::ToString(idx_t depth) {
	string function_call = name + "(";
	for (idx_t i = 0; i < parameters.size(); i++) {
		if (i > 0) {
			function_call += ", ";
		}
		function_call += parameters[i].ToString();
	}
	function_call += ")";
	return RenderWhitespace(depth) + function_call;
}

} // namespace duckdb
