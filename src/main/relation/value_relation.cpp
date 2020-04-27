#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

ValueRelation::ValueRelation(ClientContext &context, vector<vector<Value>> values, vector<string> names_p,
                             string alias_p)
    : Relation(context, RelationType::VALUE_LIST_RELATION), names(move(names_p)), alias(move(alias_p)) {
	// create constant expressions for the values
	for (idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		auto &list = values[row_idx];
		vector<unique_ptr<ParsedExpression>> expressions;
		for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			expressions.push_back(
			    make_unique<ConstantExpression>(SQLTypeFromInternalType(list[col_idx].type), list[col_idx]));
		}
		this->expressions.push_back(move(expressions));
	}
	context.TryBindRelation(*this, this->columns);
}

ValueRelation::ValueRelation(ClientContext &context, string values_list, vector<string> names_p, string alias_p)
    : Relation(context, RelationType::VALUE_LIST_RELATION), names(move(names_p)), alias(move(alias_p)) {
	this->expressions = Parser::ParseValuesList(values_list);
	context.TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> ValueRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return move(result);
}

unique_ptr<TableRef> ValueRelation::GetTableRef() {
	auto table_ref = make_unique<ExpressionListRef>();
	// set the expected types/names
	if (columns.size() == 0) {
		// no columns yet: only set up names
		for (idx_t i = 0; i < names.size(); i++) {
			table_ref->expected_names.push_back(names[i]);
		}
	} else {
		for (idx_t i = 0; i < columns.size(); i++) {
			table_ref->expected_names.push_back(columns[i].name);
			table_ref->expected_types.push_back(columns[i].type);
			assert(names.size() == 0 || columns[i].name == names[i]);
		}
	}
	// copy the expressions
	for (auto &expr_list : expressions) {
		vector<unique_ptr<ParsedExpression>> copied_list;
		for (auto &expr : expr_list) {
			copied_list.push_back(expr->Copy());
		}
		table_ref->values.push_back(move(copied_list));
	}
	table_ref->alias = GetAlias();
	return move(table_ref);
}

string ValueRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &ValueRelation::Columns() {
	return columns;
}

string ValueRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Values ";
	for (idx_t row_idx = 0; row_idx < expressions.size(); row_idx++) {
		auto &list = expressions[row_idx];
		str += row_idx > 0 ? ", (" : "(";
		for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			str += col_idx > 0 ? ", " : "";
			str += list[col_idx]->ToString();
		}
		str += ")";
	}
	str += "\n";
	return str;
}

} // namespace duckdb
