#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

ValueRelation::ValueRelation(ClientContext &context, vector<vector<Value>> values_p, vector<string> names, string alias_p) :
	Relation(context, RelationType::VALUE_LIST), values(move(values_p)), alias(move(alias_p)) {
	if (values.size() == 0) {
		// no values provided
		return;
	}
	idx_t column_count = values[0].size();
	if (names.size() == 0) {
		for(idx_t i = 0; i < column_count; i++) {
			names.push_back("V" + std::to_string(i));
		}
	} else if (column_count != names.size()) {
		throw Exception("Names must be the same size as the amount of columns provided!");
	}
	// set up the initial types
	vector<SQLType> types;
	for(idx_t col_idx = 0; col_idx < values[0].size(); col_idx++) {
		auto sql_type = SQLTypeFromInternalType(values[0][col_idx].type);
		types.push_back(sql_type);
	}
	// iterate over each of the remaining rows
	for(idx_t i = 1; i < values.size(); i++) {
		auto &list = values[i];
		if (list.size() != column_count) {
			throw Exception("All lists must be the same length!");
		}
		// potentially upgrade the type
		for(idx_t col_idx = 0; col_idx < values[0].size(); col_idx++) {
			auto sql_type = SQLTypeFromInternalType(list[col_idx].type);
			types[col_idx] = MaxSQLType(types[col_idx], sql_type);;
		}
	}
	assert(types.size() == names.size());
	for(idx_t i = 0; i < types.size(); i++) {
		columns.push_back(ColumnDefinition(names[i], types[i]));
	}
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
	for(idx_t i = 0; i < columns.size(); i++) {
		table_ref->expected_names.push_back(columns[i].name);
		table_ref->expected_types.push_back(columns[i].type);
	}
	table_ref->alias = GetAlias();
	// create constant expressions for the values
	for(idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		auto &list = values[row_idx];
		vector<unique_ptr<ParsedExpression>> expressions;
		for(idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			expressions.push_back(make_unique<ConstantExpression>(columns[col_idx].type, list[col_idx]));
		}
		table_ref->values.push_back(move(expressions));
	}
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
	for(idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		auto &list = values[row_idx];
		str += row_idx > 0 ? ", (" : "(";
		for(idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			str += col_idx > 0 ? ", " : "";
			str += list[col_idx].ToString();
		}
		str += ")";
	}
	str += "\n";
	return str;
}

}