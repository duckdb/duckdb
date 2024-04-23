#include "duckdb/main/relation/materialized_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/tableref/column_data_ref.hpp"

namespace duckdb {

MaterializedRelation::MaterializedRelation(const shared_ptr<ClientContext> &context, ColumnDataCollection &collection_p,
                                           vector<string> names, string alias_p)
    : Relation(context, RelationType::MATERIALIZED_RELATION), alias(std::move(alias_p)), collection(collection_p) {
	// create constant expressions for the values
	auto types = collection.Types();
	D_ASSERT(types.size() == names.size());

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < types.size(); i++) {
		auto &type = types[i];
		auto &name = names[i];
		auto column_definition = ColumnDefinition(name, type);
		columns.push_back(std::move(column_definition));
	}
}

unique_ptr<QueryNode> MaterializedRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> ValueRelation::GetTableRef() {
	auto table_ref = make_uniq<ColumnDataRef>();
	// set the expected types/names
	for (idx_t i = 0; i < columns.size(); i++) {
		table_ref->expected_names.push_back(columns[i].Name());
		table_ref->expected_types.push_back(columns[i].Type());
		D_ASSERT(names.size() == 0 || columns[i].Name() == names[i]);
	}
	// copy the expressions
	for (auto &expr_list : expressions) {
		vector<unique_ptr<ParsedExpression>> copied_list;
		copied_list.reserve(expr_list.size());
		for (auto &expr : expr_list) {
			copied_list.push_back(expr->Copy());
		}
		table_ref->values.push_back(std::move(copied_list));
	}
	table_ref->alias = GetAlias();
	return std::move(table_ref);
}

// BoundStatement MaterializedRelation::Bind(Binder &binder) {
//	auto return_types = collection.Types();
//	vector<string> names;

//	for (auto &col : columns) {
//		names.push_back(col.Name());
//	}
//	auto to_scan = make_uniq<ColumnDataCollection>(collection);
//	auto logical_get = make_uniq_base<LogicalOperator, LogicalColumnDataGet>(binder.GenerateTableIndex(), return_types,
//std::move(to_scan));
//	// FIXME: add Binding???

//	BoundStatement result;
//	result.plan = std::move(logical_get);
//	result.types = std::move(return_types);
//	result.names = std::move(names);
//	return result;
//}

string MaterializedRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &MaterializedRelation::Columns() {
	return columns;
}

string MaterializedRelation::ToString(idx_t depth) {
	return collection.ToString();
}

} // namespace duckdb
