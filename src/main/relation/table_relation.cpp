#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

TableRelation::TableRelation(ClientContext &context, unique_ptr<TableDescription> description) :
	Relation(context, RelationType::TABLE), description(move(description)) {

}

BoundStatement TableRelation::Bind(Binder &binder) {
	BoundStatement result;

	BaseTableRef ref;
	ref.schema_name = description->schema;
	ref.table_name = description->table;

	auto bound_ref = binder.Bind((TableRef&) ref);
	auto &bound_tableref = (BoundBaseTableRef&) *bound_ref;
	auto &table = ((LogicalGet&) *bound_tableref.get).table;
	for(idx_t i = 0; i < table->columns.size(); i++) {
		result.names.push_back(table->columns[i].name);
		result.types.push_back(table->columns[i].type);
	}
	result.plan = binder.CreatePlan(*bound_ref);
	return result;
}

unique_ptr<QueryResult> TableRelation::Execute() {
	// if we execute a table relation directly, we push a SELECT * over the expression to force all the columns to be emitted
	auto proj = this->Project("*");
	return proj->Execute();
}

const vector<ColumnDefinition> &TableRelation::Columns() {
	return description->columns;
}

string TableRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Scan Table [" + description->table + "]";
}

}