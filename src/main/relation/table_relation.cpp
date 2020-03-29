#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

TableRelation::TableRelation(ClientContext &context, unique_ptr<TableDescription> description) :
	Relation(context, RelationType::TABLE), description(move(description)) {

}

const vector<ColumnDefinition> &TableRelation::Columns() {
	return description->columns;
}

string TableRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Scan Table [" + description->table + "]";
}

}