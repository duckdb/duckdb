#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {


vector<SingleJoinRelation> RelationManager::GetRelations() {
	return relations;
}


void RelationManager::AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats) {

	// if parent is null, then this is a root relation
	// if parent is not null, it should have multiple children
	D_ASSERT(!parent || parent->children.size() >= 2);
	auto relation = make_uniq<SingleJoinRelation>(op, parent);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	if (table_indexes.empty()) {
		// relation represents a non-reorderable relation, most likely a join relation
		// Get the tables referenced in the non-reorderable relation and add them to the relation mapping
		// This should all table references, even if there are nested non-reorderable joins.
		unordered_set<idx_t> table_references;
		LogicalJoin::GetTableReferences(op, table_references);
		D_ASSERT(table_references.size() > 0);
		for (auto &reference : table_references) {
			D_ASSERT(relation_mapping.find(reference) == relation_mapping.end());
			relation_mapping[reference] = relation_id;
		}
	} else {
		// Relations should never return more than 1 table index
		D_ASSERT(table_indexes.size() == 1);
		idx_t table_index = table_indexes.at(0);
		D_ASSERT(relation_mapping.find(table_index) == relation_mapping.end());
		relation_mapping[table_index] = relation_id;
	}
	// Add binding information from the nonreorderable join to this relation.
//	auto relation_name = GetRelationName(op);
}

} // namespace duckdb
