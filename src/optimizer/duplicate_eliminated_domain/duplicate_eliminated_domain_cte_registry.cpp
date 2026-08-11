//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp"

#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

DuplicateEliminatedDomainCTERegistry::DuplicateEliminatedDomainCTERegistry(LogicalOperator &root) {
	Collect(root);
}

void DuplicateEliminatedDomainCTERegistry::Collect(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op.children.size() == 2) {
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		entries.emplace(cte.table_index, Entry(*op.children[0], cte.materialize));
	}
	for (auto &child : op.children) {
		Collect(*child);
	}
}

optional_ptr<LogicalOperator> DuplicateEliminatedDomainCTERegistry::FindDefinition(TableIndex cte_index) const {
	auto entry = entries.find(cte_index);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.definition.get();
}

bool DuplicateEliminatedDomainCTERegistry::IsAlwaysMaterialized(TableIndex cte_index) const {
	auto entry = entries.find(cte_index);
	return entry != entries.end() && entry->second.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
}

bool DuplicateEliminatedDomainCTERegistry::IsNeverMaterialized(TableIndex cte_index) const {
	auto entry = entries.find(cte_index);
	return entry != entries.end() && entry->second.materialize == CTEMaterialize::CTE_MATERIALIZE_NEVER;
}

} // namespace duckdb
