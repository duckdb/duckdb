//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class LogicalOperator;

//! Read-only CTE definitions available during one duplicate-eliminated-domain analysis.
class DuplicateEliminatedDomainCTERegistry {
public:
	explicit DuplicateEliminatedDomainCTERegistry(LogicalOperator &root);

	optional_ptr<LogicalOperator> FindDefinition(TableIndex cte_index) const;
	bool IsAlwaysMaterialized(TableIndex cte_index) const;
	bool IsNeverMaterialized(TableIndex cte_index) const;

private:
	struct Entry {
		Entry(LogicalOperator &definition_p, CTEMaterialize materialize_p)
		    : definition(definition_p), materialize(materialize_p) {
		}

		reference<LogicalOperator> definition;
		CTEMaterialize materialize;
	};

	void Collect(LogicalOperator &op);

private:
	unordered_map<TableIndex, Entry> entries;
};

} // namespace duckdb
