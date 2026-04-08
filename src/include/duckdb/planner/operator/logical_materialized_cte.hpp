//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_materialized_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

class LogicalMaterializedCTE : public LogicalCTE {
	explicit LogicalMaterializedCTE() : LogicalCTE(LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_MATERIALIZED_CTE;

public:
	LogicalMaterializedCTE(string ctename_p, TableIndex table_index, idx_t column_count,
	                       unique_ptr<LogicalOperator> cte, unique_ptr<LogicalOperator> child,
	                       CTEMaterialize materialize)
	    : LogicalCTE(std::move(ctename_p), table_index, column_count, std::move(cte), std::move(child),
	                 LogicalOperatorType::LOGICAL_MATERIALIZED_CTE),
	      materialize(materialize) {
	}

	CTEMaterialize materialize = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override {
		return children[1]->GetColumnBindings();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<TableIndex> GetTableIndex() const override;

protected:
	void ResolveTypes() override {
		types = children[1]->types;
	}
};
} // namespace duckdb
