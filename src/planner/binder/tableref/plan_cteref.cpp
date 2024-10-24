#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"

#include "duckdb/planner/operator/logical_cte.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	auto cteref =
	    make_uniq<LogicalCTERef>(ref.bind_index, ref.cte_index, ref.types, ref.bound_columns, ref.materialized_cte);
	auto current = this;
	while (current) {
		auto rec_cte = current->recursive_ctes.find(ref.cte_index);
		if (rec_cte != current->recursive_ctes.end()) {
			if (rec_cte->second->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
			    rec_cte->second->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
				auto &rec_cte_op = rec_cte->second->Cast<LogicalCTE>();

				for (auto &c : rec_cte_op.correlated_columns) {
					cteref->chunk_types.push_back(c.type);
					cteref->bound_columns.push_back(c.name);
				}
				cteref->correlated_columns += rec_cte_op.correlated_columns.size();
				break;
			}
		} else {
			current = current->parent.get();
		}
	}

	return std::move(cteref);
}

} // namespace duckdb
