//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/transfer_bf_linker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {

//! This class is to link each PhysicalUseBF with its PhysicalCreateBF. It uses the FilterPlan information instead of
//! shared ptr to link.
class TransferBFLinker {
public:
	TransferBFLinker() : state(State::COLLECT_BF_CREATORS) {
	}

	void LinkBFOperators(LogicalOperator &op);

protected:
	void VisitOperator(LogicalOperator &op);

protected:
	enum class State { COLLECT_BF_CREATORS, LINK_BF_USERS, CLEAN_USELESS_OPERATORS };
	State state;

	struct FilterPlanHash {
		size_t operator()(const FilterPlan *fp) const {
			hash_t hash = 0;
			for (const auto &expr : fp->build) {
				hash = CombineHash(hash, expr->Hash());
			}
			for (const auto &expr : fp->apply) {
				hash = CombineHash(hash, expr->Hash());
			}
			return hash;
		}
	};
	struct FilterPlanEquality {
		bool operator()(const FilterPlan *lhs, const FilterPlan *rhs) const {
			// If the pointers are the same, trivially equal
			if (lhs == rhs) {
				return true;
			}
			// Otherwise compare the build/apply vectors item by item
			if (lhs->build.size() != rhs->build.size()) {
				return false;
			}
			for (size_t i = 0; i < lhs->build.size(); i++) {
				auto &lhs_binding = lhs->build[i]->Cast<BoundColumnRefExpression>().binding;
				auto &rhs_binding = rhs->build[i]->Cast<BoundColumnRefExpression>().binding;
				if (lhs_binding.table_index != rhs_binding.table_index ||
				    lhs_binding.column_index != rhs_binding.column_index) {
					return false;
				}
			}
			// Similarly compare apply …
			if (lhs->apply.size() != rhs->apply.size()) {
				return false;
			}
			for (size_t i = 0; i < lhs->apply.size(); i++) {
				auto &lhs_binding = lhs->apply[i]->Cast<BoundColumnRefExpression>().binding;
				auto &rhs_binding = rhs->apply[i]->Cast<BoundColumnRefExpression>().binding;
				if (lhs_binding.table_index != rhs_binding.table_index ||
				    lhs_binding.column_index != rhs_binding.column_index) {
					return false;
				}
			}
			return true;
		}
	};
	unordered_set<LogicalOperator *> useful_creator;
	unordered_map<FilterPlan *, LogicalCreateBF *, FilterPlanHash, FilterPlanEquality> bf_creators;
};
} // namespace duckdb
