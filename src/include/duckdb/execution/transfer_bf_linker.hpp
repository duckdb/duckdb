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

	static idx_t FindPlanIndex(const shared_ptr<FilterPlan> &plan, const vector<shared_ptr<FilterPlan>> &filter_plans);

	static void UpdateMinMaxBinding(LogicalOperator &op, vector<ColumnBinding> &updated_bindings,
	                                shared_ptr<DynamicTableFilterSet> &filter_set);

protected:
	enum class State { COLLECT_BF_CREATORS, LINK_BF_USERS, CLEAN_USELESS_OPERATORS, UPDATE_MIN_MAX_BINDING };
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
			if (lhs == rhs) {
				return true;
			}
			if (!lhs || !rhs) {
				return false;
			}
			return *lhs == *rhs;
		}
	};
	unordered_set<LogicalOperator *> useful_creator;
	unordered_map<FilterPlan *, LogicalCreateBF *, FilterPlanHash, FilterPlanEquality> bf_creators;
};
} // namespace duckdb
