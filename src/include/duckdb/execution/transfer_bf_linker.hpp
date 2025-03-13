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

	void RemoveUselessOperators(LogicalOperator &op);

protected:
	void VisitOperator(LogicalOperator &op);

protected:
	enum class State { COLLECT_BF_CREATORS, LINK_BF_USERS, CLEAN_USELESS_OPERATORS };
	State state;

	struct FilterPlanHash {
		std::size_t operator()(const BloomFilterPlan &fp) const {
			size_t h = 0;
			for (const auto &v : fp.build) {
				h ^= std::hash<idx_t> {}(v.table_index) ^ (std::hash<idx_t> {}(v.column_index));
			}
			for (const auto &v : fp.apply) {
				h ^= std::hash<idx_t> {}(v.table_index) ^ (std::hash<idx_t> {}(v.column_index));
			}
			return h;
		}
	};
	unordered_set<LogicalOperator *> useful_creator;
	unordered_map<BloomFilterPlan, LogicalCreateBF *, FilterPlanHash> bf_creators;
};
} // namespace duckdb
