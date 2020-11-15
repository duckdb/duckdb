#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

void StatisticsPropagator::PropagateStatistics(LogicalSetOperation &setop, unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate statistics in the child nodes
	PropagateStatistics(setop.children[0]);
	PropagateStatistics(setop.children[1]);

	// now fetch the column bindings on both sides
	auto left_bindings = setop.children[0]->GetColumnBindings();
	auto right_bindings = setop.children[1]->GetColumnBindings();

	D_ASSERT(left_bindings.size() == right_bindings.size());
	D_ASSERT(left_bindings.size() == setop.column_count);
	for(idx_t i = 0; i < setop.column_count; i++) {
		// for each column binding, we fetch the statistics from both the lhs and the rhs
		auto left_entry  = statistics_map.find(left_bindings[i]);
		auto right_entry = statistics_map.find(right_bindings[i]);
		if (left_entry == statistics_map.end() || right_entry == statistics_map.end()) {
			// no statistics on one of the sides: can't propagate stats
			continue;
		}
		unique_ptr<BaseStatistics> new_stats;
		switch(setop.type) {
		case LogicalOperatorType::LOGICAL_UNION:
			// union: merge the stats of the LHS and RHS together
			new_stats = left_entry->second->Copy();
			new_stats->Merge(*right_entry->second);
			break;
		case LogicalOperatorType::LOGICAL_EXCEPT:
			// except: use the stats of the LHS
			new_stats = left_entry->second->Copy();
			break;
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: intersect the two stats
			// FIXME: for now we just use the stats of the LHS, as this is correct
			// however, the stats can be further refined to the minimal subset of the LHS and RHS
			new_stats = left_entry->second->Copy();
			break;
		default:
			throw InternalException("Unsupported setop type");
		}
		ColumnBinding binding(setop.table_index, i);
		statistics_map[binding] = move(new_stats);
	}
}

}
