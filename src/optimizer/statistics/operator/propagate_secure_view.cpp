#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalSecureView &op,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// remember which statistics we knew about before descending into the view
	column_binding_set_t bindings_before;
	for (auto &entry : statistics_map) {
		bindings_before.insert(entry.first);
	}

	// propagate into the view - the view definition itself is still optimized using its own statistics
	PropagateChildren(op, node_ptr);

	// discard every statistic that was derived from the contents of the view. Statistics describe the data itself
	// (min/max, null counts, distinct counts) - leaking them past a secure view would expose values from rows that
	// the view does not return, both through stats() and by allowing filters on top of the view to be pruned.
	vector<ColumnBinding> leaked_bindings;
	for (auto &entry : statistics_map) {
		if (bindings_before.find(entry.first) == bindings_before.end()) {
			leaked_bindings.push_back(entry.first);
		}
	}
	for (auto &binding : leaked_bindings) {
		statistics_map.erase(binding);
	}

	// no cardinality information escapes the view either
	return nullptr;
}

} // namespace duckdb
