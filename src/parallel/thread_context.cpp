#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context)  : profiler(QueryProfiler::Get(context).IsEnabled()) {
  	const auto &tree_map = QueryProfiler::Get(context).GetTreeMap();
//	D_ASSERT(tree_map.empty());
	if (!tree_map.empty()) {
		 const auto &first = tree_map.begin()->second.get();

		if (first.settings.setting_enabled(TreeNodeSettingsType::OPERATOR_TIMING) || first.settings.setting_enabled(TreeNodeSettingsType::CPU_TIME)) {
			profiler.EnableOperatorTiming();
		}
		if (first.settings.setting_enabled(TreeNodeSettingsType::OPERATOR_CARDINALITY)) {
			profiler.EnableOperatorCardinality();
		}
	}
}

} // namespace duckdb
