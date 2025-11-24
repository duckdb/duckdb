#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "yyjson.hpp"
#include "yyjson_utils.hpp"

#include <algorithm>
#include <utility>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false) {
}

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze || ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return !is_explain_analyze && ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat(ExplainFormat format) const {
	auto print_format = ClientConfig::GetConfig(context).profiler_print_format;
	switch (format) {
	case ExplainFormat::DEFAULT:
		if (print_format != ProfilerPrintFormat::NO_OUTPUT) {
			return print_format;
		}
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case ExplainFormat::TEXT:
		return ProfilerPrintFormat::QUERY_TREE;
	case ExplainFormat::JSON:
		return ProfilerPrintFormat::JSON;
	case ExplainFormat::HTML:
		return ProfilerPrintFormat::HTML;
	case ExplainFormat::GRAPHVIZ:
		return ProfilerPrintFormat::GRAPHVIZ;
	case ExplainFormat::MERMAID:
		return ProfilerPrintFormat::MERMAID;
	default:
		throw NotImplementedException("No mapping from ExplainFormat::%s to ProfilerPrintFormat",
		                              EnumUtil::ToString(format));
	}
}

ExplainFormat QueryProfiler::GetExplainFormat(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return ExplainFormat::TEXT;
	case ProfilerPrintFormat::JSON:
		return ExplainFormat::JSON;
	case ProfilerPrintFormat::HTML:
		return ExplainFormat::HTML;
	case ProfilerPrintFormat::GRAPHVIZ:
		return ExplainFormat::GRAPHVIZ;
	case ProfilerPrintFormat::MERMAID:
		return ExplainFormat::MERMAID;
	case ProfilerPrintFormat::NO_OUTPUT:
		throw InternalException("Should not attempt to get ExplainFormat for ProfilerPrintFormat::NO_OUTPUT");
	default:
		throw NotImplementedException("No mapping from ProfilePrintFormat::%s to ExplainFormat",
		                              EnumUtil::ToString(format));
	}
}

bool QueryProfiler::PrintOptimizerOutput() const {
	return GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled();
}

string QueryProfiler::GetSaveLocation() const {
	return is_explain_analyze ? string() : ClientConfig::GetConfig(context).profiler_save_location;
}

QueryProfiler &QueryProfiler::Get(ClientContext &context) {
	return *ClientData::Get(context).profiler;
}

void QueryProfiler::Start(const string &query) {
	Reset();
	running = true;
	query_metrics.query = query;
	query_metrics.latency.Start();
}

void QueryProfiler::Reset() {
	tree_map.clear();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();
	running = false;
	query_metrics.Reset();
}

void QueryProfiler::StartQuery(const string &query, bool is_explain_analyze_p, bool start_at_optimizer) {
	lock_guard<std::mutex> guard(lock);
	if (is_explain_analyze_p) {
		StartExplainAnalyze();
	}
	if (!IsEnabled()) {
		return;
	}
	if (start_at_optimizer && !PrintOptimizerOutput()) {
		// This is the StartQuery call before the optimizer, but we don't have to print optimizer output
		return;
	}
	if (running) {
		// Called while already running: this should only happen when we print optimizer output
		D_ASSERT(PrintOptimizerOutput());
		return;
	}
	Start(query);
}

bool QueryProfiler::OperatorRequiresProfiling(const PhysicalOperatorType op_type) {
	const auto &config = ClientConfig::GetConfig(context);
	if (config.profiling_coverage == ProfilingCoverage::ALL) {
		return true;
	}

	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::RECURSIVE_KEY_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
	case PhysicalOperatorType::EXTENSION:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::Finalize(ProfilingNode &node) {
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		Finalize(*child);

		auto &info = node.GetProfilingInfo();
		auto type = PhysicalOperatorType(info.GetMetricValue<uint8_t>(MetricsType::OPERATOR_TYPE));
		if (type == PhysicalOperatorType::UNION &&
		    info.Enabled(info.expanded_settings, MetricsType::OPERATOR_CARDINALITY)) {
			auto &child_info = child->GetProfilingInfo();
			auto value = child_info.metrics[MetricsType::OPERATOR_CARDINALITY].GetValue<idx_t>();
			info.MetricSum(MetricsType::OPERATOR_CARDINALITY, value);
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

template <class METRIC_TYPE>
static void AggregateMetric(ProfilingNode &node, MetricsType aggregated_metric, MetricsType child_metric,
                            const std::function<METRIC_TYPE(const METRIC_TYPE &, const METRIC_TYPE &)> &update_fun) {
	auto &info = node.GetProfilingInfo();
	info.metrics[aggregated_metric] = info.metrics[child_metric];

	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		AggregateMetric<METRIC_TYPE>(*child, aggregated_metric, child_metric, update_fun);

		auto &child_info = child->GetProfilingInfo();
		auto value = child_info.GetMetricValue<METRIC_TYPE>(aggregated_metric);
		info.MetricUpdate<METRIC_TYPE>(aggregated_metric, value, update_fun);
	}
}

template <class METRIC_TYPE>
static void GetCumulativeMetric(ProfilingNode &node, MetricsType cumulative_metric, MetricsType child_metric) {
	AggregateMetric<METRIC_TYPE>(
	    node, cumulative_metric, child_metric,
	    [](const METRIC_TYPE &old_value, const METRIC_TYPE &new_value) { return old_value + new_value; });
}

Value GetCumulativeOptimizers(ProfilingNode &node) {
	auto &metrics = node.GetProfilingInfo().metrics;
	double count = 0;
	for (auto &metric : metrics) {
		if (MetricsUtils::IsOptimizerMetric(metric.first)) {
			count += metric.second.GetValue<double>();
		}
	}
	return Value::CreateValue(count);
}

void QueryProfiler::EndQuery() {
	unique_lock<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	query_metrics.latency.End();
	if (root) {
		auto &info = root->GetProfilingInfo();
		if (info.Enabled(info.expanded_settings, MetricsType::OPERATOR_CARDINALITY)) {
			Finalize(*root->GetChild(0));
		}
	}
	running = false;
	bool emit_output = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze) {
		if (root) {
			auto &info = root->GetProfilingInfo();
			info = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			auto &child_info = root->children[0]->GetProfilingInfo();
			info.metrics[MetricsType::QUERY_NAME] = query_metrics.query;

			auto &settings = info.expanded_settings;
			for (const auto &global_info_entry : query_metrics.query_global_info.metrics) {
				info.metrics[global_info_entry.first] = global_info_entry.second;
			}
			if (info.Enabled(settings, MetricsType::LATENCY)) {
				info.metrics[MetricsType::LATENCY] = query_metrics.latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::TOTAL_BYTES_READ)) {
				info.metrics[MetricsType::TOTAL_BYTES_READ] = Value::UBIGINT(query_metrics.total_bytes_read);
			}
			if (info.Enabled(settings, MetricsType::TOTAL_BYTES_WRITTEN)) {
				info.metrics[MetricsType::TOTAL_BYTES_WRITTEN] = Value::UBIGINT(query_metrics.total_bytes_written);
			}
			if (info.Enabled(settings, MetricsType::TOTAL_MEMORY_ALLOCATED)) {
				info.metrics[MetricsType::TOTAL_MEMORY_ALLOCATED] =
				    Value::UBIGINT(query_metrics.total_memory_allocated);
			}
			if (info.Enabled(settings, MetricsType::ROWS_RETURNED)) {
				info.metrics[MetricsType::ROWS_RETURNED] = child_info.metrics[MetricsType::OPERATOR_CARDINALITY];
			}
			if (info.Enabled(settings, MetricsType::CPU_TIME)) {
				GetCumulativeMetric<double>(*root, MetricsType::CPU_TIME, MetricsType::OPERATOR_TIMING);
			}
			if (info.Enabled(settings, MetricsType::CUMULATIVE_CARDINALITY)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_CARDINALITY,
				                           MetricsType::OPERATOR_CARDINALITY);
			}
			if (info.Enabled(settings, MetricsType::CUMULATIVE_ROWS_SCANNED)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_ROWS_SCANNED,
				                           MetricsType::OPERATOR_ROWS_SCANNED);
			}
			if (info.Enabled(settings, MetricsType::RESULT_SET_SIZE)) {
				info.metrics[MetricsType::RESULT_SET_SIZE] = child_info.metrics[MetricsType::RESULT_SET_SIZE];
			}
			if (info.Enabled(settings, MetricsType::WAITING_TO_ATTACH_LATENCY)) {
				info.metrics[MetricsType::WAITING_TO_ATTACH_LATENCY] =
				    query_metrics.waiting_to_attach_latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::ATTACH_LOAD_STORAGE_LATENCY)) {
				info.metrics[MetricsType::ATTACH_LOAD_STORAGE_LATENCY] =
				    query_metrics.attach_load_storage_latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::ATTACH_REPLAY_WAL_LATENCY)) {
				info.metrics[MetricsType::ATTACH_REPLAY_WAL_LATENCY] =
				    query_metrics.attach_replay_wal_latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::COMMIT_LOCAL_STORAGE_LATENCY)) {
				info.metrics[MetricsType::COMMIT_LOCAL_STORAGE_LATENCY] =
				    query_metrics.commit_local_storage_latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::WRITE_TO_WAL_LATENCY)) {
				info.metrics[MetricsType::WRITE_TO_WAL_LATENCY] = query_metrics.write_to_wal_latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::WAL_REPLAY_ENTRY_COUNT)) {
				info.metrics[MetricsType::WAL_REPLAY_ENTRY_COUNT] =
				    Value::UBIGINT(query_metrics.wal_replay_entry_count);
			}
			if (info.Enabled(settings, MetricsType::CHECKPOINT_LATENCY)) {
				info.metrics[MetricsType::CHECKPOINT_LATENCY] = query_metrics.checkpoint_latency.Elapsed();
			}

			MoveOptimizerPhasesToRoot();
			if (info.Enabled(settings, MetricsType::CUMULATIVE_OPTIMIZER_TIMING)) {
				info.metrics.at(MetricsType::CUMULATIVE_OPTIMIZER_TIMING) = GetCumulativeOptimizers(*root);
			}
		}

		if (ClientConfig::GetConfig(context).emit_profiler_output) {
			emit_output = true;
		}
	}

	is_explain_analyze = false;

	guard.unlock();

	// To log is inexpensive, whether to log or not depends on whether logging is active
	ToLog();

	if (emit_output) {
		string tree = ToString();
		auto save_location = GetSaveLocation();

		if (save_location.empty()) {
			Printer::Print(tree);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), tree);
		}
	}
}

void QueryProfiler::AddToCounter(const MetricsType type, const idx_t amount) {
	if (!IsEnabled()) {
		return;
	}

	switch (type) {
	case MetricsType::TOTAL_BYTES_READ:
		query_metrics.total_bytes_read += amount;
		return;
	case MetricsType::TOTAL_BYTES_WRITTEN:
		query_metrics.total_bytes_written += amount;
		return;
	case MetricsType::TOTAL_MEMORY_ALLOCATED:
		query_metrics.total_memory_allocated += amount;
		return;
	case MetricsType::WAL_REPLAY_ENTRY_COUNT:
		query_metrics.wal_replay_entry_count += amount;
		return;
	default:
		return;
	}
}

idx_t QueryProfiler::GetBytesRead() const {
	return query_metrics.total_bytes_read;
}

idx_t QueryProfiler::GetBytesWritten() const {
	return query_metrics.total_bytes_written;
}

void QueryProfiler::StartTimer(const MetricsType type) {
	if (!IsEnabled()) {
		return;
	}

	switch (type) {
	case MetricsType::WAITING_TO_ATTACH_LATENCY:
		query_metrics.waiting_to_attach_latency.Start();
		return;
	case MetricsType::ATTACH_LOAD_STORAGE_LATENCY:
		query_metrics.attach_load_storage_latency.Start();
		return;
	case MetricsType::ATTACH_REPLAY_WAL_LATENCY:
		query_metrics.attach_replay_wal_latency.Start();
		return;
	case MetricsType::CHECKPOINT_LATENCY:
		query_metrics.checkpoint_latency.Start();
		return;
	case MetricsType::COMMIT_LOCAL_STORAGE_LATENCY:
		query_metrics.commit_local_storage_latency.Start();
		return;
	case MetricsType::WRITE_TO_WAL_LATENCY:
		query_metrics.write_to_wal_latency.Start();
		return;
	default:
		return;
	}
}

void QueryProfiler::EndTimer(MetricsType type) {
	if (!IsEnabled()) {
		return;
	}

	switch (type) {
	case MetricsType::WAITING_TO_ATTACH_LATENCY:
		query_metrics.waiting_to_attach_latency.End();
		return;
	case MetricsType::ATTACH_LOAD_STORAGE_LATENCY:
		query_metrics.attach_load_storage_latency.End();
		return;
	case MetricsType::ATTACH_REPLAY_WAL_LATENCY:
		query_metrics.attach_replay_wal_latency.End();
		return;
	case MetricsType::CHECKPOINT_LATENCY:
		query_metrics.checkpoint_latency.End();
		return;
	case MetricsType::COMMIT_LOCAL_STORAGE_LATENCY:
		query_metrics.commit_local_storage_latency.End();
		return;
	case MetricsType::WRITE_TO_WAL_LATENCY:
		query_metrics.write_to_wal_latency.End();
		return;
	default:
		return;
	}
}

string QueryProfiler::ToString(ExplainFormat explain_format) const {
	return ToString(GetPrintFormat(explain_format));
}

string QueryProfiler::ToString(ProfilerPrintFormat format) const {
	if (!IsEnabled()) {
		return RenderDisabledMessage(format);
	}
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::HTML:
	case ProfilerPrintFormat::GRAPHVIZ:
	case ProfilerPrintFormat::MERMAID: {
		lock_guard<std::mutex> guard(lock);
		// checking the tree to ensure the query is really empty
		// the query string is empty when a logical plan is deserialized
		if (query_metrics.query.empty() && !root) {
			return "";
		}
		auto renderer = TreeRenderer::CreateRenderer(GetExplainFormat(format));
		duckdb::stringstream str;
		auto &info = root->GetProfilingInfo();
		if (info.Enabled(info.expanded_settings, MetricsType::OPERATOR_TIMING)) {
			info.metrics[MetricsType::OPERATOR_TIMING] = query_metrics.latency.Elapsed();
		}
		renderer->Render(*root, str);
		return str.str();
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::StartPhase(MetricsType phase_metric) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	// start a new phase
	phase_stack.push_back(phase_metric);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	D_ASSERT(!phase_stack.empty());

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (!phase_stack.empty()) {
		phase_profiler.Start();
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
	auto &context_metrics = ClientConfig::GetConfig(context).profiler_settings;

	// Expand.
	for (const auto metric : context_metrics) {
		settings.insert(metric);
		ProfilingInfo::Expand(settings, metric);
	}

	// Reduce.
	auto root_metrics = ProfilingInfo::RootScopeSettings();
	for (const auto metric : root_metrics) {
		settings.erase(metric);
	}
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}
	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}
	active_operator = phys_op;

	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricsType::EXTRA_INFO)) {
			if (!OperatorInfoIsInitialized(*active_operator)) {
				// first time calling into this operator - fetch the info
				auto &info = GetOperatorInfo(*active_operator);
				auto params = active_operator->ParamsToString();
				info.extra_info = params;
			}
		}

		// Start the timing of the current operator.
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_TIMING)) {
			op.Start();
		}
	}
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while no operator is active");
	}

	if (!settings.empty()) {
		auto &info = GetOperatorInfo(*active_operator);
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_TIMING)) {
			op.End();
			info.AddTime(op.Elapsed());
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_CARDINALITY) && chunk) {
			info.AddReturnedElements(chunk->size());
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::RESULT_SET_SIZE) && chunk) {
			auto result_set_size = chunk->GetAllocationSize();
			info.AddResultSetSize(result_set_size);
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			auto used_memory = BufferManager::GetBufferManager(context).GetBufferPool().GetUsedMemory(false);
			info.UpdateSystemPeakBufferManagerMemory(used_memory);
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			auto used_swap = BufferManager::GetBufferManager(context).GetUsedSwap();
			info.UpdateSystemPeakTempDirectorySize(used_swap);
		}
	}
	active_operator = nullptr;
}

void OperatorProfiler::FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call FinishSource while no operator is active");
	}
	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricsType::EXTRA_INFO)) {
			// we're emitting extra info - get the extra source info
			auto &info = GetOperatorInfo(*active_operator);
			auto extra_info = active_operator->ExtraSourceParams(gstate, lstate);
			for (auto &new_info : extra_info) {
				auto entry = info.extra_info.find(new_info.first);
				if (entry != info.extra_info.end()) {
					// entry exists - override
					entry->second = std::move(new_info.second);
				} else {
					// entry does not exist yet - insert
					info.extra_info.insert(std::move(new_info));
				}
			}
		}
	}
}

bool OperatorProfiler::OperatorInfoIsInitialized(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	return entry != operator_infos.end();
}

OperatorInformation &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry != operator_infos.end()) {
		return entry->second;
	}

	// Add a new entry.
	operator_infos[phys_op] = OperatorInformation();
	return operator_infos[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry == operator_infos.end()) {
		return;
	}

	auto &info = operator_infos.find(phys_op)->second;
	info.name = phys_op.GetName();
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.operator_infos) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetProfilingInfo();

		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_TIMING)) {
			info.MetricSum<double>(MetricsType::OPERATOR_TIMING, node.second.time);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_CARDINALITY)) {
			info.MetricSum<idx_t>(MetricsType::OPERATOR_CARDINALITY, node.second.elements_returned);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_ROWS_SCANNED)) {
			if (op.type == PhysicalOperatorType::TABLE_SCAN) {
				auto &scan_op = op.Cast<PhysicalTableScan>();
				auto &bind_data = scan_op.bind_data;

				if (bind_data && scan_op.function.cardinality) {
					auto cardinality = scan_op.function.cardinality(context, &(*bind_data));
					if (cardinality && cardinality->has_estimated_cardinality) {
						info.MetricSum<idx_t>(MetricsType::OPERATOR_ROWS_SCANNED, cardinality->estimated_cardinality);
					}
				}
			}
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::RESULT_SET_SIZE)) {
			info.MetricSum<idx_t>(MetricsType::RESULT_SET_SIZE, node.second.result_set_size);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::EXTRA_INFO)) {
			info.metrics[MetricsType::EXTRA_INFO] = Value::MAP(node.second.extra_info);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			query_metrics.query_global_info.MetricMax(MetricsType::SYSTEM_PEAK_BUFFER_MEMORY,
			                                          node.second.system_peak_buffer_manager_memory);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			query_metrics.query_global_info.MetricMax(MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE,
			                                          node.second.system_peak_temp_directory_size);
		}
	}
	profiler.operator_infos.clear();
}

void QueryProfiler::SetBlockedTime(const double &blocked_thread_time) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	auto &info = root->GetProfilingInfo();
	if (info.Enabled(info.expanded_settings, MetricsType::BLOCKED_THREAD_TIME)) {
		query_metrics.query_global_info.metrics[MetricsType::BLOCKED_THREAD_TIME] = blocked_thread_time;
	}
}

string QueryProfiler::DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		auto half_spaces = width / 2;
		auto extra_left_space = NumericCast<idx_t>(width % 2 != 0 ? 1 : 0);
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = NumericCast<char>(toupper(str[0]));
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = NumericCast<char>(toupper(str[i + 1]));
			}
		}
	}
	return str;
}

static string RenderTiming(double timing) {
	string timing_s;
	if (timing >= 1) {
		timing_s = StringUtil::Format("%.2f", timing);
	} else if (timing >= 0.1) {
		timing_s = StringUtil::Format("%.3f", timing);
	} else {
		timing_s = StringUtil::Format("%.4f", timing);
	}
	return timing_s + "s";
}

string QueryProfiler::QueryTreeToString() const {
	duckdb::stringstream str;
	QueryTreeToStream(str);
	return str.str();
}

void RenderPhaseTimings(std::ostream &ss, const pair<string, double> &head, map<string, double> &timings, idx_t width) {
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│" + QueryProfiler::DrawPadded(RenderTitleCase(head.first) + ": " + RenderTiming(head.second), width - 2) +
	          "│\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";

	for (const auto &entry : timings) {
		ss << "││" +
		          QueryProfiler::DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
		                                    width - 4) +
		          "││\n";
	}
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
}

void PrintPhaseTimingsToStream(std::ostream &ss, const ProfilingInfo &info, idx_t width) {
	map<string, double> optimizer_timings;
	map<string, double> planner_timings;
	map<string, double> physical_planner_timings;

	pair<string, double> optimizer_head;
	pair<string, double> planner_head;
	pair<string, double> physical_planner_head;

	for (const auto &entry : info.metrics) {
		if (MetricsUtils::IsOptimizerMetric(entry.first)) {
			optimizer_timings[EnumUtil::ToString(entry.first).substr(10)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::IsPhaseTimingMetric(entry.first)) {
			switch (entry.first) {
			case MetricsType::CUMULATIVE_OPTIMIZER_TIMING:
				continue;
			case MetricsType::ALL_OPTIMIZERS:
				optimizer_head = {"Optimizer", entry.second.GetValue<double>()};
				break;
			case MetricsType::PHYSICAL_PLANNER:
				physical_planner_head = {"Physical Planner", entry.second.GetValue<double>()};
				break;
			case MetricsType::PLANNER:
				planner_head = {"Planner", entry.second.GetValue<double>()};
				break;
			default:
				break;
			}

			auto metric = EnumUtil::ToString(entry.first);
			if (StringUtil::StartsWith(metric, "PHYSICAL_PLANNER") && entry.first != MetricsType::PHYSICAL_PLANNER) {
				physical_planner_timings[metric.substr(17)] = entry.second.GetValue<double>();
			} else if (StringUtil::StartsWith(metric, "PLANNER") && entry.first != MetricsType::PLANNER) {
				planner_timings[metric.substr(8)] = entry.second.GetValue<double>();
			}
		}
	}

	RenderPhaseTimings(ss, optimizer_head, optimizer_timings, width);
	RenderPhaseTimings(ss, physical_planner_head, physical_planner_timings, width);
	RenderPhaseTimings(ss, planner_head, planner_timings, width);
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	lock_guard<std::mutex> guard(lock);
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << StringUtil::Replace(query_metrics.query, "\n", " ") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_metrics.query.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(query_metrics.latency.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// render the main operator tree
	if (root) {
		// print phase timings
		if (PrintOptimizerOutput()) {
			PrintPhaseTimingsToStream(ss, root->GetProfilingInfo(), TOTAL_BOX_WIDTH);
		}
		Render(*root, ss);
	}
}

Value QueryProfiler::JSONSanitize(const Value &input) {
	D_ASSERT(input.type().id() == LogicalTypeId::MAP);

	InsertionOrderPreservingMap<string> result;
	auto children = MapValue::GetChildren(input);
	for (auto &child : children) {
		auto struct_children = StructValue::GetChildren(child);
		auto key = struct_children[0].GetValue<string>();
		auto value = struct_children[1].GetValue<string>();

		if (StringUtil::StartsWith(key, "__")) {
			key = StringUtil::Replace(key, "__", "");
			key = StringUtil::Replace(key, "_", " ");
			key = StringUtil::Title(key);
		}
		result[key] = value;
	}
	return Value::MAP(result);
}

string QueryProfiler::JSONSanitize(const std::string &text) {
	string result;
	result.reserve(text.size());
	for (char i : text) {
		switch (i) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += i;
			break;
		}
	}
	return result;
}

static yyjson_mut_val *ToJSONRecursive(yyjson_mut_doc *doc, ProfilingNode &node) {
	auto result_obj = yyjson_mut_obj(doc);
	auto &profiling_info = node.GetProfilingInfo();

	if (profiling_info.Enabled(profiling_info.settings, MetricsType::EXTRA_INFO)) {
		profiling_info.metrics[MetricsType::EXTRA_INFO] =
		    QueryProfiler::JSONSanitize(profiling_info.metrics.at(MetricsType::EXTRA_INFO));
	}

	profiling_info.WriteMetricsToJSON(doc, result_obj);

	auto children_list = yyjson_mut_arr(doc);
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = ToJSONRecursive(doc, *node.GetChild(i));
		yyjson_mut_arr_add_val(children_list, child);
	}
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	return result_obj;
}

static string StringifyAndFree(ConvertedJSONHolder &json_holder, yyjson_mut_val *object) {
	json_holder.stringified_json = yyjson_mut_val_write_opts(
	    object, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr, nullptr, nullptr);
	if (!json_holder.stringified_json) {
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	auto result = string(json_holder.stringified_json);
	return result;
}

void QueryProfiler::ToLog() const {
	lock_guard<std::mutex> guard(lock);

	if (!root) {
		// No root, not much to do
		return;
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToLog(context);
}

string QueryProfiler::ToJSON() const {
	lock_guard<std::mutex> guard(lock);
	ConvertedJSONHolder json_holder;

	json_holder.doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(json_holder.doc);
	yyjson_mut_doc_set_root(json_holder.doc, result_obj);

	if (query_metrics.query.empty() && !root) {
		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "empty");
		return StringifyAndFree(json_holder, result_obj);
	}
	if (!root) {
		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "error");
		return StringifyAndFree(json_holder, result_obj);
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToJSON(json_holder.doc, result_obj);

	// recursively print the physical operator tree
	auto children_list = yyjson_mut_arr(json_holder.doc);
	yyjson_mut_obj_add_val(json_holder.doc, result_obj, "children", children_list);
	auto child = ToJSONRecursive(json_holder.doc, *root->GetChild(0));
	yyjson_mut_arr_add_val(children_list, child);
	return StringifyAndFree(json_holder, result_obj);
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	ofstream out(path);
	out << info;
	out.close();
	// throw an IO exception if it fails to write the file
	if (out.fail()) {
		throw IOException(strerror(errno));
	}
}

profiler_settings_t EraseQueryRootSettings(profiler_settings_t settings) {
	profiler_settings_t phase_timing_settings_to_erase;

	for (auto &setting : settings) {
		if (MetricsUtils::IsOptimizerMetric(setting) || MetricsUtils::IsPhaseTimingMetric(setting) ||
		    MetricsUtils::IsQueryGlobalMetric(setting)) {
			phase_timing_settings_to_erase.insert(setting);
		}
	}

	for (auto &setting : phase_timing_settings_to_erase) {
		settings.erase(setting);
	}

	return settings;
}

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, const profiler_settings_t &settings,
                                                    const idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	unique_ptr<ProfilingNode> node = make_uniq<ProfilingNode>();
	auto &info = node->GetProfilingInfo();
	info = ProfilingInfo(settings, depth);
	auto child_settings = settings;
	if (depth == 0) {
		child_settings = EraseQueryRootSettings(child_settings);
	}
	node->depth = depth;

	if (depth != 0) {
		info.metrics[MetricsType::OPERATOR_NAME] = root_p.GetName();
		info.MetricSum<uint8_t>(MetricsType::OPERATOR_TYPE, static_cast<uint8_t>(root_p.type));
	}
	if (info.Enabled(info.settings, MetricsType::EXTRA_INFO)) {
		info.metrics[MetricsType::EXTRA_INFO] = Value::MAP(root_p.ParamsToString());
	}

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), child_settings, depth + 1);
		node->AddChild(std::move(child_node));
	}
	return node;
}

string QueryProfiler::RenderDisabledMessage(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
	case ProfilerPrintFormat::HTML:
		return R"(
				<!DOCTYPE html>
                <html lang="en"><head/><body>
                  Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!
                </body></html>
			)";
	case ProfilerPrintFormat::GRAPHVIZ:
		return R"(
				digraph G {
				    node [shape=box, style=rounded, fontname="Courier New", fontsize=10];
				    node_0_0 [label="Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!"];
				}
			)";
	case ProfilerPrintFormat::MERMAID:
		return R"(flowchart TD
    node_0_0["`**DISABLED**
Query profiling is disabled.
Use 'PRAGMA enable_profiling;' to enable profiling!`"]
)";
	case ProfilerPrintFormat::JSON: {
		ConvertedJSONHolder json_holder;
		json_holder.doc = yyjson_mut_doc_new(nullptr);
		auto result_obj = yyjson_mut_obj(json_holder.doc);
		yyjson_mut_doc_set_root(json_holder.doc, result_obj);

		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "disabled");
		return StringifyAndFree(json_holder, result_obj);
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	query_requires_profiling = false;
	ClientConfig &config = ClientConfig::GetConfig(context);
	root = CreateTree(root_op, config.profiler_settings, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

void QueryProfiler::Render(const ProfilingNode &node, std::ostream &ss) const {
	TextTreeRenderer renderer;
	if (IsDetailedEnabled()) {
		renderer.EnableDetailed();
	} else {
		renderer.EnableStandard();
	}
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(QueryTreeToString());
}

void QueryProfiler::MoveOptimizerPhasesToRoot() {
	auto &root_info = root->GetProfilingInfo();
	auto &root_metrics = root_info.metrics;

	for (auto &entry : phase_timings) {
		auto &phase = entry.first;
		auto &timing = entry.second;
		if (root_info.Enabled(root_info.expanded_settings, phase)) {
			root_metrics[phase] = Value::CreateValue(timing);
		}
	}
}

} // namespace duckdb
