#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/gathered_metrics.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "yyjson.hpp"
#include "yyjson_utils.hpp"

#include <utility>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

void QueryProfileResult::AddValue(const string &k, Value val) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::VALUE;
	child->key = k;
	child->value = std::move(val);
	children.push_back(std::move(child));
}

QueryProfileResult &QueryProfileResult::AddObject(const string &k) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::OBJECT;
	child->key = k;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfileResult &QueryProfileResult::AddList(const string &k) {
	D_ASSERT(kind == QueryProfileResultKind::OBJECT);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::LIST;
	child->key = k;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfileResult &QueryProfileResult::AppendObject() {
	D_ASSERT(kind == QueryProfileResultKind::LIST);
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::OBJECT;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfileResult &QueryProfileResult::AppendList() {
	auto child = make_uniq<QueryProfileResult>();
	child->kind = QueryProfileResultKind::LIST;
	auto &ref = *child;
	children.push_back(std::move(child));
	return ref;
}

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false),
      metrics_finalized(false) {
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
	if (GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled()) {
		return true;
	}
	if (metrics) {
		return metrics->MetricIsEnabled("optimizer.join_order");
	}
	// Fall back to checking tracked_metrics patterns directly
	auto &config = ClientConfig::GetConfig(context);
	for (const auto &pattern : config.tracked_metrics) {
		if (pattern == "*" || StringUtil::StartsWith(pattern, "optimizer")) {
			return true;
		}
	}
	return false;
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
	query_metrics.query_sql = query;
	query_metrics.latency_timer = make_uniq<ActiveTimer>(StartTimer("query.time"));
}

void QueryProfiler::Reset() {
	tree_map.clear();
	root = nullptr;
	metrics.reset();
	running = false;
	query_metrics.Reset();
	result_tree.reset();
	metrics_finalized = false;
}

void QueryProfiler::StartQuery(const string &query, bool is_explain_analyze_p, bool start_at_optimizer) {
	lock_guard<std::mutex> guard(lock);
	// Always reset byte counters at the start of each query so the progress bar shows per-query values
	query_metrics.bytes_read = 0;
	query_metrics.bytes_written = 0;
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
		// D_ASSERT(PrintOptimizerOutput());
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

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

void QueryProfiler::EndQuery() {
	unique_lock<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	FinalizeMetricsInternal();
	running = false;
	bool emit_output = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze && ClientConfig::GetConfig(context).emit_profiler_output) {
		emit_output = true;
	}

	is_explain_analyze = false;

	// To log is inexpensive, whether to log or not depends on whether logging is active
	ToLogInternal();

	guard.unlock();

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

void QueryProfiler::FinalizeMetrics() {
	lock_guard<std::mutex> guard(lock);
	FinalizeMetricsInternal();
}

void QueryProfiler::TrackBytesRead(const idx_t amount) {
	query_metrics.UpdateBytesRead(amount);
}

void QueryProfiler::TrackBytesWritten(const idx_t amount) {
	query_metrics.UpdateBytesWritten(amount);
}

void QueryProfiler::AddToStringCounter(const string &key, const idx_t amount) {
	if (IsEnabled()) {
		query_metrics.UpdateStringCounter(key, amount);
	}
}

idx_t QueryProfiler::GetBytesRead() const {
	return query_metrics.GetBytesRead();
}

idx_t QueryProfiler::GetBytesWritten() const {
	return query_metrics.GetBytesWritten();
}

ActiveTimer QueryProfiler::StartTimer(const string &key) {
	return ActiveTimer(query_metrics, key, IsEnabled());
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
		if (query_metrics.query_sql.empty() || !root) {
			return "";
		}
		auto renderer = TreeRenderer::CreateRenderer(GetExplainFormat(format));
		stringstream str;
		renderer->Render(*root, str);
		return str.str();
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}
	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}
	active_operator = phys_op;

	if (!OperatorMetricsIsInitialized(*active_operator)) {
		// first time calling into this operator - fetch the info
		auto &info = GetOperatorMetrics(*active_operator);
		auto params = active_operator->ParamsToString();
		info.extra_info = params;
		info.extra_info_dirty = true;
	}

	// Start the timing of the current operator.
	op.Start();
}

void OperatorMetrics::GatherMetrics(ClientContext &context, double elapsed_time, optional_ptr<DataChunk> chunk) {
	time += elapsed_time;
	if (chunk) {
		elements_returned += chunk->size();
		result_set_size += LossyNumericCast<idx_t>(chunk->GetDataSize());
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto used_memory = buffer_manager.GetBufferPool().GetUsedMemory(false);
	if (used_memory > system_peak_buffer_manager_memory) {
		system_peak_buffer_manager_memory = used_memory;
	}
	auto used_swap = buffer_manager.GetUsedSwap();
	if (used_swap > system_peak_temp_directory_size) {
		system_peak_temp_directory_size = used_swap;
	}
}

void OperatorMetrics::Merge(const OperatorMetrics &other) {
	time += other.time;
	elements_returned += other.elements_returned;
	result_set_size += other.result_set_size;
	rows_scanned += other.rows_scanned;
	if (other.extra_info_dirty) {
		for (auto &entry : other.extra_info) {
			auto it = extra_info.find(entry.first);
			if (it != extra_info.end()) {
				it->second = entry.second;
			} else {
				extra_info.insert(entry.first, entry.second);
			}
		}
		extra_info_dirty = true;
	}
	if (other.system_peak_buffer_manager_memory > system_peak_buffer_manager_memory) {
		system_peak_buffer_manager_memory = other.system_peak_buffer_manager_memory;
	}
	if (other.system_peak_temp_directory_size > system_peak_temp_directory_size) {
		system_peak_temp_directory_size = other.system_peak_temp_directory_size;
	}
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while no operator is active");
	}

	auto &info = GetOperatorMetrics(*active_operator);
	op.End();
	info.GatherMetrics(context, op.Elapsed(), chunk);
	active_operator = nullptr;
}

void OperatorProfiler::FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call FinishSource while no operator is active");
	}

	// we're emitting extra info - get the extra source info
	auto &info = GetOperatorMetrics(*active_operator);
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
	info.extra_info_dirty = info.extra_info_dirty || !extra_info.empty();

	if (active_operator.get()->type == PhysicalOperatorType::TABLE_SCAN) {
		const auto &table_scan = active_operator->Cast<PhysicalTableScan>();
		const auto rows_scanned = table_scan.GetRowsScanned(gstate, lstate);
		auto &info = GetOperatorMetrics(*active_operator);
		if (rows_scanned.IsValid()) {
			// Use exact value if available.
			info.rows_scanned += rows_scanned.GetIndex();
		} else {
			// Otherwise estimate as the cardinality of the table scan, if there is no exact value available.
			auto &bind_data = table_scan.bind_data;
			if (bind_data && table_scan.function.cardinality) {
				auto cardinality = table_scan.function.cardinality(context, &(*bind_data));
				if (cardinality && cardinality->has_estimated_cardinality) {
					info.rows_scanned += cardinality->estimated_cardinality;
				}
			}
		}
	}
}

bool OperatorProfiler::OperatorMetricsIsInitialized(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	return entry != operator_metrics.end();
}

OperatorMetrics &OperatorProfiler::GetOperatorMetrics(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	if (entry != operator_metrics.end()) {
		return entry->second;
	}

	// Add a new entry.
	operator_metrics[phys_op] = OperatorMetrics();
	return operator_metrics[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = operator_metrics.find(phys_op);
	if (entry == operator_metrics.end()) {
		return;
	}

	auto &info = entry->second;
	if (info.name.empty()) {
		info.name = EnumUtil::ToString(phys_op.type);
	}
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.operator_metrics) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetOperatorMetrics();
		info.Merge(node.second);

		if (node.second.system_peak_buffer_manager_memory > query_metrics.system_peak_buffer_memory) {
			query_metrics.system_peak_buffer_memory = node.second.system_peak_buffer_manager_memory;
		}
		if (node.second.system_peak_temp_directory_size > query_metrics.system_peak_temp_dir_size) {
			query_metrics.system_peak_temp_dir_size = node.second.system_peak_temp_directory_size;
		}
		node.second.ResetMetrics();
	}
}

void QueryProfiler::SetBlockedTime(const double &blocked_thread_time) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	query_metrics.blocked_thread_time = blocked_thread_time;
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

void PrintPhaseTimingsToStream(std::ostream &ss, const GatheredMetrics &info, idx_t width) {
	map<string, double> optimizer_timings;
	map<string, double> planner_timings;
	map<string, double> parser_timings;
	map<string, double> physical_planner_timings;

	pair<string, double> optimizer_head;
	pair<string, double> planner_head;
	pair<string, double> parser_head;
	pair<string, double> physical_planner_head;

	for (const auto &entry : info.GetMetrics()) {
		const auto &metric = entry.first;
		if (MetricsUtils::IsOptimizerMetricKey(metric)) {
			// "optimizer.expression_rewriter" -> display as "expression_rewriter"
			optimizer_timings[metric.substr(10)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::IsStorageMetricKey(metric)) {
			// storage metrics are not phase timings; skip them here
			continue;
		} else if (MetricsUtils::IsPhysicalPlannerMetricKey(metric)) {
			// "physical_planner.total_time" -> head; others -> sub-timings
			if (metric == "physical_planner.total_time") {
				physical_planner_head = {"Physical Planner", entry.second.GetValue<double>()};
			} else {
				// "physical_planner.column_binding" -> display as "column_binding"
				physical_planner_timings[metric.substr(17)] = entry.second.GetValue<double>();
			}
		} else if (MetricsUtils::IsPhaseTimingKey(metric)) {
			if (metric == "optimizers.total_time") {
				optimizer_head = {"Optimizer", entry.second.GetValue<double>()};
			} else if (metric == "planner.total_time") {
				planner_head = {"Planner", entry.second.GetValue<double>()};
			} else if (metric == "parsers.total_time") {
				parser_head = {"Parser", entry.second.GetValue<double>()};
			} else if (metric == "planner.binding_time") {
				planner_timings["binding_time"] = entry.second.GetValue<double>();
			}
		}
	}

	RenderPhaseTimings(ss, optimizer_head, optimizer_timings, width);
	RenderPhaseTimings(ss, physical_planner_head, physical_planner_timings, width);
	RenderPhaseTimings(ss, planner_head, planner_timings, width);
	RenderPhaseTimings(ss, parser_head, parser_timings, width);
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	lock_guard<std::mutex> guard(lock);

	bool show_query_name = false;
	if (root) {
		auto &info = *metrics;
		show_query_name = info.MetricIsEnabled<MetricQuerySQL>();
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << (show_query_name ? StringUtil::Replace(query_metrics.query_sql, "\n", " ") : "") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_metrics.query_sql.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(query_metrics.GetStringMetricInSeconds("query.time"));
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// render the main operator tree
	if (root) {
		// print phase timings
		if (PrintOptimizerOutput()) {
			PrintPhaseTimingsToStream(ss, *metrics, TOTAL_BOX_WIDTH);
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

profiler_metrics_t OperatorMetrics::GetMetrics(const GatheredMetrics &info) const {
	profiler_metrics_t result;
	if (info.MetricIsEnabled<MetricOperatorName>()) {
		result["name"] = Value(name);
	}
	if (info.MetricIsEnabled<MetricOperatorType>()) {
		result["type"] = Value(EnumUtil::ToString(operator_type));
	}
	if (info.MetricIsEnabled<MetricOperatorTiming>()) {
		result["timing"] = Value::DOUBLE(time);
	}
	if (info.MetricIsEnabled<MetricOperatorIntermediateRows>()) {
		result["intermediate_rows"] = Value::UBIGINT(elements_returned);
	}
	if (info.MetricIsEnabled<MetricOperatorIntermediateSizeBytes>()) {
		result["intermediate_size_bytes"] = Value::UBIGINT(result_set_size);
	}
	if (info.MetricIsEnabled<MetricOperatorRowsScanned>() && operator_type == PhysicalOperatorType::TABLE_SCAN) {
		result["rows_scanned"] = Value::UBIGINT(rows_scanned);
	}
	if (info.MetricIsEnabled<MetricOperatorExtraInfo>()) {
		if (!extra_info.empty()) {
			result["extra_info"] = QueryProfiler::JSONSanitize(Value::MAP(extra_info));
		} else {
			result["extra_info"] = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {}, {});
		}
	}
	return result;
}

static yyjson_mut_val *ValueToJSON(yyjson_mut_doc *doc, const Value &val) {
	if (val.IsNull()) {
		return yyjson_mut_null(doc);
	}
	auto &type = val.type();
	if (type.id() == LogicalTypeId::MAP) {
		// MAP values (e.g. extra_info) become JSON objects; multiline string values become arrays
		auto obj = yyjson_mut_obj(doc);
		for (auto &child : MapValue::GetChildren(val)) {
			auto kv = StructValue::GetChildren(child);
			auto k = kv[0].GetValue<string>();
			auto v = kv[1].GetValue<string>();
			auto key_ptr = yyjson_mut_get_str(yyjson_mut_strcpy(doc, k.c_str()));
			auto splits = StringUtil::Split(v, "\n");
			if (splits.size() > 1) {
				auto arr = yyjson_mut_arr(doc);
				for (auto &s : splits) {
					yyjson_mut_arr_add_strcpy(doc, arr, s.c_str());
				}
				yyjson_mut_obj_add_val(doc, obj, key_ptr, arr);
			} else {
				yyjson_mut_obj_add_strcpy(doc, obj, key_ptr, v.c_str());
			}
		}
		return obj;
	}
	if (type.IsIntegral()) {
		return yyjson_mut_uint(doc, val.GetValue<uint64_t>());
	}
	if (type.IsNumeric()) {
		return yyjson_mut_real(doc, val.GetValue<double>());
	}
	auto str = val.GetValue<string>();
	return yyjson_mut_strncpy(doc, str.c_str(), str.size());
}

static yyjson_mut_val *QueryProfileResultToJSON(yyjson_mut_doc *doc, const QueryProfileResult &node) {
	switch (node.kind) {
	case QueryProfileResultKind::VALUE:
		return ValueToJSON(doc, node.value);
	case QueryProfileResultKind::LIST: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &child : node.children) {
			yyjson_mut_arr_add_val(arr, QueryProfileResultToJSON(doc, *child));
		}
		return arr;
	}
	case QueryProfileResultKind::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		// Sort children alphabetically by key for deterministic output
		vector<reference<const QueryProfileResult>> sorted_children;
		sorted_children.reserve(node.children.size());
		for (auto &child : node.children) {
			sorted_children.push_back(*child);
		}
		std::sort(sorted_children.begin(), sorted_children.end(),
		          [](const QueryProfileResult &a, const QueryProfileResult &b) {
			          if (a.IsNested() != b.IsNested()) {
				          return !a.IsNested();
			          }
			          return a.key < b.key;
		          });
		for (const QueryProfileResult &child : sorted_children) {
			D_ASSERT(!child.key.empty());
			auto key_ptr = yyjson_mut_get_str(yyjson_mut_strcpy(doc, child.key.c_str()));
			yyjson_mut_obj_add_val(doc, obj, key_ptr, QueryProfileResultToJSON(doc, child));
		}
		return obj;
	}
	default:
		throw InternalException("Unknown QueryProfileResultKind");
	}
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

void QueryProfiler::ToLogInternal() const {
	if (!root) {
		return;
	}
	metrics->WriteMetricsToLog(context);
}

void QueryProfiler::ToLog() const {
	lock_guard<std::mutex> guard(lock);
	ToLogInternal();
}

static void OperatorToResultTree(const GatheredMetrics &settings, ProfilingNode &node, QueryProfileResult &result) {
	auto operator_metrics = node.GetOperatorMetrics().GetMetrics(settings);
	for (auto &entry : operator_metrics) {
		result.AddValue(entry.first, std::move(entry.second));
	}
	if (node.GetChildCount() > 0) {
		auto &children_list = result.AddList("children");
		for (idx_t i = 0; i < node.GetChildCount(); i++) {
			auto &child_result = children_list.AppendObject();
			OperatorToResultTree(settings, *node.GetChild(i), child_result);
		}
	}
}

unique_ptr<QueryProfileResult> QueryProfiler::ToResultTree() const {
	auto result = make_uniq<QueryProfileResult>();
	if (!root) {
		result->AddValue("result", Value(query_metrics.query_sql.empty() ? "empty" : "error"));
		return result;
	}
	metrics->MetricsToProfileResult(*result);
	auto &op_list = result->AddList("operator_info");
	auto &op_node = op_list.AppendObject();
	OperatorToResultTree(*metrics, *root, op_node);
	return result;
}

QueryProfileResult &QueryProfiler::GetResult() {
	lock_guard<std::mutex> guard(lock);
	if (!result_tree) {
		result_tree = ToResultTree();
	}
	return *result_tree;
}

bool QueryProfiler::HasRoot() const {
	return root != nullptr;
}

string QueryProfiler::ToJSON() const {
	lock_guard<std::mutex> guard(lock);
	ConvertedJSONHolder json_holder;
	json_holder.doc = yyjson_mut_doc_new(nullptr);
	auto result = ToResultTree();
	auto root_val = QueryProfileResultToJSON(json_holder.doc, *result);
	yyjson_mut_doc_set_root(json_holder.doc, root_val);
	return StringifyAndFree(json_holder, root_val);
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
	auto file = fs.OpenFile(path, flags);
	file->Write((void *)info.c_str(), info.size());
	file->Close();
}

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, const idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	auto node = make_uniq<ProfilingNode>();
	auto &info = node->GetOperatorMetrics();
	node->depth = depth;

	info.name = EnumUtil::ToString(root_p.type);
	info.operator_type = root_p.type;
	info.extra_info = root_p.ParamsToString();

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), depth + 1);
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
	root = CreateTree(root_op, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		running = false;
		tree_map.clear();
		root = nullptr;
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		metrics = make_uniq<GatheredMetrics>(client_config.tracked_metrics);
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

static void MergeOperatorMeasurements(ProfilingNode &root, OperatorMetrics &result) {
	// merge in this layer
	result.Merge(root.GetOperatorMetrics());
	// recurse into children
	for (idx_t i = 0; i < root.GetChildCount(); i++) {
		auto child = root.GetChild(i);
		MergeOperatorMeasurements(*child, result);
	}
}

void QueryProfiler::FinalizeMetricsInternal() {
	if (metrics_finalized || !IsEnabled() || !metrics) {
		return;
	}
	if (query_metrics.latency_timer) {
		query_metrics.latency_timer->EndTimer();
	}
	if (root) {
		OperatorMetrics cumulative_metrics;
		MergeOperatorMeasurements(*root, cumulative_metrics);
		metrics->SetMetric<MetricQueryCPUTime>(cumulative_metrics.time);
		metrics->SetMetric<MetricQueryTotalIntermediateRows>(cumulative_metrics.elements_returned);
		metrics->SetMetric<MetricQueryTotalRowsScanned>(cumulative_metrics.rows_scanned);
		metrics->SetMetric<MetricQueryResultSetSize>(cumulative_metrics.result_set_size);
	}
	query_metrics.FinalizeMetrics(*metrics);
	metrics_finalized = true;
}

} // namespace duckdb
