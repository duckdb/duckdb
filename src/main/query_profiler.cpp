#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "yyjson.hpp"

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
	if (format == ExplainFormat::DEFAULT) {
		return print_format;
	}
	switch (format) {
	case ExplainFormat::TEXT:
		return ProfilerPrintFormat::QUERY_TREE;
	case ExplainFormat::JSON:
		return ProfilerPrintFormat::JSON;
	default:
		throw NotImplementedException("No mapping from ExplainFormat::%s to ProfilerPrintFormat",
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

void QueryProfiler::StartQuery(string query, bool is_explain_analyze_p, bool start_at_optimizer) {
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

	running = true;
	query_info.query_name = std::move(query);
	tree_map.clear();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();
	main_query.Start();
}

bool QueryProfiler::OperatorRequiresProfiling(PhysicalOperatorType op_type) {
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
			info.AddToMetric(MetricsType::OPERATOR_CARDINALITY, value);
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

template <class METRIC_TYPE>
static void GetCumulativeMetric(ProfilingNode &node, MetricsType cumulative_metric, MetricsType child_metric) {
	auto &info = node.GetProfilingInfo();
	info.metrics[cumulative_metric] = info.metrics[child_metric];

	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		GetCumulativeMetric<METRIC_TYPE>(*child, cumulative_metric, child_metric);
		auto value = child->GetProfilingInfo().metrics[cumulative_metric].GetValue<METRIC_TYPE>();
		info.AddToMetric(cumulative_metric, value);
	}
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
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	main_query.End();
	if (root) {
		auto &info = root->GetProfilingInfo();
		if (info.Enabled(info.expanded_settings, MetricsType::OPERATOR_CARDINALITY)) {
			Finalize(*root->GetChild(0));
		}
	}
	running = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze) {
		if (root) {
			auto &info = root->GetProfilingInfo();
			info = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			auto &child_info = root->children[0]->GetProfilingInfo();
			info.metrics[MetricsType::QUERY_NAME] = query_info.query_name;

			auto &settings = info.expanded_settings;
			if (info.Enabled(settings, MetricsType::BLOCKED_THREAD_TIME)) {
				info.metrics[MetricsType::BLOCKED_THREAD_TIME] = query_info.blocked_thread_time;
			}
			if (info.Enabled(settings, MetricsType::LATENCY)) {
				info.metrics[MetricsType::LATENCY] = main_query.Elapsed();
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

			MoveOptimizerPhasesToRoot();
			if (info.Enabled(settings, MetricsType::CUMULATIVE_OPTIMIZER_TIMING)) {
				info.metrics.at(MetricsType::CUMULATIVE_OPTIMIZER_TIMING) = GetCumulativeOptimizers(*root);
			}
		}

		string tree = ToString();
		auto save_location = GetSaveLocation();

		if (!ClientConfig::GetConfig(context).emit_profiler_output) {
			// disable output
		} else if (save_location.empty()) {
			Printer::Print(tree);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), tree);
		}
	}

	is_explain_analyze = false;
}

string QueryProfiler::ToString(ExplainFormat explain_format) const {
	const auto format = GetPrintFormat(explain_format);
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::StartPhase(MetricsType phase_metric) {
	if (!IsEnabled() || !running) {
		return;
	}

	// start a new phase
	phase_stack.push_back(phase_metric);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
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
	auto root_metrics = ProfilingInfo::DefaultRootSettings();
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

	// Start the timing of the current operator.
	if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_TIMING)) {
		op.Start();
	}
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while another operator is active");
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
	}
	active_operator = nullptr;
}

OperatorInformation &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = timings.find(phys_op);
	if (entry != timings.end()) {
		return entry->second;
	}
	// Add a new entry.
	timings[phys_op] = OperatorInformation();
	return timings[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = timings.find(phys_op);
	if (entry == timings.end()) {
		return;
	}
	auto &operator_timing = timings.find(phys_op)->second;
	operator_timing.name = phys_op.GetName();
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.timings) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetProfilingInfo();

		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_TIMING)) {
			info.AddToMetric<double>(MetricsType::OPERATOR_TIMING, node.second.time);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_CARDINALITY)) {
			info.AddToMetric<idx_t>(MetricsType::OPERATOR_CARDINALITY, node.second.elements_returned);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_ROWS_SCANNED)) {
			if (op.type == PhysicalOperatorType::TABLE_SCAN) {
				auto &scan_op = op.Cast<PhysicalTableScan>();
				auto &bind_data = scan_op.bind_data;

				if (bind_data && scan_op.function.cardinality) {
					auto cardinality = scan_op.function.cardinality(context, &(*bind_data));
					if (cardinality && cardinality->has_estimated_cardinality) {
						info.AddToMetric<idx_t>(MetricsType::OPERATOR_ROWS_SCANNED, cardinality->estimated_cardinality);
					}
				}
			}
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::RESULT_SET_SIZE)) {
			info.AddToMetric<idx_t>(MetricsType::RESULT_SET_SIZE, node.second.result_set_size);
		}
	}
	profiler.timings.clear();
}

void QueryProfiler::SetInfo(const double &blocked_thread_time) {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	auto &info = root->GetProfilingInfo();
	auto metric_enabled = info.Enabled(info.expanded_settings, MetricsType::BLOCKED_THREAD_TIME);
	if (!metric_enabled) {
		return;
	}
	query_info.blocked_thread_time = blocked_thread_time;
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
	std::stringstream str;
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
	if (!IsEnabled()) {
		ss << "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
		return;
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << StringUtil::Replace(query_info.query_name, "\n", " ") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_info.query_name.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(main_query.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// print phase timings
	if (PrintOptimizerOutput()) {
		PrintPhaseTimingsToStream(ss, root->GetProfilingInfo(), TOTAL_BOX_WIDTH);
	}
	// render the main operator tree
	if (root) {
		Render(*root, ss);
	}
}

InsertionOrderPreservingMap<string> QueryProfiler::JSONSanitize(const InsertionOrderPreservingMap<string> &input) {
	InsertionOrderPreservingMap<string> result;
	for (auto &it : input) {
		auto key = it.first;
		if (StringUtil::StartsWith(key, "__")) {
			key = StringUtil::Replace(key, "__", "");
			key = StringUtil::Replace(key, "_", " ");
			key = StringUtil::Title(key);
		}
		result[key] = it.second;
	}
	return result;
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
	profiling_info.extra_info = QueryProfiler::JSONSanitize(profiling_info.extra_info);
	profiling_info.WriteMetricsToJSON(doc, result_obj);

	auto children_list = yyjson_mut_arr(doc);
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = ToJSONRecursive(doc, *node.GetChild(i));
		yyjson_mut_arr_add_val(children_list, child);
	}
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	return result_obj;
}

static string StringifyAndFree(yyjson_mut_doc *doc, yyjson_mut_val *object) {
	auto data = yyjson_mut_val_write_opts(object, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr,
	                                      nullptr, nullptr);
	if (!data) {
		yyjson_mut_doc_free(doc);
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	auto result = string(data);
	free(data);
	yyjson_mut_doc_free(doc);
	return result;
}

string QueryProfiler::ToJSON() const {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, result_obj);

	if (!IsEnabled()) {
		yyjson_mut_obj_add_str(doc, result_obj, "result", "disabled");
		return StringifyAndFree(doc, result_obj);
	}
	if (query_info.query_name.empty() && !root) {
		yyjson_mut_obj_add_str(doc, result_obj, "result", "empty");
		return StringifyAndFree(doc, result_obj);
	}
	if (!root) {
		yyjson_mut_obj_add_str(doc, result_obj, "result", "error");
		return StringifyAndFree(doc, result_obj);
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToJSON(doc, result_obj);

	// recursively print the physical operator tree
	auto children_list = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	auto child = ToJSONRecursive(doc, *root->GetChild(0));
	yyjson_mut_arr_add_val(children_list, child);
	return StringifyAndFree(doc, result_obj);
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
		    setting == MetricsType::BLOCKED_THREAD_TIME) {
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
		info.AddToMetric<uint8_t>(MetricsType::OPERATOR_TYPE, static_cast<uint8_t>(root_p.type));
	}
	if (info.Enabled(info.settings, MetricsType::EXTRA_INFO)) {
		info.extra_info = root_p.ParamsToString();
	}

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), child_settings, depth + 1);
		node->AddChild(std::move(child_node));
	}
	return node;
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	if (!IsEnabled() || !running) {
		return;
	}
	query_requires_profiling = false;
	ClientConfig &config = ClientConfig::GetConfig(context);
	root = CreateTree(root_op, config.profiler_settings, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		this->running = false;
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

void QueryProfiler::Propagate(QueryProfiler &) {
}

} // namespace duckdb
