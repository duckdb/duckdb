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
		if (type == PhysicalOperatorType::UNION && info.Enabled(MetricsType::OPERATOR_CARDINALITY)) {
			info.AddToMetric(MetricsType::OPERATOR_CARDINALITY,
			                 child->GetProfilingInfo().metrics[MetricsType::OPERATOR_CARDINALITY].GetValue<idx_t>());
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

template <class METRIC_TYPE>
static void GetCumulativeMetric(ProfilingNode &node, MetricsType cumulative_metric, MetricsType child_metric) {
	node.GetProfilingInfo().metrics[cumulative_metric] = node.GetProfilingInfo().metrics[child_metric];
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		GetCumulativeMetric<METRIC_TYPE>(*child, cumulative_metric, child_metric);
		node.GetProfilingInfo().AddToMetric(
		    cumulative_metric, child->GetProfilingInfo().metrics[cumulative_metric].GetValue<METRIC_TYPE>());
	}
}

void QueryProfiler::EndQuery() {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	main_query.End();
	if (root && root->GetProfilingInfo().Enabled(MetricsType::OPERATOR_CARDINALITY)) {
		Finalize(*root->GetChild(0));
	}
	running = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze) {
		// Expand the query info.
		if (root) {
			auto &info = root->GetProfilingInfo();
			info = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			info.metrics[MetricsType::QUERY_NAME] = query_info.query_name;

			if (info.Enabled(MetricsType::BLOCKED_THREAD_TIME)) {
				info.metrics[MetricsType::BLOCKED_THREAD_TIME] = query_info.blocked_thread_time;
			}
			if (info.Enabled(MetricsType::OPERATOR_TIMING)) {
				info.metrics[MetricsType::OPERATOR_TIMING] = main_query.Elapsed();
			}
			if (info.Enabled(MetricsType::CPU_TIME)) {
				GetCumulativeMetric<double>(*root, MetricsType::CPU_TIME, MetricsType::OPERATOR_TIMING);
			}
			if (info.Enabled(MetricsType::CUMULATIVE_CARDINALITY)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_CARDINALITY,
				                           MetricsType::OPERATOR_CARDINALITY);
			}
			if (info.Enabled(MetricsType::CUMULATIVE_ROWS_SCANNED)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_ROWS_SCANNED,
				                           MetricsType::OPERATOR_ROWS_SCANNED);
			}

			if (info.Enabled(MetricsType::OPERATOR_TYPE)) {
				info.settings.erase(MetricsType::OPERATOR_TYPE);
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

void QueryProfiler::StartPhase(string new_phase) {
	if (!IsEnabled() || !running) {
		return;
	}

	if (!phase_stack.empty()) {
		// there are active phases
		phase_profiler.End();
		// add the timing to all phases prior to this one
		string prefix = "";
		for (auto &phase : phase_stack) {
			phase_timings[phase] += phase_profiler.Elapsed();
			prefix += phase + " > ";
		}
		// when there are previous phases, we prefix the current phase with those phases
		new_phase = prefix + new_phase;
	}

	// start a new phase
	phase_stack.push_back(new_phase);
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

bool SettingIsEnabled(const profiler_settings_t &settings, MetricsType metric) {
	if (settings.find(metric) != settings.end()) {
		return true;
	}

	switch (metric) {
	case MetricsType::OPERATOR_TIMING:
		return SettingIsEnabled(settings, MetricsType::CPU_TIME);
	case MetricsType::OPERATOR_CARDINALITY:
		return SettingIsEnabled(settings, MetricsType::CUMULATIVE_CARDINALITY);
	case MetricsType::OPERATOR_ROWS_SCANNED:
		return SettingIsEnabled(settings, MetricsType::CUMULATIVE_ROWS_SCANNED);
	default:
		break;
	}

	return false;
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
	auto &settings = ClientConfig::GetConfig(context).profiler_settings;

	profiler_settings_t op_metrics = ProfilingInfo::DefaultOperatorSettings();
	for (auto &metric : op_metrics) {
		if (SettingIsEnabled(settings, metric)) {
			operator_settings.insert(metric);
		}
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

	// start timing for current element
	if (HasOperatorSetting(MetricsType::OPERATOR_TIMING)) {
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

	if (!operator_settings.empty()) {
		// get the operator info for the current element
		auto &curr_operator_info = GetOperatorInfo(*active_operator);

		// finish timing for the current element
		if (HasOperatorSetting(MetricsType::OPERATOR_TIMING)) {
			op.End();
			curr_operator_info.AddTime(op.Elapsed());
		}
		if (HasOperatorSetting(MetricsType::OPERATOR_CARDINALITY) && chunk) {
			curr_operator_info.AddReturnedElements(chunk->size());
		}
	}
	active_operator = nullptr;
}

OperatorInformation &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = timings.find(phys_op);
	if (entry != timings.end()) {
		return entry->second;
	} else {
		// add new entry
		timings[phys_op] = OperatorInformation();
		return timings[phys_op];
	}
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

		if (profiler.HasOperatorSetting(MetricsType::OPERATOR_TIMING)) {
			tree_node.GetProfilingInfo().AddToMetric<double>(MetricsType::OPERATOR_TIMING, node.second.time);
		}
		if (profiler.HasOperatorSetting(MetricsType::OPERATOR_CARDINALITY)) {
			tree_node.GetProfilingInfo().AddToMetric<idx_t>(MetricsType::OPERATOR_CARDINALITY,
			                                                node.second.elements_returned);
		}
		if (profiler.HasOperatorSetting(MetricsType::OPERATOR_ROWS_SCANNED)) {
			if (op.type == PhysicalOperatorType::TABLE_SCAN) {
				auto &scan_op = op.Cast<PhysicalTableScan>();
				auto &bind_data = scan_op.bind_data;
				if (bind_data && scan_op.function.cardinality) {
					auto cardinality = scan_op.function.cardinality(context, &(*bind_data));
					if (cardinality && cardinality->has_estimated_cardinality) {
						tree_node.GetProfilingInfo().AddToMetric<idx_t>(MetricsType::OPERATOR_ROWS_SCANNED,
						                                                cardinality->estimated_cardinality);
					}
				}
			}
		}
	}
	profiler.timings.clear();
}

void QueryProfiler::SetInfo(const double &blocked_thread_time) {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running || !root->GetProfilingInfo().Enabled(MetricsType::BLOCKED_THREAD_TIME)) {
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

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(main_query.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	// print phase timings
	if (PrintOptimizerOutput()) {
		bool has_previous_phase = false;
		for (const auto &entry : GetOrderedPhaseTimings()) {
			if (!StringUtil::Contains(entry.first, " > ")) {
				// primary phase!
				if (has_previous_phase) {
					ss << "│└───────────────────────────────────┘│\n";
					ss << "└─────────────────────────────────────┘\n";
				}
				ss << "┌─────────────────────────────────────┐\n";
				ss << "│" +
				          DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
				                     TOTAL_BOX_WIDTH - 2) +
				          "│\n";
				ss << "│┌───────────────────────────────────┐│\n";
				has_previous_phase = true;
			} else {
				string entry_name = StringUtil::Split(entry.first, " > ")[1];
				ss << "││" +
				          DrawPadded(RenderTitleCase(entry_name) + ": " + RenderTiming(entry.second),
				                     TOTAL_BOX_WIDTH - 4) +
				          "││\n";
			}
		}
		if (has_previous_phase) {
			ss << "│└───────────────────────────────────┘│\n";
			ss << "└─────────────────────────────────────┘\n";
		}
	}
	// render the main operator tree
	if (root) {
		Render(*root, ss);
	}
}

InsertionOrderPreservingMap<string> QueryProfiler::JSONSanitize(const InsertionOrderPreservingMap<string> &input) {
	InsertionOrderPreservingMap<string> result;
	for (auto &it : input) {
		result[it.first] = JSONSanitize(it.second);
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

	node.GetProfilingInfo().WriteMetricsToJSON(doc, result_obj);

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
	if (settings.Enabled(MetricsType::EXTRA_INFO)) {
		auto timings_list = yyjson_mut_arr(doc);
		const auto &ordered_phase_timings = GetOrderedPhaseTimings();
		for (idx_t i = 0; i < ordered_phase_timings.size(); i++) {
			auto timing_object = yyjson_mut_arr_add_obj(doc, timings_list);
			yyjson_mut_obj_add_strcpy(doc, timing_object, "annotation", ordered_phase_timings[i].first.c_str());
			yyjson_mut_obj_add_real(doc, timing_object, "timing", ordered_phase_timings[i].second);
		}
		yyjson_mut_obj_add_val(doc, result_obj, "timings", timings_list);
	}

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

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, profiler_settings_t settings,
                                                    idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	unique_ptr<ProfilingNode> node = make_uniq<ProfilingNode>();
	auto &info = node->GetProfilingInfo();
	info = ProfilingInfo(settings, depth);
	node->depth = depth;

	if (depth != 0) {
		info.AddToMetric<uint8_t>(MetricsType::OPERATOR_TYPE, static_cast<uint8_t>(root_p.type));
		if (info.Enabled(MetricsType::QUERY_NAME)) {
			info.settings.erase(MetricsType::QUERY_NAME);
		}
	}
	if (info.Enabled(MetricsType::EXTRA_INFO)) {
		info.extra_info = root_p.ParamsToString();
	}

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), settings, depth + 1);
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

vector<QueryProfiler::PhaseTimingItem> QueryProfiler::GetOrderedPhaseTimings() const {
	vector<PhaseTimingItem> result;
	// first sort the phases alphabetically
	vector<string> phases;
	for (auto &entry : phase_timings) {
		phases.push_back(entry.first);
	}
	std::sort(phases.begin(), phases.end());
	for (const auto &phase : phases) {
		auto entry = phase_timings.find(phase);
		D_ASSERT(entry != phase_timings.end());
		result.emplace_back(entry->first, entry->second);
	}
	return result;
}

void QueryProfiler::Propagate(QueryProfiler &) {
}

} // namespace duckdb
