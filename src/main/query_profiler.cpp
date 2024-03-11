#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_left_delim_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <algorithm>
#include <utility>

namespace duckdb {

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false) {
}

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze ? true : ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return is_explain_analyze ? false : ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat() const {
	return ClientConfig::GetConfig(context).profiler_print_format;
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

void QueryProfiler::StartQuery(string query, bool is_explain_analyze, bool start_at_optimizer) {
	if (is_explain_analyze) {
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
	this->running = true;
	this->query = std::move(query);
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

void QueryProfiler::Finalize(TreeNode &node) {
	for (auto &child : node.children) {
		Finalize(*child);
		if (node.type == PhysicalOperatorType::UNION && node.settings.Enabled(MetricsType::OPERATOR_CARDINALITY)) {
			node.settings.metrics.operator_cardinality += child->settings.metrics.operator_cardinality;
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	this->is_explain_analyze = true;
}

static void GetTotalCPUTime(QueryProfiler::TreeNode &node) {
	node.settings.metrics.cpu_time = node.settings.metrics.operator_timing;
	if (!node.children.empty()) {
		for (const auto &i : node.children) {
			GetTotalCPUTime(*i);
			node.settings.metrics.cpu_time += i->settings.metrics.cpu_time;
		}
	}
}

void QueryProfiler::EndQuery() {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	main_query.End();
	if (root && root->settings.Enabled(MetricsType::OPERATOR_CARDINALITY)) {
		Finalize(*root);
	}
	this->running = false;
	// print or output the query profiling after termination
	// EXPLAIN ANALYSE should not be outputted by the profiler
	if (IsEnabled() && !is_explain_analyze) {
		// add an outer node for the query
		if (root) {
			query_info = make_uniq<QueryProfiler::QueryInfo>();
			query_info->query = query;
			query_info->settings = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			if (query_info->settings.Enabled(MetricsType::OPERATOR_TIMING)) {
				query_info->settings.metrics.operator_timing = main_query.Elapsed();
			}
			if (query_info->settings.Enabled(MetricsType::CPU_TIME)) {
				GetTotalCPUTime(*root);
				query_info->settings.metrics.cpu_time = root->settings.metrics.cpu_time;
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
	this->is_explain_analyze = false;
}

string QueryProfiler::ToString() const {
	const auto format = GetPrintFormat();
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", format);
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
	D_ASSERT(phase_stack.size() > 0);

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

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	if (!IsEnabled() || !running) {
		return;
	}
	this->query_requires_profiling = false;
	ClientConfig &config = ClientConfig::GetConfig(context);
	this->root = CreateTree(root_op, config.profiler_settings, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		this->running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
	settings = ClientConfig::GetConfig(context).profiler_settings;
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
	if (SettingEnabled(MetricsType::OPERATOR_TIMING)) {
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

	bool timing_enabled = SettingEnabled(MetricsType::OPERATOR_TIMING);
	bool cardinality_enabled = SettingEnabled(MetricsType::OPERATOR_CARDINALITY);
	if (timing_enabled || cardinality_enabled) {
		// get the operator info for the current element
		auto &curr_operator_info = GetOperatorInfo(*active_operator);

		// finish timing for the current element
		if (timing_enabled) {
			op.End();
			curr_operator_info.addTime(op.Elapsed());
		}
		if (cardinality_enabled && chunk) {
			curr_operator_info.addElements(chunk->size());
		}
	}
	active_operator = nullptr;
}

OperatorInfo &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = timings.find(phys_op);
	if (entry != timings.end()) {
		return entry->second;
	} else {
		// add new entry
		timings[phys_op] = OperatorInfo();
		return timings[phys_op];
	}
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op, ExpressionExecutor &expression_executor,
                             const string &name, int id) {
	auto entry = timings.find(phys_op);
	if (entry == timings.end()) {
		return;
	}
	auto &operator_timing = timings.find(phys_op)->second;
	if (int(operator_timing.executors_info.size()) <= id) {
		operator_timing.executors_info.resize(id + 1);
	}
	operator_timing.executors_info[id] = make_uniq<ExpressionExecutorInfo>(expression_executor, name, id);
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

		if (profiler.SettingEnabled(MetricsType::OPERATOR_TIMING)) {
			tree_node.settings.metrics.operator_timing += node.second.time;
		}
		if (profiler.SettingEnabled(MetricsType::OPERATOR_CARDINALITY)) {
			tree_node.settings.metrics.operator_cardinality += node.second.elements;
		}
		tree_node.executors_info = std::move(node.second.executors_info);
		if (!IsDetailedEnabled()) {
			continue;
		}
		for (auto &info : node.second.executors_info) {
			if (!info) {
				continue;
			}
			auto info_id = info->id;
			if (tree_node.executors_info.size() <= info_id) {
				tree_node.executors_info.resize(info_id + 1);
			}
			tree_node.executors_info[info_id] = std::move(info);
		}
	}
	profiler.timings.clear();
}

static string DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		auto half_spaces = width / 2;
		auto extra_left_space = width % 2 != 0 ? 1 : 0;
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
		ss << "Query profiling is disabled. Call "
		      "Connection::EnableProfiling() to enable profiling!";
		return;
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << StringUtil::Replace(query, "\n", " ") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query.empty() && !root) {
		return;
	}

	auto http_state = HTTPState::TryGetState(context, false);
	if (http_state && !http_state->IsEmpty()) {
		string read = "in: " + StringUtil::BytesToHumanReadableString(http_state->total_bytes_received);
		string written = "out: " + StringUtil::BytesToHumanReadableString(http_state->total_bytes_sent);
		string head = "#HEAD: " + to_string(http_state->head_count);
		string get = "#GET: " + to_string(http_state->get_count);
		string put = "#PUT: " + to_string(http_state->put_count);
		string post = "#POST: " + to_string(http_state->post_count);

		constexpr idx_t TOTAL_BOX_WIDTH = 39;
		ss << "┌─────────────────────────────────────┐\n";
		ss << "│┌───────────────────────────────────┐│\n";
		ss << "││            HTTP Stats:            ││\n";
		ss << "││                                   ││\n";
		ss << "││" + DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "│└───────────────────────────────────┘│\n";
		ss << "└─────────────────────────────────────┘\n";
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

string QueryProfiler::JSONSanitize(const std::string &text) {
	string result;
	result.reserve(text.size());
	for (idx_t i = 0; i < text.size(); i++) {
		switch (text[i]) {
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
			result += text[i];
			break;
		}
	}
	return result;
}

// Print a row
static void PrintRow(std::ostream &ss, const string &annotation, int id, const string &name, double time,
                     int sample_counter, int tuple_counter, const string &extra_info, int depth) {
	ss << string(depth * 3, ' ') << " {\n";
	ss << string(depth * 3, ' ') << "   \"annotation\": \"" + QueryProfiler::JSONSanitize(annotation) + "\",\n";
	ss << string(depth * 3, ' ') << "   \"id\": " + to_string(id) + ",\n";
	ss << string(depth * 3, ' ') << "   \"name\": \"" + QueryProfiler::JSONSanitize(name) + "\",\n";
#if defined(RDTSC)
	ss << string(depth * 3, ' ') << "   \"timing\": \"NULL\" ,\n";
	ss << string(depth * 3, ' ') << "   \"cycles_per_tuple\": " + StringUtil::Format("%.4f", time) + ",\n";
#else
	ss << string(depth * 3, ' ') << "   \"timing\":" + to_string(time) + ",\n";
	ss << string(depth * 3, ' ') << "   \"cycles_per_tuple\": \"NULL\" ,\n";
#endif
	ss << string(depth * 3, ' ') << "   \"sample_size\": " << to_string(sample_counter) + ",\n";
	ss << string(depth * 3, ' ') << "   \"input_size\": " << to_string(tuple_counter) + ",\n";
	ss << string(depth * 3, ' ') << "   \"extra_info\": \"" << QueryProfiler::JSONSanitize(extra_info) + "\"\n";
	ss << string(depth * 3, ' ') << " },\n";
}

static void ExtractFunctions(std::ostream &ss, ExpressionInfo &info, idx_t &fun_id, int depth) {
	if (info.hasfunction) {
		double time = info.sample_tuples_count == 0 ? 0 : int(info.function_time) / double(info.sample_tuples_count);
		PrintRow(ss, "Function", fun_id++, info.function_name, time, NumericCast<int>(info.sample_tuples_count),
		         NumericCast<int>(info.tuples_count), "", NumericCast<int>(depth));
	}
	if (info.children.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : info.children) {
		ExtractFunctions(ss, *child, fun_id, depth);
	}
}

static void ToJSONRecursive(QueryProfiler::TreeNode &node, std::ostream &ss, idx_t depth = 1) {
	auto &settings = node.settings;

	ss << string(depth * 3, ' ') << " {\n";
	ss << string(depth * 3, ' ') << "   \"name\": \"" + QueryProfiler::JSONSanitize(node.name) + "\",\n";
	settings.PrintAllMetricsToSS(ss, string(depth * 3, ' '));
	if (settings.Enabled(MetricsType::EXTRA_INFO)) {
		ss << string(depth * 3, ' ') << "   \"timings\": [";
		idx_t function_counter = 1;
		idx_t expression_counter = 1;
		ss << "\n ";
		for (const auto &expr_executor : node.executors_info) {
			// For each Expression tree
			if (!expr_executor) {
				continue;
			}
			for (const auto &expr_timer : expr_executor->roots) {
				double time = expr_timer->sample_tuples_count == 0
				                  ? 0
				                  : double(expr_timer->time) / double(expr_timer->sample_tuples_count);
				PrintRow(ss, "ExpressionRoot", expression_counter++, expr_timer->name, time,
				         NumericCast<int>(expr_timer->sample_tuples_count), NumericCast<int>(expr_timer->tuples_count),
				         expr_timer->extra_info, depth + 1);
				// Extract all functions inside the tree
				ExtractFunctions(ss, *expr_timer->root, function_counter, depth + 1);
			}
		}
		ss.seekp(-2, ss.cur);
		ss << "\n";
		ss << string(depth * 3, ' ') << "   ],\n";
	}
	ss << string(depth * 3, ' ') << "   \"children\": [\n";
	if (node.children.empty()) {
		ss << string(depth * 3, ' ') << "   ]\n";
	} else {
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				ss << ",\n";
			}
			ToJSONRecursive(*node.children[i], ss, depth + 1);
		}
		ss << string(depth * 3, ' ') << "   ]\n";
	}
	ss << string(depth * 3, ' ') << " }\n";
}

string QueryProfiler::ToJSON() const {
	if (!IsEnabled()) {
		return "{ \"result\": \"disabled\" }\n";
	}
	if (query.empty() && !root) {
		return "{ \"result\": \"empty\" }\n";
	}
	if (!root) {
		return "{ \"result\": \"error\" }\n";
	}

	auto &settings = query_info->settings;

	std::stringstream ss;
	ss << "{\n";
	ss << "   \"Query\": \"" + JSONSanitize(query_info->query) + "\",\n";

	settings.PrintAllMetricsToSS(ss, "");
	// JSON cannot have literal control characters in string literals
	if (settings.Enabled(MetricsType::EXTRA_INFO)) {
		ss << "   \"timings\": [\n";
		const auto &ordered_phase_timings = GetOrderedPhaseTimings();
		for (idx_t i = 0; i < ordered_phase_timings.size(); i++) {
			if (i > 0) {
				ss << ",\n";
			}
			ss << "   {\n";
			ss << "   \"annotation\": \"" + ordered_phase_timings[i].first + "\", \n";
			ss << "   \"timing\": " + to_string(ordered_phase_timings[i].second) + "\n";
			ss << "   }";
		}
		ss << "\n";
		ss << "   ],\n";
	}
	// recursively print the physical operator tree
	ss << "   \"children\": [\n";
	ToJSONRecursive(*root->children[0], ss);
	ss << "   ]\n";
	ss << "}";
	return ss.str();
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

unique_ptr<QueryProfiler::TreeNode> QueryProfiler::CreateTree(const PhysicalOperator &root, SettingsSet settings,
                                                              idx_t depth) {
	if (OperatorRequiresProfiling(root.type)) {
		this->query_requires_profiling = true;
	}
	auto node = make_uniq<QueryProfiler::TreeNode>();
	node->type = root.type;
	node->name = root.GetName();
	node->depth = depth;
	node->settings = ProfilingInfo(settings);
	if (node->settings.Enabled(MetricsType::EXTRA_INFO)) {
		node->settings.metrics.extra_info = root.ParamsToString();
	}
	tree_map.insert(make_pair(reference<const PhysicalOperator>(root), reference<QueryProfiler::TreeNode>(*node)));
	auto children = root.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), settings, depth + 1);
		node->children.push_back(std::move(child_node));
	}
	return node;
}

void QueryProfiler::Render(const QueryProfiler::TreeNode &node, std::ostream &ss) const {
	TreeRenderer renderer;
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
void QueryProfiler::Propagate(QueryProfiler &qp) {
}

void ExpressionInfo::ExtractExpressionsRecursive(unique_ptr<ExpressionState> &state) {
	if (state->child_states.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : state->child_states) {
		auto expr_info = make_uniq<ExpressionInfo>();
		if (child->expr.expression_class == ExpressionClass::BOUND_FUNCTION) {
			expr_info->hasfunction = true;
			expr_info->function_name = child->expr.Cast<BoundFunctionExpression>().function.ToString();
			expr_info->function_time = child->profiler.time;
			expr_info->sample_tuples_count = child->profiler.sample_tuples_count;
			expr_info->tuples_count = child->profiler.tuples_count;
		}
		expr_info->ExtractExpressionsRecursive(child);
		children.push_back(std::move(expr_info));
	}
	return;
}

ExpressionExecutorInfo::ExpressionExecutorInfo(ExpressionExecutor &executor, const string &name, int id) : id(id) {
	// Extract Expression Root Information from ExpressionExecutorStats
	for (auto &state : executor.GetStates()) {
		roots.push_back(make_uniq<ExpressionRootInfo>(*state, name));
	}
}

ExpressionRootInfo::ExpressionRootInfo(ExpressionExecutorState &state, string name)
    : current_count(state.profiler.current_count), sample_count(state.profiler.sample_count),
      sample_tuples_count(state.profiler.sample_tuples_count), tuples_count(state.profiler.tuples_count),
      name("expression"), time(state.profiler.time) {
	// Use the name of expression-tree as extra-info
	extra_info = std::move(name);
	auto expression_info_p = make_uniq<ExpressionInfo>();
	// Maybe root has a function
	if (state.root_state->expr.expression_class == ExpressionClass::BOUND_FUNCTION) {
		expression_info_p->hasfunction = true;
		expression_info_p->function_name = (state.root_state->expr.Cast<BoundFunctionExpression>()).function.name;
		expression_info_p->function_time = state.root_state->profiler.time;
		expression_info_p->sample_tuples_count = state.root_state->profiler.sample_tuples_count;
		expression_info_p->tuples_count = state.root_state->profiler.tuples_count;
	}
	expression_info_p->ExtractExpressionsRecursive(state.root_state);
	root = std::move(expression_info_p);
}
} // namespace duckdb
