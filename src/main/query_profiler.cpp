#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
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

void QueryProfiler::Finalize(ProfilingNode &node) {
	auto &op_node = node.Cast<OperatorProfilingNode>();

	for (idx_t i = 0; i < op_node.GetChildCount(); i++) {
		auto child = op_node.GetChild(i);
		Finalize(*child);
		if (op_node.type == PhysicalOperatorType::UNION &&
		    op_node.GetProfilingInfo().Enabled(MetricsType::OPERATOR_CARDINALITY)) {
			op_node.GetProfilingInfo().metrics.operator_cardinality +=
			    child->GetProfilingInfo().metrics.operator_cardinality;
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	this->is_explain_analyze = true;
}

static void GetTotalCPUTime(ProfilingNode &node) {
	node.GetProfilingInfo().metrics.cpu_time = node.GetProfilingInfo().metrics.operator_timing;
	if (node.GetChildCount() > 0) {
		for (idx_t i = 0; i < node.GetChildCount(); i++) {
			auto child = node.GetChild(i);
			GetTotalCPUTime(*child);
			node.GetProfilingInfo().metrics.cpu_time += child->GetProfilingInfo().metrics.cpu_time;
		}
	}
}

void QueryProfiler::EndQuery() {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	main_query.End();
	if (root && root->GetProfilingInfo().Enabled(MetricsType::OPERATOR_CARDINALITY)) {
		Finalize(root->GetChild(0)->Cast<OperatorProfilingNode>());
	}
	this->running = false;
	// print or output the query profiling after termination
	// EXPLAIN ANALYSE should not be outputted by the profiler
	if (IsEnabled() && !is_explain_analyze) {
		// Expand the query info
		if (root) {
			auto &query_info = root->Cast<QueryProfilingNode>();
			query_info.query = query;
			query_info.GetProfilingInfo() = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			if (query_info.GetProfilingInfo().Enabled(MetricsType::OPERATOR_TIMING)) {
				query_info.GetProfilingInfo().metrics.operator_timing = main_query.Elapsed();
			}
			if (query_info.GetProfilingInfo().Enabled(MetricsType::CPU_TIME)) {
				GetTotalCPUTime(*root);
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
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
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
			curr_operator_info.AddTime(op.Elapsed());
		}
		if (cardinality_enabled && chunk) {
			curr_operator_info.AddElements(chunk->size());
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

void OperatorProfiler::Flush(const PhysicalOperator &phys_op, ExpressionExecutor &expression_executor,
                             const string &name, int id) {
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

		if (profiler.SettingEnabled(MetricsType::OPERATOR_TIMING)) {
			tree_node.GetProfilingInfo().metrics.operator_timing += node.second.time;
		}
		if (profiler.SettingEnabled(MetricsType::OPERATOR_CARDINALITY)) {
			tree_node.GetProfilingInfo().metrics.operator_cardinality += node.second.elements;
		}
	}
	profiler.timings.clear();
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

	for (auto &it : context.registered_state) {
		it.second->WriteProfilingInformation(ss);
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

static void ToJSONRecursive(ProfilingNode &node, std::stringstream &ss, idx_t depth = 1) {
	auto &op_node = node.Cast<OperatorProfilingNode>();

	ss << string(depth * 3, ' ') << " {\n";
	ss << string(depth * 3, ' ') << "   \"name\": \"" + QueryProfiler::JSONSanitize(op_node.name) + "\",\n";
	op_node.GetProfilingInfo().PrintAllMetricsToSS(ss, string(depth * 3, ' '));
	ss << string(depth * 3, ' ') << "   \"children\": [\n";
	if (op_node.GetChildCount() == 0) {
		ss << string(depth * 3, ' ') << "   ]\n";
	} else {
		for (idx_t i = 0; i < op_node.GetChildCount(); i++) {
			if (i > 0) {
				ss << ",\n";
			}
			ToJSONRecursive(*op_node.GetChild(i), ss, depth + 1);
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

	auto &query_info = root->Cast<QueryProfilingNode>();
	auto &settings = query_info.GetProfilingInfo();

	std::stringstream ss;
	ss << "{\n";
	ss << "   \"query\": \"" + JSONSanitize(query_info.query) + "\",\n";

	settings.PrintAllMetricsToSS(ss, "");
	// JSON cannot have literal control characters in string literals
	if (settings.Enabled(MetricsType::EXTRA_INFO)) {
		ss << "   \"timings\": [\n";
		const auto &ordered_phase_timings = GetOrderedPhaseTimings();
		for (idx_t i = 0; i < ordered_phase_timings.size(); i++) {
			if (i > 0) {
				ss << ",\n";
			}
			ss << "      {\n";
			ss << "         \"annotation\": \"" + ordered_phase_timings[i].first + "\", \n";
			ss << "         \"timing\": " + to_string(ordered_phase_timings[i].second) + "\n";
			ss << "      }";
		}
		ss << "\n";
		ss << "   ],\n";
	}
	// recursively print the physical operator tree
	ss << "   \"children\": [\n";
	ToJSONRecursive(*root->GetChild(0), ss);
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

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root, profiler_settings_t settings,
                                                    idx_t depth) {
	if (OperatorRequiresProfiling(root.type)) {
		this->query_requires_profiling = true;
	}

	unique_ptr<ProfilingNode> node;

	if (depth == 0) {
		auto query_node = make_uniq<QueryProfilingNode>();
		query_node->node_type = ProfilingNodeType::QUERY_ROOT;
		node = std::move(query_node);
	} else {
		auto op_node = make_uniq<OperatorProfilingNode>();
		op_node->type = root.type;
		op_node->name = root.GetName();
		node = std::move(op_node);
	}

	node->depth = depth;
	node->GetProfilingInfo() = ProfilingInfo(settings);
	if (node->GetProfilingInfo().Enabled(MetricsType::EXTRA_INFO)) {
		node->GetProfilingInfo().metrics.extra_info = root.ParamsToString();
	}
	tree_map.insert(make_pair(reference<const PhysicalOperator>(root), reference<ProfilingNode>(*node)));
	auto children = root.GetChildren();
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

} // namespace duckdb
