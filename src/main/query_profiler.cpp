#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/limits.hpp"

#include <iostream>
#include <utility>
#include <algorithm>

using namespace std;

namespace duckdb {

void QueryProfiler::StartQuery(string query, SQLStatement &statement) {
	if (!enabled) {
		return;
	}
	this->running = true;
	this->query = query;
	tree_map.clear();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();

	main_query.Start();
}

bool QueryProfiler::OperatorRequiresProfiling(PhysicalOperatorType op_type) {
	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::AGGREGATE:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::DISTINCT:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::SORT_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXTERNAL_FILE_SCAN:
	case PhysicalOperatorType::QUERY_DERIVED_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::EndQuery() {
	if (!enabled || !running) {
		return;
	}

	main_query.End();
	this->running = false;
	// print or output the query profiling after termination, if this is enabled
	if (automatic_print_format != ProfilerPrintFormat::NONE) {
		// check if this query should be output based on the operator types
		string query_info;
		if (automatic_print_format == ProfilerPrintFormat::JSON) {
			query_info = ToJSON();
		} else if (automatic_print_format == ProfilerPrintFormat::QUERY_TREE) {
			query_info = ToString();
		}

		if (save_location.empty()) {
			Printer::Print(query_info);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), query_info);
		}
	}
}

void QueryProfiler::StartPhase(string new_phase) {
	if (!enabled || !running) {
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
	if (!enabled || !running) {
		return;
	}
	assert(phase_stack.size() > 0);

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (phase_stack.size() > 0) {
		phase_profiler.Start();
	}
}

void QueryProfiler::Initialize(PhysicalOperator *root_op) {
	if (!enabled || !running) {
		return;
	}
	this->query_requires_profiling = false;
	this->root = CreateTree(root_op);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		this->running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

OperatorProfiler::OperatorProfiler(bool enabled_) : enabled(enabled_) {
	execution_stack = stack<PhysicalOperator *>();
}

void OperatorProfiler::StartOperator(PhysicalOperator *phys_op) {
	if (!enabled) {
		return;
	}

	if (!execution_stack.empty()) {
		// add timing for the previous element
		op.End();

		AddTiming(execution_stack.top(), op.Elapsed(), 0);
	}

	execution_stack.push(phys_op);

	// start timing for current element
	op.Start();
}

void OperatorProfiler::EndOperator(DataChunk *chunk) {
	if (!enabled) {
		return;
	}

	// finish timing for the current element
	op.End();

	AddTiming(execution_stack.top(), op.Elapsed(), chunk ? chunk->size() : 0);

	assert(!execution_stack.empty());
	execution_stack.pop();

	// start timing again for the previous element, if any
	if (!execution_stack.empty()) {
		op.Start();
	}
}

void OperatorProfiler::AddTiming(PhysicalOperator *op, double time, idx_t elements) {
	if (!enabled) {
		return;
	}

	auto entry = timings.find(op);
	if (entry == timings.end()) {
		// add new entry
		timings[op] = OperatorTimingInformation(time, elements);
	} else {
		// add to existing entry
		entry->second.time += time;
		entry->second.elements += elements;
	}
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	if (!enabled || !running) {
		return;
	}

	for (auto &node : profiler.timings) {
		auto entry = tree_map.find(node.first);
		assert(entry != tree_map.end());

		entry->second->info.time += node.second.time;
		entry->second->info.elements += node.second.elements;
	}
}

static string DrawPadded(string str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		int half_spaces = width / 2;
		int extra_left_space = width % 2 != 0 ? 1 : 0;
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = toupper(str[0]);
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = toupper(str[i + 1]);
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

string QueryProfiler::ToString() const {
	std::stringstream str;
	ToStream(str);
	return str.str();
}

void QueryProfiler::ToStream(std::ostream &ss) const {
	if (!enabled) {
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
	if (query.empty()) {
		return;
	}

	idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(main_query.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	// print phase timings
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
			          DrawPadded(RenderTitleCase(entry_name) + ": " + RenderTiming(entry.second), TOTAL_BOX_WIDTH - 4) +
			          "││\n";
		}
	}
	if (has_previous_phase) {
		ss << "│└───────────────────────────────────┘│\n";
		ss << "└─────────────────────────────────────┘\n";
	}
	// render the main operator tree
	if (root) {
		Render(*root, ss);
	}
}

static void ToJSONRecursive(QueryProfiler::TreeNode &node, std::ostream &ss, int depth = 1) {
	ss << "{\n";
	ss << string(depth * 3, ' ') << "\"name\": \"" + node.name + "\",\n";
	ss << string(depth * 3, ' ') << "\"timing\":" + StringUtil::Format("%.2f", node.info.time) + ",\n";
	ss << string(depth * 3, ' ') << "\"cardinality\":" + to_string(node.info.elements) + ",\n";
	ss << string(depth * 3, ' ') << "\"extra_info\": \"" + StringUtil::Replace(node.extra_info, "\n", "\\n") + "\",\n";
	ss << string(depth * 3, ' ') << "\"children\": [";
	if (node.children.size() == 0) {
		ss << "]\n";
	} else {
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				ss << ",";
			}
			ss << "\n" << string(depth * 3, ' ');
			ToJSONRecursive(*node.children[i], ss, depth + 1);
		}
		ss << "\n";
		ss << string(depth * 3, ' ') << "]\n";
	}
	ss << string(depth * 3, ' ') << "}";
}

string QueryProfiler::ToJSON() const {
	if (!enabled) {
		return "{ \"result\": \"disabled\" }\n";
	}
	if (query.empty()) {
		return "{ \"result\": \"empty\" }\n";
	}
	if (!root) {
		return "{ \"result\": \"error\" }\n";
	}
	std::stringstream ss;
	ss << "{\n";
	ss << "   \"result\": " + to_string(main_query.Elapsed()) + ",\n";
	// print the phase timings
	ss << "   \"timings\": {\n";
	const auto &ordered_phase_timings = GetOrderedPhaseTimings();
	for (idx_t i = 0; i < ordered_phase_timings.size(); i++) {
		if (i > 0) {
			ss << ",\n";
		}
		ss << "      \"";
		ss << ordered_phase_timings[i].first;
		ss << "\": ";
		ss << to_string(ordered_phase_timings[i].second);
	}
	ss << "\n   },\n";
	// recursively print the physical operator tree
	ss << "   \"tree\": ";
	ToJSONRecursive(*root, ss);
	ss << "\n}";
	return ss.str();
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	ofstream out(path);
	out << info;
	out.close();
}

unique_ptr<QueryProfiler::TreeNode> QueryProfiler::CreateTree(PhysicalOperator *root, idx_t depth) {
	if (OperatorRequiresProfiling(root->type)) {
		this->query_requires_profiling = true;
	}
	auto node = make_unique<QueryProfiler::TreeNode>();
	node->name = root->GetName();
	node->extra_info = root->ParamsToString();
	node->depth = depth;
	tree_map[root] = node.get();
	for (auto &child : root->children) {
		auto child_node = CreateTree(child.get(), depth + 1);
		node->children.push_back(move(child_node));
	}
	switch (root->type) {
	case PhysicalOperatorType::DELIM_JOIN: {
		auto &delim_join = (PhysicalDelimJoin &)*root;
		auto child_node = CreateTree((PhysicalOperator *)delim_join.join.get(), depth + 1);
		node->children.push_back(move(child_node));
		child_node = CreateTree((PhysicalOperator *)delim_join.distinct.get(), depth + 1);
		node->children.push_back(move(child_node));
		break;
	}
	case PhysicalOperatorType::EXECUTE: {
		auto &execute = (PhysicalExecute &)*root;
		auto child_node = CreateTree((PhysicalOperator *)execute.plan, depth + 1);
		node->children.push_back(move(child_node));
		break;
	}
	default:
		break;
	}
	return node;
}

void QueryProfiler::Render(QueryProfiler::TreeNode &node, std::ostream &ss) {
	TreeRenderer renderer;
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(ToString());
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
		assert(entry != phase_timings.end());
		result.emplace_back(entry->first, entry->second);
	}
	return result;
}

} // namespace duckdb
