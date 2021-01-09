//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/physical_operator.hpp"

#include <stack>
#include <unordered_map>

namespace duckdb {
class PhysicalOperator;
class SQLStatement;

struct OperatorTimingInformation {
	double time = 0;
	idx_t elements = 0;

	OperatorTimingInformation(double time_ = 0, idx_t elements_ = 0) : time(time_), elements(elements_) {
	}
};

//! The OperatorProfiler measures timings of individual operators
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API OperatorProfiler(bool enabled);

	DUCKDB_API void StartOperator(PhysicalOperator *phys_op);
	DUCKDB_API void EndOperator(DataChunk *chunk);

private:
	void AddTiming(PhysicalOperator *op, double time, idx_t elements);

	//! Whether or not the profiler is enabled
	bool enabled;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	std::stack<PhysicalOperator *> execution_stack;
	//! A mapping of physical operators to recorded timings
	unordered_map<PhysicalOperator *, OperatorTimingInformation> timings;
};

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	struct TreeNode {
		string name;
		string extra_info;
		OperatorTimingInformation info;
		vector<unique_ptr<TreeNode>> children;
		idx_t depth = 0;
	};

private:
	unique_ptr<TreeNode> CreateTree(PhysicalOperator *root, idx_t depth = 0);

	static void Render(TreeNode &node, std::ostream &str);

public:
	DUCKDB_API QueryProfiler() : automatic_print_format(ProfilerPrintFormat::NONE), enabled(false), running(false) {
	}

	DUCKDB_API void Enable() {
		enabled = true;
	}

	DUCKDB_API void Disable() {
		enabled = false;
	}

	DUCKDB_API bool IsEnabled() {
		return enabled;
	}

	DUCKDB_API void StartQuery(string query);
	DUCKDB_API void EndQuery();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);

	DUCKDB_API void StartPhase(string phase);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(PhysicalOperator *root);

	DUCKDB_API string ToString(bool print_optimizer_output = false) const;
	DUCKDB_API void ToStream(std::ostream &str, bool print_optimizer_output = false) const;
	DUCKDB_API void Print();

	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;

	//! The format to automatically print query profiling information in (default: disabled)
	ProfilerPrintFormat automatic_print_format;
	//! The file to save query profiling information to, instead of printing it to the console (empty = print to
	//! console)
	string save_location;

private:
	//! Whether or not query profiling is enabled
	bool enabled;
	//! Whether or not the query profiler is running
	bool running;

	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<TreeNode> root;
	//! The query string
	string query;

	//! The timer used to time the execution time of the entire query
	Profiler main_query;
	//! A map of a Physical Operator pointer to a tree node
	unordered_map<PhysicalOperator *, TreeNode *> tree_map;

	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<string, double>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<string> phase_stack;

private:
	vector<PhaseTimingItem> GetOrderedPhaseTimings() const;

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(PhysicalOperatorType op_type);
};
} // namespace duckdb