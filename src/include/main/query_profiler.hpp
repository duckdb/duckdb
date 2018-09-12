//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// main/query_profiler.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stack>
#include <unordered_map>

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/profiler.hpp"
#include "common/string_util.hpp"

namespace duckdb {
//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler : public Printable {
  public:
	void StartQuery(std::string query) {
		this->query = query;
		operator_timings.clear();

		main_query.Start();
	}

	void EndQuery() { main_query.End(); }

	void StartOperator(PhysicalOperatorType type) {
		if (!types.empty()) {
			// add timing for the previous element
			op.End();
			operator_timings[(int)types.top()] += op.Elapsed();
		}

		// push new element onto stack
		types.push(type);

		// start timing for current element
		op.Start();
	}

	void EndOperator() {
		op.End();
		operator_timings[(int)types.top()] += op.Elapsed();

		assert(!types.empty());
		types.pop();

		// start timing again for the previous element, if any
		if (!types.empty()) {
			op.Start();
		}
	}

	virtual std::string ToString() const override {
		if (query.empty()) {
			return "<<Empty Profiling Information>>";
		}
		std::string result = "<<Query Profiling Information>>\n";
		result += StringUtil::Replace(query, "\n", "") + "\n";
		result += "<<Timing>>\n";
		result += "Total Time: " + std::to_string(main_query.Elapsed()) + "s\n";
		result += "<<Operators>>\n";
		// add timing for individual operators
		// sort by most time consuming
		std::vector<std::pair<int, double>> values;
		for (auto &it : operator_timings) {
			values.push_back(it);
		}
		std::sort(
		    values.begin(), values.end(),
		    [](std::pair<int, double> &left, std::pair<int, double> &right) {
			    return left.second > right.second;
		    });
		for (auto &it : values) {
			result += PhysicalOperatorToString((PhysicalOperatorType)it.first) +
			          ": " + std::to_string(it.second) + "s\n";
		}
		return result;
	}

  private:
	std::string query;

	Profiler main_query;

	Profiler op;
	std::unordered_map<int, double> operator_timings;
	std::stack<PhysicalOperatorType> types;
};
} // namespace duckdb
