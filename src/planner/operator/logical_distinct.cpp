#include "planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

using namespace duckdb;
using namespace std;

string LogicalDistinct::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (distinct_targets.size() > 0) {
		result += "[";
		for (index_t i = 0; i < distinct_targets.size(); i++) {
			auto &child = distinct_targets[i];
			result += child->GetName();
			if (i < distinct_targets.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}
