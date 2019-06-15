#include "planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

using namespace duckdb;
using namespace std;

/*void LogicalDistinct::ResolveTypes() {
    for (auto &expr : distinct_targets) {
        types.push_back(expr->return_type);
    }
    // get the chunk types from the projection list
    for (auto &expr : expressions) {
        types.push_back(expr->return_type);
    }
}*/

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
