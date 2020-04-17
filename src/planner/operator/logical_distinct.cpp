#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

using namespace duckdb;
using namespace std;

string LogicalDistinct::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (distinct_targets.size() > 0) {
		result += "[";
		StringUtil::Join(distinct_targets, distinct_targets.size(), ", ",
		                 [](const unique_ptr<Expression> &child) { return child->GetName(); });
		result += "]";
	}

	return result;
}
