
#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalProjection::GetNames() {
	vector<string> names;
	for (auto &exp : expressions) {
		names.push_back(exp->GetName());
	}
	return names;
}