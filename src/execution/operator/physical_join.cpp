
#include "execution/operator/physical_join.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalJoin::GetNames() {
	auto left = children[0]->GetNames();
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		auto right = children[1]->GetNames();
		left.insert(left.end(), right.begin(), right.end());
	}
	return left;
}

vector<TypeId> PhysicalJoin::GetTypes() {
	auto types = children[0]->GetTypes();
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		auto right_types = children[1]->GetTypes();
		types.insert(types.end(), right_types.begin(), right_types.end());
	}
	return types;
}

string PhysicalJoin::ExtraRenderInformation() {
	string extra_info = JoinTypeToString(type) + "\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->ToString() + op + it.right->ToString() + "\n";
	}
	return extra_info;
}
