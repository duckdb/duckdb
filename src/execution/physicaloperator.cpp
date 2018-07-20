
#include "execution/physicaloperator.hpp"

using namespace duckdb;
using namespace std;

void PhysicalOperator::InitializeChunk(DataChunk& chunk) {

}


string PhysicalOperator::ToString() const {
	string result = PhysicalOperatorToString(type);
	if (children.size() > 0) {
		result += " ( ";
		for (auto& child : children) {
			result += child->ToString();
		}
		result += " )";
	}
	return result;
}