#include "duckdb/common/enums/join_type.hpp"

using namespace std;

namespace duckdb {

string JoinTypeToString(JoinType type) {
	switch (type) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "OUTER";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID:
	default:
		return "INVALID";
	}
}

} // namespace duckdb
