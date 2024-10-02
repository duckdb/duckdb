#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

bool IsLeftOuterJoin(JoinType type) {
	return type == JoinType::LEFT || type == JoinType::OUTER;
}

bool IsRightOuterJoin(JoinType type) {
	return type == JoinType::OUTER || type == JoinType::RIGHT;
}

bool PropagatesBuildSide(JoinType type) {
	return type == JoinType::OUTER || type == JoinType::RIGHT || type == JoinType::RIGHT_ANTI ||
	       type == JoinType::RIGHT_SEMI;
}

bool HasInverseJoinType(JoinType type) {
	return type != JoinType::SINGLE && type != JoinType::MARK;
}

JoinType InverseJoinType(JoinType type) {
	D_ASSERT(HasInverseJoinType(type));
	switch (type) {
	case JoinType::LEFT:
		return JoinType::RIGHT;
	case JoinType::RIGHT:
		return JoinType::LEFT;
	case JoinType::INNER:
		return JoinType::INNER;
	case JoinType::OUTER:
		return JoinType::OUTER;
	case JoinType::SEMI:
		return JoinType::RIGHT_SEMI;
	case JoinType::ANTI:
		return JoinType::RIGHT_ANTI;
	case JoinType::RIGHT_SEMI:
		return JoinType::SEMI;
	case JoinType::RIGHT_ANTI:
		return JoinType::ANTI;
	default:
		throw NotImplementedException("InverseJoinType for JoinType::%s", EnumUtil::ToString(type));
	}
}

// **DEPRECATED**: Use EnumUtil directly instead.
string JoinTypeToString(JoinType type) {
	return EnumUtil::ToString(type);
}

} // namespace duckdb
