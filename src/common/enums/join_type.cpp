#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/serializer/enum_serializer.hpp"

namespace duckdb {

string JoinTypeToString(JoinType type) {
	return EnumSerializer::EnumToString(type);
}

template <>
const char *EnumSerializer::EnumToString(JoinType value) {
	switch (value) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "FULL";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID:
		return "INVALID";
	}
	return "INVALID";
}

template <>
JoinType EnumSerializer::StringToEnum(const char *value) {
	if (strcmp(value, "LEFT") == 0) {
		return JoinType::LEFT;
	} else if (strcmp(value, "RIGHT") == 0) {
		return JoinType::RIGHT;
	} else if (strcmp(value, "INNER") == 0) {
		return JoinType::INNER;
	} else if (strcmp(value, "FULL") == 0) {
		return JoinType::OUTER;
	} else if (strcmp(value, "SEMI") == 0) {
		return JoinType::SEMI;
	} else if (strcmp(value, "ANTI") == 0) {
		return JoinType::ANTI;
	} else if (strcmp(value, "SINGLE") == 0) {
		return JoinType::SINGLE;
	} else if (strcmp(value, "MARK") == 0) {
		return JoinType::MARK;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

bool IsLeftOuterJoin(JoinType type) {
	return type == JoinType::LEFT || type == JoinType::OUTER;
}

bool IsRightOuterJoin(JoinType type) {
	return type == JoinType::OUTER || type == JoinType::RIGHT;
}

} // namespace duckdb
