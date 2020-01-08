#include "duckdb/function/cast_rules.hpp"

using namespace duckdb;
using namespace std;

//! The target type determines the preferred implicit casts
static int64_t TargetTypeCost(SQLType type) {
	switch (type.id) {
	case SQLTypeId::INTEGER:
		return 103;
	case SQLTypeId::BIGINT:
		return 101;
	case SQLTypeId::DOUBLE:
		return 102;
	case SQLTypeId::VARCHAR:
		return 199;
	default:
		return 110;
	}
}

static int64_t ImplicitCastTinyint(SQLType to) {
	switch (to.id) {
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastSmallint(SQLType to) {
	switch (to.id) {
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastInteger(SQLType to) {
	switch (to.id) {
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastBigint(SQLType to) {
	switch (to.id) {
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastFloat(SQLType to) {
	switch (to.id) {
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastDouble(SQLType to) {
	switch (to.id) {
	case SQLTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

int64_t CastRules::ImplicitCast(SQLType from, SQLType to) {
	if (to.id == SQLTypeId::ANY) {
		// anything can be cast to ANY type for no cost
		return 0;
	}
	if (from.id == SQLTypeId::SQLNULL || from.id == SQLTypeId::UNKNOWN) {
		// NULL expression or parameter expression can be cast to anything
		return TargetTypeCost(to);
	}
	if (to.id == SQLTypeId::VARCHAR) {
		// everything can be cast to VARCHAR, but this cast has a high cost
		return TargetTypeCost(to);
	}
	switch (from.id) {
	case SQLTypeId::TINYINT:
		return ImplicitCastTinyint(to);
	case SQLTypeId::SMALLINT:
		return ImplicitCastSmallint(to);
	case SQLTypeId::INTEGER:
		return ImplicitCastInteger(to);
	case SQLTypeId::BIGINT:
		return ImplicitCastBigint(to);
	case SQLTypeId::FLOAT:
		return ImplicitCastFloat(to);
	case SQLTypeId::DOUBLE:
		return ImplicitCastDouble(to);
	default:
		return -1;
	}
}
