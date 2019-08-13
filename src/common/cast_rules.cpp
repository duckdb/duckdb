#include "common/cast_rules.hpp"

using namespace duckdb;
using namespace std;

static bool ImplicitCastTinyint(SQLType to) {
	switch(to.id) {
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

static bool ImplicitCastSmallint(SQLType to) {
	switch(to.id) {
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

static bool ImplicitCastInteger(SQLType to) {
	switch(to.id) {
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

static bool ImplicitCastBigint(SQLType to) {
	switch(to.id) {
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

static bool ImplicitCastFloat(SQLType to) {
	switch(to.id) {
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

bool CastRules::ImplicitCast(SQLType from, SQLType to) {
	if (from.id == to.id) {
		return true;
	}
	if (from.id == SQLTypeId::SQLNULL) {
		// NULL can be cast to anything
		return true;
	}
	switch(from.id) {
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
	default:
		return false;
	}
}
