#include "duckdb/function/cast_rules.hpp"

namespace duckdb {

//! The target type determines the preferred implicit casts
static int64_t TargetTypeCost(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		return 103;
	case LogicalTypeId::BIGINT:
		return 101;
	case LogicalTypeId::DOUBLE:
		return 102;
	case LogicalTypeId::HUGEINT:
		return 120;
	case LogicalTypeId::TIMESTAMP:
		return 120;
	case LogicalTypeId::VARCHAR:
		return 149;
	case LogicalTypeId::DECIMAL:
		return 104;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
		return 160;
	default:
		return 110;
	}
}

static int64_t ImplicitCastTinyint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastSmallint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastInteger(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastBigint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastUTinyint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastUSmallint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastUInteger(const LogicalType &to) {
	switch (to.id()) {

	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastUBigint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastFloat(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::DOUBLE:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastDouble(const LogicalType &to) {
	switch (to.id()) {
	default:
		return -1;
	}
}

static int64_t ImplicitCastDecimal(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastHugeint(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastDate(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::TIMESTAMP:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

int64_t CastRules::ImplicitCast(const LogicalType &from, const LogicalType &to) {
	if (to.id() == LogicalTypeId::ANY) {
		// anything can be cast to ANY type for no cost
		return 0;
	}
	if (from.id() == LogicalTypeId::SQLNULL) {
		// NULL expression can be cast to anything
		return TargetTypeCost(to);
	}
	if (from.id() == LogicalTypeId::UNKNOWN) {
		// parameter expression can be cast to anything for no cost
		return 0;
	}
	if (from.id() == LogicalTypeId::BLOB && to.id() == LogicalTypeId::VARCHAR) {
		// Implicit cast not allowed from BLOB to VARCHAR
		return -1;
	}
	if ((from.id() == LogicalTypeId::VARCHAR && to.id() == LogicalTypeId::JSON) ||
	    (from.id() == LogicalTypeId::JSON && to.id() == LogicalTypeId::VARCHAR)) {
		// Virtually no cost, just a different tag
		return 1;
	}
	if (to.id() == LogicalTypeId::VARCHAR || to.id() == LogicalTypeId::JSON) {
		// everything can be cast to VARCHAR/JSON, but this cast has a high cost
		return TargetTypeCost(to);
	}
	if (from.id() == LogicalTypeId::LIST && to.id() == LogicalTypeId::LIST) {
		// Lists can be cast if their child types can be cast
		return ImplicitCast(ListType::GetChildType(from), ListType::GetChildType(to));
	}
	if ((from.id() == LogicalTypeId::TIMESTAMP_SEC || from.id() == LogicalTypeId::TIMESTAMP_MS ||
	     from.id() == LogicalTypeId::TIMESTAMP_NS) &&
	    to.id() == LogicalTypeId::TIMESTAMP) {
		//! Any timestamp type can be converted to the default (us) type at low cost
		return 101;
	}
	if ((to.id() == LogicalTypeId::TIMESTAMP_SEC || to.id() == LogicalTypeId::TIMESTAMP_MS ||
	     to.id() == LogicalTypeId::TIMESTAMP_NS) &&
	    from.id() == LogicalTypeId::TIMESTAMP) {
		//! Any timestamp type can be converted to the default (us) type at low cost
		return 100;
	}
	switch (from.id()) {
	case LogicalTypeId::TINYINT:
		return ImplicitCastTinyint(to);
	case LogicalTypeId::SMALLINT:
		return ImplicitCastSmallint(to);
	case LogicalTypeId::INTEGER:
		return ImplicitCastInteger(to);
	case LogicalTypeId::BIGINT:
		return ImplicitCastBigint(to);
	case LogicalTypeId::UTINYINT:
		return ImplicitCastUTinyint(to);
	case LogicalTypeId::USMALLINT:
		return ImplicitCastUSmallint(to);
	case LogicalTypeId::UINTEGER:
		return ImplicitCastUInteger(to);
	case LogicalTypeId::UBIGINT:
		return ImplicitCastUBigint(to);
	case LogicalTypeId::HUGEINT:
		return ImplicitCastHugeint(to);
	case LogicalTypeId::FLOAT:
		return ImplicitCastFloat(to);
	case LogicalTypeId::DOUBLE:
		return ImplicitCastDouble(to);
	case LogicalTypeId::DATE:
		return ImplicitCastDate(to);
	case LogicalTypeId::DECIMAL:
		return ImplicitCastDecimal(to);
	default:
		return -1;
	}
}

} // namespace duckdb
