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
	case LogicalTypeId::UNION:
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
	if (from.id() == LogicalTypeId::SQLNULL) {
		// NULL expression can be cast to anything
		return TargetTypeCost(to);
	}
	if (from.id() == LogicalTypeId::UNKNOWN) {
		// parameter expression can be cast to anything for no cost
		return 0;
	}
	if (to.id() == LogicalTypeId::ANY) {
		// anything can be cast to ANY type for (almost no) cost
		return 1;
	}
	if (from.GetAlias() != to.GetAlias()) {
		// if aliases are different, an implicit cast is not possible
		return -1;
	}
	if (from.id() == LogicalTypeId::LIST && to.id() == LogicalTypeId::LIST) {
		// Lists can be cast if their child types can be cast
		auto child_cost = ImplicitCast(ListType::GetChildType(from), ListType::GetChildType(to));
		if (child_cost >= 100) {
			// subtract one from the cost because we prefer LIST[X] -> LIST[VARCHAR] over LIST[X] -> VARCHAR
			child_cost--;
		}
		return child_cost;
	}
	if (from.id() == to.id()) {
		// arguments match: do nothing
		return 0;
	}
	if (from.id() == LogicalTypeId::BLOB && to.id() == LogicalTypeId::VARCHAR) {
		// Implicit cast not allowed from BLOB to VARCHAR
		return -1;
	}
	if (to.id() == LogicalTypeId::VARCHAR) {
		// everything can be cast to VARCHAR, but this cast has a high cost
		return TargetTypeCost(to);
	}

	if (from.id() == LogicalTypeId::UNION && to.id() == LogicalTypeId::UNION) {
		// Unions can be cast if the source tags are a subset of the target tags
		// in which case the most expensive cost is used
		int cost = -1;
		for (idx_t from_member_idx = 0; from_member_idx < UnionType::GetMemberCount(from); from_member_idx++) {
			auto &from_member_name = UnionType::GetMemberName(from, from_member_idx);

			bool found = false;
			for (idx_t to_member_idx = 0; to_member_idx < UnionType::GetMemberCount(to); to_member_idx++) {
				auto &to_member_name = UnionType::GetMemberName(to, to_member_idx);

				if (from_member_name == to_member_name) {
					auto &from_member_type = UnionType::GetMemberType(from, from_member_idx);
					auto &to_member_type = UnionType::GetMemberType(to, to_member_idx);

					int child_cost = ImplicitCast(from_member_type, to_member_type);
					if (child_cost > cost) {
						cost = child_cost;
					}
					found = true;
					break;
				}
			}
			if (!found) {
				return -1;
			}
		}
		return cost;
	}

	if (to.id() == LogicalTypeId::UNION) {
		// check that the union type is fully resolved.
		if (to.AuxInfo() == nullptr) {
			return -1;
		}
		// every type can be implicitly be cast to a union if the source type is a member of the union
		for (idx_t i = 0; i < UnionType::GetMemberCount(to); i++) {
			auto member = UnionType::GetMemberType(to, i);
			if (from == member) {
				return 0;
			}
		}
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
