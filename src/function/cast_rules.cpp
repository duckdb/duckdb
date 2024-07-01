#include "duckdb/function/cast_rules.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

//! The target type determines the preferred implicit casts
static int64_t TargetTypeCost(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BIGINT:
		return 101;
	case LogicalTypeId::INTEGER:
		return 102;
	case LogicalTypeId::HUGEINT:
		return 103;
	case LogicalTypeId::DOUBLE:
		return 104;
	case LogicalTypeId::DECIMAL:
		return 105;
	case LogicalTypeId::TIMESTAMP_NS:
		return 119;
	case LogicalTypeId::TIMESTAMP:
		return 120;
	case LogicalTypeId::TIMESTAMP_MS:
		return 121;
	case LogicalTypeId::TIMESTAMP_SEC:
		return 122;
	case LogicalTypeId::TIMESTAMP_TZ:
		return 123;
	case LogicalTypeId::VARCHAR:
		return 149;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UNION:
	case LogicalTypeId::ARRAY:
		return 160;
	case LogicalTypeId::ANY:
		return int64_t(AnyType::GetCastScore(type));
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
	case LogicalTypeId::UHUGEINT:
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
	case LogicalTypeId::UHUGEINT:
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
	case LogicalTypeId::UHUGEINT:
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
	case LogicalTypeId::UHUGEINT:
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

static int64_t ImplicitCastUhugeint(const LogicalType &to) {
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
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastEnum(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::VARCHAR:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastTimestampSec(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastTimestampMS(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastTimestampNS(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::TIMESTAMP:
		// we allow casting ALL timestamps, including nanosecond ones, to TimestampNS
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

static int64_t ImplicitCastTimestamp(const LogicalType &to) {
	switch (to.id()) {
	case LogicalTypeId::TIMESTAMP_NS:
		return TargetTypeCost(to);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TargetTypeCost(to);
	default:
		return -1;
	}
}

bool LogicalTypeIsValid(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::DECIMAL:
		// these types are only valid with auxiliary info
		if (!type.AuxInfo()) {
			return false;
		}
		break;
	default:
		break;
	}
	switch (type.id()) {
	case LogicalTypeId::ANY:
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
		return false;
	case LogicalTypeId::STRUCT: {
		auto child_count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < child_count; i++) {
			if (!LogicalTypeIsValid(StructType::GetChildType(type, i))) {
				return false;
			}
		}
		return true;
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < member_count; i++) {
			if (!LogicalTypeIsValid(UnionType::GetMemberType(type, i))) {
				return false;
			}
		}
		return true;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return LogicalTypeIsValid(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return LogicalTypeIsValid(ArrayType::GetChildType(type));
	default:
		return true;
	}
}

int64_t CastRules::ImplicitCast(const LogicalType &from, const LogicalType &to) {
	if (from.id() == LogicalTypeId::SQLNULL || to.id() == LogicalTypeId::ANY) {
		// NULL expression can be cast to anything
		return TargetTypeCost(to);
	}
	if (from.id() == LogicalTypeId::UNKNOWN) {
		// parameter expression can be cast to anything for no cost
		return 0;
	}
	if (from.id() == LogicalTypeId::STRING_LITERAL) {
		// string literals can be cast to any type for low cost as long as the type is valid
		// i.e. we cannot cast to LIST(ANY) as we don't know what "ANY" should be
		// we cannot cast to DECIMAL without precision/width specified
		if (!LogicalTypeIsValid(to)) {
			return -1;
		}
		if (to.id() == LogicalTypeId::VARCHAR && to.GetAlias().empty()) {
			return 1;
		}
		return 20;
	}
	if (from.id() == LogicalTypeId::INTEGER_LITERAL) {
		// the integer literal has an underlying type - this type always matches
		if (IntegerLiteral::GetType(from).id() == to.id()) {
			return 0;
		}
		// integer literals can be cast to any other integer type for a low cost, but only if the literal fits
		if (IntegerLiteral::FitsInType(from, to)) {
			// to avoid ties we prefer BIGINT, INT, ...
			auto target_cost = TargetTypeCost(to);
			if (target_cost < 100) {
				throw InternalException("Integer literal implicit cast - TargetTypeCost should be >= 100");
			}
			return target_cost - 90;
		}
		// in any other case we use the casting rules of the preferred type of the literal
		return CastRules::ImplicitCast(IntegerLiteral::GetType(from), to);
	}
	if (from.GetAlias() != to.GetAlias()) {
		// if aliases are different, an implicit cast is not possible
		return -1;
	}
	if (from.id() == LogicalTypeId::LIST && to.id() == LogicalTypeId::LIST) {
		// Lists can be cast if their child types can be cast
		auto child_cost = ImplicitCast(ListType::GetChildType(from), ListType::GetChildType(to));
		if (child_cost >= 1) {
			// subtract one from the cost because we prefer LIST[X] -> LIST[VARCHAR] over LIST[X] -> VARCHAR
			child_cost--;
		}
		return child_cost;
	}
	if (from.id() == LogicalTypeId::ARRAY && to.id() == LogicalTypeId::ARRAY) {
		// Arrays can be cast if their child types can be cast and the source and target has the same size
		// or the target type has a unknown (any) size.
		auto from_size = ArrayType::GetSize(from);
		auto to_size = ArrayType::GetSize(to);
		auto to_is_any_size = ArrayType::IsAnySize(to);
		if (from_size == to_size || to_is_any_size) {
			auto child_cost = ImplicitCast(ArrayType::GetChildType(from), ArrayType::GetChildType(to));
			if (child_cost >= 100) {
				// subtract one from the cost because we prefer ARRAY[X] -> ARRAY[VARCHAR] over ARRAY[X] -> VARCHAR
				child_cost--;
			}
			return child_cost;
		}
		return -1; // Not possible if the sizes are different
	}
	if (from.id() == LogicalTypeId::ARRAY && to.id() == LogicalTypeId::LIST) {
		// Arrays can be cast to lists for the cost of casting the child type
		auto child_cost = ImplicitCast(ArrayType::GetChildType(from), ListType::GetChildType(to));
		if (child_cost < 0) {
			return -1;
		}
		// add 1 because we prefer ARRAY->ARRAY casts over ARRAY->LIST casts
		return child_cost + 1;
	}
	if (from.id() == LogicalTypeId::LIST && (to.id() == LogicalTypeId::ARRAY && !ArrayType::IsAnySize(to))) {
		// Lists can be cast to arrays for the cost of casting the child type, if the target size is known
		// there is no way for us to resolve the size at bind-time without inspecting the list values.
		// TODO: if we can access the expression we could resolve the size if the list is constant.
		return ImplicitCast(ListType::GetChildType(from), ArrayType::GetChildType(to));
	}
	if (from.id() == to.id()) {
		// arguments match: do nothing
		return 0;
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

					int child_cost = NumericCast<int>(ImplicitCast(from_member_type, to_member_type));
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
	case LogicalTypeId::UHUGEINT:
		return ImplicitCastUhugeint(to);
	case LogicalTypeId::FLOAT:
		return ImplicitCastFloat(to);
	case LogicalTypeId::DOUBLE:
		return ImplicitCastDouble(to);
	case LogicalTypeId::DATE:
		return ImplicitCastDate(to);
	case LogicalTypeId::DECIMAL:
		return ImplicitCastDecimal(to);
	case LogicalTypeId::ENUM:
		return ImplicitCastEnum(to);
	case LogicalTypeId::TIMESTAMP_SEC:
		return ImplicitCastTimestampSec(to);
	case LogicalTypeId::TIMESTAMP_MS:
		return ImplicitCastTimestampMS(to);
	case LogicalTypeId::TIMESTAMP_NS:
		return ImplicitCastTimestampNS(to);
	case LogicalTypeId::TIMESTAMP:
		return ImplicitCastTimestamp(to);
	default:
		return -1;
	}
}

} // namespace duckdb
