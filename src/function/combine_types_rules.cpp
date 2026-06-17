#include "duckdb/function/combine_types_rule.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace duckdb {

// ===========================================
// ================== ALIASES ================
// ===========================================
static bool CombineNoAliasTypes(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                                const LogicalType &right, LogicalType &result) {
	// we always prefer aliased types
	if (!left.GetAlias().empty()) {
		result = left;
		return true;
	}
	result = right;
	return true;
}

static bool MatchesNoAlias(const LogicalType &left, const LogicalType &right) {
	return !left.GetAlias().empty() || !right.GetAlias().empty();
}

// ===========================================
// ================== EQUAL ==================
// ===========================================
static bool CombineStructTypes(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                               const LogicalType &right, LogicalType &result) {
	auto &left_children = StructType::GetChildTypes(left);
	auto &right_children = StructType::GetChildTypes(right);

	auto left_unnamed = StructType::IsUnnamed(left);
	auto is_unnamed = left_unnamed || StructType::IsUnnamed(right);
	child_list_t<LogicalType> child_types;

	// At least one side is unnamed, so we attempt positional casting.
	if (is_unnamed) {
		if (left_children.size() != right_children.size()) {
			// We can't cast, or create the super-set.
			return false;
		}

		for (idx_t i = 0; i < left_children.size(); i++) {
			LogicalType child_type;
			if (!logical_type_resolver.Operation(left_children[i].second, right_children[i].second, child_type)) {
				return false;
			}
			auto &child_name = left_unnamed ? right_children[i].first : left_children[i].first;
			child_types.emplace_back(child_name, std::move(child_type));
		}
		result = LogicalType::STRUCT(child_types);
		return true;
	}

	// Create a super-set of the STRUCT fields.
	// First, create a name->index map of the right children.
	InsertionOrderPreservingMap<idx_t, Identifier, identifier_map_t<idx_t>> right_children_map;
	for (idx_t i = 0; i < right_children.size(); i++) {
		auto &name = right_children[i].first;
		right_children_map[name] = i;
	}

	for (idx_t i = 0; i < left_children.size(); i++) {
		auto &left_child = left_children[i];
		auto right_child_it = right_children_map.find(left_child.first);

		if (right_child_it == right_children_map.end()) {
			// We can directly put the left child.
			child_types.emplace_back(left_child.first, left_child.second);
			continue;
		}

		// We need to recurse to ensure the children have a maximum logical type.
		LogicalType child_type;
		auto &right_child = right_children[right_child_it->second];
		if (!logical_type_resolver.Operation(left_child.second, right_child.second, child_type)) {
			return false;
		}
		child_types.emplace_back(left_child.first, std::move(child_type));
		right_children_map.erase(right_child_it);
	}

	// Add all remaining right children.
	for (const auto &right_child_it : right_children_map) {
		auto &right_child = right_children[right_child_it.second];
		child_types.emplace_back(right_child.first, right_child.second);
	}

	result = LogicalType::STRUCT(child_types);
	return true;
}

static bool CombineEqualStringLiteral(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                                      const LogicalType &right, LogicalType &result) {
	result = LogicalType::VARCHAR;
	return true;
}
static bool CombineEqualIntegerLiteral(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                                       const LogicalType &right, LogicalType &result) {
	// for two integer literals we unify the underlying types
	return logical_type_resolver.Operation(IntegerLiteral::GetType(left), IntegerLiteral::GetType(right), result);
}
static bool CombineEqualEnum(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                             const LogicalType &right, LogicalType &result) {
	// If both types are different ENUMs we do a string comparison.
	result = left == right ? left : LogicalType::VARCHAR;
	return true;
}
static bool CombineEqualVarchar(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                                const LogicalType &right, LogicalType &result) {
	// varchar: use type that has collation (if any)
	if (StringType::GetCollation(right).empty()) {
		result = left;
	} else {
		result = right;
	}
	return true;
}
static bool CombineEqualDecimal(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                                const LogicalType &right, LogicalType &result) {
	// unify the width/scale so that the resulting decimal always fits
	// "width - scale" gives us the number of digits on the left side of the decimal point
	// "scale" gives us the number of digits allowed on the right of the decimal point
	// using the max of these of the two types gives us the new decimal size
	auto extra_width_left = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
	auto extra_width_right = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
	auto extra_width =
	    MaxValue<uint8_t>(NumericCast<uint8_t>(extra_width_left), NumericCast<uint8_t>(extra_width_right));
	auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
	auto width = NumericCast<uint8_t>(extra_width + scale);
	if (width > DecimalType::MaxWidth()) {
		// if the resulting decimal does not fit, we truncate the scale
		width = DecimalType::MaxWidth();
		scale = NumericCast<uint8_t>(width - extra_width);
	}
	result = LogicalType::DECIMAL(width, scale);
	return true;
}
static bool CombineEqualList(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                             const LogicalType &right, LogicalType &result) {
	// list: perform max recursively on child type
	LogicalType new_child;
	if (!logical_type_resolver.Operation(ListType::GetChildType(left), ListType::GetChildType(right), new_child)) {
		return false;
	}
	result = LogicalType::LIST(new_child);
	return true;
}
static bool CombineEqualArray(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                              const LogicalType &right, LogicalType &result) {
	LogicalType new_child;
	if (!logical_type_resolver.Operation(ArrayType::GetChildType(left), ArrayType::GetChildType(right), new_child)) {
		return false;
	}
	auto new_size = MaxValue(ArrayType::GetSize(left), ArrayType::GetSize(right));
	result = LogicalType::ARRAY(new_child, new_size);
	return true;
}
static bool CombineEqualMap(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                            const LogicalType &right, LogicalType &result) {
	// map: perform max recursively on child type
	LogicalType new_child;
	if (!logical_type_resolver.Operation(ListType::GetChildType(left), ListType::GetChildType(right), new_child)) {
		return false;
	}
	result = LogicalType::MAP(new_child);
	return true;
}
static bool CombineEqualUnion(LogicalTypeResolver &logical_type_resolver, const LogicalType &left,
                              const LogicalType &right, LogicalType &result) {
	auto left_member_count = UnionType::GetMemberCount(left);
	auto right_member_count = UnionType::GetMemberCount(right);
	if (left_member_count != right_member_count) {
		// return the "larger" type, with the most members
		result = left_member_count > right_member_count ? left : right;
		return true;
	}
	// otherwise, keep left, don't try to meld the two together.
	result = left;
	return true;
}

static bool EqualMatchesStringLiteral(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::STRING_LITERAL;
}

static bool EqualMatchesIntegerLiteral(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::INTEGER_LITERAL;
}

static bool EqualMatchesEnum(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::ENUM;
}

static bool EqualMatchesVarchar(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::VARCHAR;
}

static bool EqualMatchesDecimal(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::DECIMAL;
}

static bool EqualMatchesList(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::LIST;
}

static bool EqualMatchesArray(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::ARRAY;
}

static bool EqualMatchesMap(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::MAP;
}

static bool EqualMatchesStruct(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::STRUCT;
}

static bool EqualMatchesUnion(const LogicalType &left, const LogicalType &right) {
	if (left.id() != right.id()) {
		// rule only applies to equal types
		return false;
	}
	return left.id() == LogicalTypeId::UNION;
}

// ===========================================
// ================== UNEQUAL ================
// ===========================================

static bool CombineNullOrUnknown(LogicalTypeResolver &, const LogicalType &left, const LogicalType &right,
                                 LogicalType &result) {
	// NULL/unknown (parameter) types always take the other type
	if (left.id() == LogicalTypeId::SQLNULL || right.id() == LogicalTypeId::SQLNULL) {
		result = LogicalType::NormalizeType(left.id() == LogicalTypeId::SQLNULL ? right : left);
	} else {
		result = LogicalType::NormalizeType(left.id() == LogicalTypeId::UNKNOWN ? right : left);
	}
	return true;
}

static bool CombineEnum(LogicalTypeResolver &logical_type_resolver, const LogicalType &left, const LogicalType &right,
                        LogicalType &result) {
	// for enums, match the varchar rules
	if (left.id() == LogicalTypeId::ENUM) {
		return logical_type_resolver.Operation(LogicalType::VARCHAR, right, result);
	}
	return logical_type_resolver.Operation(left, LogicalType::VARCHAR, result);
}

static bool CombineVariant(LogicalTypeResolver &, const LogicalType &left, const LogicalType &right,
                           LogicalType &result) {
	result = right.id() == LogicalTypeId::VARIANT ? right : left;
	return true;
}

// for everything but enums - string literals also take the other type
static bool CombineStringLiteral(LogicalTypeResolver &, const LogicalType &left, const LogicalType &right,
                                 LogicalType &result) {
	result = LogicalType::NormalizeType(left.id() == LogicalTypeId::STRING_LITERAL ? right : left);
	return true;
}

static bool UnequalMatchesVariant(const LogicalType &left, const LogicalType &right) {
	if (left.id() == right.id()) {
		// rule only applies to unequal types
		return false;
	}
	return left.id() == LogicalTypeId::VARIANT || right.id() == LogicalTypeId::VARIANT;
}

static bool UnequalMatchesNullOrUnknown(const LogicalType &left, const LogicalType &right) {
	if (left.id() == right.id()) {
		// rule only applies to unequal types
		return false;
	}
	auto null_or_unknown = [](const LogicalType &type) {
		return type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::UNKNOWN;
	};
	return null_or_unknown(left) || null_or_unknown(right);
}

static bool UnequalMatchesEnum(const LogicalType &left, const LogicalType &right) {
	if (left.id() == right.id()) {
		// rule only applies to unequal types
		return false;
	}
	return left.id() == LogicalTypeId::ENUM || right.id() == LogicalTypeId::ENUM;
}

static bool UnequalMatchesStringLiteral(const LogicalType &left, const LogicalType &right) {
	if (left.id() == right.id()) {
		// rule only applies to unequal types
		return false;
	}
	return left.id() == LogicalTypeId::STRING_LITERAL || right.id() == LogicalTypeId::STRING_LITERAL;
}

// Rules are matched in order, so the first matching rule wins
const vector<CombineTypesRule> &DefaultCombineTypesRules() {
	static const vector<CombineTypesRule> rules = {
	    {MatchesNoAlias, CombineNoAliasTypes},
	    {UnequalMatchesVariant, CombineVariant},
	    {UnequalMatchesNullOrUnknown, CombineNullOrUnknown},
	    {UnequalMatchesEnum, CombineEnum},
	    {UnequalMatchesStringLiteral, CombineStringLiteral},
	    {EqualMatchesStringLiteral, CombineEqualStringLiteral},
	    {EqualMatchesIntegerLiteral, CombineEqualIntegerLiteral},
	    {EqualMatchesEnum, CombineEqualEnum},
	    {EqualMatchesVarchar, CombineEqualVarchar},
	    {EqualMatchesDecimal, CombineEqualDecimal},
	    {EqualMatchesList, CombineEqualList},
	    {EqualMatchesArray, CombineEqualArray},
	    {EqualMatchesMap, CombineEqualMap},
	    {EqualMatchesStruct, CombineStructTypes},
	    {EqualMatchesUnion, CombineEqualUnion},
	};
	return rules;
}

} // namespace duckdb
