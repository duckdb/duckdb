#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

static LogicalType MakeEnum(const duckdb::vector<string> &values) {
	Vector data(LogicalType::VARCHAR, values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		data.SetValue(i, Value(values[i]));
	}
	return LogicalType::ENUM(data, values.size());
}

static LogicalType MakeStruct(const duckdb::vector<pair<string, LogicalType>> &children) {
	child_list_t<LogicalType> child_list;
	for (auto &child : children) {
		child_list.emplace_back(child.first, child.second);
	}
	return LogicalType::STRUCT(std::move(child_list));
}

static duckdb::vector<LogicalType> GetTestTypes() {
	duckdb::vector<LogicalType> types {LogicalType::SQLNULL,
	                           LogicalType::BOOLEAN,
	                           LogicalType::TINYINT,
	                           LogicalType::SMALLINT,
	                           LogicalType::INTEGER,
	                           LogicalType::BIGINT,
	                           LogicalType::HUGEINT,
	                           LogicalType::UTINYINT,
	                           LogicalType::USMALLINT,
	                           LogicalType::UINTEGER,
	                           LogicalType::UBIGINT,
	                           LogicalType::UHUGEINT,
	                           LogicalType::FLOAT,
	                           LogicalType::DOUBLE,
	                           LogicalType::DECIMAL(4, 1),
	                           LogicalType::DECIMAL(9, 0),
	                           LogicalType::DECIMAL(18, 3),
	                           LogicalType::DECIMAL(38, 10),
	                           LogicalType::BIGNUM,
	                           LogicalType::DATE,
	                           LogicalType::TIME,
	                           LogicalType::TIME_NS,
	                           LogicalType::TIME_TZ,
	                           LogicalType::TIMESTAMP_S,
	                           LogicalType::TIMESTAMP_MS,
	                           LogicalType::TIMESTAMP,
	                           LogicalType::TIMESTAMP_NS,
	                           LogicalType::TIMESTAMP_TZ,
	                           LogicalType::TIMESTAMP_TZ_NS,
	                           LogicalType::INTERVAL,
	                           LogicalType::VARCHAR,
	                           LogicalType::VARCHAR_COLLATION("nocase"),
	                           LogicalType::VARCHAR_COLLATION("noaccent"),
	                           LogicalType::BLOB,
	                           LogicalType::BIT,
	                           LogicalType::UUID,
	                           LogicalType::GEOMETRY(),
	                           LogicalType::VARIANT(),
	                           MakeEnum({"a", "b"}),
	                           MakeEnum({"a", "b", "c"}),
	                           LogicalType(LogicalTypeId::STRING_LITERAL),
	                           LogicalType::INTEGER_LITERAL(Value::INTEGER(42)),
	                           LogicalType::INTEGER_LITERAL(Value::BIGINT(NumericLimits<int64_t>::Maximum())),
	                           LogicalType(LogicalType::INTEGER).WithAlias("my_int"),
	                           LogicalType(LogicalType::INTEGER).WithAlias("my_other_int"),
	                           LogicalType(LogicalType::VARCHAR).WithAlias("my_text"),
	                           LogicalType::JSON()};

	// nested types over a few interesting children
	duckdb::vector<LogicalType> children {LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::BIGINT,
	                              LogicalType::DOUBLE,  LogicalType::VARCHAR,  LogicalType::DECIMAL(4, 1),
	                              LogicalType::DATE,    LogicalType::TIMESTAMP};
	for (auto &child : children) {
		types.push_back(LogicalType::LIST(child));
		types.push_back(LogicalType::ARRAY(child, 2));
		types.push_back(LogicalType::MAP(LogicalType::VARCHAR, child));
		types.push_back(MakeStruct({{"a", child}}));
		types.push_back(MakeStruct({{"a", child}, {"b", LogicalType::VARCHAR}}));
		types.push_back(LogicalType::TUPLE(duckdb::vector<LogicalType> {child}));
		types.push_back(LogicalType::UNION({{"a", child}}));
	}
	types.push_back(LogicalType::ARRAY(LogicalType::INTEGER, 3));
	types.push_back(MakeStruct({{"b", LogicalType::INTEGER}}));
	types.push_back(MakeStruct({{"b", LogicalType::VARCHAR}, {"a", LogicalType::INTEGER}}));
	types.push_back(LogicalType::UNION({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
	types.push_back(LogicalType::UNION({{"b", LogicalType::VARCHAR}}));
	types.push_back(LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER)));
	types.push_back(LogicalType::LIST(LogicalType::LIST(LogicalType::UINTEGER)));
	types.push_back(LogicalType::LIST(LogicalType::ARRAY(LogicalType::INTEGER, 2)));
	types.push_back(LogicalType::LIST(MakeStruct({{"a", LogicalType::INTEGER}})));
	types.push_back(LogicalType::LIST(MakeStruct({{"b", LogicalType::VARCHAR}})));
	return types;
}

//! Sorts the members of named STRUCTs and UNIONs - combining them is only commutative up to member order
static LogicalType SortMembers(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return LogicalType::LIST(SortMembers(ListType::GetChildType(type)));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(SortMembers(ArrayType::GetChildType(type)), ArrayType::GetSize(type));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(SortMembers(MapType::KeyType(type)), SortMembers(MapType::ValueType(type)));
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION: {
		if (type.id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(type)) {
			return type;
		}
		auto members = type.id() == LogicalTypeId::STRUCT ? StructType::GetChildTypes(type)
		                                                  : UnionType::CopyMemberTypes(type);
		for (auto &member : members) {
			member.second = SortMembers(member.second);
		}
		std::sort(members.begin(), members.end(),
		          [](const child_list_t<LogicalType>::value_type &a, const child_list_t<LogicalType>::value_type &b) {
			          return a.first < b.first;
		          });
		return type.id() == LogicalTypeId::STRUCT ? LogicalType::STRUCT(std::move(members))
		                                          : LogicalType::UNION(std::move(members));
	}
	default:
		return type;
	}
}

static bool IsImplicitlyCastable(ClientContext &context, const LogicalType &source, const LogicalType &target) {
	if (source == target) {
		return true;
	}
	return CastFunctionSet::ImplicitCastCost(context, source, target) >= 0;
}

using max_type_function_t = std::function<bool(const LogicalType &, const LogicalType &, LogicalType &)>;

static duckdb::vector<string> CheckMaxLogicalTypeProperties(ClientContext &context, const max_type_function_t &max_type) {
	duckdb::vector<string> violations;
	auto types = GetTestTypes();
	for (auto &left : types) {
		for (auto &right : types) {
			LogicalType left_right;
			LogicalType right_left;
			auto left_right_ok = max_type(left, right, left_right);
			auto right_left_ok = max_type(right, left, right_left);
			auto pair_str = left.ToString() + " | " + right.ToString();
			// property 1: the max logical type is commutative
			if (left_right_ok != right_left_ok) {
				violations.push_back(StringUtil::Format("not commutative (one side fails): %s -> %s / %s", pair_str,
				                                        left_right_ok ? left_right.ToString() : "<error>",
				                                        right_left_ok ? right_left.ToString() : "<error>"));
				continue;
			}
			if (!left_right_ok) {
				continue;
			}
			if (SortMembers(left_right) != SortMembers(right_left)) {
				violations.push_back(StringUtil::Format("not commutative: %s -> %s / %s", pair_str,
				                                        left_right.ToString(), right_left.ToString()));
			}
			// property 2: both inputs can be implicitly cast to the max logical type
			for (auto &source : {left, right}) {
				if (!IsImplicitlyCastable(context, source, left_right)) {
					violations.push_back(StringUtil::Format("not implicitly castable: %s -> %s (from %s)", pair_str,
					                                        left_right.ToString(), source.ToString()));
				}
			}
			// property 3: the max logical type is a fixpoint
			LogicalType again;
			if (!max_type(left_right, left, again) || SortMembers(again) != SortMembers(left_right)) {
				violations.push_back(StringUtil::Format("not absorbing: %s -> %s, but combining with %s gives %s",
				                                        pair_str, left_right.ToString(), left.ToString(),
				                                        again.ToString()));
			}
		}
	}
	return violations;
}

static duckdb::vector<string> CheckForceMaxLogicalTypeCommutative(ClientContext &context) {
	duckdb::vector<string> violations;
	auto types = GetTestTypes();
	for (auto &left : types) {
		for (auto &right : types) {
			auto left_right = LogicalType::ForceMaxLogicalType(context, left, right);
			auto right_left = LogicalType::ForceMaxLogicalType(context, right, left);
			if (SortMembers(left_right) != SortMembers(right_left)) {
				violations.push_back(StringUtil::Format("not commutative: %s | %s -> %s / %s", left.ToString(),
				                                        right.ToString(), left_right.ToString(),
				                                        right_left.ToString()));
			}
		}
	}
	return violations;
}

static void RequireNoViolations(const duckdb::vector<string> &violations) {
	for (auto &violation : violations) {
		UNSCOPED_INFO(violation);
	}
	REQUIRE(violations.empty());
}

TEST_CASE("Test properties of LogicalType::TryGetMaxLogicalType", "[max_logical_type]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;

	SECTION("with a client context") {
		RequireNoViolations(CheckMaxLogicalTypeProperties(
		    context, [&](const LogicalType &left, const LogicalType &right, LogicalType &result) {
			    return LogicalType::TryGetMaxLogicalType(context, left, right, result);
		    }));
	}
	SECTION("forced max logical type") {
		RequireNoViolations(CheckForceMaxLogicalTypeCommutative(context));
	}
	SECTION("legacy type unification") {
		LogicalType result;
		REQUIRE(!LogicalType::TryGetMaxLogicalType(context, LogicalType::BOOLEAN, LogicalType::INTEGER, result));
		REQUIRE(LogicalType::TryGetMaxLogicalType(context, LogicalType::TINYINT, LogicalType::UHUGEINT, result));
		REQUIRE(result == LogicalType::DOUBLE);

		REQUIRE_NO_FAIL(con.Query("SET old_type_unification = true"));
		REQUIRE(LogicalType::TryGetMaxLogicalType(context, LogicalType::BOOLEAN, LogicalType::INTEGER, result));
		REQUIRE(result == LogicalType::INTEGER);
		REQUIRE(LogicalType::TryGetMaxLogicalType(context, LogicalType::TINYINT, LogicalType::UHUGEINT, result));
		REQUIRE(result == LogicalType::SMALLINT);
	}
	SECTION("without a client context") {
		RequireNoViolations(CheckMaxLogicalTypeProperties(
		    context, [&](const LogicalType &left, const LogicalType &right, LogicalType &result) {
			    return LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(left, right, result);
		    }));
	}
}
