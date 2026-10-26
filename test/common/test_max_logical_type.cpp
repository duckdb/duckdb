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

//! The children of the nested test types
static duckdb::vector<LogicalType> GetNestedChildTypes() {
	return {LogicalType::INTEGER, LogicalType::UINTEGER,      LogicalType::BIGINT, LogicalType::DOUBLE,
	        LogicalType::VARCHAR, LogicalType::DECIMAL(4, 1), LogicalType::DATE,   LogicalType::TIMESTAMP};
}

static duckdb::vector<LogicalType> GetTestTypes(const duckdb::vector<LogicalType> &children = GetNestedChildTypes()) {
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
		auto members =
		    type.id() == LogicalTypeId::STRUCT ? StructType::GetChildTypes(type) : UnionType::CopyMemberTypes(type);
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

static duckdb::vector<string> CheckMaxLogicalTypeProperties(ClientContext &context,
                                                            const max_type_function_t &max_type) {
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

//! Checks that combining a set of three types does not depend on their order (up to the member order of the result)
static duckdb::vector<string> CheckMaxLogicalTypeOrderIndependent(ClientContext &context,
                                                                  const duckdb::vector<LogicalType> &types) {
	duckdb::vector<string> violations;
	auto set_string = [&](const idx_t(&indexes)[3]) {
		return types[indexes[0]].ToString() + " | " + types[indexes[1]].ToString() + " | " +
		       types[indexes[2]].ToString();
	};
	for (idx_t a = 0; a < types.size(); a++) {
		for (idx_t b = a; b < types.size(); b++) {
			for (idx_t c = b; c < types.size(); c++) {
				// iterate over all orders of the set by permuting the indexes
				idx_t indexes[3] = {a, b, c};
				LogicalType expected;
				bool expected_ok = false;
				bool first = true;
				do {
					duckdb::vector<LogicalType> set {types[indexes[0]], types[indexes[1]], types[indexes[2]]};
					LogicalType result;
					bool ok;
					try {
						ok = LogicalType::TryGetMaxLogicalType(context, set, result);
					} catch (std::exception &ex) {
						violations.push_back(StringUtil::Format("exception: %s: %s", set_string(indexes), ex.what()));
						break;
					}
					if (first) {
						expected = std::move(result);
						expected_ok = ok;
						first = false;
						continue;
					}
					if (ok != expected_ok || (ok && SortMembers(result) != SortMembers(expected))) {
						violations.push_back(StringUtil::Format("order dependent: %s -> %s / %s", set_string(indexes),
						                                        ok ? result.ToString() : "<error>",
						                                        expected_ok ? expected.ToString() : "<error>"));
						break;
					}
				} while (std::next_permutation(indexes, indexes + 3));
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

TEST_CASE("Test that LogicalType::TryGetMaxLogicalType over a set of types is order independent",
          "[max_logical_type]") {
	DuckDB db(nullptr);
	Connection con(db);
	// the number of sets grows cubically - only nest a few interesting types here
	auto types = GetTestTypes({LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::DECIMAL(4, 1)});
	RequireNoViolations(CheckMaxLogicalTypeOrderIndependent(*con.context, types));
}

TEST_CASE("Test that LogicalType::TryGetMaxLogicalType over a set of all test types is order independent",
          "[.][max_logical_type_order]") {
	DuckDB db(nullptr);
	Connection con(db);
	RequireNoViolations(CheckMaxLogicalTypeOrderIndependent(*con.context, GetTestTypes()));
}
