#include "catch.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Casting vectors", "[vector_ops]") {
	Vector v(TypeId::BOOLEAN, true, false);
	v.count = 3;
	v.SetNull(0, 1);
	v.SetValue(1, Value::BOOLEAN(true));
	v.SetValue(2, Value::BOOLEAN(false));

	v.Cast(TypeId::TINYINT);
	v.Cast(TypeId::SMALLINT);
	v.Cast(TypeId::INTEGER);
	v.Cast(TypeId::BIGINT);
	v.Cast(TypeId::FLOAT);
	v.Cast(TypeId::DOUBLE);
	v.Cast(TypeId::VARCHAR);

	v.Cast(TypeId::DOUBLE);
	v.Cast(TypeId::FLOAT);
	v.Cast(TypeId::BIGINT);
	v.Cast(TypeId::INTEGER);
	v.Cast(TypeId::SMALLINT);
	v.Cast(TypeId::TINYINT);

	v.Cast(TypeId::BOOLEAN);

	REQUIRE(v.GetValue(0).is_null);
	REQUIRE(v.GetValue(1) == Value::BOOLEAN(true));
	REQUIRE(v.GetValue(2) == Value::BOOLEAN(false));
}

TEST_CASE("Aggregating boolean vectors", "[vector_ops]") {
	Vector v(TypeId::BOOLEAN, true, false);
	v.count = 3;
	v.SetNull(0, 1);
	v.SetValue(1, Value::BOOLEAN(true));
	v.SetValue(2, Value::BOOLEAN(false));

	REQUIRE(VectorOperations::AnyTrue(v));
	REQUIRE(!VectorOperations::AllTrue(v));
	REQUIRE(VectorOperations::HasNull(v));
}

static void require_aggrs(Vector &v) {
	REQUIRE(VectorOperations::Min(v).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(VectorOperations::Max(v).CastAs(TypeId::BIGINT) == Value::BIGINT(40));
	REQUIRE(VectorOperations::Count(v).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(VectorOperations::Sum(v).CastAs(TypeId::BIGINT) == Value::BIGINT(42));
}

TEST_CASE("Aggregating numeric vectors", "[vector_ops]") {
	Vector v(TypeId::TINYINT, true, false);
	v.count = 2;
	v.SetValue(0, Value::TINYINT(40));
	v.SetValue(1, Value::TINYINT(2));

	REQUIRE(!VectorOperations::HasNull(v));

	require_aggrs(v);
	v.Cast(TypeId::SMALLINT);
	require_aggrs(v);
	v.Cast(TypeId::INTEGER);
	require_aggrs(v);
	v.Cast(TypeId::BIGINT);
	require_aggrs(v);
	v.Cast(TypeId::FLOAT);
	require_aggrs(v);
	v.Cast(TypeId::DOUBLE);
	require_aggrs(v);
}

TEST_CASE("Aggregating string vectors", "[vector_ops]") {
	Vector v(TypeId::VARCHAR, true, false);
	v.count = 3;
	v.SetStringValue(0, "Das Pferd");
	v.SetStringValue(1, "frisst keinen Gurkensalat");
	v.SetNull(2, true);

	REQUIRE(VectorOperations::MaximumStringLength(v).CastAs(TypeId::BIGINT) == Value::BIGINT(25));
}

static void require_case(Vector &val) {
	Vector check(TypeId::BOOLEAN, true, false);
	check.count = val.count;
	check.SetValue(0, Value::BOOLEAN(true));
	check.SetValue(1, Value::BOOLEAN(false));
	check.SetNull(2, true);

	Vector v1(val.type, true, false);
	Vector v2(val.type, true, false);
	val.Copy(v1);
	val.Copy(v2);

	Vector res(val.type, true, false);
	res.count = val.count;

	VectorOperations::Case(check, v1, v2, res);

	REQUIRE(res.GetValue(0) == val.GetValue(0));
	REQUIRE(res.GetValue(1) == val.GetValue(1));
	REQUIRE(res.GetValue(2).is_null);
}

TEST_CASE("Case vectors", "[vector_ops]") {
	Vector v(TypeId::BOOLEAN, true, false);
	v.count = 3;
	v.SetValue(0, Value::BOOLEAN(true));
	v.SetValue(1, Value::BOOLEAN(false));
	v.SetNull(2, true);

	require_case(v);
	v.Cast(TypeId::SMALLINT);
	require_case(v);
	v.Cast(TypeId::INTEGER);
	require_case(v);
	v.Cast(TypeId::BIGINT);
	require_case(v);
	v.Cast(TypeId::FLOAT);
	require_case(v);
	v.Cast(TypeId::DOUBLE);
	require_case(v);
	v.Cast(TypeId::VARCHAR);
	require_case(v);
}

static void require_compare(Vector &val) {
	Vector v1(val.type, true, false);
	val.Copy(v1);

	Vector res(TypeId::BOOLEAN, true, false);

	VectorOperations::Equals(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(2).is_null);

	VectorOperations::NotEquals(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(2).is_null);

	VectorOperations::GreaterThanEquals(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(2).is_null);

	VectorOperations::LessThanEquals(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(true));
	REQUIRE(res.GetValue(2).is_null);

	VectorOperations::GreaterThan(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(2).is_null);

	VectorOperations::LessThan(val, v1, res);

	REQUIRE(res.GetValue(0) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(1) == Value::BOOLEAN(false));
	REQUIRE(res.GetValue(2).is_null);
}

TEST_CASE("Compare vectors", "[vector_ops]") {
	Vector v(TypeId::BOOLEAN, true, false);
	v.count = 3;
	v.SetValue(0, Value::BOOLEAN(true));
	v.SetValue(1, Value::BOOLEAN(false));
	v.SetNull(2, true);

	require_compare(v);
	v.Cast(TypeId::SMALLINT);
	require_compare(v);
	v.Cast(TypeId::INTEGER);
	require_compare(v);
	v.Cast(TypeId::BIGINT);
	require_compare(v);
	v.Cast(TypeId::FLOAT);
	require_compare(v);
	v.Cast(TypeId::DOUBLE);
	require_compare(v);
	v.Cast(TypeId::VARCHAR);
	require_compare(v);
}

static void require_sg(Vector &v) {
	uint64_t ptrs[v.count];

	Vector p(TypeId::POINTER, true, false);
	p.count = v.count;
	p.SetValue(0, Value::POINTER((uint64_t)&ptrs[0]));
	p.SetValue(1, Value::POINTER((uint64_t)&ptrs[1]));

	VectorOperations::Scatter::Set(v, p);

	Vector r(v.type, true, false);
	r.count = p.count;

	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(1));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(0));

	VectorOperations::Scatter::Add(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(0));

	VectorOperations::Scatter::Max(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(0));

	VectorOperations::Scatter::Min(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(1));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(0));

}


TEST_CASE("Scatter/gather numeric vectors", "[vector_ops]") {
	Vector v(TypeId::TINYINT, true, false);
	v.count = 2;
	v.SetValue(0, Value::TINYINT(true));
	v.SetValue(1, Value::TINYINT(false));

	require_sg(v);
	v.Cast(TypeId::SMALLINT);
	require_sg(v);
	v.Cast(TypeId::INTEGER);
	require_sg(v);
	v.Cast(TypeId::BIGINT);
	require_sg(v);
	v.Cast(TypeId::FLOAT);
	require_sg(v);
	v.Cast(TypeId::DOUBLE);
	require_sg(v);
}
