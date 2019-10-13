#include "catch.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

// TODO test selection vectors
// TODO add bitwise ops
// TODO add like
// TODO add boolean ops
// TODO add null checks

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
	uint64_t ptrs[2];

	Vector p(TypeId::POINTER, true, false);
	p.count = v.count;
	p.SetValue(0, Value::POINTER((uintptr_t)&ptrs[0]));
	p.SetValue(1, Value::POINTER((uintptr_t)&ptrs[1]));

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

static void require_generate(TypeId t) {
	Vector v(t, true, false);
	v.count = 100;
	VectorOperations::GenerateSequence(v, 42, 1);
	for (size_t i = 0; i < v.count; i++) {
		REQUIRE(v.GetValue(i).CastAs(TypeId::BIGINT) == Value::BIGINT(i + 42));
	}
	Vector hash(TypeId::HASH, true, false);
	hash.count = v.count;
	VectorOperations::Hash(v, hash);
}

TEST_CASE("Generator sequence vectors", "[vector_ops]") {
	require_generate(TypeId::SMALLINT);
	require_generate(TypeId::INTEGER);
	require_generate(TypeId::BIGINT);
	require_generate(TypeId::FLOAT);
	require_generate(TypeId::DOUBLE);
}

static void require_arith(TypeId t) {
	Vector v1(t, true, false);
	v1.count = 6;

	Vector v2(t, true, false);
	v2.count = v1.count;

	v1.SetValue(0, Value::BIGINT(1));
	v1.SetValue(1, Value::BIGINT(2));
	v1.SetValue(2, Value::BIGINT(3));
	v1.SetNull(3, true);
	v1.SetValue(4, Value::BIGINT(42));
	v1.SetNull(5, true);

	v2.SetValue(0, Value::BIGINT(4));
	v2.SetValue(1, Value::BIGINT(5));
	v2.SetValue(2, Value::BIGINT(6));
	v2.SetValue(3, Value::BIGINT(7));
	v2.SetNull(4, true);
	v2.SetNull(5, true);

	Vector r(t, true, false);
	r.count = v1.count;

	VectorOperations::Add(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(5));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(7));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(9));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::AddInPlace(r, v2);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(9));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(12));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(15));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::AddInPlace(r, 10);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(19));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(22));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(25));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Subtract(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Subtract(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Multiply(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(4));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(10));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(18));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	Vector r2(t, true, false);
	r2.count = v1.count;

	Vector prec(TypeId::INTEGER, true, false);
	prec.count = v1.count;
	VectorOperations::Set(prec, Value::TINYINT(0));
	VectorOperations::Divide(v2, v1, r);
	VectorOperations::Round(r, prec, r2);

	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(4));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	REQUIRE(r2.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(4));
	REQUIRE(r2.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r2.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r2.GetValue(3).is_null);
	REQUIRE(r2.GetValue(4).is_null);
	REQUIRE(r2.GetValue(5).is_null);

	Vector m1(t, true, false);
	m1.count = v1.count;
	VectorOperations::Set(m1, Value::TINYINT(-1));
	VectorOperations::Multiply(v1, m1, r);
	VectorOperations::Abs(r, r2);

	REQUIRE(r2.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(1));
	REQUIRE(r2.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r2.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(3));
	REQUIRE(r2.GetValue(3).is_null);
	REQUIRE(r2.GetValue(4).CastAs(TypeId::BIGINT) == Value::BIGINT(42));
	REQUIRE(r2.GetValue(5).is_null);
}

static void require_mod(TypeId t) {
	Vector v1(t, true, false);
	v1.count = 7;

	Vector v2(t, true, false);
	v2.count = v1.count;

	v1.SetValue(0, Value::BIGINT(10));
	v1.SetValue(1, Value::BIGINT(10));
	v1.SetValue(2, Value::BIGINT(10));
	v1.SetValue(3, Value::BIGINT(10));
	v1.SetNull(4, true);
	v1.SetValue(5, Value::BIGINT(10));
	v1.SetNull(6, true);

	v2.SetValue(0, Value::BIGINT(2));
	v2.SetValue(1, Value::BIGINT(4));
	v2.SetValue(2, Value::BIGINT(7));
	v2.SetValue(3, Value::BIGINT(0));
	v2.SetValue(4, Value::BIGINT(42));
	v2.SetNull(5, true);
	v2.SetNull(6, true);

	Vector r(t, true, false);
	r.count = v1.count;

	VectorOperations::Modulo(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(0));
	REQUIRE(r.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(r.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);
	REQUIRE(r.GetValue(6).is_null);

	VectorOperations::ModuloInPlace(v1, v2);
	REQUIRE(v1.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(0));
	REQUIRE(v1.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(2));
	REQUIRE(v1.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(3));
	REQUIRE(v1.GetValue(3).is_null);
	REQUIRE(v1.GetValue(4).is_null);
	REQUIRE(v1.GetValue(5).is_null);
	REQUIRE(v2.GetValue(6).is_null);

	VectorOperations::ModuloInPlace(v1, 2);
	REQUIRE(v1.GetValue(0).CastAs(TypeId::BIGINT) == Value::BIGINT(0));
	REQUIRE(v1.GetValue(1).CastAs(TypeId::BIGINT) == Value::BIGINT(0));
	REQUIRE(v1.GetValue(2).CastAs(TypeId::BIGINT) == Value::BIGINT(1));
	REQUIRE(v1.GetValue(3).is_null);
	REQUIRE(v1.GetValue(4).is_null);
	REQUIRE(v1.GetValue(5).is_null);
	REQUIRE(v2.GetValue(6).is_null);
}

static void require_mod_double() {
	Vector v1(TypeId::DOUBLE, true, false);
	v1.count = 2;

	Vector v2(TypeId::DOUBLE, true, false);
	v2.count = v1.count;

	v1.SetValue(0, Value::DOUBLE(10));
	v1.SetValue(1, Value::DOUBLE(10));

	v2.SetValue(0, Value::DOUBLE(2));
	v2.SetValue(1, Value::DOUBLE(4));

	Vector r(TypeId::DOUBLE, true, false);
	r.count = v1.count;

	VectorOperations::Modulo(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::DOUBLE) == Value::DOUBLE(0));
	REQUIRE(r.GetValue(1).CastAs(TypeId::DOUBLE) == Value::DOUBLE(2));

	VectorOperations::ModuloInPlace(v1, v2);
	REQUIRE(v1.GetValue(0).CastAs(TypeId::DOUBLE) == Value::DOUBLE(0));
	REQUIRE(v1.GetValue(1).CastAs(TypeId::DOUBLE) == Value::DOUBLE(2));

	VectorOperations::ModuloInPlace(v1, 2);
	REQUIRE(v1.GetValue(0).CastAs(TypeId::DOUBLE) == Value::DOUBLE(0));
	REQUIRE(v1.GetValue(1).CastAs(TypeId::DOUBLE) == Value::DOUBLE(0));
}

TEST_CASE("Arithmetic operations on vectors", "[vector_ops]") {
	require_arith(TypeId::SMALLINT);
	require_arith(TypeId::INTEGER);
	require_arith(TypeId::BIGINT);
	require_arith(TypeId::FLOAT);
	require_arith(TypeId::DOUBLE);

	require_mod(TypeId::SMALLINT);
	require_mod(TypeId::INTEGER);
	require_mod(TypeId::BIGINT);
	require_mod_double();
}
