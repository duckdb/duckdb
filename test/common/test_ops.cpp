#include "catch.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

// TODO test selection vectors
// TODO add bitwise ops
// TODO add like
// TODO add boolean ops
// TODO add null checks

TEST_CASE("Casting vectors", "[vector_ops]") {
	Vector v(TypeId::BOOL, true, false);
	v.SetCount(3);

	v.SetValue(0, Value());
	v.SetValue(1, Value::BOOLEAN(true));
	v.SetValue(2, Value::BOOLEAN(false));

	v.Cast(TypeId::INT8);
	v.Cast(TypeId::INT16);
	v.Cast(TypeId::INT32);
	v.Cast(TypeId::INT64);
	v.Cast(TypeId::FLOAT);
	v.Cast(TypeId::DOUBLE);
	v.Cast(TypeId::VARCHAR);

	v.Cast(TypeId::DOUBLE);
	v.Cast(TypeId::FLOAT);
	v.Cast(TypeId::INT64);
	v.Cast(TypeId::INT32);
	v.Cast(TypeId::INT16);
	v.Cast(TypeId::INT8);

	v.Cast(TypeId::BOOL);

	REQUIRE(v.GetValue(0).is_null);
	REQUIRE(v.GetValue(1) == Value::BOOLEAN(true));
	REQUIRE(v.GetValue(2) == Value::BOOLEAN(false));
}

TEST_CASE("Aggregating boolean vectors", "[vector_ops]") {
	Vector v(TypeId::BOOL, true, false);
	v.SetCount(3);

	v.SetValue(0, Value());
	v.SetValue(1, Value::BOOLEAN(true));
	v.SetValue(2, Value::BOOLEAN(false));

	REQUIRE(VectorOperations::HasNull(v));
}

static void require_compare(Vector &val) {
	Vector v1(val.type, true, false);
	val.Copy(v1);

	Vector res(TypeId::BOOL, true, false);

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
	Vector v(TypeId::BOOL, true, false);
	v.SetCount(3);
	v.SetValue(0, Value::BOOLEAN(true));
	v.SetValue(1, Value::BOOLEAN(false));
	v.SetValue(2, Value());

	require_compare(v);
	v.Cast(TypeId::INT16);
	require_compare(v);
	v.Cast(TypeId::INT32);
	require_compare(v);
	v.Cast(TypeId::INT64);
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
	p.SetCount(v.size());
	p.SetValue(0, Value::POINTER((uintptr_t)&ptrs[0]));
	p.SetValue(1, Value::POINTER((uintptr_t)&ptrs[1]));

	VectorOperations::Scatter::Set(v, p);

	Vector r(v.type, true, false);
	r.SetCount(p.size());

	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(1));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(0));

	VectorOperations::Scatter::Add(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(2));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(0));

	VectorOperations::Scatter::Max(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(2));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(0));

	VectorOperations::Scatter::Min(v, p);
	VectorOperations::Gather::Set(p, r, false);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(1));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(0));
}

TEST_CASE("Scatter/gather numeric vectors", "[vector_ops]") {
	Vector v(TypeId::INT8, true, false);
	v.SetCount(2);
	v.SetValue(0, Value::TINYINT(true));
	v.SetValue(1, Value::TINYINT(false));

	require_sg(v);
	v.Cast(TypeId::INT16);
	require_sg(v);
	v.Cast(TypeId::INT32);
	require_sg(v);
	v.Cast(TypeId::INT64);
	require_sg(v);
	v.Cast(TypeId::FLOAT);
	require_sg(v);
	v.Cast(TypeId::DOUBLE);
	require_sg(v);
}

static void require_generate(TypeId t) {
	Vector v(t, true, false);
	v.SetCount(100);
	VectorOperations::GenerateSequence(v, 42, 1);
	for (size_t i = 0; i < v.size(); i++) {
		REQUIRE(v.GetValue(i).CastAs(TypeId::INT64) == Value::BIGINT(i + 42));
	}
	Vector hash(TypeId::HASH, true, false);
	hash.SetCount(v.size());
	VectorOperations::Hash(v, hash);
}

TEST_CASE("Generator sequence vectors", "[vector_ops]") {
	require_generate(TypeId::INT16);
	require_generate(TypeId::INT32);
	require_generate(TypeId::INT64);
	require_generate(TypeId::FLOAT);
	require_generate(TypeId::DOUBLE);
}

static void require_arith(TypeId t) {
	Vector v1(t, true, false);
	v1.SetCount(6);

	Vector v2(t, true, false);
	v2.SetCount(v1.size());

	// v1: 1, 2, 3, NULL, 42, NULL
	// v2: 4, 5, 6, 7, NULL, NULL
	v1.SetValue(0, Value::BIGINT(1));
	v1.SetValue(1, Value::BIGINT(2));
	v1.SetValue(2, Value::BIGINT(3));
	v1.SetValue(3, Value());
	v1.SetValue(4, Value::BIGINT(42));
	v1.SetValue(5, Value());

	v2.SetValue(0, Value::BIGINT(4));
	v2.SetValue(1, Value::BIGINT(5));
	v2.SetValue(2, Value::BIGINT(6));
	v2.SetValue(3, Value::BIGINT(7));
	v2.SetValue(4, Value());
	v2.SetValue(5, Value());

	Vector r(t, true, false);
	r.SetCount(v1.size());

	VectorOperations::Add(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(5));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(7));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(9));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::AddInPlace(r, v2);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(9));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(12));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(15));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::AddInPlace(r, 10);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(19));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(22));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(25));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Subtract(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Subtract(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(-3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);

	VectorOperations::Multiply(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(4));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(10));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(18));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);
}

static void require_mod(TypeId t) {
	Vector v1(t, true, false);
	v1.SetCount(7);

	Vector v2(t, true, false);
	v2.SetCount(v1.size());

	v1.SetValue(0, Value::BIGINT(10));
	v1.SetValue(1, Value::BIGINT(10));
	v1.SetValue(2, Value::BIGINT(10));
	v1.SetValue(3, Value::BIGINT(10));
	v1.SetValue(4, Value());
	v1.SetValue(5, Value::BIGINT(10));
	v1.SetValue(6, Value());

	v2.SetValue(0, Value::BIGINT(2));
	v2.SetValue(1, Value::BIGINT(4));
	v2.SetValue(2, Value::BIGINT(7));
	v2.SetValue(3, Value::BIGINT(0));
	v2.SetValue(4, Value::BIGINT(42));
	v2.SetValue(5, Value());
	v2.SetValue(6, Value());

	Vector r(t, true, false);
	r.SetCount(v1.size());

	VectorOperations::Modulo(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(0));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(2));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(3));
	REQUIRE(r.GetValue(3).is_null);
	REQUIRE(r.GetValue(4).is_null);
	REQUIRE(r.GetValue(5).is_null);
	REQUIRE(r.GetValue(6).is_null);
}

static void require_mod_double() {
	Vector v1(TypeId::DOUBLE, true, false);
	v1.SetCount(2);

	Vector v2(TypeId::DOUBLE, true, false);
	v2.SetCount(v1.size());

	v1.SetValue(0, Value::DOUBLE(10));
	v1.SetValue(1, Value::DOUBLE(10));

	v2.SetValue(0, Value::DOUBLE(2));
	v2.SetValue(1, Value::DOUBLE(4));

	Vector r(TypeId::DOUBLE, true, false);
	r.SetCount(v1.size());

	VectorOperations::Modulo(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::DOUBLE) == Value::DOUBLE(0));
	REQUIRE(r.GetValue(1).CastAs(TypeId::DOUBLE) == Value::DOUBLE(2));
}

TEST_CASE("Arithmetic operations on vectors", "[vector_ops]") {
	require_arith(TypeId::INT16);
	require_arith(TypeId::INT32);
	require_arith(TypeId::INT64);
	require_arith(TypeId::FLOAT);
	require_arith(TypeId::DOUBLE);

	require_mod(TypeId::INT16);
	require_mod(TypeId::INT32);
	require_mod(TypeId::INT64);
	require_mod_double();
}
