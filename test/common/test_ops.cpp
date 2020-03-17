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

#if STANDARD_VECTOR_SIZE >= 8
TEST_CASE("Casting vectors", "[vector_ops]") {
	vector<TypeId> types{TypeId::BOOL,  TypeId::INT8,  TypeId::INT16,  TypeId::INT32,
	                     TypeId::INT64, TypeId::FLOAT, TypeId::DOUBLE, TypeId::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(types);

	chunk.SetValue(0, 0, Value());
	chunk.SetValue(0, 1, Value::BOOLEAN(false));
	chunk.SetValue(0, 2, Value::BOOLEAN(true));
	chunk.SetCardinality(3);

	// cast up the chain of types (bool -> int8 -> int16 -> int32 -> etc)
	for (idx_t i = 0; i < types.size() - 1; i++) {
		VectorOperations::Cast(chunk.data[i], chunk.data[i + 1]);
	}
	// cast down the chain of types again (str -> double -> float -> int64 -> etc)
	for (idx_t i = types.size(); i > 1; i--) {
		VectorOperations::Cast(chunk.data[i - 1], chunk.data[i - 2]);
	}

	REQUIRE(chunk.GetValue(0, 0).is_null);
	REQUIRE(chunk.GetValue(0, 1) == Value::BOOLEAN(false));
	REQUIRE(chunk.GetValue(0, 2) == Value::BOOLEAN(true));
}

TEST_CASE("Aggregating boolean vectors", "[vector_ops]") {
	vector<TypeId> types{TypeId::BOOL};
	DataChunk chunk;
	chunk.Initialize(types);
	chunk.SetCardinality(3);
	chunk.SetValue(0, 0, Value());
	chunk.SetValue(0, 1, Value::BOOLEAN(true));
	chunk.SetValue(0, 2, Value::BOOLEAN(false));

	REQUIRE(VectorOperations::HasNull(chunk.data[0]));
}

static void require_compare(DataChunk &chunk, idx_t idx) {
	auto &val = chunk.data[idx];
	auto &v1 = val;

	Vector res(chunk, TypeId::BOOL);

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
	vector<TypeId> types{TypeId::BOOL,  TypeId::INT8,  TypeId::INT16,  TypeId::INT32,
	                     TypeId::INT64, TypeId::FLOAT, TypeId::DOUBLE, TypeId::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(types);

	chunk.SetCardinality(3);
	chunk.SetValue(0, 0, Value::BOOLEAN(true));
	chunk.SetValue(0, 1, Value::BOOLEAN(false));
	chunk.SetValue(0, 2, Value());

	for (idx_t i = 0; i < types.size(); i++) {
		require_compare(chunk, i);
		if (i + 1 < types.size()) {
			VectorOperations::Cast(chunk.data[i], chunk.data[i + 1]);
		}
	}
}

static void require_generate(TypeId t) {
	VectorCardinality cardinality(7);
	Vector v(cardinality, t);
	VectorOperations::GenerateSequence(v, 42, 1);
	for (size_t i = 0; i < v.size(); i++) {
		REQUIRE(v.GetValue(i).CastAs(TypeId::INT64) == Value::BIGINT(i + 42));
	}
	Vector hash(cardinality, TypeId::HASH);
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
	vector<TypeId> types{t, t, t};
	DataChunk chunk;
	chunk.Initialize(types);
	chunk.SetCardinality(6);

	auto &v1 = chunk.data[0];
	auto &v2 = chunk.data[1];
	auto &r = chunk.data[2];

	// v1: 1, 2, 3, NULL, 42, NULL
	chunk.SetValue(0, 0, Value::BIGINT(1));
	chunk.SetValue(0, 1, Value::BIGINT(2));
	chunk.SetValue(0, 2, Value::BIGINT(3));
	chunk.SetValue(0, 3, Value());
	chunk.SetValue(0, 4, Value::BIGINT(42));
	chunk.SetValue(0, 5, Value());

	// v2: 4, 5, 6, 7, NULL, NULL
	chunk.SetValue(1, 0, Value::BIGINT(4));
	chunk.SetValue(1, 1, Value::BIGINT(5));
	chunk.SetValue(1, 2, Value::BIGINT(6));
	chunk.SetValue(1, 3, Value::BIGINT(7));
	chunk.SetValue(1, 4, Value());
	chunk.SetValue(1, 5, Value());

	VectorOperations::Add(v1, v2, r);
	REQUIRE(r.GetValue(0).CastAs(TypeId::INT64) == Value::BIGINT(5));
	REQUIRE(r.GetValue(1).CastAs(TypeId::INT64) == Value::BIGINT(7));
	REQUIRE(r.GetValue(2).CastAs(TypeId::INT64) == Value::BIGINT(9));
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
	vector<TypeId> types{t, t, t};
	DataChunk chunk;
	chunk.Initialize(types);
	chunk.SetCardinality(7);

	auto &v1 = chunk.data[0];
	auto &v2 = chunk.data[1];
	auto &r = chunk.data[2];

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
	vector<TypeId> types{TypeId::DOUBLE, TypeId::DOUBLE, TypeId::DOUBLE};
	DataChunk chunk;
	chunk.Initialize(types);
	chunk.SetCardinality(7);

	auto &v1 = chunk.data[0];
	auto &v2 = chunk.data[1];
	auto &r = chunk.data[2];

	v1.SetValue(0, Value::DOUBLE(10));
	v1.SetValue(1, Value::DOUBLE(10));

	v2.SetValue(0, Value::DOUBLE(2));
	v2.SetValue(1, Value::DOUBLE(4));

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
#endif
