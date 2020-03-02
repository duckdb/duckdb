#include "catch.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

template <class T> bool IsSorted(Vector &v) {
	auto data = (T *)v.GetData();
	auto vsel = v.sel_vector();
	for (size_t i = 1; i < v.size(); i++) {
		auto lindex = vsel[i - 1], rindex = vsel[i];
		bool left_null = v.nullmask[lindex];
		bool right_null = v.nullmask[rindex];
		if (!left_null && right_null) {
			return false;
		} else if (left_null) {
			continue;
		} else {
			if (!LessThanEquals::Operation<T>(data[lindex], data[rindex])) {
				return false;
			}
		}
	}
	return true;
}

TEST_CASE("Sorting vectors works", "[sort]") {
	sel_t sel[STANDARD_VECTOR_SIZE];
	VectorCardinality cardinality(STANDARD_VECTOR_SIZE);
	Vector v(cardinality, TypeId::INT32);
	auto data = (int *)v.GetData();
	// sort without NULLs
	VectorOperations::Exec(v, [&](size_t i, size_t k) { data[i] = i % 6; });
	VectorOperations::Sort(v, sel);

	cardinality.sel_vector = sel;
	REQUIRE(IsSorted<int>(v));

	// sort with NULLs
	VectorOperations::Exec(v, [&](size_t i, size_t k) {
		data[i] = i % 6;
		if (data[i] == 5) {
			v.nullmask[i] = true;
		}
	});
	VectorOperations::Sort(v, sel);

	cardinality.sel_vector = sel;
	REQUIRE(IsSorted<int>(v));
}
