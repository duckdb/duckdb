#include "catch.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

template <class T> bool IsSorted(Vector &v) {
	auto data = (T *)v.GetData();
	for (size_t i = 1; i < v.count; i++) {
		auto lindex = v.sel_vector[i - 1], rindex = v.sel_vector[i];
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
	Vector v(TypeId::INT32, true, false);
	v.count = STANDARD_VECTOR_SIZE;
	auto data = (int *)v.GetData();
	// sort without NULLs
	VectorOperations::Exec(v, [&](size_t i, size_t k) { data[i] = i % 6; });
	VectorOperations::Sort(v, sel);

	v.sel_vector = sel;
	REQUIRE(IsSorted<int>(v));

	// sort with NULLs
	VectorOperations::Exec(v, [&](size_t i, size_t k) {
		data[i] = i % 6;
		if (data[i] == 5) {
			v.nullmask[i] = true;
		}
	});
	VectorOperations::Sort(v, sel);

	v.sel_vector = sel;
	REQUIRE(IsSorted<int>(v));
}
