#include "catch.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

template <class T> bool IsSorted(Vector &v, SelectionVector &sel) {
	auto data = FlatVector::GetData<T>(v);
	auto &nullmask = FlatVector::Nullmask(v);
	for (size_t i = 1; i < v.size(); i++) {
		auto lindex = sel.get_index(i - 1), rindex = sel.get_index(i);
		bool left_null = nullmask[lindex];
		bool right_null = nullmask[rindex];
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
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	VectorCardinality cardinality(STANDARD_VECTOR_SIZE);
	Vector v(cardinality, TypeId::INT32);
	auto data = FlatVector::GetData<int>(v);
	// sort without NULLs
	for(idx_t i = 0; i < v.size(); i++) {
		data[i] = i % 6;
	}
	VectorOperations::Sort(v, sel);

	REQUIRE(IsSorted<int>(v, sel));

	// sort with NULLs
	auto &nullmask = FlatVector::Nullmask(v);
	for(idx_t i = 0; i < v.size(); i++) {
		data[i] = i % 6;
		if (data[i] == 5) {
			nullmask[i] = true;
		}
	}
	VectorOperations::Sort(v, sel);

	REQUIRE(IsSorted<int>(v, sel));
}
