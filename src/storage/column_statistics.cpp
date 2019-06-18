#include "storage/column_statistics.hpp"

#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

void ColumnStatistics::Update(Vector &new_vector) {
	lock_guard<mutex> lock(stats_lock);

	if (!can_have_null) {
		can_have_null = VectorOperations::HasNull(new_vector);
	}
	if (TypeIsNumeric(new_vector.type)) {
		Value new_min = VectorOperations::Min(new_vector);
		Value new_max = VectorOperations::Max(new_vector);

		if (TypeIsIntegral(new_min.type) && new_min.type != TypeId::BIGINT) {
			new_min = new_min.CastAs(TypeId::BIGINT);
			new_max = new_max.CastAs(TypeId::BIGINT);
		}

		min = ValueOperations::Min(min, new_min);
		max = ValueOperations::Max(max, new_max);
	}
	if (new_vector.type == TypeId::VARCHAR) {
		Value new_max_strlen = VectorOperations::MaximumStringLength(new_vector);
		maximum_string_length = std::max(maximum_string_length, (index_t)new_max_strlen.value_.bigint);
	}
}

void ColumnStatistics::Initialize(ExpressionStatistics &target) {
	target.has_stats = true;
	target.can_have_null = can_have_null;
	target.min = min;
	target.max = max;
	target.maximum_string_length = maximum_string_length;
}
