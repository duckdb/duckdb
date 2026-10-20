#include "duckdb/common/vector/numeric_vector.hpp"

#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

bool NumericVector::TryGetTyped(const Vector &vector, unique_ptr<Vector> &typed) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::NUMERIC);
	if (vector.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &child = ShreddedVector::GetShreddedVector(vector);
		D_ASSERT(child.size() == vector.size());
		typed = make_uniq<Vector>(child.GetType());
		typed->Reference(child);
		return true;
	}
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// a canonical constant: decode into the narrowest representation that holds it
		Vector constant(LogicalType::NUMERIC);
		constant.Reference(vector);
		auto type = NumericBlob::RangeType(constant, 1);
		typed = make_uniq<Vector>(type, idx_t(1));
		NumericBlob::Decode(constant, 1, *typed);
		typed->SetVectorType(VectorType::CONSTANT_VECTOR);
		return true;
	}
	return false;
}

//! The vector in another exact representation (every value fits by construction)
static unique_ptr<Vector> CastTyped(unique_ptr<Vector> source, const LogicalType &type) {
	if (source->GetType() == type) {
		return source;
	}
	if (source->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto result = make_uniq<Vector>(type, idx_t(1));
		VectorOperations::DefaultCast(*source, *result, 1, true);
		result->SetVectorType(VectorType::CONSTANT_VECTOR);
		return result;
	}
	auto count = source->size();
	auto result = make_uniq<Vector>(type, count);
	VectorOperations::DefaultCast(*source, *result, count, true);
	return result;
}

bool NumericVector::TryNativePair(const Vector &left, const Vector &right, unique_ptr<Vector> &left_typed,
                                  unique_ptr<Vector> &right_typed) {
	if (!TryGetTyped(left, left_typed) || !TryGetTyped(right, right_typed)) {
		return false;
	}
	auto common = NumericType::Widen(left_typed->GetType(), right_typed->GetType());
	left_typed = CastTyped(std::move(left_typed), common);
	right_typed = CastTyped(std::move(right_typed), common);
	// a constant carries the row count of its partner
	auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;
	idx_t count;
	if (!left_constant) {
		count = left.size();
	} else if (!right_constant) {
		count = right.size();
	} else {
		count = MinValue(left.size(), right.size());
	}
	for (auto typed : {&left_typed, &right_typed}) {
		if ((*typed)->GetVectorType() == VectorType::CONSTANT_VECTOR && (*typed)->size() != count) {
			auto value = (*typed)->GetValue(0);
			*typed = make_uniq<Vector>(value, count_t(count));
		}
	}
	return true;
}

} // namespace duckdb
