//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <bool INVERSE>
static void IsNullLoop(const Vector &input, Vector &result) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);

	auto count = input.size();
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = INVERSE ? !ConstantVector::IsNull(input) : ConstantVector::IsNull(input);
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::Writer<bool>(result, count);
		auto entries = input.Validity();
		for (idx_t i = 0; i < count; i++) {
			result_data.WriteValue(INVERSE ? entries.IsValid(i) : !entries.IsValid(i));
		}
		FlatVector::SetSize(result, count);
	}
}

void VectorOperations::IsNotNull(const Vector &input, Vector &result) {
	IsNullLoop<true>(input, result);
}

void VectorOperations::IsNull(const Vector &input, Vector &result) {
	IsNullLoop<false>(input, result);
}

bool VectorOperations::HasNotNull(const Vector &input) {
	auto count = input.size();
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return !ConstantVector::IsNull(input);
	} else {
		auto entries = input.Validity();
		if (!entries.CanHaveNull()) {
			return true;
		}
		for (idx_t i = 0; i < count; i++) {
			if (entries.IsValid(i)) {
				return true;
			}
		}
		return false;
	}
}

bool VectorOperations::HasNull(const Vector &input) {
	auto count = input.size();
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::IsNull(input);
	} else {
		auto entries = input.Validity();
		if (!entries.CanHaveNull()) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			if (!entries.IsValid(i)) {
				return true;
			}
		}
		return false;
	}
}

idx_t VectorOperations::CountNotNull(const Vector &input) {
	auto count = input.size();

	switch (input.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		return FlatVector::Validity(input).CountValid(count);
	case VectorType::CONSTANT_VECTOR:
		if (!ConstantVector::IsNull(input)) {
			return count;
		}
		return 0;
	default: {
		auto validity = input.Validity();
		if (validity.CannotHaveNull()) {
			return count;
		}
		idx_t valid = 0;
		for (idx_t i = 0; i < count; ++i) {
			if (validity.IsValid(i)) {
				valid++;
			}
		}
		return valid;
	}
	}
}

} // namespace duckdb
