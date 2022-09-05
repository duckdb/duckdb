//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <bool INVERSE>
void IsNullLoop(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = INVERSE ? !ConstantVector::IsNull(input) : ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<bool>(result);
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			result_data[i] = INVERSE ? data.validity.RowIsValid(idx) : !data.validity.RowIsValid(idx);
		}
	}
}

void VectorOperations::IsNotNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<true>(input, result, count);
}

void VectorOperations::IsNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<false>(input, result, count);
}

bool VectorOperations::HasNotNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return !ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		if (data.validity.AllValid()) {
			return true;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

bool VectorOperations::HasNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		if (data.validity.AllValid()) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (!data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

idx_t VectorOperations::CountNotNull(Vector &input, const idx_t count) {
	idx_t valid = 0;

	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	if (vdata.validity.AllValid()) {
		return count;
	}
	switch (input.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		valid += vdata.validity.CountValid(count);
		break;
	case VectorType::CONSTANT_VECTOR:
		valid += vdata.validity.CountValid(1) * count;
		break;
	default:
		for (idx_t i = 0; i < count; ++i) {
			const auto row_idx = vdata.sel->get_index(i);
			valid += int(vdata.validity.RowIsValid(row_idx));
		}
		break;
	}

	return valid;
}

} // namespace duckdb
