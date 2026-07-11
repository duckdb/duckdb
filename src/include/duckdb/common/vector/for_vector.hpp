//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/for_vector_helpers.hpp"

namespace duckdb {

//! FORVector: compressed vector backed by smaller unsigned integer payload values.
//! Stores non-negative logical values directly in the vector payload using UINT8/16/32/64.
struct FORVector {
	template <class T>
	struct ScanData {
		const Vector *for_vec = nullptr;
		const SelectionVector *sel = nullptr;
		PhysicalType stored_type = PhysicalType::INVALID;
		const_data_ptr_t data = nullptr;
		optional_ptr<const ValidityMask> validity;
		T max_value;
	};

	//! Try to get the FOR vector, unwrapping one DICTIONARY layer if needed.
	//! Returns the FOR vector and sets sel to the dictionary's selection (or nullptr if direct FOR).
	static inline const Vector *TryGetFOR(const Vector &vec, const SelectionVector *&sel) {
		if (vec.GetVectorType() == VectorType::FOR_VECTOR) {
			sel = nullptr;
			return &vec;
		}
		if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			auto &child = DictionaryVector::Child(vec);
			if (child.GetVectorType() == VectorType::FOR_VECTOR) {
				sel = &DictionaryVector::SelVector(vec);
				return &child;
			}
		}
		return nullptr;
	}

	template <class T>
	static inline bool TryGetScanData(const Vector &vec, ScanData<T> &scan_data) {
		const SelectionVector *sel;
		auto *for_vec = TryGetFOR(vec, sel);
		if (!for_vec) {
			return false;
		}
		scan_data.for_vec = for_vec;
		scan_data.sel = sel;
		scan_data.stored_type = GetStoredType(*for_vec);
		scan_data.data = GetData(*for_vec);
		scan_data.validity = Validity(*for_vec);
		scan_data.max_value = GetMax<T>(*for_vec);
		return true;
	}

	template <class T>
	static inline void CopyResultValidity(Vector &result, const ScanData<T> *left_scan, const ScanData<T> *right_scan,
	                                      idx_t count) {
		auto &result_validity = Validity(result);
		result_validity.Reset(count);
		auto apply_validity = [&](const ScanData<T> *scan_data) {
			if (!scan_data || !scan_data->for_vec || !scan_data->validity->CanHaveNull()) {
				return;
			}
			for (idx_t i = 0; i < count; i++) {
				auto src_idx = scan_data->sel ? scan_data->sel->get_index(i) : i;
				if (!scan_data->validity->RowIsValid(src_idx)) {
					result_validity.SetInvalid(i);
				}
			}
		};
		apply_validity(left_scan);
		apply_validity(right_scan);
	}

	template <class STORED_T, class T>
	static inline const STORED_T *CompactData(const ScanData<T> &scan_data, idx_t count,
	                                          unsafe_unique_array<data_t> &compact_buf) {
		auto src = reinterpret_cast<const STORED_T *>(scan_data.data);
		if (!scan_data.sel) {
			return src;
		}
		compact_buf = make_unsafe_uniq_array_uninitialized<data_t>(count * sizeof(STORED_T));
		auto dst = reinterpret_cast<STORED_T *>(compact_buf.get());
		for (idx_t i = 0; i < count; i++) {
			dst[i] = src[scan_data.sel->get_index(i)];
		}
		return dst;
	}

	static inline data_ptr_t GetData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
		return vector.buffer->GetData();
	}
	static PhysicalType GetStoredType(const Vector &vector);
	static uint8_t GetRangeBits(const Vector &vector);
	static inline ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
		return vector.buffer->GetValidityMask();
	}

	//! Get the max logical value stored in metadata.
	template <class T>
	static T GetMax(const Vector &vector);

	//! Set FOR metadata for values in [0, max].
	template <class T>
	static void SetMetadata(Vector &vector, PhysicalType stored_type, T max_value);

	//! Widen raw FOR payload into a flat target buffer.
	static void WidenToFlat(const LogicalType &type, PhysicalType stored_type, const_data_ptr_t source,
	                        data_ptr_t target, const SelectionVector &sel, idx_t count);
	//! Widen a full-stride FOR buffer's payload in place (back-to-front) and mark the buffer FLAT.
	//! Only valid for cache-owned buffers, whose allocation stride is the full logical type size.
	static void WidenInPlace(const LogicalType &type, VectorBuffer &buffer);
	//! Read a single logical value from raw FOR payload.
	static Value GetValue(const LogicalType &type, PhysicalType stored_type, const_data_ptr_t data, idx_t index);

	//! Create a FOR vector. Reuses the existing buffer and does NOT allocate.
	//! Caller must write narrow stored data into GetData(vector) after this call.
	template <class T>
	static void Create(Vector &vector, PhysicalType stored_type, T max_value);

	//! Preserve FOR layout across chunk append where possible; otherwise flatten the target first.
	static void PrepareAppend(Vector &target, const Vector &source, bool has_selection, idx_t target_size);

	//===--------------------------------------------------------------------===//
	// Shared helpers used by comparison, filter pushdown, and arithmetic.
	//===--------------------------------------------------------------------===//

	//! Map stored PhysicalType to its LogicalType.
	static LogicalType StoredTypeToLogical(PhysicalType stored_type);
	//! Create a temporary FLAT_VECTOR view over a narrow FOR payload buffer.
	static Vector CreatePayloadView(PhysicalType stored_type, data_ptr_t payload, idx_t count);
	static inline bool IsStoredType(PhysicalType stored_type) {
		return stored_type == PhysicalType::UINT8 || stored_type == PhysicalType::UINT16 ||
		       stored_type == PhysicalType::UINT32 || stored_type == PhysicalType::UINT64;
	}
	static inline bool IsThinStoredType(PhysicalType stored_type) {
		return IsStoredType(stored_type) && GetTypeIdSize(stored_type) <= sizeof(uint32_t);
	}

	template <class LOGICAL_T>
	static bool TryGetStoredTypeForMax(LOGICAL_T max_value, PhysicalType &stored_type) {
		if (max_value < LOGICAL_T(0)) {
			return false;
		}
		using UNSIGNED_T = typename FORUnsignedType<LOGICAL_T>::type;
		auto umax = static_cast<UNSIGNED_T>(max_value);
		if (umax <= NumericLimits<uint8_t>::Maximum()) {
			stored_type = PhysicalType::UINT8;
			return true;
		}
		if (umax <= NumericLimits<uint16_t>::Maximum()) {
			stored_type = PhysicalType::UINT16;
			return true;
		}
		if (umax <= NumericLimits<uint32_t>::Maximum()) {
			stored_type = PhysicalType::UINT32;
			return true;
		}
		if (umax <= NumericLimits<uint64_t>::Maximum()) {
			stored_type = PhysicalType::UINT64;
			return true;
		}
		return false;
	}

	static inline bool TryGetStoredTypeForRangeBits(uint8_t range_bits, PhysicalType &stored_type) {
		if (range_bits <= 8) {
			stored_type = PhysicalType::UINT8;
			return true;
		}
		if (range_bits <= 16) {
			stored_type = PhysicalType::UINT16;
			return true;
		}
		if (range_bits <= 32) {
			stored_type = PhysicalType::UINT32;
			return true;
		}
		if (range_bits <= 64) {
			stored_type = PhysicalType::UINT64;
			return true;
		}
		return false;
	}

	static inline bool HasSameMetadata(const Vector &left, const Vector &right) {
		return GetStoredType(left) == GetStoredType(right);
	}

	//! Cast a FOR vector between plain integral types by re-typing the metadata over a zero-copy
	//! view of the same narrow payload. Returns false if the FOR max does not fit the target type.
	static bool TryCastType(Vector &source, Vector &result, idx_t count);

	//! Widen a stored unsigned value to the logical type.
	//! Uses static_cast: FOR values are guaranteed non-negative and in range by construction.
	template <class LOGICAL_T, class STORED_T>
	static inline LOGICAL_T WidenStored(STORED_T val) {
		return static_cast<LOGICAL_T>(val);
	}
};

// Specializations for hugeint_t/uhugeint_t: construct from (0, val)
#define FOR_WIDEN_HUGEINT(LOGICAL, STORED)                                                                             \
	template <>                                                                                                        \
	inline LOGICAL FORVector::WidenStored<LOGICAL, STORED>(STORED val) {                                               \
		return LOGICAL(0, val);                                                                                        \
	}
FOR_WIDEN_HUGEINT(hugeint_t, uint8_t)
FOR_WIDEN_HUGEINT(hugeint_t, uint16_t)
FOR_WIDEN_HUGEINT(hugeint_t, uint32_t)
FOR_WIDEN_HUGEINT(hugeint_t, uint64_t)
FOR_WIDEN_HUGEINT(uhugeint_t, uint8_t)
FOR_WIDEN_HUGEINT(uhugeint_t, uint16_t)
FOR_WIDEN_HUGEINT(uhugeint_t, uint32_t)
FOR_WIDEN_HUGEINT(uhugeint_t, uint64_t)
#undef FOR_WIDEN_HUGEINT

} // namespace duckdb
