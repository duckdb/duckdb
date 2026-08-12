//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! A FOR vector holds absolute values in a narrower physical type than its logical type.
//! It uses base=0, so the frame of reference on-disk is folded into it -- base 0 is common and faster
//! Its state lives on the vector's own cache-owned buffer, so the chunk reset un-FORs it and Flatten widens in place.
//! FOR has a keepalive/token/cooldown anti-overhead mechanism so FOR producers stop emitting if there is no benefit
struct ForVector {
	static bool IsFor(const Vector &vector) {
		return vector.GetVectorType() == VectorType::FOR_VECTOR;
	}
	//! Mark a flat vector as FOR: its buffer already holds the narrow payload for the first count rows
	static void Create(Vector &vector, PhysicalType stored_type, uint64_t max_stored, idx_t count);
	static void Discard(Vector &vector) {
		if (!IsFor(vector)) {
			return;
		}
		auto &buffer = vector.BufferMutable();
		buffer.SetVectorTypeOnly(VectorType::FLAT_VECTOR);
		buffer.for_stored_type = PhysicalType::INVALID;
	}
	static void Widen(const Vector &vector) {
		if (IsFor(vector)) {
			vector.Flatten(); //! Safety net for FOR-unaware code: widen back to the logical width.
		}
	}
	static PhysicalType StoredType(const Vector &vector) {
		return vector.GetBufferRef()->for_stored_type;
	}
	static uint64_t MaxStored(const Vector &vector) {
		return vector.GetBufferRef()->for_max; //! So consumers can prove a computation cannot overflow
	}
	//! If a FOR vector is emitted but in the plan never gets exploited and needs to widen, we lose perf
	static constexpr uint16_t COOLDOWN = 64;     // Vectors to let pass before attempting FOR emission
	static bool TokenSet(const Vector &vector) { //! True when the producer should emit FOR.
		auto &cooldown = vector.GetBufferRef()->for_cooldown;
		if (cooldown == 0) {
			return true;
		}
		cooldown--; // Counts down the cooldown. Eventually we will try again
		return false;
	}
	static void MarkExploited(const Vector &vector) {
		auto &buffer = *vector.GetBufferRef();
		buffer.for_cooldown = 0;
		buffer.for_exploited = true;
	}
	//! Hand a narrow payload straight to an integer downcast of the same width, with no copy (refills keepaline).
	//! False when the cast //! is not a pure reinterpretation of the payload, so the caller must run the real cast.
	static bool TryRetype(Vector &source, Vector &result, idx_t count);
	//! An integer upcast (decompression above a join) keeps the values narrow: the result becomes a FOR vector.
	//! This only pays if a later consumer profits, so unlike the downcast is gated on the result's keepalive token.
	static bool TryPromote(Vector &source, Vector &result, idx_t count);
	//! Rewrite a comparison constant into the stored space. False when it falls outside the payload's range:
	//! the comparison is then uniform, which the normal path handles just as well.
	static bool TryStoredConstant(const Vector &vector, const Value &constant, uint64_t &result);
	//! Widen the payload within its own allocation and turn the vector back into a flat one
	static void WidenInPlace(const LogicalType &type, VectorBuffer &buffer);
	//! Gather-widen only the selected values into a target of the logical width. For a selective filter this
	//! touches the survivors instead of the whole vector, which is the whole point of keeping the payload narrow.
	static void WidenGather(const_data_ptr_t src, PhysicalType stored, data_ptr_t target, PhysicalType target_type,
	                        const SelectionVector &sel, idx_t count, idx_t sel_offset = 0);
	//! Widen a narrow payload where it lies. Always in place: a producer abandoning FOR part-way through a vector
	//! has its source and target in the same bytes, so this must never be a forward copy.
	static void WidenInPlace(data_ptr_t data, PhysicalType stored, PhysicalType target_type, idx_t count);
};

} // namespace duckdb
