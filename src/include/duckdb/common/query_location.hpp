//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/query_location.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! A QueryLocation references a contiguous region [offset, offset + length) of the source query text.
//! An invalid QueryLocation (offset == INVALID_OFFSET) marks "no known location" - e.g. for expressions
//! that are synthesized during binding/optimization and have no corresponding source text.
struct QueryLocation {
	static constexpr uint32_t INVALID_OFFSET = uint32_t(-1);

	//! Start byte of the region (INVALID_OFFSET when the location is unknown)
	uint32_t offset = INVALID_OFFSET;
	//! Length of the region in bytes (0 for a point / unknown extent)
	uint32_t length = 0;

	QueryLocation() = default;
	//! Construct from (offset, length). If either value does not fit in a uint32_t the QueryLocation is left invalid.
	QueryLocation(idx_t offset_p, idx_t length_p) {
		if (offset_p < INVALID_OFFSET && length_p < INVALID_OFFSET) {
			offset = static_cast<uint32_t>(offset_p);
			length = static_cast<uint32_t>(length_p);
		}
	}
	//! Bridge from optional_idx (start-only), yielding a zero-length QueryLocation. An invalid or out-of-range idx
	//! yields an invalid QueryLocation. Intended mainly for extensions and the few core sites that only have an
	//! offset (casts, raw parser byte positions, legacy deserialization); prefer threading a real QueryLocation.
	QueryLocation(optional_idx idx) { // NOLINT: allow implicit conversion from optional_idx
		if (idx.IsValid() && idx.GetIndex() < INVALID_OFFSET) {
			offset = static_cast<uint32_t>(idx.GetIndex());
		}
	}

	static QueryLocation Invalid() {
		return QueryLocation();
	}

	bool IsValid() const {
		return offset != INVALID_OFFSET;
	}
	void SetInvalid() {
		offset = INVALID_OFFSET;
		length = 0;
	}

	//! Start byte of the region
	uint32_t Start() const {
		return offset;
	}
	//! One-past-the-end byte of the region
	uint32_t End() const {
		return offset + length;
	}

	//! Bridge back to optional_idx (start offset only) for legacy single-position consumers
	optional_idx GetOffset() const {
		return IsValid() ? optional_idx(offset) : optional_idx();
	}
	//! Implicit bridge to optional_idx (start offset only). Length is dropped.
	operator optional_idx() const { // NOLINT: allow implicit conversion to optional_idx
		return GetOffset();
	}

	//! Returns the smallest QueryLocation enclosing both this and other. Invalid locations are ignored.
	QueryLocation Merge(const QueryLocation &other) const {
		if (!IsValid()) {
			return other;
		}
		if (!other.IsValid()) {
			return *this;
		}
		auto new_start = offset < other.offset ? offset : other.offset;
		auto this_end = End();
		auto other_end = other.End();
		auto new_end = this_end > other_end ? this_end : other_end;
		return QueryLocation(new_start, new_end - new_start);
	}

	bool operator==(const QueryLocation &rhs) const {
		return offset == rhs.offset && length == rhs.length;
	}
	bool operator!=(const QueryLocation &rhs) const {
		return !(*this == rhs);
	}
};

} // namespace duckdb
