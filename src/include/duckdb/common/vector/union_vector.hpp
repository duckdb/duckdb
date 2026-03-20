//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/union_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

enum class UnionInvalidReason : uint8_t {
	VALID,
	TAG_OUT_OF_RANGE,
	NO_MEMBERS,
	VALIDITY_OVERLAP,
	TAG_MISMATCH,
	NULL_TAG
};

struct UnionVector {
	// Unions are stored as structs, but the first child is always the "tag"
	// vector, specifying the currently selected member for that row.
	// The remaining children are the members of the union.
	// INVARIANTS:
	//	1.	Only one member vector (the one "selected" by the tag) can be
	//		non-NULL in each row.
	//
	//	2.	The validity of the tag vector always matches the validity of the
	//		union vector itself.
	//
	//  3.  A valid union cannot have a NULL tag, but the selected member can
	//  	be NULL. therefore, there is a difference between a union that "is"
	//  	NULL and a union that "holds" a NULL. The latter still has a valid
	//  	tag.
	//
	//	4.	For each tag in the tag vector, 0 <= tag < |members|

	//! Get the tag vector of a union vector
	DUCKDB_API static const Vector &GetTags(const Vector &v);
	DUCKDB_API static Vector &GetTags(Vector &v);

	//! Try to get the tag at the specific flat index of the union vector. Returns false if the tag is NULL.
	//! This will handle and map the index properly for constant and dictionary vectors internally.
	DUCKDB_API static bool TryGetTag(const Vector &vector, idx_t index, union_tag_t &tag);

	//! Get the member vector of a union vector by index
	DUCKDB_API static const Vector &GetMember(const Vector &vector, idx_t member_index);
	DUCKDB_API static Vector &GetMember(Vector &vector, idx_t member_index);

	//! Set every entry in the UnionVector to a specific member.
	//! This is useful to set the entire vector to a single member, e.g. when "creating"
	//! a union to return in a function, when you only have one alternative to return.
	//! if 'keep_tags_for_null' is false, the tags will be set to NULL where the member is NULL.
	//! (the validity of the tag vector will match the selected member vector)
	//! otherwise, they are all set to the 'tag'.
	//! This will also handle invalidation of the non-selected members
	DUCKDB_API static void SetToMember(Vector &vector, union_tag_t tag, Vector &member_vector, idx_t count,
	                                   bool keep_tags_for_null);

	DUCKDB_API static UnionInvalidReason
	CheckUnionValidity(Vector &vector, idx_t count,
	                   const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
};

} // namespace duckdb
