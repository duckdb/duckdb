//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/variant_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

class Vector;
struct UnifiedVectorFormat;

//===--------------------------------------------------------------------===//
// VariantIterator
//===--------------------------------------------------------------------===//
// VariantIterator iterates over the logical values of a VARIANT vector *without* unshredding it.
//
// A VARIANT vector is either stored in its canonical "unshredded" layout:
//     STRUCT(
//         keys VARCHAR[],
//         children STRUCT(key_id UINTEGER, value_id UINTEGER)[],
//         values STRUCT(type_id UTINYINT, byte_offset UINTEGER)[],
//         data BLOB
//     )
// or as a SHREDDED_VECTOR which combines a typed ("shredded") representation with the leftover
// "unshredded" component for everything that did not fit the shredded schema.
//
// The unshredded component is read through plain VectorIterator<>s over the layout above (see
// UnshreddedVariantLayout) - no RecursiveUnifiedVectorFormat / UnifiedVariantVectorData is involved.
// For a shredded vector the iterator descends into the typed shredded tree directly and only falls
// back to these unshredded iterators for the leftover values, merging the two on the fly.

//! The VectorIterator<> layout of the canonical (unshredded) VARIANT representation
using UnshreddedVariantLayout = VectorStructType<         //
    VectorListType<string_t>,                             // keys VARCHAR[]
    VectorListType<VectorStructType<uint32_t, uint32_t>>, // children STRUCT(key_id, value_id)[]
    VectorListType<VectorStructType<uint8_t, uint32_t>>,  // values STRUCT(type_id, byte_offset)[]
    string_t>;                                            // data BLOB

//! Reads the canonical (unshredded) VARIANT layout (see UnshreddedVariantLayout) through plain
//! VectorIterator<>s. This is the "core" all variant iteration is built on - it is used both for a
//! fully unshredded variant and for the leftover ("unshredded") component of a shredded variant.
class UnshreddedVariantIterator {
public:
	DUCKDB_API explicit UnshreddedVariantIterator(const Vector &unshredded);

public:
	//! Whether the top-level row is valid (non-NULL)
	bool RowIsValid(idx_t row) const;
	//! The type_id of values[value_index]
	VariantLogicalType GetTypeId(idx_t row, idx_t value_index) const;
	//! The byte_offset of values[value_index]
	uint32_t GetByteOffset(idx_t row, idx_t value_index) const;
	//! The data BLOB of the row. Returned by reference: the inlined bytes of a returned string_t copy
	//! would otherwise dangle once the temporary is destroyed.
	const string_t &GetBlob(idx_t row) const;
	//! The key string of keys[key_index]
	string_t GetKey(idx_t row, idx_t key_index) const;
	//! The key_id of children[child_index]
	uint32_t GetKeysIndex(idx_t row, idx_t child_index) const;
	//! The value_id of children[child_index]
	uint32_t GetValuesIndex(idx_t row, idx_t child_index) const;

private:
	VectorIterator<UnshreddedVariantLayout> data;
};

class VariantIterator;

//! Shared state required to iterate a single VARIANT vector. Owns the vector iterators / flattened
//! vectors that the individual VariantIterator cursors point into - so it must outlive any cursor.
class VariantIteratorState {
public:
	DUCKDB_API explicit VariantIteratorState(const Vector &variant);

public:
	//! Whether the row is a (SQL) NULL variant
	DUCKDB_API bool RowIsValid(idx_t row) const;
	//! Returns a cursor pointing at the root value of the given row
	DUCKDB_API VariantIterator Root(idx_t row) const;

private:
	//! The "core": the unshredded component reader (plain vector iterators)
	UnshreddedVariantIterator unshredded;

	//! Whether the variant vector is shredded
	bool is_shredded = false;
	//! The (flattened) shredded component - the root of the shredded tree
	unique_ptr<Vector> shredded_root;
	//! The row validity of the shredded vector
	unique_ptr<UnifiedVectorFormat> row_format;

	friend class VariantIterator;
};

//! A lightweight cursor pointing at a single logical VARIANT value.
class VariantIterator {
public:
	enum class Kind {
		NULL_VALUE, //! a (SQL/variant) NULL value
		MISSING,    //! an absent value (e.g. a missing object field)
		UNSHREDDED, //! a value living in the unshredded component
		SHREDDED    //! a value living in the shredded (typed) component
	};

public:
	VariantIterator() : state(nullptr), kind(Kind::NULL_VALUE) {
	}

public:
	bool IsNull() const {
		return kind == Kind::NULL_VALUE;
	}
	bool IsMissing() const {
		return kind == Kind::MISSING;
	}

	//! The logical type of the value the cursor points at
	DUCKDB_API VariantLogicalType GetTypeId() const;

	//! Returns a pointer to the raw payload of a fixed-width primitive value
	DUCKDB_API const_data_ptr_t GetData() const;
	//! Returns the (variable-length) string payload of a VARCHAR/BLOB/BIGNUM/GEOMETRY/BITSTRING value
	DUCKDB_API string_t GetString() const;
	//! Returns the decimal payload of a DECIMAL value
	DUCKDB_API VariantDecimalData GetDecimal() const;

	//! Returns the (key, value) children of an OBJECT value (merging shredded + unshredded)
	DUCKDB_API vector<pair<string_t, VariantIterator>> GetObjectChildren() const;
	//! Returns the element values of an ARRAY value
	DUCKDB_API vector<VariantIterator> GetArrayChildren() const;

private:
	//! Resolve the shredded node (a "STRUCT(typed_value, [untyped_value_index])" wrapper, or a
	//! flattened primitive) at the given index into a concrete cursor
	static VariantIterator ResolveShredded(const VariantIteratorState &state, const Vector &node, idx_t index,
	                                       idx_t row);

	static VariantIterator MakeUnshredded(const VariantIteratorState &state, idx_t row, uint32_t value_index);
	static VariantIterator MakeShredded(const VariantIteratorState &state, const Vector &content, idx_t index,
	                                    idx_t row, uint32_t overlay_value_index);
	static VariantIterator MakeNull(const VariantIteratorState &state);
	static VariantIterator MakeMissing(const VariantIteratorState &state);

private:
	const VariantIteratorState *state;
	Kind kind;

	//! The row this value belongs to (used for the unshredded component / overlay lookups)
	idx_t row = 0;
	//! UNSHREDDED: 0-based index into the 'values' array of the unshredded component
	uint32_t value_index = 0;

	//! SHREDDED: the typed content vector (the resolved 'typed_value', i.e. the object struct, the
	//! array list, or the primitive vector)
	optional_ptr<const Vector> shredded_content;
	//! SHREDDED: the index into shredded_content
	idx_t shredded_index = 0;
	//! SHREDDED OBJECT: 1-based index into the unshredded component holding the leftover fields
	//! (0 means there is no leftover object to merge)
	uint32_t overlay_value_index = 0;

	friend class VariantIteratorState;
};

} // namespace duckdb
