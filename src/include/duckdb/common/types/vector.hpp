//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

class VectorCache;
class DictionaryBuffer;
class DictionaryEntry;
class VectorStringBuffer;
class VectorStructBuffer;
class VectorListBuffer;
struct SelCache;
enum class VectorConstructorAction;

template <class T>
class VectorValueIterator;
template <class T>
class VectorValidValueIterator;
class VectorValidityIterator;

enum class VectorDataInitialization { UNINITIALIZED, ZERO_INITIALIZE };

//! Vector of values of a specified PhysicalType.
class Vector {
	friend struct ConstantVector;
	friend struct DictionaryVector;
	friend struct FlatVector;
	friend struct ListVector;
	friend struct StringVector;
	friend struct FSSTVector;
	friend struct StructVector;
	friend struct UnionVector;
	friend struct SequenceVector;
	friend struct ArrayVector;
	friend struct ShreddedVector;

	friend class DataChunk;
	friend class VectorCacheEntry;

public:
	//! Create a vector that slices another vector
	DUCKDB_API explicit Vector(const Vector &other, const SelectionVector &sel, idx_t count);
	//! Create a vector that slices another vector between a pair of offsets
	DUCKDB_API explicit Vector(const Vector &other, idx_t offset, idx_t end);
	//! Create a vector of size one holding the passed on value
	DUCKDB_API explicit Vector(const Value &value);
	//! Create a vector of size tuple_count (non-standard)
	DUCKDB_API explicit Vector(LogicalType type, idx_t capacity = STANDARD_VECTOR_SIZE,
	                           VectorDataInitialization initialize = VectorDataInitialization::UNINITIALIZED);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	DUCKDB_API explicit Vector(const VectorCache &cache);
	//! Create a non-owning vector that references the specified data
	DUCKDB_API Vector(LogicalType type, data_ptr_t dataptr);
	//! Create a vector with an explicitly created vector buffer
	DUCKDB_API Vector(LogicalType type, VectorType vector_type, buffer_ptr<VectorBuffer> buffer);
	// but moving of vectors is allowed
	DUCKDB_API Vector(Vector &&other) noexcept;

public:
	//! Create a new vector that references the other vector
	DUCKDB_API static Vector Ref(const Vector &other);

	//! Create a vector that references the specified value.
	DUCKDB_API void Reference(const Value &value);
	//! Causes this vector to reference the data held by the other vector.
	//! The type of the "other" vector should match the type of this vector
	DUCKDB_API void Reference(const Vector &other);
	//! Reinterpret the data of the other vector as the type of this vector
	//! Note that this takes the data of the other vector as-is and places it in this vector
	//! Without changing the type of this vector
	DUCKDB_API void Reinterpret(const Vector &other);

	//! Causes this vector to reference the data held by the other vector, changes the type if required.
	DUCKDB_API void ReferenceAndSetType(const Vector &other);

	//! Resets a vector from a vector cache.
	//! This turns the vector back into an empty FlatVector with STANDARD_VECTOR_SIZE entries.
	//! The VectorCache is used so this can be done without requiring any allocations.
	DUCKDB_API void ResetFromCache(const VectorCache &cache);

	//! Creates a reference to a slice of the other vector
	DUCKDB_API void Slice(const Vector &other, idx_t offset, idx_t end);
	//! Creates a reference to a slice of the other vector
	DUCKDB_API void Slice(const Vector &other, const SelectionVector &sel, idx_t count);
	//! Turns the vector into a dictionary vector with the specified dictionary
	DUCKDB_API void Slice(const SelectionVector &sel, idx_t count);
	//! Slice the vector, keeping the result around in a cache or potentially using the cache instead of slicing
	DUCKDB_API void Slice(const SelectionVector &sel, idx_t count, SelCache &cache);
	//! Turn this vector into a dictionary vector
	DUCKDB_API void Dictionary(idx_t dictionary_size, const SelectionVector &sel, idx_t count);
	//! Creates a reference to a dictionary of the other vector
	DUCKDB_API void Dictionary(Vector &dict, idx_t dictionary_size, const SelectionVector &sel, idx_t count);
	//! Creates a dictionary on the reusable dict
	DUCKDB_API void Dictionary(buffer_ptr<DictionaryEntry> reusable_dict, const SelectionVector &sel);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	DUCKDB_API void Initialize(VectorDataInitialization data_initialize = VectorDataInitialization::UNINITIALIZED,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);

	//! Converts this Vector to a printable string representation
	DUCKDB_API string ToString(idx_t count) const;
	DUCKDB_API void Print(idx_t count) const;

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	//! While Flatten mutates the buffers / vector type, it does not change the *logical* representation of a vector
	//! As such, it can be used on constant vectors.
	DUCKDB_API void Flatten(idx_t count) const;
	//! Creates a UnifiedVectorFormat of a vector
	//! The UnifiedVectorFormat allows efficient reading of vectors regardless of their vector type
	//! It contains (1) a data pointer, (2) a validity mask, and (3) a selection vector
	//! Access to the individual vector elements can be performed through data_pointer[sel_idx[i]]/validity[sel_idx[i]]
	//! The most common vector types (flat, constant & dictionary) can be converted to the canonical format "for free"
	//! ToUnifiedFormat was originally called Orrify, as a tribute to Orri Erling who came up with it
	DUCKDB_API void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &data) const;
	//! Recursively calls UnifiedVectorFormat on a vector and its child vectors (for nested types)
	static void RecursiveToUnifiedFormat(const Vector &input, idx_t count, RecursiveUnifiedVectorFormat &data);

	//! Turn the vector into a sequence vector
	DUCKDB_API void Sequence(int64_t start, int64_t increment, idx_t count);

	//! Turn the vector into a shredded variant vector
	DUCKDB_API void Shred(Vector &shredded_data);

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	DUCKDB_API void Verify(idx_t count);
	//! Asserts that the CheckMapValidity returns MapInvalidReason::VALID
	DUCKDB_API static void VerifyMap(Vector &map, const SelectionVector &sel, idx_t count);
	DUCKDB_API static void VerifyUnion(Vector &map, const SelectionVector &sel, idx_t count);
	DUCKDB_API static void VerifyVariant(Vector &map, const SelectionVector &sel, idx_t count);
	DUCKDB_API static void Verify(Vector &vector, const SelectionVector &sel, idx_t count);
	DUCKDB_API void UTFVerify(idx_t count);
	DUCKDB_API void UTFVerify(const SelectionVector &sel, idx_t count);

	//! Returns the [index] element of the Vector as a Value.
	DUCKDB_API Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value.
	DUCKDB_API void SetValue(idx_t index, const Value &val);

	inline void CopyBuffer(Vector &other) {
		buffer = other.buffer;
	}

	void AddAuxiliaryData(unique_ptr<AuxiliaryDataHolder> data);
	void AddHeapReference(const Vector &other);

	//! Resizes the vector.
	DUCKDB_API void Resize(idx_t cur_size, idx_t new_size);
	//! Returns a vector of ResizeInfo containing each (nested) vector to resize.
	DUCKDB_API void FindResizeInfos(vector<ResizeInfo> &resize_infos, const idx_t multiplier);

	DUCKDB_API void Serialize(Serializer &serializer, idx_t count, bool compressed_serialization = true);
	DUCKDB_API void Deserialize(Deserializer &deserializer, idx_t count);

	idx_t GetAllocationSize(idx_t cardinality) const;

	// Getters
	VectorType GetVectorType() const;
	inline const LogicalType &GetType() const {
		return type;
	}

	inline buffer_ptr<VectorBuffer> GetBuffer() {
		return buffer;
	}

	// Setters
	DUCKDB_API void SetVectorType(VectorType vector_type);

	// Transform vector to an equivalent dictionary vector
	static void DebugTransformToDictionary(Vector &vector, idx_t count);
	// Transform vector to an equivalent nested vector
	static void DebugShuffleNestedVector(Vector &vector, idx_t count);

	template <class T>
	VectorValueIterator<T> Values(idx_t count) const;

	template <class T>
	VectorValidValueIterator<T> ValidValues(idx_t count) const;

	VectorValidityIterator Validity(idx_t count) const;

private:
	//! Returns the [index] element of the Vector as a Value.
	static Value GetValue(const Vector &v, idx_t index);
	//! Returns the [index] element of the Vector as a Value.
	static Value GetValueInternal(const Vector &v, idx_t index);

	//! This allows a vector to reference another vector while const
	//! This is only used internally in `Flatten` - since referencing
	// an arbitrary other vector could change the logical data contained in the vector (and not be const)
	void ConstReference(const Vector &other) const;

	//! Create a vector that references the other vector
	Vector(const Vector &other, VectorConstructorAction action);

protected:
	//! The type of the elements stored in the vector (e.g. integer, float)
	LogicalType type;
	//! The main buffer holding the data of the vector
	mutable buffer_ptr<VectorBuffer> buffer;
};

} // namespace duckdb

#include "duckdb/common/vector/vector_iterator.hpp"
