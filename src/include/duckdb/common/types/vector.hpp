//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitset.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/types/vector_buffer.hpp"

namespace duckdb {
//! Type used for nullmasks
typedef bitset<STANDARD_VECTOR_SIZE> nullmask_t;

//! Zero NULL mask: filled with the value 0 [READ ONLY]
extern nullmask_t ZERO_MASK;

class VectorCardinality {
public:
	VectorCardinality() : count(0) {
	}
	VectorCardinality(idx_t count) : count(count) {
	}

	idx_t count;
};

struct VectorData {
	const SelectionVector *sel;
	data_ptr_t data;
	nullmask_t *nullmask;
};

class VectorStructBuffer;
class VectorListBuffer;
class ChunkCollection;

//!  Vector of values of a specified TypeId.
class Vector {
	friend struct ConstantVector;
	friend struct DictionaryVector;
	friend struct FlatVector;
	friend struct ListVector;
	friend struct StringVector;
	friend struct StructVector;
	friend struct SequenceVector;

	friend class DataChunk;
public:
	Vector(const VectorCardinality &cardinality);
	//! Create a vector of size one holding the passed on value
	Vector(const VectorCardinality &cardinality, Value value);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	Vector(const VectorCardinality &cardinality, TypeId type);
	//! Create a non-owning vector that references the specified data
	Vector(const VectorCardinality &cardinality, TypeId type, data_ptr_t dataptr);
	//! Create an owning vector that holds at most STANDARD_VECTOR_SIZE entries.
	/*!
	    Create a new vector
	    If create_data is true, the vector will be an owning empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(const VectorCardinality &cardinality, TypeId type, bool create_data, bool zero_data);
	// implicit copying of Vectors is not allowed
	Vector(const Vector &) = delete;
	// but moving of vectors is allowed
	Vector(Vector &&other) noexcept;

	//! The vector type specifies how the data of the vector is physically stored (i.e. if it is a single repeated
	//! constant, if it is compressed)
	VectorType vector_type;
	//! The type of the elements stored in the vector (e.g. integer, float)
	TypeId type;
public:
	idx_t size() const {
		return vcardinality.count;
	}
	const VectorCardinality &cardinality() const {
		return vcardinality;
	}
	bool SameCardinality(const Vector &other) const {
		return size() == other.size();
	}
	bool SameCardinality(const VectorCardinality &other) const {
		return size() == other.count;
	}

	//! Create a vector that references the specified value.
	void Reference(Value &value);
	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, idx_t offset);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, SelectionVector &sel);
	//! Turns the vector into a dictionary vector with the specified dictionary
	void Slice(SelectionVector &sel);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(TypeId new_type = TypeId::INVALID, bool zero_data = false);

	//! Converts this Vector to a printable string representation
	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	void Normalify();
	//! Obtains a selection vector and data pointer through which the data of this vector can be accessed
	void Orrify(VectorData &data);

	//! Turn the vector into a sequence vector
	void Sequence(int64_t start, int64_t increment);

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify();

	//! Returns the [index] element of the Vector as a Value.
	Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value.
	void SetValue(idx_t index, Value val);
protected:
	//! The cardinality of the vector
	const VectorCardinality &vcardinality;
	//! A pointer to the data.
	data_ptr_t data;
	//! The nullmask of the vector
	nullmask_t nullmask;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The buffer holding auxiliary data of the vector
	//! e.g. a string vector uses this to store strings
	buffer_ptr<VectorBuffer> auxiliary;
};

//! The DictionaryBuffer holds a selection vector
class VectorChildBuffer : public VectorBuffer {
public:
	VectorChildBuffer(const VectorCardinality &cardinality) : VectorBuffer(VectorBufferType::VECTOR_CHILD_BUFFER), data(cardinality) {
	}
public:
	Vector data;
};

struct ConstantVector {
	static inline data_ptr_t GetData(Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR || vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	template<class T>
	static inline T* GetData(Vector &vector) {
		return (T*) ConstantVector::GetData(vector);
	}
	static inline bool IsNull(const Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		return vector.nullmask[0];
	}
	static inline void SetNull(Vector &vector, bool is_null) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		vector.nullmask[0] = is_null;
	}
	static inline nullmask_t& Nullmask(Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		return vector.nullmask;
	}

	static const sel_t zero_vector[STANDARD_VECTOR_SIZE];
	static const SelectionVector ZeroSelectionVector;
};

struct DictionaryVector {
	static inline SelectionVector &SelectionVector(const Vector &vector) {
		assert(vector.vector_type == VectorType::DICTIONARY_VECTOR);
		return ((DictionaryBuffer&) vector.buffer).GetSelVector();
	}
	static inline Vector &Child(const Vector &vector) {
		assert(vector.vector_type == VectorType::DICTIONARY_VECTOR);
		return ((VectorChildBuffer&) vector.auxiliary).data;
	}
};

struct FlatVector {
	static inline data_ptr_t GetData(Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	template<class T>
	static inline T* GetData(Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	template<class T>
	static inline T GetValue(Vector &vector, idx_t idx) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline nullmask_t& Nullmask(Vector &vector) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.nullmask;
	}
	static inline void SetNullmask(Vector &vector, nullmask_t new_mask) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		vector.nullmask = move(new_mask);
	}
	static inline void SetNull(Vector &vector, idx_t idx, bool value) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		vector.nullmask[idx] = value;
	}
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.nullmask[idx];
	}

	static const sel_t incremental_vector[STANDARD_VECTOR_SIZE];
	static const SelectionVector IncrementalSelectionVector;
};

struct ListVector {
	static ChunkCollection &GetEntry(const Vector &vector);
	static bool HasEntry(const Vector &vector);
	static void SetEntry(Vector &vector, unique_ptr<ChunkCollection> entry);
};

struct StringVector {
	static void MoveStringsToHeap(Vector &vector, StringHeap &heap);
	static void MoveStringsToHeap(Vector &vector, StringHeap &heap, SelectionVector &sel);

	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const string &data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	static string_t EmptyString(Vector &vector, idx_t len);

	//! Add a reference from this vector to the string heap of the provided vector
	static void AddHeapReference(Vector &vector, Vector &other);
};

struct StructVector {
	static child_list_t<unique_ptr<Vector>> &GetEntries(const Vector &vector);
	static void AddEntry(Vector &vector, string name, unique_ptr<Vector> entry);
};

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
		assert(vector.vector_type == VectorType::SEQUENCE_VECTOR);
		auto data = (int64_t *)vector.buffer->GetData();
		start = data[0];
		increment = data[1];
	}
};

class StandaloneVector : public Vector {
public:
	StandaloneVector() : Vector(owned_cardinality) {
	}
	StandaloneVector(TypeId type) : Vector(owned_cardinality, type) {
	}
	StandaloneVector(TypeId type, data_ptr_t dataptr) : Vector(owned_cardinality, type, dataptr) {
	}

public:
	void SetCount(idx_t count) {
		assert(count <= STANDARD_VECTOR_SIZE);
		owned_cardinality.count = count;
	}
protected:
	VectorCardinality owned_cardinality;
};

} // namespace duckdb
