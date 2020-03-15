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
	SelectionVector *sel;
	data_ptr_t data;
	nullmask_t *nullmask;
};

//!  Vector of values of a specified TypeId.
class Vector {
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
	bool is_null() {
		assert(vector_type == VectorType::CONSTANT);

	}
	nullmask_t nullmask() {
		assert(vector_type == VectorType::FLAT_VECTOR);
		return
	}
	SelectionVector &GetSelVector() const {
		assert(vector_type == VectorType::DICTIONARY_VECTOR);
		return ((DictionaryBuffer&) buffer).GetSelVector();
	}
	Vector &GetDictionaryChild() const;
	const VectorCardinality &cardinality() const {
		return vcardinality;
	}
	bool SameCardinality(const Vector &other) const {
		return size() == other.size();
	}
	bool SameCardinality(const VectorCardinality &other) const {
		return size() == other.count;
	}

	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, idx_t offset);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, buffer_ptr<VectorBuffer> dictionary);
	//! Create a vector that references the specified value.
	void Reference(Value &value);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(TypeId new_type = TypeId::INVALID, bool zero_data = false, idx_t count = STANDARD_VECTOR_SIZE);

	//! Converts this Vector to a printable string representation
	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	void Normalify();
	//! Obtains a selection vector and data pointer through which the data of this vector can be accessed
	void Orrify(SelectionVector **out_sel, data_ptr_t *out_data);

	//! Turn the vector into a sequence vector
	void Sequence(int64_t start, int64_t increment);
	//! Get the sequence attributes of a sequence vector
	void GetSequence(int64_t &start, int64_t &increment) const;

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify();

	data_ptr_t GetData() {
		return data;
	}

	void MoveStringsToHeap(StringHeap &heap);
	void MoveStringsToHeap(StringHeap &heap, SelectionVector &sel);

	//! Add a string to the string heap of the vector (auxiliary data)
	string_t AddString(const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	string_t AddString(const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	string_t AddString(string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	string_t AddString(const string &data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	string_t EmptyString(idx_t len);

	//! Add a reference from this vector to the string heap of the provided vector
	void AddHeapReference(Vector &other);

	child_list_t<unique_ptr<Vector>> &GetChildren();
	void AddChild(unique_ptr<Vector> vector, string name = "");

	//! Returns the [index] element of the Vector as a Value. Note that this does not consider any selection vectors on
	//! the vector, and returns the element that is physically in location [index].
	Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value. Note that this does not consider any selection
	//! vectors on the vector, and returns the element that is physically in location [index].
	void SetValue(idx_t index, Value val);

protected:
	//! The cardinality of the vector
	const VectorCardinality &vcardinality;
	//! A pointer to the data.
	data_ptr_t data;
	//! The buffer holding the null mask of the vector
	buffer_ptr<VectorBuffer> nulls;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The buffer holding auxiliary data of the vector
	//! e.g. a string vector uses this to store strings
	buffer_ptr<VectorBuffer> auxiliary;
	//! child vectors used for nested data
	child_list_t<unique_ptr<Vector>> children;
};

//! The DictionaryBuffer holds a selection vector
class VectorChildBuffer : public VectorBuffer {
public:
	VectorChildBuffer(const VectorCardinality &cardinality) : VectorBuffer(VectorBufferType::VECTOR_CHILD_BUFFER), data(cardinality) {
	}
public:
	Vector data;
};

class FlatVector : public Vector {
public:
	FlatVector() : Vector(owned_cardinality) {
	}
	FlatVector(TypeId type) : Vector(owned_cardinality, type) {
	}
	FlatVector(TypeId type, data_ptr_t dataptr) : Vector(owned_cardinality, type, dataptr) {
	}

public:
	void SetCount(idx_t count) {
		owned_cardinality.count = count;
	}
protected:
	VectorCardinality owned_cardinality;
};

} // namespace duckdb
