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
	VectorCardinality() : count(0), sel_vector(nullptr) {}
	VectorCardinality(index_t count, sel_t *sel_vector = nullptr) : count(count), sel_vector(sel_vector) {}

	index_t count;
	sel_t *sel_vector;

	index_t get_index(index_t idx) {
		return sel_vector ? sel_vector[idx] : idx;
	}
};

//!  Vector of values of a specified TypeId.
/*!
  The vector class is the smallest unit of data used by the execution engine. It
  represents a chunk of values of a single type. A vector can either (1) own its
  own data (in which case own_data is set to true), or (2) hold only a reference
  to a set of data (e.g. the base columns or another vector).

  The execution engine operates on vectors mostly through the the set of
  available VectorOperations.

  A vector also has an optional selection vector property. When this property is
  set, the selection vector must be used to access the data as the data pointer
  itself might hold irrelevant empty entries. A simple loop for this would be as
  follows:

  for(auto i = 0; i < count; i++) {
    sum += data[sel_vector[i]];
  }

  Selection vectors are used for two purposes:

  (1) Filtering data without requiring moving and copying data around

  (2) Ordering data
*/
class Vector {
	friend class DataChunk;
public:
	Vector();
	//! Create a vector of size one holding the passed on value
	Vector(Value value);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	Vector(TypeId type);
	//! Create a non-owning vector that references the specified data
	Vector(TypeId type, data_ptr_t dataptr);
	//! Create an owning vector that holds at most STANDARD_VECTOR_SIZE entries.
	/*!
	    Create a new vector
	    If create_data is true, the vector will be an owning empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(TypeId type, bool create_data, bool zero_data);
	// implicit copying of Vectors is not allowed
	Vector(const Vector &) = delete;
	// but moving of vectors is allowed
    Vector(Vector&& other) noexcept;

	//! The vector type specifies how the data of the vector is physically stored (i.e. if it is a single repeated
	//! constant, if it is compressed)
	VectorType vector_type;
	//! The type of the elements stored in the vector (e.g. integer, float)
	TypeId type;
	//! The null mask of the vector, if the Vector has any NULL values
	nullmask_t nullmask;
public:
	index_t size() const {
		return count;
	}
	sel_t *sel_vector() const {
		return selection_vector;
	}
	bool SameCardinality(const Vector &other) const {
		return size() == other.size() && sel_vector() == other.sel_vector();
	}
	void SetCount(index_t count) {
		this->count = count;
	}
	void SetSelVector(sel_t *sel) {
		this->selection_vector = sel;
	}

	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, index_t offset);
	//! Create a vector that references the specified value.
	void Reference(Value &value);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(TypeId new_type = TypeId::INVALID, bool zero_data = false, index_t count = STANDARD_VECTOR_SIZE);
	//! Flattens the vector, removing any selection vector
	void ClearSelectionVector();

	//! Converts this Vector to a printable string representation
	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	void Normalify();

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

	//! Add a string to the string heap of the vector (auxiliary data)
	const char *AddString(const char *data, index_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	const char *AddString(const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	const char *AddString(const string &data);

	//! Add a reference from this vector to the string heap of the provided vector
	void AddHeapReference(Vector &other);

	child_list_t<unique_ptr<Vector>>& GetChildren();
	void AddChild(unique_ptr<Vector> vector, string name="");

	//! Returns the [index] element of the Vector as a Value. Note that this does not consider any selection vectors on the vector, and returns the element that is physically in location [index].
	Value GetValue(index_t index) const;
	//! Sets the [index] element of the Vector to the specified Value. Note that this does not consider any selection vectors on the vector, and returns the element that is physically in location [index].
	void SetValue(index_t index, Value val);
protected:
	//! The amount of elements in the vector.
	index_t count;
	//! The selection vector of the vector.
	sel_t *selection_vector;
	//! A pointer to the data.
	data_ptr_t data;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The secondary buffer holding auxiliary data of the vector, for example, a string vector uses this to store
	//! strings
	buffer_ptr<VectorBuffer> auxiliary;
	//! child vectors used for nested data
	child_list_t<unique_ptr<Vector>> children;
};

class FlatVector : public Vector {
public:
	FlatVector(TypeId type) : Vector(type) {}

};

} // namespace duckdb
