//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/vector.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/types/string_heap.hpp"
#include "common/types/value.hpp"

namespace duckdb {

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
class Vector : public Printable {
	friend class DataChunk;

  public:
	//! The amount of elements in the vector.
	oid_t count;
	//! A pointer to the data.
	char *data;
	//! A flag indicating whether or not the vector owns its data.
	bool owns_data;
	//! The selection vector of the vector.
	sel_t *sel_vector;
	//! The type of the elements stored in the vector.
	TypeId type;
	//! The maximum amount of elements that can be stored in the vector.
	oid_t maximum_size;

	Vector();
	//! Create a vector of size one holding the passed on value
	Vector(Value value);
	//! Create a non-owning vector that references the specified data
	Vector(TypeId type, char *dataptr = nullptr, size_t maximum_size = 0);
	//! Create an owning vector that holds at most maximum_size entries.
	/*!
	    Create an owning vector that holds at most maximum_size entries.
	    If maximum_size is equal to 0, the vector will be an empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(TypeId type, oid_t maximum_size = 0, bool zero_data = false);
	~Vector();

	//! Destroys the vector, deleting any owned data and resetting it to an
	//! empty vector of the specified type.
	void Destroy();

	//! Returns the [index] element of the Vector as a Value.
	Value GetValue(size_t index) const;
	//! Sets the [index] element of the Vector to the specified Value
	void SetValue(size_t index, Value val);

	//! Resizes the vector to hold maximum_size, and potentially typecasts the
	//! elements as well. After the resize, the vector will become an owning
	//! vector even if it was a non-owning vector before.
	void Resize(oid_t maximum_size, TypeId new_type = TypeId::INVALID);
	//! Appends the other vector to this vector. This method will call
	//! Vector::Resize if there is no room for the append, which will cause the
	//! vector to become an owning vector.
	void Append(Vector &other);

	//! Copies the data from this vector to another vector. Note that this
	//! method will not call Vector::Resize and does not change ownership
	//! properties. If other does not have enough space to hold the data this
	//! method will throw an Exception.
	void Copy(Vector &other);
	//! Moves the data from this vector to the other vector. Effectively,
	//! "other" will become equivalent to this vector, and this vector will be
	//! turned into an empty vector.
	void Move(Vector &other);
	//! This method guarantees that the vector becomes an owning vector
	//! If the vector is already an owning vector, it does nothing
	//! Otherwise, it copies the data to the vector
	void ForceOwnership(size_t minimum_capacity = 0);
	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other, size_t offset = 0, size_t max_count = 0);

	//! Converts this Vector to a printable string representation
	std::string ToString() const;

	Vector(const Vector &) = delete;

  private:
	//! If the vector owns data, this is the unique_ptr holds the actual data.
	std::unique_ptr<char[]> owned_data;
	//! If the vector owns a StringHeap, this is the unique_ptr that holds it
	std::unique_ptr<StringHeap> string_heap;
};
} // namespace duckdb
