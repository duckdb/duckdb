/**************************************************************************/ /**
                                                                              * \file   vector.hpp
                                                                              * \date   2018-07-25
                                                                              * \author Mark Raasveldt
                                                                              ******************************************************************************/

#pragma once

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

typedef uint64_t sel_t;

// sel_vector: An optional selection vector that indicates, from [data]
// which elements actually belong to the vector
class Vector {
  public:
	oid_t count;
	char *data;
	bool owns_data;
	sel_t *sel_vector;
	TypeId type;
	size_t max_elements;

	Vector();
	Vector(Value value);
	Vector(TypeId type, char *dataptr = nullptr, size_t max_elements = 0);
	Vector(TypeId type, oid_t max_elements = 0, bool zero_data = false);
	~Vector();

	void Reset();

	Value GetValue(size_t index);
	void SetValue(size_t index, Value val);

	void Resize(oid_t max_elements, TypeId new_type = TypeId::INVALID);
	void Append(Vector &other);

	void Copy(Vector &other);
	void Move(Vector &other);
	void MoveOrCopy(Vector &other);
	void Reference(Vector &other);

	Vector(const Vector &) = delete;

  private:
	void Destroy();

	std::unique_ptr<char[]> owned_data;
	std::unique_ptr<std::unique_ptr<char[]>[]> owned_strings;
};
} // namespace duckdb
