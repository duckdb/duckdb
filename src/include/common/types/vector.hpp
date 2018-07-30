/**************************************************************************/ /**
                                                                              * \file   vector.hpp
                                                                              * \date   2018-07-25
                                                                              * \author Mark Raasveldt
                                                                              ******************************************************************************/

#pragma once

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

typedef uint32_t sel_t;

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

	Value GetValue(size_t index);
	void SetValue(size_t index, Value val);

	void Resize(oid_t max_elements, TypeId new_type = TypeId::INVALID);
	void Append(Vector &other);

	void Copy(Vector &other);
	void Move(Vector &other);
	void Reference(Vector &other);

	Vector(const Vector &) = delete;
};
} // namespace duckdb
