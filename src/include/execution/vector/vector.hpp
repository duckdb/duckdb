
#pragma once

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

typedef uint16_t sel_t;

class Vector {
  public:
	oid_t count;
	char *data;
	bool owns_data;
	sel_t *sel_vector;
	TypeId type;

	Vector();
	Vector(Value value);
	Vector(TypeId type, oid_t max_elements = 0);
	~Vector();

	Value GetValue(size_t index);
	void SetValue(size_t index, Value val);

	void Resize(oid_t max_elements);
	void Append(Vector &other);

	void Copy(Vector &other);
	void Move(Vector &other);

	static void Add(Vector &left, Vector &right, Vector &result);
	static void Subtract(Vector &left, Vector &right, Vector &result);
	static void Multiply(Vector &left, Vector &right, Vector &result);
	static void Divide(Vector &left, Vector &right, Vector &result);

	Vector(const Vector &) = delete;
};
}
