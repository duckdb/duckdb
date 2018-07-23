
#include "common/types/vector.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

Vector::Vector(TypeId type, oid_t max_elements)
    : type(type), count(0), sel_vector(nullptr), data(nullptr),
      owns_data(false) {
	if (max_elements > 0) {
		if (type == TypeId::INVALID) {
			throw Exception("Cannot create a vector of type INVALID!");
		}
		owns_data = true;
		data = new char[max_elements * GetTypeIdSize(type)];
	}
}

Vector::Vector(Value value) : type(value.type), count(1), sel_vector(nullptr) {
	owns_data = true;
	data = new char[GetTypeIdSize(type)];
	memcpy(data, &value.value_, GetTypeIdSize(type));
}

Vector::Vector()
    : type(TypeId::INVALID), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr) {}

Vector::~Vector() {
	if (data && owns_data) {
		delete[] data;
	}
}

void Vector::SetValue(size_t index, Value val) {
	Value newVal = val.CastAs(type);
	switch (type) {
	case TypeId::INTEGER:
		((int *)data)[index] = newVal.value_.integer;
		count++;
		break;
	default:
		throw NotImplementedException("Unimplemented type for adding");
	}
}

Value Vector::GetValue(size_t index) {
	if (index >= count) {
		throw Exception("Out of bounds");
	}
	switch (type) {
	case TypeId::INTEGER:
		return Value(((int *)data)[index]);
	case TypeId::DECIMAL:
		return Value(((double *)data)[index]);
	default:
		throw NotImplementedException("Unimplemented type for conversion");
	}
}

void Vector::Move(Vector &other) {
	if (other.data && other.owns_data) {
		delete[] other.data;
	}

	other.count = count;
	other.data = data;
	other.owns_data = owns_data;
	other.sel_vector = sel_vector;
	other.type = type;

	data = nullptr;
	owns_data = false;
	sel_vector = nullptr;
	count = 0;
}

void Vector::Copy(Vector &other) {
	if (other.type != type) {
		throw NotImplementedException("todo: cast");
	}

	memcpy(other.data, data, count * GetTypeIdSize(type));
	other.count = count;
}

void Vector::Resize(oid_t max_elements) {
	char *new_data = new char[max_elements * GetTypeIdSize(type)];
	memcpy(new_data, data, count * GetTypeIdSize(type));
	if (owns_data) {
		delete[] data;
	}
	owns_data = true;
	data = new_data;
}

void Vector::Append(Vector &other) {
	if (sel_vector || other.sel_vector) {
		throw NotImplementedException("fixme selvector");
	}
	if (other.type != type) {
		throw NotImplementedException("fixme cast");
	}
	memcpy(data + count * GetTypeIdSize(type), other.data,
	       other.count * GetTypeIdSize(type));
	count += other.count;
}
