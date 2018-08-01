
#include "common/types/vector.hpp"
#include "common/assert.hpp"
#include "common/exception.hpp"

#include "execution/vector/vector_operations.hpp"

using namespace duckdb;
using namespace std;

Vector::Vector(TypeId type, oid_t max_elements, bool zero_data)
    : type(type), count(0), sel_vector(nullptr), data(nullptr),
      owns_data(false), max_elements(max_elements) {
	if (max_elements > 0) {
		if (type == TypeId::INVALID) {
			throw Exception("Cannot create a vector of type INVALID!");
		}
		owns_data = true;
		owned_data =
		    unique_ptr<char[]>(new char[max_elements * GetTypeIdSize(type)]);
		data = owned_data.get();
		if (zero_data) {
			memset(data, 0, max_elements * GetTypeIdSize(type));
		}
	}
}

Vector::Vector(TypeId type, char *dataptr, size_t max_elements)
    : type(type), count(0), sel_vector(nullptr), data(dataptr),
      owns_data(false), max_elements(max_elements) {
	if (dataptr && type == TypeId::INVALID) {
		throw Exception("Cannot create a vector of type INVALID!");
	}
}
std::unique_ptr<std::unique_ptr<char[]>> owned_strings;

Vector::Vector(Value value)
    : type(value.type), count(1), sel_vector(nullptr), max_elements(1) {
	owns_data = true;
	owned_data = unique_ptr<char[]>(new char[GetTypeIdSize(type)]);
	data = owned_data.get();

	if (TypeIsConstantSize(type)) {
		memcpy(data, &value.value_, GetTypeIdSize(type));
	} else {
		assert(type == TypeId::VARCHAR);
		auto string = new char[value.str_value.size() + 1];
		strcpy(string, value.str_value.c_str());
		auto string_list = new unique_ptr<char[]>[1];
		string_list[0] = unique_ptr<char[]>(string);

		owned_strings = unique_ptr<unique_ptr<char[]>[]>(string_list);

		char **base_data = (char **)data;
		base_data[0] = string;
	}
}

Vector::Vector()
    : type(TypeId::INVALID), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr), max_elements(0) {}

Vector::~Vector() { Destroy(); }

void Vector::Destroy() {
	if (data && owns_data) {
		owned_data.reset();
		owned_strings.reset();
	}
	data = nullptr;
	owns_data = false;
}

void Vector::Reset() {
	Destroy();
	count = 0;
	sel_vector = nullptr;
	max_elements = 0;
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
	case TypeId::BOOLEAN:
		return Value(((bool *)data)[index]);
	case TypeId::TINYINT:
		return Value(((int8_t *)data)[index]);
	case TypeId::SMALLINT:
		return Value(((int16_t *)data)[index]);
	case TypeId::INTEGER:
		return Value(((int *)data)[index]);
	case TypeId::BIGINT:
		return Value(((int64_t *)data)[index]);
	case TypeId::POINTER:
		return Value(((uint64_t *)data)[index]);
	case TypeId::DECIMAL:
		return Value(((double *)data)[index]);
	case TypeId::VARCHAR:
		return Value(string(((char **)data)[index]));
	default:
		throw NotImplementedException("Unimplemented type for conversion");
	}
}

void Vector::Reference(Vector &other) {
	if (owns_data) {
		throw Exception("Vector owns data, cannot create reference!");
	}
	count = other.count;
	data = other.data;
	owns_data = false;
	sel_vector = other.sel_vector;
	type = other.type;
}

void Vector::Move(Vector &other) {
	other.Destroy();

	if (owns_data) {
		other.owned_data = move(owned_data);
		other.owned_strings = move(owned_strings);
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

void Vector::MoveOrCopy(Vector &other) {
	if (owns_data) {
		this->Move(other);
	} else {
		other.count = count;
		other.sel_vector = sel_vector;
		other.type = type;
		other.Resize(this->count);
		this->Copy(other);
	}
}

void Vector::Copy(Vector &other) {
	if (other.type != type) {
		throw NotImplementedException("FIXME cast");
	}
	if (!TypeIsConstantSize(type)) {
		throw NotImplementedException("Cannot copy varlength types yet!");
	}

	memcpy(other.data, data, count * GetTypeIdSize(type));
	other.count = count;
}

void Vector::Resize(oid_t max_elements, TypeId new_type) {
	if (sel_vector) {
		throw Exception("Cannot resize vector with selection vector!");
	}
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	this->max_elements = max_elements;
	char *new_data = new char[max_elements * GetTypeIdSize(type)];
	if (data) {
		memcpy(new_data, data, count * GetTypeIdSize(type));
	}
	Destroy();
	owns_data = true;
	owned_data = unique_ptr<char[]>(new_data);
	data = new_data;
}

void Vector::Append(Vector &other) {
	if (count + other.count >= max_elements) {
		throw Exception("Cannot append to vector: to full!");
	}
	if (sel_vector) {
		throw NotImplementedException(
		    "Cannot append to vector with selection vector");
	}
	if (other.type != type) {
		throw NotImplementedException("FIXME cast");
	}
	VectorOperations::Copy(other, data + count * GetTypeIdSize(type));
	count += other.count;
}
