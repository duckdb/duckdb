
#include "common/types/vector.hpp"
#include "common/assert.hpp"
#include "common/exception.hpp"

#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

Vector::Vector(TypeId type, oid_t maximum_size, bool zero_data)
    : type(type), count(0), sel_vector(nullptr), data(nullptr),
      owns_data(false), maximum_size(maximum_size) {
	if (maximum_size > 0) {
		if (type == TypeId::INVALID) {
			throw Exception("Cannot create a vector of type INVALID!");
		}
		owns_data = true;
		owned_data =
		    unique_ptr<char[]>(new char[maximum_size * GetTypeIdSize(type)]);
		data = owned_data.get();
		if (zero_data) {
			memset(data, 0, maximum_size * GetTypeIdSize(type));
		}
	}
}

Vector::Vector(TypeId type, char *dataptr, size_t maximum_size)
    : type(type), count(0), sel_vector(nullptr), data(dataptr),
      owns_data(false), maximum_size(maximum_size) {
	if (dataptr && type == TypeId::INVALID) {
		throw Exception("Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value)
    : type(value.type), count(1), sel_vector(nullptr), maximum_size(1) {
	owns_data = true;
	owned_data = unique_ptr<char[]>(new char[GetTypeIdSize(type)]);
	data = owned_data.get();

	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		auto string_list = new unique_ptr<char[]>[1];
		owned_strings = unique_ptr<unique_ptr<char[]>[]>(string_list);
	}

	SetValue(0, value);
}

Vector::Vector()
    : type(TypeId::INVALID), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr), maximum_size(0) {}

Vector::~Vector() { Destroy(); }

void Vector::Destroy() {
	if (data && owns_data) {
		owned_data.reset();
		owned_strings.reset();
	}
	data = nullptr;
	owns_data = false;
	count = 0;
	sel_vector = nullptr;
	maximum_size = 0;
}

void Vector::SetValue(size_t index, Value val) {
	if (index >= count) {
		throw Exception("Out of range exception!");
	}
	Value newVal = val.CastAs(type);
	switch (type) {
	case TypeId::BOOLEAN:
		((int8_t *)data)[index] =
		    val.is_null ? NullValue<int8_t>() : newVal.value_.boolean;
		break;
	case TypeId::TINYINT:
		((int8_t *)data)[index] =
		    val.is_null ? NullValue<int8_t>() : newVal.value_.tinyint;
		break;
	case TypeId::SMALLINT:
		((int16_t *)data)[index] =
		    val.is_null ? NullValue<int16_t>() : newVal.value_.smallint;
		break;
	case TypeId::INTEGER:
		((int32_t *)data)[index] =
		    val.is_null ? NullValue<int32_t>() : newVal.value_.integer;
		break;
	case TypeId::BIGINT:
		((int64_t *)data)[index] =
		    val.is_null ? NullValue<int64_t>() : newVal.value_.bigint;
		break;
	case TypeId::DECIMAL:
		((double *)data)[index] =
		    val.is_null ? NullValue<double>() : newVal.value_.decimal;
		break;
	case TypeId::POINTER:
		((uint64_t *)data)[index] =
		    val.is_null ? NullValue<uint64_t>() : newVal.value_.pointer;
		break;
	case TypeId::DATE:
		((date_t *)data)[index] =
		    val.is_null ? NullValue<date_t>() : newVal.value_.date;
		break;
	case TypeId::VARCHAR: {
		auto string = new char[newVal.str_value.size() + 1];
		strcpy(string, newVal.str_value.c_str());
		owned_strings[index] = unique_ptr<char[]>(string);
		((char **)data)[index] = newVal.is_null ? NullValue<char *>() : string;
		break;
	}
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

	Destroy();
}

void Vector::ForceOwnership() {
	if (owns_data)
		return;

	Vector other(type, count);
	Copy(other);
	other.Move(*this);
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

void Vector::Resize(oid_t maximum_size, TypeId new_type) {
	if (sel_vector) {
		throw Exception("Cannot resize vector with selection vector!");
	}
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	if (maximum_size < count) {
		throw Exception(
		    "Cannot resize vector to smaller than current count!\n");
	}
	char *new_data = new char[maximum_size * GetTypeIdSize(type)];
	if (data) {
		memcpy(new_data, data, count * GetTypeIdSize(type));
	}
	Destroy();
	this->maximum_size = maximum_size;
	owns_data = true;
	owned_data = unique_ptr<char[]>(new_data);
	data = new_data;
}

void Vector::Append(Vector &other) {
	if (count + other.count >= maximum_size) {
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

void Vector::SetSelVector(sel_t *vector, size_t new_count) {
	if (!vector) {
		this->count = new_count;
		return;
	}
	if (sel_vector) {
		// already has a selection vector! we have to merge them
		auto new_vector = new sel_t[new_count];
		for (size_t i = 0; i < new_count; i++) {
			assert(vector[i] < this->count);
			new_vector[i] = sel_vector[vector[i]];
		}
		sel_vector = new_vector;
		owned_sel_vector = unique_ptr<sel_t[]>(new_vector);
	} else {
		this->sel_vector = vector;
		owned_sel_vector = nullptr;
	}
	this->count = new_count;
}

string Vector::ToString() const {
	string retval = TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	for (size_t i = 0; i < count; i++) {
		retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
	}
	retval += "]";
	return retval;
}
