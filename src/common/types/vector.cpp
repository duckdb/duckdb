
#include "common/types/vector.hpp"
#include "common/assert.hpp"
#include "common/exception.hpp"

#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

Vector::Vector(TypeId type, size_t maximum_size, bool zero_data)
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

	SetValue(0, value);
}

Vector::Vector()
    : type(TypeId::INVALID), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr), maximum_size(0) {}

Vector::~Vector() { Destroy(); }

void Vector::Destroy() {
	if (data && owns_data) {
		owned_data.reset();
		string_heap.reset();
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
	if (sel_vector) {
		throw Exception("Cannot assign to vector with selection vector");
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
		if (val.is_null) {
			((const char **)data)[index] = nullptr;
		} else {
			if (!string_heap) {
				string_heap = make_unique<StringHeap>();
			}
			((const char **)data)[index] =
			    string_heap->AddString(newVal.str_value);
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for adding");
	}
}

void Vector::SetStringValue(size_t index, const char *value) {
	if (type != TypeId::VARCHAR) {
		throw Exception("Can only set string value of VARCHAR vectors!");
	}
	if (value) {
		if (!string_heap) {
			string_heap = make_unique<StringHeap>();
		}
		((const char **)data)[index] = string_heap->AddString(value);
	} else {
		((const char **)data)[index] = nullptr;
	}
}

Value Vector::GetValue(size_t index) const {
	if (index >= count) {
		throw Exception("Out of bounds");
	}
	size_t entry = sel_vector ? sel_vector[index] : index;
	switch (type) {
	case TypeId::BOOLEAN:
		return Value::BOOLEAN(((int8_t *)data)[entry]);
	case TypeId::TINYINT:
		return Value::TINYINT(((int8_t *)data)[entry]);
	case TypeId::SMALLINT:
		return Value::SMALLINT(((int16_t *)data)[entry]);
	case TypeId::INTEGER:
		return Value::INTEGER(((int32_t *)data)[entry]);
	case TypeId::BIGINT:
		return Value::BIGINT(((int64_t *)data)[entry]);
	case TypeId::POINTER:
		return Value::POINTER(((uint64_t *)data)[entry]);
	case TypeId::DECIMAL:
		return Value(((double *)data)[entry]);
	case TypeId::DATE:
		return Value::DATE(((date_t *)data)[entry]);
		;
	case TypeId::VARCHAR: {
		char *str = ((char **)data)[entry];
		return !str ? Value(TypeId::VARCHAR) : Value(string(str));
	}
	default:
		throw NotImplementedException("Unimplemented type for conversion");
	}
}

void Vector::Reference(Vector &other, size_t offset, size_t max_count) {
	if (owns_data) {
		throw Exception("Vector owns data, cannot create reference!");
	}

	if (max_count == 0) {
		// take the whole chunk
		count = other.count - offset;
	} else {
		count = min(other.count - offset, max_count);
	}
	owns_data = false;
	data = other.data + offset * GetTypeIdSize(other.type);
	sel_vector = other.sel_vector + offset;
	type = other.type;
}

void Vector::Move(Vector &other) {
	other.Destroy();

	if (owns_data) {
		other.owned_data = move(owned_data);
		other.string_heap = move(string_heap);
	}

	other.count = count;
	other.data = data;
	other.owns_data = owns_data;
	other.sel_vector = sel_vector;
	other.type = type;
	other.maximum_size = maximum_size;

	Destroy();
}

void Vector::ForceOwnership(size_t minimum_capacity) {
	if (maximum_size >= minimum_capacity && owns_data &&
	    type != TypeId::VARCHAR)
		return;

	minimum_capacity = std::max(count, minimum_capacity);
	Vector other(type, minimum_capacity);
	Copy(other);
	other.Move(*this);
}

void Vector::Copy(Vector &other) {
	if (other.type != type) {
		throw NotImplementedException(
		    "Copying to vector of different type not supported!");
	}
	if (other.maximum_size < count) {
		throw Exception("Cannot copy to target, not enough space!");
	}
	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		other.count = count;
		const char **source = (const char **)data;
		const char **target = (const char **)other.data;
		other.string_heap = make_unique<StringHeap>();

		for (size_t i = 0; i < count; i++) {
			const char *str = sel_vector ? source[sel_vector[i]] : source[i];
			target[i] = str ? other.string_heap->AddString(str) : nullptr;
		}
	} else {
		VectorOperations::Copy(*this, other);
	}
}

void Vector::Resize(size_t maximum_size, TypeId new_type) {
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
		if (!TypeIsConstantSize(type)) {
			assert(type == TypeId::VARCHAR);
			// we only need to copy the pointers on a resize
			// since the original ownership should not change
			const char **input_ptrs = (const char **)data;
			const char **result_ptrs = (const char **)new_data;
			for (size_t i = 0; i < count; i++) {
				result_ptrs[i] = input_ptrs[i];
			}
		} else {
			VectorOperations::Copy(*this, new_data);
		}
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
	size_t old_count = count;
	count += other.count;
	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		const char **source = (const char **)other.data;
		const char **target = (const char **)data;
		if (!string_heap) {
			string_heap = make_unique<StringHeap>();
		}

		for (size_t i = 0; i < other.count; i++) {
			const char *str =
			    other.sel_vector ? source[other.sel_vector[i]] : source[i];
			target[old_count + i] = str ? string_heap->AddString(str) : nullptr;
		}
	} else {
		VectorOperations::Copy(other, data + old_count * GetTypeIdSize(type));
	}
}

string Vector::ToString() const {
	string retval = TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	for (size_t i = 0; i < count; i++) {
		retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
	}
	retval += "]";
	return retval;
}
