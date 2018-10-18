
#include "common/types/vector.hpp"
#include "common/assert.hpp"
#include "common/exception.hpp"

#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

Vector::Vector(TypeId type, bool create_data, bool zero_data)
    : type(type), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(TypeId type, char *dataptr)
    : type(type), count(0), data(dataptr), owns_data(false),
      sel_vector(nullptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type,
		                           "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value) : Vector(value.type, true, false) {
	count = 1;
	SetValue(0, value);
}

Vector::Vector()
    : type(TypeId::INVALID), count(0), data(nullptr), owns_data(false),
      sel_vector(nullptr) {}

Vector::~Vector() { Destroy(); }

void Vector::Initialize(TypeId new_type, bool zero_data) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	string_heap.Destroy();
	owns_data = true;
	owned_data = unique_ptr<char[]>(
	    new char[STANDARD_VECTOR_SIZE * GetTypeIdSize(type)]);
	data = owned_data.get();
	if (zero_data) {
		memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
	}
}

void Vector::Destroy() {
	if (data && owns_data) {
		owned_data.reset();
		string_heap.Destroy();
	}
	data = nullptr;
	owns_data = false;
	count = 0;
	sel_vector = nullptr;
	nullmask.reset();
}

void Vector::SetValue(size_t index_, Value val) {
	if (index_ >= count) {
		throw OutOfRangeException("SetValue() out of range!");
	}
	Value newVal = val.CastAs(type);

	// set the NULL bit in the null mask
	SetNull(index_, newVal.is_null);
	size_t index = sel_vector ? sel_vector[index_] : index_;
	switch (type) {
	case TypeId::BOOLEAN:
		((int8_t *)data)[index] = val.is_null ? 0 : newVal.value_.boolean;
		break;
	case TypeId::TINYINT:
		((int8_t *)data)[index] = val.is_null ? 0 : newVal.value_.tinyint;
		break;
	case TypeId::SMALLINT:
		((int16_t *)data)[index] = val.is_null ? 0 : newVal.value_.smallint;
		break;
	case TypeId::INTEGER:
		((int32_t *)data)[index] = val.is_null ? 0 : newVal.value_.integer;
		break;
	case TypeId::BIGINT:
		((int64_t *)data)[index] = val.is_null ? 0 : newVal.value_.bigint;
		break;
	case TypeId::DECIMAL:
		((double *)data)[index] = val.is_null ? 0 : newVal.value_.decimal;
		break;
	case TypeId::POINTER:
		((uint64_t *)data)[index] = val.is_null ? 0 : newVal.value_.pointer;
		break;
	case TypeId::DATE:
		((date_t *)data)[index] = val.is_null ? 0 : newVal.value_.date;
		break;
	case TypeId::VARCHAR: {
		if (val.is_null) {
			((const char **)data)[index] = nullptr;
		} else {
			((const char **)data)[index] =
			    string_heap.AddString(newVal.str_value);
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for adding");
	}
}

void Vector::SetStringValue(size_t index, const char *value) {
	if (type != TypeId::VARCHAR) {
		throw InvalidTypeException(
		    type, "Can only set string value of VARCHAR vectors!");
	}
	SetNull(index, value ? false : true);
	if (value) {
		((const char **)data)[index] = string_heap.AddString(value);
	} else {
		((const char **)data)[index] = nullptr;
	}
}

Value Vector::GetValue(size_t index) const {
	if (index >= count) {
		throw OutOfRangeException("GetValue() out of range");
	}
	if (ValueIsNull(index)) {
		return Value(type);
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

void Vector::Reference(Vector &other) {
	assert(!owns_data);

	count = other.count;
	owns_data = false;
	data = other.data;
	sel_vector = other.sel_vector;
	type = other.type;
	nullmask = other.nullmask;
}

void Vector::Move(Vector &other) {
	other.Destroy();

	if (owns_data) {
		other.owned_data = move(owned_data);
		string_heap.Move(other.string_heap);
	}

	other.count = count;
	other.data = data;
	other.owns_data = owns_data;
	other.sel_vector = sel_vector;
	other.type = type;
	other.nullmask = nullmask;

	Destroy();
}

void Vector::Flatten() {
	if (!sel_vector) {
		return;
	}
	Vector other(type, true, false);
	this->Copy(other);
	other.Move(*this);
}

void Vector::Copy(Vector &other, size_t offset) {
	if (other.type != type) {
		throw TypeMismatchException(type, other.type,
		                            "Copying to vector of different type not "
		                            "supported! Call Cast instead!");
	}
	if (other.sel_vector) {
		throw NotImplementedException(
		    "Copy to vector with sel_vector not supported!");
	}

	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		other.count = count - offset;
		const char **source = (const char **)data;
		const char **target = (const char **)other.data;
		for (size_t i = 0; i < other.count; i++) {
			const char *str = sel_vector ? source[sel_vector[i + offset]]
			                             : source[i + offset];
			target[i] = str ? other.string_heap.AddString(str) : nullptr;
			if (sel_vector) {
				for (size_t i = 0; i < count; i++) {
					other.nullmask[i] = nullmask[sel_vector[offset + i]];
				}
			} else {
				other.nullmask = nullmask << offset;
			}
		}
	} else {
		VectorOperations::Copy(*this, other, offset);
	}
}

void Vector::Cast(TypeId new_type) {
	if (new_type == TypeId::INVALID) {
		throw InvalidTypeException(new_type,
		                           "Cannot create a vector of type invalid!");
	}
	if (type == new_type) {
		return;
	}
	type = new_type;
	Vector new_vector(new_type, true, false);
	VectorOperations::Cast(*this, new_vector);
	new_vector.Move(*this);
}

void Vector::Append(Vector &other) {
	if (sel_vector) {
		throw NotImplementedException(
		    "Append to vector with selection vector not supported!");
	}
	if (other.type != type) {
		throw TypeMismatchException(type, other.type,
		                            "Can only append vectors of similar types");
	}
	if (count + other.count > STANDARD_VECTOR_SIZE) {
		throw OutOfRangeException("Cannot append to vector: vector is full!");
	}
	size_t old_count = count;
	count += other.count;
	if (!other.sel_vector) {
		// we can simply shift the NULL mask and OR it
		nullmask |= other.nullmask << old_count;
	} else {
		// have to merge NULL mask
		for (size_t i = 0; i < other.count; i++) {
			nullmask[old_count + i] = other.nullmask[other.sel_vector[i]];
		}
	}
	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		const char **source = (const char **)other.data;
		const char **target = (const char **)data;
		for (size_t i = 0; i < other.count; i++) {
			const char *str =
			    other.sel_vector ? source[other.sel_vector[i]] : source[i];
			target[old_count + i] = str ? string_heap.AddString(str) : nullptr;
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
