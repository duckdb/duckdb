#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
nullmask_t ZERO_MASK = nullmask_t(0);
}

Vector::Vector(TypeId type, bool create_data, bool zero_data)
    : type(type), count(0), data(nullptr), sel_vector(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(TypeId type, data_ptr_t dataptr) : type(type), count(0), data(dataptr), sel_vector(nullptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value) : Vector(value.type, true, false) {
	count = 1;
	SetValue(0, value);
}

Vector::Vector() : type(TypeId::INVALID), count(0), data(nullptr), sel_vector(nullptr) {
}

Vector::~Vector() {
	Destroy();
}

void Vector::Reference(Value &value) {
	Destroy();
	type = value.type;
	count = 1;
	if (value.is_null) {
		nullmask[0] = true;
	}
	switch (value.type) {
	case TypeId::BOOLEAN:
		data = (data_ptr_t)&value.value_.boolean;
		break;
	case TypeId::TINYINT:
		data = (data_ptr_t)&value.value_.tinyint;
		break;
	case TypeId::SMALLINT:
		data = (data_ptr_t)&value.value_.smallint;
		break;
	case TypeId::INTEGER:
		data = (data_ptr_t)&value.value_.integer;
		break;
	case TypeId::BIGINT:
		data = (data_ptr_t)&value.value_.bigint;
		break;
	case TypeId::FLOAT:
		data = (data_ptr_t)&value.value_.float_;
		break;
	case TypeId::DOUBLE:
		data = (data_ptr_t)&value.value_.double_;
		break;
	case TypeId::HASH:
		data = (data_ptr_t)&value.value_.hash;
		break;
	case TypeId::POINTER:
		data = (data_ptr_t)&value.value_.pointer;
		break;
	case TypeId::VARCHAR: {
		// make size-1 array of char vector
		owned_data = unique_ptr<data_t[]>(new data_t[sizeof(data_ptr_t)]);
		data = owned_data.get();
		// reference the string value of the Value
		auto strings = (const char **)data;
		strings[0] = value.str_value.c_str();
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

void Vector::Initialize(TypeId new_type, bool zero_data) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	string_heap.Destroy();
	owned_data = unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * GetTypeIdSize(type)]);
	data = owned_data.get();
	if (zero_data) {
		memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
	}
	nullmask.reset();
}

void Vector::Destroy() {
	owned_data.reset();
	string_heap.Destroy();
	data = nullptr;
	count = 0;
	sel_vector = nullptr;
	nullmask.reset();
}

void Vector::SetValue(uint64_t index_, Value val) {
	if (index_ >= count) {
		throw OutOfRangeException("SetValue() out of range!");
	}
	Value newVal = val.CastAs(type);

	// set the NULL bit in the null mask
	SetNull(index_, newVal.is_null);
	uint64_t index = sel_vector ? sel_vector[index_] : index_;
	switch (type) {
	case TypeId::BOOLEAN:
		((int8_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.boolean;
		break;
	case TypeId::TINYINT:
		((int8_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.tinyint;
		break;
	case TypeId::SMALLINT:
		((int16_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.smallint;
		break;
	case TypeId::INTEGER:
		((int32_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.integer;
		break;
	case TypeId::BIGINT:
		((int64_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.bigint;
		break;
	case TypeId::FLOAT:
		((float *)data)[index] = newVal.is_null ? 0 : newVal.value_.float_;
		break;
	case TypeId::DOUBLE:
		((double *)data)[index] = newVal.is_null ? 0 : newVal.value_.double_;
		break;
	case TypeId::POINTER:
		((uintptr_t *)data)[index] = newVal.is_null ? 0 : newVal.value_.pointer;
		break;
	case TypeId::VARCHAR: {
		if (newVal.is_null) {
			((const char **)data)[index] = nullptr;
		} else {
			((const char **)data)[index] = string_heap.AddString(newVal.str_value);
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for adding");
	}
}

void Vector::SetStringValue(uint64_t index, const char *value) {
	if (type != TypeId::VARCHAR) {
		throw InvalidTypeException(type, "Can only set string value of VARCHAR vectors!");
	}
	SetNull(index, value ? false : true);
	if (value) {
		((const char **)data)[index] = string_heap.AddString(value);
	} else {
		((const char **)data)[index] = nullptr;
	}
}

Value Vector::GetValue(uint64_t index) const {
	if (index >= count) {
		throw OutOfRangeException("GetValue() out of range");
	}
	if (ValueIsNull(index)) {
		return Value(type);
	}
	uint64_t entry = sel_vector ? sel_vector[index] : index;
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
	case TypeId::HASH:
		return Value::HASH(((uint64_t *)data)[entry]);
	case TypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[entry]);
	case TypeId::FLOAT:
		return Value(((float *)data)[entry]);
	case TypeId::DOUBLE:
		return Value(((double *)data)[entry]);
	case TypeId::VARCHAR: {
		char *str = ((char **)data)[entry];
		assert(str);
		return Value(string(str));
	}
	default:
		throw NotImplementedException("Unimplemented type for conversion");
	}
}

void Vector::Reference(Vector &other) {
	assert(!owned_data);

	count = other.count;
	data = other.data;
	sel_vector = other.sel_vector;
	type = other.type;
	nullmask = other.nullmask;
}

void Vector::Move(Vector &other) {
	other.Destroy();

	other.owned_data = move(owned_data);
	string_heap.Move(other.string_heap);
	other.count = count;
	other.data = data;
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

void Vector::Copy(Vector &other, uint64_t offset) {
	if (other.type != type) {
		throw TypeMismatchException(type, other.type,
		                            "Copying to vector of different type not "
		                            "supported! Call Cast instead!");
	}
	if (other.sel_vector) {
		throw NotImplementedException("Copy to vector with sel_vector not supported!");
	}

	other.nullmask.reset();
	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		other.count = count - offset;
		auto source = (const char **)data;
		auto target = (const char **)other.data;
		VectorOperations::Exec(
		    *this,
		    [&](uint64_t i, uint64_t k) {
			    if (nullmask[i]) {
				    other.nullmask[k - offset] = true;
				    target[k - offset] = nullptr;
			    } else {
				    target[k - offset] = other.string_heap.AddString(source[i]);
			    }
		    },
		    offset);
	} else {
		VectorOperations::Copy(*this, other, offset);
	}
}

void Vector::Cast(TypeId new_type) {
	if (new_type == TypeId::INVALID) {
		throw InvalidTypeException(new_type, "Cannot create a vector of type invalid!");
	}
	if (type == new_type) {
		return;
	}
	Vector new_vector(new_type, true, false);
	VectorOperations::Cast(*this, new_vector);
	new_vector.Move(*this);
}

void Vector::Append(Vector &other) {
	if (sel_vector) {
		throw NotImplementedException("Append to vector with selection vector not supported!");
	}
	if (other.type != type) {
		throw TypeMismatchException(type, other.type, "Can only append vectors of similar types");
	}
	if (count + other.count > STANDARD_VECTOR_SIZE) {
		throw OutOfRangeException("Cannot append to vector: vector is full!");
	}
	uint64_t old_count = count;
	count += other.count;
	// merge NULL mask
	VectorOperations::Exec(other, [&](uint64_t i, uint64_t k) { nullmask[old_count + k] = other.nullmask[i]; });
	if (!TypeIsConstantSize(type)) {
		assert(type == TypeId::VARCHAR);
		auto source = (const char **)other.data;
		auto target = (const char **)data;
		VectorOperations::Exec(other, [&](uint64_t i, uint64_t k) {
			if (other.nullmask[i]) {
				target[old_count + k] = nullptr;
			} else {
				target[old_count + k] = string_heap.AddString(source[i]);
			}
		});
	} else {
		VectorOperations::Copy(other, data + old_count * GetTypeIdSize(type));
	}
}

uint64_t Vector::NotNullSelVector(const Vector &vector, sel_t *not_null_vector, sel_t *&result_assignment,
                                  sel_t *null_vector) {
	if (vector.nullmask.any()) {
		uint64_t result_count = 0, null_count = 0;
		VectorOperations::Exec(vector.sel_vector, vector.count, [&](uint64_t i, uint64_t k) {
			if (!vector.nullmask[i]) {
				not_null_vector[result_count++] = i;
			} else if (null_vector) {
				null_vector[null_count++] = i;
			}
		});
		result_assignment = not_null_vector;
		return result_count;
	} else {
		result_assignment = vector.sel_vector;
		return vector.count;
	}
}

string Vector::ToString() const {
	string retval = TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	for (index_t i = 0; i < count; i++) {
		retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	Printer::Print(ToString());
}

void Vector::Verify() {
#ifdef DEBUG
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		VectorOperations::ExecType<const char *>(*this, [&](const char *string, uint64_t i, uint64_t k) {
			if (!nullmask[i]) {
				assert(string);
				assert(strlen(string) != (size_t)-1);
				assert(Value::IsUTF8String(string));
			}
		});
	}
#endif
}
