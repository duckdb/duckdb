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
    : vector_type(VectorType::FLAT_VECTOR), type(type), count(0), sel_vector(nullptr), data(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(TypeId type, data_ptr_t dataptr) :
	vector_type(VectorType::FLAT_VECTOR), type(type), count(0), sel_vector(nullptr), data(dataptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value) : vector_type(VectorType::CONSTANT_VECTOR), sel_vector(nullptr) {
	Reference(value);
}

Vector::Vector() : vector_type(VectorType::FLAT_VECTOR), type(TypeId::INVALID), count(0), sel_vector(nullptr), data(nullptr) {
}

Vector::~Vector() {
	Destroy();
}

void Vector::Reference(Value &value) {
	Destroy();
	vector_type = VectorType::CONSTANT_VECTOR;
	sel_vector = nullptr;
	type = value.type;
	owned_data = unique_ptr<data_t[]>(new data_t[GetTypeIdSize(type)]);
	data = owned_data.get();
	count = 1;
	SetValue(0, value);
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

	uint64_t index = sel_vector ? sel_vector[index_] : index_;
	nullmask[index] = newVal.is_null;
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
	assert(type == TypeId::VARCHAR);
	SetValue(index, value ? Value(value) : Value());
}

Value Vector::GetValue(uint64_t index) const {
	if (index >= count) {
		throw OutOfRangeException("GetValue() out of range");
	}
	uint64_t entry = sel_vector ? sel_vector[index] : index;
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		entry = 0;
	}
	if (nullmask[entry]) {
		return Value(type);
	}
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

	vector_type = other.vector_type;
	count = other.count;
	data = other.data;
	sel_vector = other.sel_vector;
	type = other.type;
	nullmask = other.nullmask;
}

void Vector::Move(Vector &other) {
	other.Destroy();

	other.vector_type = vector_type;
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
	if (vector_type == VectorType::FLAT_VECTOR) {
		for (index_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
	} else {
		retval += GetValue(0).ToString();
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	Printer::Print(ToString());
}

void Vector::Normalify() {
	switch(vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		return;
	case VectorType::CONSTANT_VECTOR: {
		auto constant_value = GetValue(0);
		Vector new_result(type, true, false);
		new_result.count = count;
		new_result.sel_vector = sel_vector;
		VectorOperations::Set(new_result, constant_value);
		new_result.Move(*this);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented type for normalify");
	}
}

void Vector::Verify() {
#ifdef DEBUG
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!nullmask[0]) {
				auto string = ((const char**) data)[0];
				assert(string);
				assert(strlen(string) != (size_t)-1);
				assert(Value::IsUTF8String(string));
			}
		} else {
			VectorOperations::ExecType<const char *>(*this, [&](const char *string, uint64_t i, uint64_t k) {
				if (!nullmask[i]) {
					assert(string);
					assert(strlen(string) != (size_t)-1);
					assert(Value::IsUTF8String(string));
				}
			});
		}
	}
#endif
}
