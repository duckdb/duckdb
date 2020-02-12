#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

nullmask_t ZERO_MASK = nullmask_t(0);

Vector::Vector(TypeId type, bool create_data, bool zero_data)
    : vector_type(VectorType::FLAT_VECTOR), type(type), count(0), selection_vector(nullptr), data(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(TypeId type) : Vector(type, true, false) {
}

Vector::Vector(TypeId type, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(type), count(0), selection_vector(nullptr), data(dataptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value) : vector_type(VectorType::CONSTANT_VECTOR), selection_vector(nullptr) {
	Reference(value);
}

Vector::Vector()
    : vector_type(VectorType::FLAT_VECTOR), type(TypeId::INVALID), count(0), selection_vector(nullptr), data(nullptr) {
}

Vector::Vector(Vector&& other) noexcept :
	vector_type(other.vector_type), type(other.type), nullmask(other.nullmask), count(other.count), selection_vector(other.selection_vector), data(other.data), buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(Value &value) {
	vector_type = VectorType::CONSTANT_VECTOR;
	selection_vector = nullptr;
	type = value.type;
	buffer = VectorBuffer::CreateConstantVector(type);
	data = buffer->GetData();
	count = 1;
	SetValue(0, value);
}

void Vector::Initialize(TypeId new_type, bool zero_data, index_t count) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	auxiliary.reset();
	if (GetTypeIdSize(type) > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, count);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, count * GetTypeIdSize(type));
		}
	}
	nullmask.reset();
}

void Vector::SetValue(index_t index, Value val) {
	Value newVal = val.CastAs(type);

	nullmask[index] = newVal.is_null;
	if (newVal.is_null) {
		return;
	}
	switch (type) {
	case TypeId::BOOL:
		((int8_t *)data)[index] = newVal.value_.boolean;
		break;
	case TypeId::INT8:
		((int8_t *)data)[index] = newVal.value_.tinyint;
		break;
	case TypeId::INT16:
		((int16_t *)data)[index] = newVal.value_.smallint;
		break;
	case TypeId::INT32:
		((int32_t *)data)[index] = newVal.value_.integer;
		break;
	case TypeId::INT64:
		((int64_t *)data)[index] = newVal.value_.bigint;
		break;
	case TypeId::FLOAT:
		((float *)data)[index] = newVal.value_.float_;
		break;
	case TypeId::DOUBLE:
		((double *)data)[index] = newVal.value_.double_;
		break;
	case TypeId::POINTER:
		((uintptr_t *)data)[index] = newVal.value_.pointer;
		break;
	case TypeId::VARCHAR: {
		((const char **)data)[index] = AddString(newVal.str_value);
		break;
	}
	case TypeId::STRUCT: {
		for (size_t i = 0; i < val.struct_value.size(); i++) {
			auto &struct_child = val.struct_value[i];
			assert(vector_type == VectorType::CONSTANT_VECTOR || vector_type == VectorType::FLAT_VECTOR);
			if (i == children.size()) {
				// TODO should this child vector already exist here?
				auto cv = make_unique<Vector>();
				cv->Initialize(struct_child.second.type, true);
				cv->vector_type = vector_type;
				cv->count = count;
				children.push_back(pair<string, unique_ptr<Vector>>(struct_child.first, move(cv)));
			}
			auto &vec_child = children[i];
			if (vec_child.first != struct_child.first) {
				throw Exception("Struct child name mismatch");
			}
			vec_child.second->SetValue(index, struct_child.second);
		}
	} break;
	case TypeId::LIST: {
		// TODO hmmm how do we make space for the value?
	}
	default:
		throw NotImplementedException("Unimplemented type for adding");
	}
}

Value Vector::GetValue(index_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (nullmask[index]) {
		return Value(type);
	}
	switch (type) {
	case TypeId::BOOL:
		return Value::BOOLEAN(((int8_t *)data)[index]);
	case TypeId::INT8:
		return Value::TINYINT(((int8_t *)data)[index]);
	case TypeId::INT16:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case TypeId::INT32:
		return Value::INTEGER(((int32_t *)data)[index]);
	case TypeId::INT64:
		return Value::BIGINT(((int64_t *)data)[index]);
	case TypeId::HASH:
		return Value::HASH(((uint64_t *)data)[index]);
	case TypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case TypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case TypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case TypeId::VARCHAR: {
		char *str = ((char **)data)[index];
		assert(str);
		return Value(string(str));
	}
	case TypeId::STRUCT: {
		Value ret(TypeId::STRUCT);
		ret.is_null = false;
		// we can derive the value schema from the vector schema
		for (auto &struct_child : children) {
			ret.struct_value.push_back(pair<string, Value>(struct_child.first, struct_child.second->GetValue(index)));
		}
		return ret;
	}
	case TypeId::LIST: {
		Value ret(TypeId::LIST);
		ret.is_null = false;
		// get offset and length
		auto offlen = ((list_entry_t *)data)[index];
		assert(children.size() == 1);
		for (index_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(children[0].second->GetValue(i));
		}
		return ret;
	}
	default:
		throw NotImplementedException("Unimplemented type for value access");
	}
}

void Vector::Reference(Vector &other) {
	vector_type = other.vector_type;
	count = other.count;
	buffer = other.buffer;
	auxiliary = other.auxiliary;
	data = other.data;
	selection_vector = other.selection_vector;
	type = other.type;
	nullmask = other.nullmask;
	if (type == TypeId::STRUCT || type == TypeId::LIST) {
		for (size_t i = 0; i < other.children.size(); i++) {
			auto &other_child_vec = other.children[i].second;
			auto child_ref = make_unique<Vector>();
			child_ref->type = other_child_vec->type;
			child_ref->Reference(*other_child_vec.get());
			children.push_back(pair<string, unique_ptr<Vector>>(other.children[i].first, move(child_ref)));
		}
	}
}

void Vector::Flatten() {
	Normalify();
	if (!selection_vector) {
		return;
	}
	Vector other(type, true, false);
	this->Copy(other);
	this->Reference(other);
}

void Vector::Copy(Vector &other, uint64_t offset) {
	if (other.type != type) {
		throw TypeMismatchException(type, other.type,
		                            "Copying to vector of different type not "
		                            "supported! Call Cast instead!");
	}
	if (other.selection_vector) {
		throw NotImplementedException("Copy to vector with sel_vector not supported!");
	}
	Normalify();

	other.nullmask.reset();
	if (!TypeIsConstantSize(type)) {
		switch (type) {
		case TypeId::VARCHAR: {
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
					    target[k - offset] = other.AddString(source[i]);
				    }
			    },
			    offset);
		} break;
		case TypeId::STRUCT: {
			// the main vector only has a nullmask, so set that with offset
			// recursively apply to children
			assert(offset <= count);
			other.count = count - offset;
			other.selection_vector = nullptr;
			assert(offset == 0);
			VectorOperations::Exec(
			    *this, [&](index_t i, index_t k) { other.nullmask[k - offset] = nullmask[i]; }, offset);
			for (auto &child : children) {
				auto child_copy = make_unique<Vector>();
				child_copy->Initialize(child.second->type);

				// TODO this needs to be set elsewhere!
				child.second->selection_vector = selection_vector;
				child.second->count = count;

				child.second->Copy(*child_copy, offset); // TODO the offset is obvious for struct, not so for list/map
				other.children.push_back(pair<string, unique_ptr<Vector>>(child.first, move(child_copy)));
			}
		} break;
		case TypeId::LIST: {
			// copy main vector
			// FIXME
			assert(offset == 0);
			other.Initialize(TypeId::LIST);
			other.count = count - offset;
			other.selection_vector = nullptr;
			VectorOperations::Exec(
			    *this, [&](index_t i, index_t k) { other.nullmask[k - offset] = nullmask[i]; }, offset);

			// FIXME :/
			memcpy(other.data, data, count * GetTypeIdSize(type));

			// copy child
			assert(children.size() == 1); // TODO if size() = 0, we would have all NULLs?
			auto &child = children[0].second;
			auto child_copy = make_unique<Vector>();
			child_copy->Initialize(child->type, true, child->count);
			child->Copy(*child_copy.get(), offset);

			other.children.push_back(pair<string, unique_ptr<Vector>>("", move(child_copy)));
		} break;
		default:
			throw NotImplementedException("Copy type ");
		}

	} else {
		VectorOperations::Copy(*this, other, offset);
	}
}

void Vector::Append(Vector &other) {
	if (selection_vector) {
		throw NotImplementedException("Append to vector with selection vector not supported!");
	}
	if (other.type != type) {
		throw TypeMismatchException(type, other.type, "Can only append vectors of similar types");
	}
	if (count + other.count > STANDARD_VECTOR_SIZE) {
		throw OutOfRangeException("Cannot append to vector: vector is full!");
	}
	other.Normalify();
	assert(vector_type == VectorType::FLAT_VECTOR);
	uint64_t old_count = count;
	count += other.count;
	// merge NULL mask
	VectorOperations::Exec(other, [&](uint64_t i, uint64_t k) { nullmask[old_count + k] = other.nullmask[i]; });
	if (!TypeIsConstantSize(type)) {
		switch (type) {
		case TypeId::VARCHAR: {
			auto source = (const char **)other.data;
			auto target = (const char **)data;
			VectorOperations::Exec(other, [&](uint64_t i, uint64_t k) {
				if (other.nullmask[i]) {
					target[old_count + k] = nullptr;
				} else {
					target[old_count + k] = AddString(source[i]);
				}
			});
		} break;
		case TypeId::STRUCT: {
			// the main vector only has a nullmask, so set that with offset
			// recursively apply to children
			assert(children.size() == other.children.size());
			for (size_t i = 0; i < children.size(); i++) {
				assert(children[i].first == other.children[i].first);
				assert(children[i].second->type == other.children[i].second->type);
				children[i].second->Append(*other.children[i].second.get());
			}
		} break;
		default:
			throw NotImplementedException("Append type ");
		}

	} else {
		VectorOperations::Copy(other, data + old_count * GetTypeIdSize(type));
	}
}

string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString() const {
	string retval = VectorTypeToString(vector_type) + " " + TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		for (index_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		GetSequence(start, increment);
		for (index_t i = 0; i < count; i++) {
			index_t idx = selection_vector ? selection_vector[i] : i;
			retval += to_string(start + increment * idx) + (i == count - 1 ? "" : ", ");
		}
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	Printer::Print(ToString());
}

template <class T>
static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, index_t count, sel_t *sel_vector) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) { output[i] = constant; });
}

void Vector::Normalify() {
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::CONSTANT_VECTOR: {
		vector_type = VectorType::FLAT_VECTOR;
		// allocate a new buffer for the vector
		auto old_buffer = move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		if (nullmask[0]) {
			// constant NULL, set nullmask
			nullmask.set();
			return;
		}
		// non-null constant: have to repeat the constant
		switch (type) {
		case TypeId::BOOL:
		case TypeId::INT8:
			flatten_constant_vector_loop<int8_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::INT16:
			flatten_constant_vector_loop<int16_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::INT32:
			flatten_constant_vector_loop<int32_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::INT64:
			flatten_constant_vector_loop<int64_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::FLOAT:
			flatten_constant_vector_loop<float>(data, old_data, count, selection_vector);
			break;
		case TypeId::DOUBLE:
			flatten_constant_vector_loop<double>(data, old_data, count, selection_vector);
			break;
		case TypeId::HASH:
			flatten_constant_vector_loop<uint64_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::POINTER:
			flatten_constant_vector_loop<uintptr_t>(data, old_data, count, selection_vector);
			break;
		case TypeId::VARCHAR:
			flatten_constant_vector_loop<const char *>(data, old_data, count, selection_vector);
			break;
		case TypeId::STRUCT: {
			for (auto& child : GetChildren()) {
				assert(child.second->vector_type == VectorType::CONSTANT_VECTOR);
				child.second->count = count;
				child.second->selection_vector = selection_vector;
				child.second->Normalify();
			}
			selection_vector = nullptr;
		}
			break;
		default:
			throw NotImplementedException("Unimplemented type for VectorOperations::Normalify");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		GetSequence(start, increment);

		vector_type = VectorType::FLAT_VECTOR;
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, start, increment);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented type for normalify");
	}
}

void Vector::Sequence(int64_t start, int64_t increment, index_t count) {
	vector_type = VectorType::SEQUENCE_VECTOR;
	this->count = count;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 2);
	auto data = buffer->GetData();
	memcpy(data, &start, sizeof(int64_t));
	memcpy(data + sizeof(int64_t), &increment, sizeof(int64_t));
	nullmask.reset();
	auxiliary.reset();
}

void Vector::GetSequence(int64_t &start, int64_t &increment) const {
	assert(vector_type == VectorType::SEQUENCE_VECTOR);
	auto data = buffer->GetData();
	start = *((int64_t *)data);
	increment = *((int64_t *)(data + sizeof(int64_t)));
}

void Vector::Verify() {
	if (count == 0) {
		return;
	}
#ifdef DEBUG
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!nullmask[0]) {
				auto string = ((const char **)data)[0];
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

const char *Vector::AddString(const char *data, index_t len) {
	if (!auxiliary) {
		auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*auxiliary;
	return string_buffer.AddString(data, len);
}

const char *Vector::AddString(const char *data) {
	return AddString(data, strlen(data));
}

const char *Vector::AddString(const string &data) {
	return AddString(data.c_str(), data.size());
}

void Vector::AddHeapReference(Vector &other) {
	if (!other.auxiliary) {
		return;
	}
	if (!auxiliary) {
		auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(auxiliary->type == VectorBufferType::STRING_BUFFER);
	assert(other.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*auxiliary;
	string_buffer.AddHeapReference(other.auxiliary);
}

child_list_t<unique_ptr<Vector>>& Vector::GetChildren() {
	return children;
}

void Vector::AddChild(unique_ptr<Vector> vector, string name) {
	children.push_back(make_pair(name, move(vector)));
}

} // namespace duckdb
