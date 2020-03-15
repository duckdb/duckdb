#include <cstring> // strlen() on Solaris

#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

nullmask_t ZERO_MASK = nullmask_t(0);

Vector::Vector(const VectorCardinality &vcardinality, TypeId type, bool create_data, bool zero_data)
    : vector_type(VectorType::FLAT_VECTOR), type(type), vcardinality(vcardinality), data(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(const VectorCardinality &vcardinality, TypeId type) : Vector(vcardinality, type, true, false) {
}

Vector::Vector(const VectorCardinality &vcardinality, TypeId type, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(type), vcardinality(vcardinality), data(dataptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const VectorCardinality &vcardinality, Value value)
    : vector_type(VectorType::CONSTANT_VECTOR), vcardinality(vcardinality) {
	Reference(value);
}

Vector::Vector(const VectorCardinality &vcardinality)
    : vector_type(VectorType::FLAT_VECTOR), type(TypeId::INVALID), vcardinality(vcardinality), data(nullptr) {
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(other.type), nullmask(other.nullmask), vcardinality(other.vcardinality),
      data(other.data), buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(Value &value) {
	vector_type = VectorType::CONSTANT_VECTOR;
	type = value.type;
	buffer = VectorBuffer::CreateConstantVector(type);
	data = buffer->GetData();
	SetValue(0, value);
}

void Vector::Reference(Vector &other) {
	vector_type = other.vector_type;
	buffer = other.buffer;
	auxiliary = other.auxiliary;
	data = other.data;
	type = other.type;
	nullmask = other.nullmask;
	if (type == TypeId::STRUCT || type == TypeId::LIST) {
		for (size_t i = 0; i < other.children.size(); i++) {
			auto &other_child_vec = other.children[i].second;
			auto child_ref = make_unique<Vector>(vcardinality);
			child_ref->type = other_child_vec->type;
			child_ref->Reference(*other_child_vec.get());
			children.push_back(pair<string, unique_ptr<Vector>>(other.children[i].first, move(child_ref)));
		}
	}
}

void Vector::Slice(Vector &other, idx_t offset) {
	if (other.vector_type == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	assert(other.vector_type == VectorType::FLAT_VECTOR);

	// create a reference to the other vector
	Reference(other);
	if (offset > 0) {
		data = data + GetTypeIdSize(type) * offset;
		nullmask <<= offset;
	}
}

void Vector::Slice(Vector &other, buffer_ptr<VectorBuffer> dictionary) {
	if (other.vector_type == VectorType::CONSTANT_VECTOR) {
		// slicing a constant vector just results in that constant
		Reference(other);
		return;
	}

	assert(dictionary->type == VectorBufferType::DICTIONARY_BUFFER);
	auto child_ref = make_buffer<VectorChildBuffer>(vcardinality);;
	child_ref->data.Reference(other);

	buffer = move(dictionary);
	auxiliary = move(child_ref);
	vector_type = VectorType::DICTIONARY_VECTOR;
}

void Vector::Initialize(TypeId new_type, bool zero_data, idx_t count) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	buffer.reset();
	auxiliary.reset();
	nullmask.reset();
	if (GetTypeIdSize(type) > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, count);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, count * GetTypeIdSize(type));
		}
	}
}

void Vector::SetValue(idx_t index, Value val) {
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = GetSelVector();
		auto &child = GetDictionaryChild();
		return child.SetValue(sel_vector.get_index(index), move(val));
	}
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
		((string_t *)data)[index] = AddString(newVal.str_value);
		break;
	}
	case TypeId::STRUCT: {
		for (size_t i = 0; i < val.struct_value.size(); i++) {
			auto &struct_child = val.struct_value[i];
			assert(vector_type == VectorType::CONSTANT_VECTOR || vector_type == VectorType::FLAT_VECTOR);
			if (i == children.size()) {
				// TODO should this child vector already exist here?
				auto cv = make_unique<Vector>(vcardinality, struct_child.second.type);
				cv->vector_type = vector_type;
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

Value Vector::GetValue(idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	} else if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = GetSelVector();
		auto &child = GetDictionaryChild();
		return child.GetValue(sel_vector.get_index(index));
	} else {
		assert(vector_type == VectorType::FLAT_VECTOR);
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
		auto str = ((string_t *)data)[index];
		return Value(str.GetString());
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
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(children[0].second->GetValue(i));
		}
		return ret;
	}
	default:
		throw NotImplementedException("Unimplemented type for value access");
	}
}

string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::DICTIONARY_VECTOR:
		return "DICTIONARY";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString() const {
	auto count = size();

	string retval = VectorTypeToString(vector_type) + " " + TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		GetSequence(start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * i) + (i == count - 1 ? "" : ", ");
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
static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, idx_t count, sel_t *sel_vector) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) { output[i] = constant; });
}

void Vector::Normalify() {
	auto count = size();

	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		throw NotImplementedException("FIXME: flatten dicitonary vector");

		// // create a new vector with the same size, but without a selection vector
		// VectorCardinality other_cardinality(size());
		// Vector other(other_cardinality, type);
		// // now copy the data of this vector to the other vector, removing the selection vector in the process
		// VectorOperations::Copy(*this, other);
		// // create a reference to the data in the other vector
		// this->Reference(other);
		break;
	}
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
			flatten_constant_vector_loop<int8_t>(data, old_data, count, sel);
			break;
		case TypeId::INT16:
			flatten_constant_vector_loop<int16_t>(data, old_data, count, sel);
			break;
		case TypeId::INT32:
			flatten_constant_vector_loop<int32_t>(data, old_data, count, sel);
			break;
		case TypeId::INT64:
			flatten_constant_vector_loop<int64_t>(data, old_data, count, sel);
			break;
		case TypeId::FLOAT:
			flatten_constant_vector_loop<float>(data, old_data, count, sel);
			break;
		case TypeId::DOUBLE:
			flatten_constant_vector_loop<double>(data, old_data, count, sel);
			break;
		case TypeId::HASH:
			flatten_constant_vector_loop<uint64_t>(data, old_data, count, sel);
			break;
		case TypeId::POINTER:
			flatten_constant_vector_loop<uintptr_t>(data, old_data, count, sel);
			break;
		case TypeId::VARCHAR:
			flatten_constant_vector_loop<string_t>(data, old_data, count, sel);
			break;
		case TypeId::STRUCT:
			for (auto &child : GetChildren()) {
				assert(child.second->vector_type == VectorType::CONSTANT_VECTOR);
				child.second->Normalify();
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

void Vector::Orrify(SelectionVector **out_sel, data_ptr_t *out_data) {
	switch (vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = GetSelVector();
		auto &child = GetDictionaryChild();
		child.Normalify();

		*out_sel = &sel;
		*out_data = child.GetData();
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		*out_sel = Vector::CONSTANT_SEL;
		*out_data = GetData();
		break;
	default:
		Normalify();

		*out_sel = Vector::INCREMENTAL_SEL;
		*out_data = GetData();
		break;
	}
}

void Vector::Sequence(int64_t start, int64_t increment) {
	vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 2);
	auto data = (int64_t *)buffer->GetData();
	data[0] = start;
	data[1] = increment;
	nullmask.reset();
	auxiliary.reset();
}

void Vector::GetSequence(int64_t &start, int64_t &increment) const {
	assert(vector_type == VectorType::SEQUENCE_VECTOR);
	auto data = (int64_t *)buffer->GetData();
	start = data[0];
	increment = data[1];
}

void Vector::Verify() {
	if (size() == 0) {
		return;
	}
#ifdef DEBUG
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch(vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			auto strings = (string_t *) GetData();
			if (!nullmask[0]) {
				strings[0].Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = (string_t *) GetData();
			for(idx_t i = 0; i < size(); i++) {
				if (!nullmask[i]) {
					strings[i].Verify();
				}
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &child = GetDictionaryChild();
			child.Verify();
			break;
		}
		default:
			break;
		}
	}
#endif
}


Vector &Vector::GetDictionaryChild() const {
	assert(vector_type == VectorType::DICTIONARY_VECTOR);
	return ((VectorChildBuffer&) auxiliary).data;
}

void Vector::MoveStringsToHeap(StringHeap &heap, SelectionVector &sel) {
	Normalify();

	assert(vector_type == VectorType::FLAT_VECTOR);
	auto source_strings = (string_t *)GetData();
	auto old_buffer = move(buffer);

	buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
	data = buffer->GetData();
	auto target_strings = (string_t *)GetData();
	for(idx_t i = 0; i < size(); i++) {
		auto idx = sel.get_index(i);
		if (!nullmask[idx] && !source_strings[idx].IsInlined()) {
			target_strings[idx] = heap.AddString(source_strings[idx]);
		} else {
			target_strings[idx] = source_strings[idx];
		}
	}
}

void Vector::MoveStringsToHeap(StringHeap &heap) {
	if (type != TypeId::VARCHAR) {
		return;
	}

	switch(vector_type) {
	case VectorType::CONSTANT_VECTOR: {
		auto source_strings = (string_t *)GetData();
		if (nullmask[0] || source_strings[0].IsInlined()) {
			return;
		}
		auto old_buffer = move(buffer);

		this->buffer = VectorBuffer::CreateConstantVector(TypeId::VARCHAR);
		this->data = buffer->GetData();
		auto target_strings = (string_t *)GetData();
		target_strings[0] = heap.AddString(source_strings[0]);
		break;
	}
	case VectorType::FLAT_VECTOR: {
		auto source_strings = (string_t *)GetData();
		auto old_buffer = move(buffer);

		buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
		data = buffer->GetData();
		auto target_strings = (string_t *)GetData();
		for(idx_t i = 0; i < size(); i++) {
			if (!nullmask[i] && !source_strings[i].IsInlined()) {
				target_strings[i] = heap.AddString(source_strings[i]);
			} else {
				target_strings[i] = source_strings[i];
			}
		}
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = GetSelVector();
		auto &child = GetDictionaryChild();
		child.MoveStringsToHeap(heap, sel);
		break;
	}
	default:
		throw NotImplementedException("");
	}
}

string_t Vector::AddString(const char *data, idx_t len) {
	return AddString(string_t(data, len));
}

string_t Vector::AddString(const char *data) {
	return AddString(string_t(data, strlen(data)));
}

string_t Vector::AddString(const string &data) {
	return AddString(string_t(data.c_str(), data.size()));
}

string_t Vector::AddString(string_t data) {
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!auxiliary) {
		auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*auxiliary;
	return string_buffer.AddString(data);
}

string_t Vector::EmptyString(idx_t len) {
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!auxiliary) {
		auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*auxiliary;
	return string_buffer.EmptyString(len);
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

child_list_t<unique_ptr<Vector>> &Vector::GetChildren() {
	assert(type == TypeId::STRUCT);
	return children;
}

void Vector::AddChild(unique_ptr<Vector> vector, string name) {
	assert(type == TypeId::STRUCT);
	children.push_back(make_pair(name, move(vector)));
}

} // namespace duckdb
