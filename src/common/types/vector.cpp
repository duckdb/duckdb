#include <cstring> // strlen() on Solaris

#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

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
    : vector_type(other.vector_type), type(other.type), vcardinality(other.vcardinality),
      data(other.data), nullmask(other.nullmask), buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
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
	data = other.data;
	type = other.type;
	nullmask = other.nullmask;
	auxiliary = nullptr;

	switch (type) {
	case TypeId::STRUCT:
		for (auto &child : StructVector::GetEntries(other)) {
			auto child_copy = make_unique<Vector>(cardinality(), child.second->type);
			child_copy->Reference(*child.second);
			StructVector::AddEntry(*this, child.first, move(child_copy));
		}
		break;

	default:
		auxiliary = other.auxiliary;
		break;
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

void Vector::Slice(Vector &other, SelectionVector &sel) {
	if (other.vector_type == VectorType::CONSTANT_VECTOR) {
		// slicing a constant vector just results in that constant
		Reference(other);
		return;
	}

	throw NotImplementedException("FIXME: slice");
	// assert(dictionary->type == VectorBufferType::DICTIONARY_BUFFER);
	// auto child_ref = make_buffer<VectorChildBuffer>(vcardinality);
	// child_ref->data.Reference(other);

	// buffer = move(dictionary);
	// auxiliary = move(child_ref);
	// vector_type = VectorType::DICTIONARY_VECTOR;
}

void Vector::Initialize(TypeId new_type, bool zero_data) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	buffer.reset();
	auxiliary.reset();
	nullmask.reset();
	if (GetTypeIdSize(type) > 0) {
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
		}
	}
}

void Vector::SetValue(idx_t index, Value val) {
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
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
		((string_t *)data)[index] = StringVector::AddString(*this, newVal.str_value);
		break;
	}
	case TypeId::STRUCT: {
		if (!auxiliary || StructVector::GetEntries(*this).size() == 0) {
			for (size_t i = 0; i < val.struct_value.size(); i++) {
				auto &struct_child = val.struct_value[i];
				auto cv = make_unique<Vector>(vcardinality, struct_child.second.type);
				cv->vector_type = vector_type;
				StructVector::AddEntry(*this, struct_child.first, move(cv));
			}
		}

		auto &children = StructVector::GetEntries(*this);
		assert(children.size() == val.struct_value.size());

		for (size_t i = 0; i < val.struct_value.size(); i++) {
			auto &struct_child = val.struct_value[i];
			assert(vector_type == VectorType::CONSTANT_VECTOR || vector_type == VectorType::FLAT_VECTOR);
			auto &vec_child = children[i];
			assert(vec_child.first == struct_child.first);
			vec_child.second->SetValue(index, struct_child.second);
		}
	} break;

	case TypeId::LIST: {
		if (!auxiliary) {
			auto cc = make_unique<ChunkCollection>();
			ListVector::SetEntry(*this, move(cc));
		}
		auto &child_cc = ListVector::GetEntry(*this);
		// TODO optimization: in-place update if fits
		auto offset = child_cc.count;
		if (val.list_value.size() > 0) {
			idx_t append_idx = 0;
			while (append_idx < val.list_value.size()) {
				idx_t this_append_len = min((idx_t)STANDARD_VECTOR_SIZE, val.list_value.size() - append_idx);

				DataChunk child_append_chunk;
				child_append_chunk.count = this_append_len;
				vector<TypeId> types;
				types.push_back(val.list_value[0].type);
				child_append_chunk.Initialize(types);
				for (idx_t i = 0; i < this_append_len; i++) {
					child_append_chunk.data[0].SetValue(i, val.list_value[i + append_idx]);
				}
				child_cc.Append(child_append_chunk);
				append_idx += this_append_len;
			}
		}
		// now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val.list_value.size();
		entry.offset = offset;
	} break;
	default:
		throw NotImplementedException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValue(idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	} else if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
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
		for (auto &struct_child : StructVector::GetEntries(*this)) {
			ret.struct_value.push_back(pair<string, Value>(struct_child.first, struct_child.second->GetValue(index)));
		}
		return ret;
	}
	case TypeId::LIST: {
		Value ret(TypeId::LIST);
		ret.is_null = false;
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_cc = ListVector::GetEntry(*this);
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(child_cc.GetValue(0, i));
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
		SequenceVector::GetSequence(*this, start, increment);
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
static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	for(idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

// void Vector::Normalify(SelectionVector &sel) {
// }

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
			flatten_constant_vector_loop<int8_t>(data, old_data, count);
			break;
		case TypeId::INT16:
			flatten_constant_vector_loop<int16_t>(data, old_data, count);
			break;
		case TypeId::INT32:
			flatten_constant_vector_loop<int32_t>(data, old_data, count);
			break;
		case TypeId::INT64:
			flatten_constant_vector_loop<int64_t>(data, old_data, count);
			break;
		case TypeId::FLOAT:
			flatten_constant_vector_loop<float>(data, old_data, count);
			break;
		case TypeId::DOUBLE:
			flatten_constant_vector_loop<double>(data, old_data, count);
			break;
		case TypeId::HASH:
			flatten_constant_vector_loop<uint64_t>(data, old_data, count);
			break;
		case TypeId::POINTER:
			flatten_constant_vector_loop<uintptr_t>(data, old_data, count);
			break;
		case TypeId::VARCHAR:
			flatten_constant_vector_loop<string_t>(data, old_data, count);
			break;
		case TypeId::LIST: {
			flatten_constant_vector_loop<list_entry_t>(data, old_data, count);
			break;
		}
		case TypeId::STRUCT: {
			for (auto &child : StructVector::GetEntries(*this)) {
				assert(child.second->vector_type == VectorType::CONSTANT_VECTOR);
				child.second->Normalify();
			}
		} break;
		default:
			throw NotImplementedException("Unimplemented type for VectorOperations::Normalify");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

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

void Vector::Orrify(VectorData &data) {
	switch (vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
		child.Normalify();

		data.sel = &sel;
		data.data = FlatVector::GetData(child);
		data.nullmask = &child.nullmask;
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		// data.sel = &Vector::CONSTANT_SEL;
		data.data = ConstantVector::GetData(*this);
		data.nullmask = &nullmask;
		break;
	default:
		Normalify();
		// data.sel = &Vector::INCREMENTAL_SEL;
		data.data = FlatVector::GetData(*this);
		data.nullmask = &nullmask;
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
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for(idx_t i = 0; i < size(); i++) {
				if (!nullmask[i]) {
					strings[i].Verify();
				}
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &child = DictionaryVector::Child(*this);
			child.Verify();
			break;
		}
		default:
			break;
		}
	}

	if (type == TypeId::STRUCT) {
		auto &children = StructVector::GetEntries(*this);
		assert(children.size() > 0);
		for (auto &child : children) {
			assert(child.second->SameCardinality(*this));
			child.second->Verify();
		}
	}

	if (type == TypeId::LIST) {
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*this)) {
				ListVector::GetEntry(*this).Verify();
				auto le = ConstantVector::GetData<list_entry_t>(*this);
				assert(le->offset + le->length <= ListVector::GetEntry(*this).count);
			}
		} else if (vector_type == VectorType::FLAT_VECTOR) {
			if (VectorOperations::HasNotNull(*this)) {
				ListVector::GetEntry(*this).Verify();
			}
			auto list_data = FlatVector::GetData<list_entry_t>(*this);
			for(idx_t i = 0; i < size(); i++) {
				auto &le = list_data[i];
				if (!nullmask[i]) {
					assert(le.offset + le.length <= ListVector::GetEntry(*this).count);
				}
			}
		}
	}
// TODO verify list and struct
#endif
}

void StringVector::MoveStringsToHeap(Vector &vector, StringHeap &heap, SelectionVector &sel) {
	assert(vector.type == TypeId::VARCHAR);

	vector.Normalify();

	assert(vector.vector_type == VectorType::FLAT_VECTOR);
	auto source_strings = FlatVector::GetData<string_t>(vector);
	auto old_buffer = move(vector.buffer);

	vector.buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
	vector.data = vector.buffer->GetData();
	auto target_strings = FlatVector::GetData<string_t>(vector);
	auto &nullmask = FlatVector::Nullmask(vector);
	for(idx_t i = 0; i < vector.size(); i++) {
		auto idx = sel.get_index(i);
		if (!nullmask[idx] && !source_strings[idx].IsInlined()) {
			target_strings[idx] = heap.AddString(source_strings[idx]);
		} else {
			target_strings[idx] = source_strings[idx];
		}
	}
}

void StringVector::MoveStringsToHeap(Vector &vector, StringHeap &heap) {
	assert(vector.type == TypeId::VARCHAR);

	switch(vector.vector_type) {
	case VectorType::CONSTANT_VECTOR: {
		auto source_strings = ConstantVector::GetData<string_t>(vector);
		if (ConstantVector::IsNull(vector) || source_strings[0].IsInlined()) {
			return;
		}
		auto old_buffer = move(vector.buffer);

		vector.buffer = VectorBuffer::CreateConstantVector(TypeId::VARCHAR);
		vector.data = vector.buffer->GetData();
		auto target_strings = ConstantVector::GetData<string_t>(vector);
		target_strings[0] = heap.AddString(source_strings[0]);
		break;
	}
	case VectorType::FLAT_VECTOR: {
		auto source_strings = FlatVector::GetData<string_t>(vector);
		auto old_buffer = move(vector.buffer);

		vector.buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
		vector.data = vector.buffer->GetData();
		auto target_strings = FlatVector::GetData<string_t>(vector);
		for(idx_t i = 0; i < vector.size(); i++) {
			if (!vector.nullmask[i] && !source_strings[i].IsInlined()) {
				target_strings[i] = heap.AddString(source_strings[i]);
			} else {
				target_strings[i] = source_strings[i];
			}
		}
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelectionVector(vector);
		auto &child = DictionaryVector::Child(vector);;
		StringVector::MoveStringsToHeap(child, heap, sel);
		break;
	}
	default:
		throw NotImplementedException("");
	}
}

string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, len));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, strlen(data)));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), data.size()));
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	assert(vector.type == TypeId::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddString(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	assert(vector.type == TypeId::VARCHAR);
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.EmptyString(len);
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	assert(vector.type == TypeId::VARCHAR);
	assert(other.type == TypeId::VARCHAR);

	if (!other.auxiliary) {
		return;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	assert(other.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(other.auxiliary);
}

child_list_t<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	assert(vector.type == TypeId::STRUCT);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

void StructVector::AddEntry(Vector &vector, string name, unique_ptr<Vector> entry) {
	// TODO asser that an entry with this name does not already exist
	assert(vector.type == TypeId::STRUCT);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStructBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	((VectorStructBuffer *)vector.auxiliary.get())->AddChild(name, move(entry));
}

bool ListVector::HasEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	return vector.auxiliary != nullptr;
}

ChunkCollection &ListVector::GetEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

void ListVector::SetEntry(Vector &vector, unique_ptr<ChunkCollection> cc) {
	assert(vector.type == TypeId::LIST);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorListBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	((VectorListBuffer *)vector.auxiliary.get())->SetChild(move(cc));
}

} // namespace duckdb
