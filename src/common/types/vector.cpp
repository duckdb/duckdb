#include <cstring> // strlen() on Solaris

#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace std;

namespace duckdb {

Vector::Vector(TypeId type, bool create_data, bool zero_data)
    : vector_type(VectorType::FLAT_VECTOR), type(type), data(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(TypeId type) : Vector(type, true, false) {
}

Vector::Vector(TypeId type, data_ptr_t dataptr) : vector_type(VectorType::FLAT_VECTOR), type(type), data(dataptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(Value value) : vector_type(VectorType::CONSTANT_VECTOR) {
	Reference(value);
}

Vector::Vector() : vector_type(VectorType::FLAT_VECTOR), type(TypeId::INVALID), data(nullptr) {
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(other.type), data(other.data), nullmask(other.nullmask),
      buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(Value &value) {
	vector_type = VectorType::CONSTANT_VECTOR;
	type = value.type;
	buffer = VectorBuffer::CreateConstantVector(type);
	auxiliary.reset();
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

void Vector::Slice(Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// already a dictionary, slice the current dictionary
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_unique<DictionaryBuffer>(move(sliced_dictionary));
		return;
	}
	auto child_ref = make_buffer<VectorChildBuffer>();
	child_ref->data.Reference(*this);

	auto dict_buffer = make_unique<DictionaryBuffer>(sel);
	buffer = move(dict_buffer);
	auxiliary = move(child_ref);
	vector_type = VectorType::DICTIONARY_VECTOR;
}

void Vector::Slice(const SelectionVector &sel, idx_t count, sel_cache_t &cache) {
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto target_data = current_sel.data();
		auto entry = cache.find(target_data);
		if (entry != cache.end()) {
			// cached entry exists: use that
			this->buffer = entry->second;
		} else {
			Slice(sel, count);
			cache[target_data] = this->buffer;
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(TypeId new_type, bool zero_data) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	vector_type = VectorType::FLAT_VECTOR;
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
		auto &sel_vector = DictionaryVector::SelVector(*this);
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
		((bool *)data)[index] = newVal.value_.boolean;
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
		((string_t *)data)[index] = StringVector::AddBlob(*this, newVal.str_value);
		break;
	}
	case TypeId::STRUCT: {
		if (!auxiliary || StructVector::GetEntries(*this).size() == 0) {
			for (size_t i = 0; i < val.struct_value.size(); i++) {
				auto &struct_child = val.struct_value[i];
				auto cv = make_unique<Vector>(struct_child.second.type);
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
				child_append_chunk.SetCardinality(this_append_len);
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
		auto &sel_vector = DictionaryVector::SelVector(*this);
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
		return Value::BOOLEAN(((bool *)data)[index]);
	case TypeId::INT8:
		return Value::TINYINT(((int8_t *)data)[index]);
	case TypeId::INT16:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case TypeId::INT32:
		return Value::INTEGER(((int32_t *)data)[index]);
	case TypeId::INT64:
		return Value::BIGINT(((int64_t *)data)[index]);
	case TypeId::HASH:
		return Value::HASH(((hash_t *)data)[index]);
	case TypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case TypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case TypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case TypeId::VARCHAR: {
		auto str = ((string_t *)data)[index];
		return Value::BLOB(str.GetString());
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

string Vector::ToString(idx_t count) const {
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

void Vector::Print(idx_t count) {
	Printer::Print(ToString(count));
}

string Vector::ToString() const {
	string retval = VectorTypeToString(vector_type) + " " + TypeIdToString(type) + ": (UNKNOWN COUNT) [ ";
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
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

template <class T> static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Normalify(idx_t count) {
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(type);
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
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
			flatten_constant_vector_loop<hash_t>(data, old_data, count);
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
				child.second->Normalify(count);
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
		VectorOperations::GenerateSequence(*this, count, start, increment);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented type for normalify");
	}
}

void Vector::Normalify(const SelectionVector &sel, idx_t count) {
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		vector_type = VectorType::FLAT_VECTOR;
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::Orrify(idx_t count, VectorData &data) {
	switch (vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		if (child.vector_type == VectorType::FLAT_VECTOR) {
			data.sel = &sel;
			data.data = FlatVector::GetData(child);
			data.nullmask = &FlatVector::Nullmask(child);
		} else {
			// dictionary with non-flat child: create a new reference to the child and normalify it
			auto new_aux = make_unique<VectorChildBuffer>();
			new_aux->data.Reference(child);
			new_aux->data.Normalify(sel, count);

			data.sel = &sel;
			data.data = FlatVector::GetData(new_aux->data);
			data.nullmask = &FlatVector::Nullmask(new_aux->data);
			this->auxiliary = move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		data.sel = &ConstantVector::ZeroSelectionVector;
		data.data = ConstantVector::GetData(*this);
		data.nullmask = &nullmask;
		break;
	default:
		Normalify(count);
		data.sel = &FlatVector::IncrementalSelectionVector;
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

void Vector::Serialize(idx_t count, Serializer &serializer) {
	if (TypeIsConstantSize(type)) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(type) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteData(ptr.get(), write_size);
	} else {
		VectorData vdata;
		Orrify(count, vdata);

		switch (type) {
		case TypeId::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = (*vdata.nullmask)[idx] ? NullValue<const char *>() : strings[idx].GetData();
				serializer.WriteString(source);
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for Vector::Serialize!");
		}
	}
}

void Vector::Deserialize(idx_t count, Deserializer &source) {
	if (TypeIsConstantSize(type)) {
		// constant size type: read fixed amount of data from
		auto column_size = GetTypeIdSize(type) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[column_size]);
		source.ReadData(ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		auto strings = FlatVector::GetData<string_t>(*this);
		auto &nullmask = FlatVector::Nullmask(*this);
		for (idx_t i = 0; i < count; i++) {
			// read the strings
			auto str = source.Read<string>();
			// now add the string to the StringHeap of the vector
			// and write the pointer into the vector
			if (IsNullValue<const char *>((const char *)str.c_str())) {
				nullmask[i] = true;
			} else {
				strings[i] = StringVector::AddString(*this, str);
			}
		}
	}
}

void Vector::UTFVerify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch (vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (!nullmask[oidx]) {
					strings[oidx].Verify();
				}
			}
			break;
		}
		default:
			break;
		}
	}
#endif
}

void Vector::UTFVerify(idx_t count) {
	UTFVerify(FlatVector::IncrementalSelectionVector, count);
}

void Vector::Verify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*this);
		auto &dict_sel = DictionaryVector::SelVector(*this);
		for (idx_t i = 0; i < count; i++) {
			auto oidx = sel.get_index(i);
			auto idx = dict_sel.get_index(oidx);
			assert(idx < STANDARD_VECTOR_SIZE);
		}
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(sel, count);
		SelectionVector new_sel(new_buffer);
		child.Verify(new_sel, count);
		return;
	}
	if (type == TypeId::DOUBLE) {
		// verify that there are no INF or NAN values
		switch (vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			auto dbl = ConstantVector::GetData<double>(*this);
			if (!ConstantVector::IsNull(*this)) {
				assert(Value::DoubleIsValid(*dbl));
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto doubles = FlatVector::GetData<double>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (!nullmask[oidx]) {
					assert(Value::DoubleIsValid(doubles[oidx]));
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (type == TypeId::STRUCT) {
		if (vector_type == VectorType::FLAT_VECTOR || vector_type == VectorType::CONSTANT_VECTOR) {
			auto &children = StructVector::GetEntries(*this);
			assert(children.size() > 0);
			for (auto &child : children) {
				child.second->Verify(sel, count);
			}
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
			if (ListVector::HasEntry(*this)) {
				ListVector::GetEntry(*this).Verify();
			}
			auto list_data = FlatVector::GetData<list_entry_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto &le = list_data[idx];
				if (!nullmask[idx]) {
					assert(le.offset + le.length <= ListVector::GetEntry(*this).count);
				}
			}
		}
	}
// TODO verify list and struct
#endif
}

void Vector::Verify(idx_t count) {
	Verify(FlatVector::IncrementalSelectionVector, count);
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

string_t StringVector::AddBlob(Vector &vector, string_t data) {
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
	return string_buffer.AddBlob(data);
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

	if (other.vector_type == VectorType::DICTIONARY_VECTOR) {
		StringVector::AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
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

bool StructVector::HasEntries(const Vector &vector) {
	assert(vector.type == TypeId::STRUCT);
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	assert(vector.auxiliary == nullptr || vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	return vector.auxiliary != nullptr;
}

child_list_t<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	assert(vector.type == TypeId::STRUCT);
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

void StructVector::AddEntry(Vector &vector, string name, unique_ptr<Vector> entry) {
	// TODO asser that an entry with this name does not already exist
	assert(vector.type == TypeId::STRUCT);
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStructBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	((VectorStructBuffer *)vector.auxiliary.get())->AddChild(name, move(entry));
}

bool ListVector::HasEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	if (vector.vector_type == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::HasEntry(child);
	}
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	return vector.auxiliary != nullptr;
}

ChunkCollection &ListVector::GetEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	if (vector.vector_type == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

void ListVector::SetEntry(Vector &vector, unique_ptr<ChunkCollection> cc) {
	assert(vector.type == TypeId::LIST);
	assert(vector.vector_type == VectorType::FLAT_VECTOR || vector.vector_type == VectorType::CONSTANT_VECTOR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorListBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	((VectorListBuffer *)vector.auxiliary.get())->SetChild(move(cc));
}

} // namespace duckdb
