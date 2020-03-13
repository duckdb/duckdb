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
	data = other.data;
	type = other.type;
	nullmask = other.nullmask;
	auxiliary = nullptr;

	switch (type) {
	case TypeId::STRUCT:
		for (auto &child : other.GetStructEntries()) {
			auto child_copy = make_unique<Vector>(cardinality(), child.second->type);
			child_copy->Reference(*child.second);
			AddStructEntry(child.first, move(child_copy));
		}
		break;

	default:
		auxiliary = other.auxiliary;
		break;
	}
}

void Vector::Slice(Vector &other, idx_t offset) {
	assert(!other.sel_vector());

	// create a reference to the other vector
	Reference(other);
	if (offset > 0) {
		data = data + GetTypeIdSize(type) * offset;
		nullmask <<= offset;
	}
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
		if (!auxiliary || GetStructEntries().size() == 0) {
			for (size_t i = 0; i < val.struct_value.size(); i++) {
				auto &struct_child = val.struct_value[i];
				auto cv = make_unique<Vector>(vcardinality, struct_child.second.type);
				cv->vector_type = vector_type;
				AddStructEntry(struct_child.first, move(cv));
			}
		}

		auto &children = GetStructEntries();
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
			SetListEntry(move(cc));
		}
		auto &child_cc = GetListEntry();
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
		auto &entry = ((list_entry_t *)GetData())[index];
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
		for (auto &struct_child : GetStructEntries()) {
			ret.struct_value.push_back(pair<string, Value>(struct_child.first, struct_child.second->GetValue(index)));
		}
		return ret;
	}
	case TypeId::LIST: {
		Value ret(TypeId::LIST);
		ret.is_null = false;
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_cc = GetListEntry();
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(child_cc.GetValue(0, i));
		}
		return ret;
	}
	default:
		throw NotImplementedException("Unimplemented type for value access");
	}
}

void Vector::ClearSelectionVector() {
	Normalify();
	if (!sel_vector()) {
		return;
	}
	// create a new vector with the same size, but without a selection vector
	VectorCardinality other_cardinality(size());
	Vector other(other_cardinality, type);
	// now copy the data of this vector to the other vector, removing the selection vector in the process
	VectorOperations::Copy(*this, other);
	// create a reference to the data in the other vector
	this->Reference(other);
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
	auto count = size();
	auto sel = sel_vector();

	string retval = VectorTypeToString(vector_type) + " " + TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(sel ? sel[i] : i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		GetSequence(start, increment);
		for (idx_t i = 0; i < count; i++) {
			idx_t idx = sel ? sel[i] : i;
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
static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, idx_t count, sel_t *sel_vector) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) { output[i] = constant; });
}

void Vector::Normalify() {
	auto count = size();
	auto sel = sel_vector();

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
		case TypeId::LIST: {
			flatten_constant_vector_loop<list_entry_t>(data, old_data, count, sel);
			break;
		}
		case TypeId::STRUCT: {
			for (auto &child : GetStructEntries()) {
				assert(child.second->vector_type == VectorType::CONSTANT_VECTOR);
				child.second->Normalify();
			}
			// sel_vector = nullptr;
		} break;
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
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!nullmask[0]) {
				auto string = ((string_t *)data)[0];
				string.Verify();
			}
		} else {
			VectorOperations::ExecType<string_t>(*this, [&](string_t string, uint64_t i, uint64_t k) {
				if (!nullmask[i]) {
					string.Verify();
				}
			});
		}
	}

	if (type == TypeId::STRUCT) {
		auto &children = GetStructEntries();
		assert(children.size() > 0);
		for (auto &child : children) {
			assert(child.second->SameCardinality(*this));
			child.second->Verify();
		}
	}

	if (type == TypeId::LIST) {
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!nullmask[0]) {
				GetListEntry().Verify();
				auto le = ((list_entry_t *)data)[0];
				assert(le.offset + le.length <= GetListEntry().count);
			}
		} else {
			if (VectorOperations::HasNotNull(*this)) {
				GetListEntry().Verify();
			}
			VectorOperations::ExecType<list_entry_t>(*this, [&](list_entry_t le, uint64_t i, uint64_t k) {
				if (!nullmask[i]) {
					assert(le.offset + le.length <= GetListEntry().count);
				}
			});
		}
	}
// TODO verify list and struct
#endif
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
	assert(type == TypeId::VARCHAR);
	assert(other.type == TypeId::VARCHAR);

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

child_list_t<unique_ptr<Vector>> &Vector::GetStructEntries() const {
	assert(type == TypeId::STRUCT);
	assert(auxiliary);
	assert(auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)auxiliary.get())->GetChildren();
}

void Vector::AddStructEntry(string name, unique_ptr<Vector> vector) {
	// TODO asser that an entry with this name does not already exist
	assert(type == TypeId::STRUCT);
	if (!auxiliary) {
		auxiliary = make_buffer<VectorStructBuffer>();
	}
	assert(auxiliary);
	assert(auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	((VectorStructBuffer *)auxiliary.get())->AddChild(name, move(vector));
}

bool Vector::HasListEntry() const {
	assert(type == TypeId::LIST);
	return auxiliary != nullptr;
}


ChunkCollection &Vector::GetListEntry() const {
	assert(type == TypeId::LIST);
	assert(auxiliary);
	assert(auxiliary->type == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)auxiliary.get())->GetChild();
}
void Vector::SetListEntry(unique_ptr<ChunkCollection> cc) {
	assert(type == TypeId::LIST);
	if (!auxiliary) {
		auxiliary = make_buffer<VectorListBuffer>();
	}
	assert(auxiliary);
	assert(auxiliary->type == VectorBufferType::LIST_BUFFER);
	((VectorListBuffer *)auxiliary.get())->SetChild(move(cc));
}

} // namespace duckdb
