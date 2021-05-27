#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include <cstring> // strlen() on Solaris

namespace duckdb {

Vector::Vector(const LogicalType &type, bool create_data, bool zero_data) : data(nullptr) {
	buffer = make_buffer<VectorBuffer>(VectorType::FLAT_VECTOR, type);
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(const LogicalType &type) : Vector(type, true, false) {
}

Vector::Vector(const LogicalType &type, data_ptr_t dataptr) : data(dataptr) {
	buffer = make_buffer<VectorBuffer>(VectorType::FLAT_VECTOR, type);
	if (dataptr && type.id() == LogicalTypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const Value &value) {
	buffer = make_buffer<VectorBuffer>(VectorType::CONSTANT_VECTOR);
	Reference(value);
}

Vector::Vector() : data(nullptr) {
	buffer = make_buffer<VectorBuffer>(VectorType::FLAT_VECTOR, LogicalTypeId::INVALID);
}

Vector::Vector(Vector &&other) noexcept
    : data(other.data), validity(move(other.validity)), buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(const Value &value) {
	buffer = VectorBuffer::CreateConstantVector(VectorType::CONSTANT_VECTOR, value.type());
	auxiliary.reset();
	data = buffer->GetData();
	SetValue(0, value);
}

void Vector::Reference(Vector &other) {
	buffer = other.buffer;
	auxiliary = other.auxiliary;
	data = other.data;
	validity = other.validity;
}

void Vector::Slice(Vector &other, idx_t offset) {
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	D_ASSERT(GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR);

	// create a reference to the other vector
	Reference(other);
	if (offset > 0) {
		data = data + GetTypeIdSize(GetType().InternalType()) * offset;
		validity.Slice(other.validity, offset);
	}
}

void Vector::Slice(Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// already a dictionary, slice the current dictionary
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_buffer<DictionaryBuffer>(move(sliced_dictionary), GetType(), GetVectorType());
		return;
	}
	auto child_ref = make_buffer<VectorChildBuffer>();
	child_ref->data.Reference(*this);

	auto dict_buffer = make_buffer<DictionaryBuffer>(sel, GetType(), VectorType::DICTIONARY_VECTOR);
	buffer = move(dict_buffer);
	auxiliary = move(child_ref);
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto target_data = current_sel.data();
		auto entry = cache.cache.find(target_data);
		if (entry != cache.cache.end()) {
			// cached entry exists: use that
			this->buffer = make_buffer<DictionaryBuffer>(((DictionaryBuffer &)*entry->second).GetSelVector(),
			                                             buffer->GetType(), buffer->GetVectorType());
		} else {
			Slice(sel, count);
			cache.cache[target_data] = this->buffer;
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(const LogicalType &new_type, bool zero_data) {
	if (new_type.id() != LogicalTypeId::INVALID) {
		SetType(new_type);
	}
	auxiliary.reset();
	validity.Reset();
	if (GetTypeIdSize(GetType().InternalType()) > 0) {
		buffer = VectorBuffer::CreateStandardVector(VectorType::FLAT_VECTOR, GetType());
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(new_type.InternalType()));
		}
	} else {
		buffer = VectorBuffer::CreateStandardVector(VectorType::FLAT_VECTOR, GetType());
	}
}

struct DataArrays {
	Vector &vec;
	data_ptr_t data;
	VectorBuffer *buffer;
	idx_t type_size;
	bool is_nested;
	DataArrays(Vector &vec, data_ptr_t data, VectorBuffer *buffer, idx_t type_size, bool is_nested)
	    : vec(vec), data(data), buffer(buffer), type_size(type_size), is_nested(is_nested) {};
};

void FindChildren(std::vector<DataArrays> &to_resize, VectorBuffer &auxiliary) {
	if (auxiliary.GetBufferType() == VectorBufferType::LIST_BUFFER) {
		auto &buffer = (VectorListBuffer &)auxiliary;
		auto &child = buffer.GetChild();
		auto data = child.GetData();
		if (!data) {
			//! Nested type
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  true);
			to_resize.emplace_back(arrays);
			FindChildren(to_resize, *child.GetAuxiliary());
		} else {
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  false);
			to_resize.emplace_back(arrays);
		}
	} else if (auxiliary.GetBufferType() == VectorBufferType::STRUCT_BUFFER) {
		auto &buffer = (VectorStructBuffer &)auxiliary;
		auto &children = buffer.GetChildren();
		for (auto &child : children) {
			auto data = child.second->GetData();
			if (!data) {
				//! Nested type
				DataArrays arrays(*child.second, data, child.second->GetBuffer().get(),
				                  GetTypeIdSize(child.second->GetType().InternalType()), true);
				to_resize.emplace_back(arrays);
				FindChildren(to_resize, *child.second->GetAuxiliary());
			} else {
				DataArrays arrays(*child.second, data, child.second->GetBuffer().get(),
				                  GetTypeIdSize(child.second->GetType().InternalType()), false);
				to_resize.emplace_back(arrays);
			}
		}
	}
}
void Vector::Resize(idx_t cur_size, idx_t new_size) {
	std::vector<DataArrays> to_resize;
	if (!data) {
		//! this is a nested structure
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), true);
		to_resize.emplace_back(arrays);
		FindChildren(to_resize, *auxiliary);
	} else {
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), false);
		to_resize.emplace_back(arrays);
	}
	for (auto &data_to_resize : to_resize) {
		if (!data_to_resize.is_nested) {
			auto new_data = unique_ptr<data_t[]>(new data_t[new_size * data_to_resize.type_size]);
			memcpy(new_data.get(), data_to_resize.data, cur_size * data_to_resize.type_size * sizeof(data_t));
			data_to_resize.buffer->SetData(move(new_data));
			data_to_resize.vec.data = data_to_resize.buffer->GetData();
		}
		data_to_resize.vec.validity.Resize(cur_size, new_size);
	}
}

void Vector::SetValue(idx_t index, const Value &val) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), val);
	}
	if (val.type() != GetType()) {
		SetValue(index, val.CastAs(GetType()));
		return;
	}
	if (GetType().id() != LogicalTypeId::STRUCT || GetType().id() != LogicalTypeId::MAP) {
		validity.EnsureWritable();
		validity.Set(index, !val.is_null);
		if (val.is_null) {
			return;
		}
	}

	switch (GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		((bool *)data)[index] = val.value_.boolean;
		break;
	case LogicalTypeId::TINYINT:
		((int8_t *)data)[index] = val.value_.tinyint;
		break;
	case LogicalTypeId::SMALLINT:
		((int16_t *)data)[index] = val.value_.smallint;
		break;
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		((int32_t *)data)[index] = val.value_.integer;
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::HASH:
	case LogicalTypeId::TIME:
	case LogicalTypeId::BIGINT:
		((int64_t *)data)[index] = val.value_.bigint;
		break;
	case LogicalTypeId::UTINYINT:
		((uint8_t *)data)[index] = val.value_.utinyint;
		break;
	case LogicalTypeId::USMALLINT:
		((uint16_t *)data)[index] = val.value_.usmallint;
		break;
	case LogicalTypeId::UINTEGER:
		((uint32_t *)data)[index] = val.value_.uinteger;
		break;
	case LogicalTypeId::UBIGINT:
		((uint64_t *)data)[index] = val.value_.ubigint;
		break;
	case LogicalTypeId::HUGEINT:
		((hugeint_t *)data)[index] = val.value_.hugeint;
		break;
	case LogicalTypeId::DECIMAL:
		D_ASSERT(GetType().width() == val.type().width() && GetType().scale() == val.type().scale());
		switch (GetType().InternalType()) {
		case PhysicalType::INT16:
			((int16_t *)data)[index] = val.value_.smallint;
			break;
		case PhysicalType::INT32:
			((int32_t *)data)[index] = val.value_.integer;
			break;
		case PhysicalType::INT64:
			((int64_t *)data)[index] = val.value_.bigint;
			break;
		case PhysicalType::INT128:
			((hugeint_t *)data)[index] = val.value_.hugeint;
			break;
		default:
			throw NotImplementedException("Widths bigger than 38 are not supported");
		}
		break;
	case LogicalTypeId::FLOAT:
		((float *)data)[index] = val.value_.float_;
		break;
	case LogicalTypeId::DOUBLE:
		((double *)data)[index] = val.value_.double_;
		break;
	case LogicalTypeId::POINTER:
		((uintptr_t *)data)[index] = val.value_.pointer;
		break;
	case LogicalTypeId::INTERVAL:
		((interval_t *)data)[index] = val.value_.interval;
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		((string_t *)data)[index] = StringVector::AddStringOrBlob(*this, val.str_value);
		break;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT: {
		if (!auxiliary || StructVector::GetEntries(*this).empty()) {
			for (size_t i = 0; i < val.struct_value.size(); i++) {
				auto &struct_child = val.struct_value[i];
				auto cv = make_unique<Vector>(struct_child.second.type());
				cv->SetVectorType(GetVectorType());
				StructVector::AddEntry(*this, struct_child.first, move(cv));
			}
		}

		auto &children = StructVector::GetEntries(*this);
		D_ASSERT(children.size() == val.struct_value.size());

		for (size_t i = 0; i < val.struct_value.size(); i++) {
			auto &struct_child = val.struct_value[i];
			D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);
			auto &vec_child = children[i];
			D_ASSERT(vec_child.first == struct_child.first);
			vec_child.second->SetValue(index, struct_child.second);
		}
	} break;

	case LogicalTypeId::LIST: {
		if (!auxiliary) {
			auto vec_list = make_unique<Vector>(GetType().child_types()[0].second);
			ListVector::SetEntry(*this, move(vec_list));
		}
		auto offset = ListVector::GetListSize(*this);
		if (!val.list_value.empty()) {
			for (idx_t i = 0; i < val.list_value.size(); i++) {
				Value v(val.list_value[i]);
				ListVector::PushBack(*this, v);
			}
		}
		//! now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val.list_value.size();
		entry.offset = offset;
	} break;
	default:
		throw NotImplementedException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValue(idx_t index) const {
	switch (GetVectorType()) {
	case VectorType::CONSTANT_VECTOR:
		index = 0;
		break;
	case VectorType::FLAT_VECTOR:
		break;
		// dictionary: apply dictionary and forward to child
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.GetValue(sel_vector.get_index(index));
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		return Value::Numeric(GetType(), start + increment * index);
	}
	default:
		throw NotImplementedException("Unimplemented vector type for Vector::GetValue");
	}

	if (!validity.RowIsValid(index)) {
		return Value(GetType());
	}
	switch (GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(((bool *)data)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(((int8_t *)data)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(((int32_t *)data)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(((date_t *)data)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(((dtime_t *)data)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(((int64_t *)data)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(((uint8_t *)data)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(((uint16_t *)data)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(((uint32_t *)data)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(((uint64_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TimestampNs(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TimestampMs(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TimestampSec(((timestamp_t *)data)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(((hugeint_t *)data)[index]);
	case LogicalTypeId::DECIMAL: {
		switch (GetType().InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(((int16_t *)data)[index], GetType().width(), GetType().scale());
		case PhysicalType::INT32:
			return Value::DECIMAL(((int32_t *)data)[index], GetType().width(), GetType().scale());
		case PhysicalType::INT64:
			return Value::DECIMAL(((int64_t *)data)[index], GetType().width(), GetType().scale());
		case PhysicalType::INT128:
			return Value::DECIMAL(((hugeint_t *)data)[index], GetType().width(), GetType().scale());
		default:
			throw NotImplementedException("Widths bigger than 38 are not supported");
		}
	}
	case LogicalTypeId::HASH:
		return Value::HASH(((hash_t *)data)[index]);
	case LogicalTypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(((interval_t *)data)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = ((string_t *)data)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::BLOB: {
		auto str = ((string_t *)data)[index];
		return Value::BLOB((const_data_ptr_t)str.GetDataUnsafe(), str.GetSize());
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT: {
		Value ret(GetType());
		ret.is_null = false;
		// we can derive the value schema from the vector schema
		for (auto &struct_child : StructVector::GetEntries(*this)) {
			ret.struct_value.push_back(pair<string, Value>(struct_child.first, struct_child.second->GetValue(index)));
		}
		return ret;
	}
	case LogicalTypeId::LIST: {
		Value ret(GetType());
		ret.is_null = false;
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_vec = ListVector::GetEntry(*this);
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(child_vec.GetValue(i));
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
	string retval =
	    VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": " + to_string(count) + " = [ ";
	switch (GetVectorType()) {
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
	string retval = VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": (UNKNOWN COUNT) [ ";
	switch (GetVectorType()) {
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

template <class T>
static void TemplatedFlattenConstantVector(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = Load<T>(old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Normalify(idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType());
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		bool is_null = ConstantVector::IsNull(*this);
		// allocate a new buffer for the vector
		auto old_buffer = move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(VectorType::FLAT_VECTOR, old_buffer->GetType());
		data = buffer->GetData();
		if (is_null) {
			// constant NULL, set nullmask
			validity.EnsureWritable();
			validity.SetAllInvalid(count);
			return;
		}
		// non-null constant: have to repeat the constant
		switch (GetType().InternalType()) {
		case PhysicalType::BOOL:
			TemplatedFlattenConstantVector<bool>(data, old_data, count);
			break;
		case PhysicalType::INT8:
			TemplatedFlattenConstantVector<int8_t>(data, old_data, count);
			break;
		case PhysicalType::INT16:
			TemplatedFlattenConstantVector<int16_t>(data, old_data, count);
			break;
		case PhysicalType::INT32:
			TemplatedFlattenConstantVector<int32_t>(data, old_data, count);
			break;
		case PhysicalType::INT64:
			TemplatedFlattenConstantVector<int64_t>(data, old_data, count);
			break;
		case PhysicalType::UINT8:
			TemplatedFlattenConstantVector<uint8_t>(data, old_data, count);
			break;
		case PhysicalType::UINT16:
			TemplatedFlattenConstantVector<uint16_t>(data, old_data, count);
			break;
		case PhysicalType::UINT32:
			TemplatedFlattenConstantVector<uint32_t>(data, old_data, count);
			break;
		case PhysicalType::UINT64:
			TemplatedFlattenConstantVector<uint64_t>(data, old_data, count);
			break;
		case PhysicalType::INT128:
			TemplatedFlattenConstantVector<hugeint_t>(data, old_data, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenConstantVector<float>(data, old_data, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenConstantVector<double>(data, old_data, count);
			break;
		case PhysicalType::HASH:
			TemplatedFlattenConstantVector<hash_t>(data, old_data, count);
			break;
		case PhysicalType::POINTER:
			TemplatedFlattenConstantVector<uintptr_t>(data, old_data, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedFlattenConstantVector<interval_t>(data, old_data, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedFlattenConstantVector<string_t>(data, old_data, count);
			break;
		case PhysicalType::LIST: {
			TemplatedFlattenConstantVector<list_entry_t>(data, old_data, count);
			break;
		}
		case PhysicalType::MAP:
		case PhysicalType::STRUCT: {
			for (auto &child : StructVector::GetEntries(*this)) {
				D_ASSERT(child.second->GetVectorType() == VectorType::CONSTANT_VECTOR);
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

		buffer = VectorBuffer::CreateStandardVector(VectorType::FLAT_VECTOR, GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, start, increment);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented type for normalify");
	}
}

void Vector::Normalify(const SelectionVector &sel, idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(VectorType::FLAT_VECTOR, GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::Orrify(idx_t count, VectorData &data) {
	switch (GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		if (child.GetVectorType() == VectorType::FLAT_VECTOR) {
			data.sel = &sel;
			data.data = FlatVector::GetData(child);
			data.validity = FlatVector::Validity(child);
		} else {
			// dictionary with non-flat child: create a new reference to the child and normalify it
			auto new_aux = make_buffer<VectorChildBuffer>();
			new_aux->data.Reference(child);
			new_aux->data.Normalify(sel, count);

			data.sel = &sel;
			data.data = FlatVector::GetData(new_aux->data);
			data.validity = FlatVector::Validity(new_aux->data);
			this->auxiliary = move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		data.sel = &ConstantVector::ZERO_SELECTION_VECTOR;
		data.data = ConstantVector::GetData(*this);
		data.validity = ConstantVector::Validity(*this);
		break;
	default:
		Normalify(count);
		data.sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
		data.data = FlatVector::GetData(*this);
		data.validity = FlatVector::Validity(*this);
		break;
	}
}

void Vector::Sequence(int64_t start, int64_t increment) {
	this->buffer = make_buffer<VectorBuffer>(VectorType::SEQUENCE_VECTOR, GetType(), sizeof(int64_t) * 2);
	auto data = (int64_t *)buffer->GetData();
	data[0] = start;
	data[1] = increment;
	validity.Reset();
	auxiliary.reset();
}

void Vector::Serialize(idx_t count, Serializer &serializer) {
	auto &type = GetType();
	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteData(ptr.get(), write_size);
	} else {
		VectorData vdata;
		Orrify(count, vdata);

		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
				serializer.WriteStringLen((const_data_ptr_t)source.GetDataUnsafe(), source.GetSize());
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for Vector::Serialize!");
		}
	}
}

void Vector::Deserialize(idx_t count, Deserializer &source) {
	auto &type = GetType();
	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: read fixed amount of data from
		auto column_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[column_size]);
		source.ReadData(ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		auto strings = FlatVector::GetData<string_t>(*this);
		auto &validity = FlatVector::Validity(*this);
		for (idx_t i = 0; i < count; i++) {
			// read the strings
			auto str = source.Read<string>();
			// now add the string to the StringHeap of the vector
			// and write the pointer into the vector
			if (IsNullValue<const char *>((const char *)str.c_str())) {
				validity.SetInvalid(i);
			} else {
				strings[i] = StringVector::AddStringOrBlob(*this, str);
			}
		}
	}
}

void Vector::UTFVerify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetType().InternalType() == PhysicalType::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch (GetVectorType()) {
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
				if (validity.RowIsValid(oidx)) {
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
	UTFVerify(FlatVector::INCREMENTAL_SELECTION_VECTOR, count);
}

void Vector::Verify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*this);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*this);
		for (idx_t i = 0; i < count; i++) {
			auto oidx = sel.get_index(i);
			auto idx = dict_sel.get_index(oidx);
			D_ASSERT(idx < STANDARD_VECTOR_SIZE);
		}
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(sel, count);
		SelectionVector new_sel(new_buffer);
		child.Verify(new_sel, count);
		return;
	}
	if (TypeIsConstantSize(GetType().InternalType()) &&
	    (GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR)) {
		D_ASSERT(!auxiliary);
	}
	if (GetType().InternalType() == PhysicalType::DOUBLE) {
		// verify that there are no INF or NAN values
		switch (GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			auto dbl = ConstantVector::GetData<double>(*this);
			if (!ConstantVector::IsNull(*this)) {
				D_ASSERT(Value::DoubleIsValid(*dbl));
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto doubles = FlatVector::GetData<double>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					D_ASSERT(Value::DoubleIsValid(doubles[oidx]));
				}
			}
			break;
		}
		default:
			break;
		}
	}
	if (GetType().id() == LogicalTypeId::VARCHAR) {
		// verify that there are no '\0' bytes in string values
		switch (GetVectorType()) {
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].VerifyNull();
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (GetType().InternalType() == PhysicalType::STRUCT || GetType().InternalType() == PhysicalType::MAP) {
		D_ASSERT(!GetType().child_types().empty());
		if (GetVectorType() == VectorType::FLAT_VECTOR || GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (StructVector::HasEntries(*this)) {
				auto &children = StructVector::GetEntries(*this);
				D_ASSERT(!children.empty());
				for (auto &child : children) {
					child.second->Verify(sel, count);
				}
			}
		}
	}

	if (GetType().InternalType() == PhysicalType::LIST) {
		D_ASSERT(GetType().child_types().size() == 1);
		if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*this)) {
				ListVector::GetEntry(*this).Verify(ListVector::GetListSize(*this));
				auto le = ConstantVector::GetData<list_entry_t>(*this);
				D_ASSERT(le->offset + le->length <= ListVector::GetListSize(*this));
			}
		} else if (GetVectorType() == VectorType::FLAT_VECTOR) {
			if (ListVector::HasEntry(*this)) {
				ListVector::GetEntry(*this).Verify(ListVector::GetListSize(*this));
			}
			auto list_data = FlatVector::GetData<list_entry_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx) && ListVector::HasEntry(*this)) {
					D_ASSERT(le.offset + le.length <= ListVector::GetListSize(*this));
				}
			}
		}
	}
#endif
}

void Vector::Verify(idx_t count) {
	if (count > STANDARD_VECTOR_SIZE) {
		SelectionVector selection_vector(count);
		for (size_t i = 0; i < count; i++) {
			selection_vector.set_index(i, i);
		}
		Verify(selection_vector, count);
	} else {
		Verify(FlatVector::INCREMENTAL_SELECTION_VECTOR, count);
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
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.EmptyString(len);
}

void StringVector::AddHandle(Vector &vector, unique_ptr<BufferHandle> handle) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(make_buffer<ManagedVectorBuffer>(move(handle)));
}

void StringVector::AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(move(buffer));
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(other.GetType().InternalType() == PhysicalType::VARCHAR);

	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		StringVector::AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
	if (!other.auxiliary) {
		return;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	D_ASSERT(other.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(other.auxiliary);
}

bool StructVector::HasEntries(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary == nullptr || vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.auxiliary != nullptr;
}

const child_list_t<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return ((const VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

void StructVector::AddEntry(Vector &vector, const string &name, unique_ptr<Vector> entry) {
	// TODO asser that an entry with this name does not already exist
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStructBuffer>();
	}
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	((VectorStructBuffer *)vector.auxiliary.get())->AddChild(name, move(entry));
}

bool ListVector::HasEntry(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::HasEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	return vector.auxiliary != nullptr;
}

const Vector &ListVector::GetEntry(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

Vector &ListVector::GetEntry(Vector &vector) {
	const Vector &cvector = vector;
	return const_cast<Vector &>(ListVector::GetEntry(cvector));
}

void ListVector::Initialize(Vector &vec) {
	if (!ListVector::HasEntry(vec)) {
		auto vec_child = make_unique<Vector>(vec.GetType().child_types()[0].second);
		ListVector::SetEntry(vec, move(vec_child));
	}
}
template <class T>
void TemplatedSearchInMap(Vector &list, T key, vector<idx_t> &offsets, bool is_key_null, idx_t offset, idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (T *)vector_data.data;
	auto validity_mask = vector_data.validity;

	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (key == data[i]) {
				offsets.push_back(i);
			}
		}
	}
}

void SearchString(Vector &list, string &key, vector<idx_t> &offsets, bool is_key_null, idx_t offset, idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (string_t *)vector_data.data;
	auto validity_mask = vector_data.validity;
	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		string_t key_str_t(key);
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (Equals::Operation<string_t>(data[i], key_str_t)) {
				offsets.push_back(i);
			}
		}
	}
}

vector<idx_t> ListVector::Search(Vector &list, Value &key, idx_t row) {
	vector<idx_t> offsets;
	if (!ListVector::HasEntry(list)) {
		return offsets;
	}

	auto &list_vector = ListVector::GetEntry(list);
	auto &entry = ((list_entry_t *)list.GetData())[row];
	switch (list_vector.GetType().id()) {

	case LogicalTypeId::SQLNULL:
		if (key.is_null) {
			for (idx_t i = entry.offset; i < entry.offset + entry.length; i++) {
				offsets.push_back(i);
			}
		}
		break;
	case LogicalTypeId::UTINYINT:
		::duckdb::TemplatedSearchInMap<uint8_t>(list, key.value_.utinyint, offsets, key.is_null, entry.offset,
		                                        entry.length);
		break;
	case LogicalTypeId::TINYINT:
		::duckdb::TemplatedSearchInMap<int8_t>(list, key.value_.tinyint, offsets, key.is_null, entry.offset,
		                                       entry.length);
		break;
	case LogicalTypeId::USMALLINT:
		::duckdb::TemplatedSearchInMap<uint16_t>(list, key.value_.usmallint, offsets, key.is_null, entry.offset,
		                                         entry.length);
		break;
	case LogicalTypeId::SMALLINT:
		::duckdb::TemplatedSearchInMap<int16_t>(list, key.value_.smallint, offsets, key.is_null, entry.offset,
		                                        entry.length);
		break;
	case LogicalTypeId::UINTEGER:
		::duckdb::TemplatedSearchInMap<uint32_t>(list, key.value_.uinteger, offsets, key.is_null, entry.offset,
		                                         entry.length);
		break;
	case LogicalTypeId::INTEGER:
		::duckdb::TemplatedSearchInMap<int32_t>(list, key.value_.integer, offsets, key.is_null, entry.offset,
		                                        entry.length);
		break;
	case LogicalTypeId::UBIGINT:
		::duckdb::TemplatedSearchInMap<uint64_t>(list, key.value_.ubigint, offsets, key.is_null, entry.offset,
		                                         entry.length);
		break;
	case LogicalTypeId::BIGINT:
		::duckdb::TemplatedSearchInMap<int64_t>(list, key.value_.bigint, offsets, key.is_null, entry.offset,
		                                        entry.length);
		break;
	case LogicalTypeId::HUGEINT:
		::duckdb::TemplatedSearchInMap<hugeint_t>(list, key.value_.hugeint, offsets, key.is_null, entry.offset,
		                                          entry.length);
		break;
	case LogicalTypeId::FLOAT:
		::duckdb::TemplatedSearchInMap<float>(list, key.value_.float_, offsets, key.is_null, entry.offset,
		                                      entry.length);
		break;
	case LogicalTypeId::DOUBLE:
		::duckdb::TemplatedSearchInMap<double>(list, key.value_.double_, offsets, key.is_null, entry.offset,
		                                       entry.length);
		break;
	case LogicalTypeId::DATE:
		::duckdb::TemplatedSearchInMap<date_t>(list, key.value_.date, offsets, key.is_null, entry.offset, entry.length);
		break;
	case LogicalTypeId::TIME:
		::duckdb::TemplatedSearchInMap<dtime_t>(list, key.value_.time, offsets, key.is_null, entry.offset,
		                                        entry.length);
		break;
	case LogicalTypeId::TIMESTAMP:
		::duckdb::TemplatedSearchInMap<timestamp_t>(list, key.value_.timestamp, offsets, key.is_null, entry.offset,
		                                            entry.length);
		break;
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		::duckdb::SearchString(list, key.str_value, offsets, key.is_null, entry.offset, entry.length);
		break;
	default:
		throw InvalidTypeException(list.GetType().id(), "Invalid type for List Vector Search");
	}
	return offsets;
}

Value ListVector::GetValuesFromOffsets(Vector &list, vector<idx_t> &offsets) {
	Value ret(list.GetType().child_types()[0].second);
	ret.is_null = false;
	auto &child_vec = ListVector::GetEntry(list);
	for (auto &offset : offsets) {
		ret.list_value.push_back(child_vec.GetValue(offset));
	}
	return ret;
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	return ((VectorListBuffer &)*vec.auxiliary).size;
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = other.auxiliary;
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	ListVector::Initialize(vec);
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
	}
	((VectorListBuffer &)*vec.auxiliary).size = size;
}

void ListVector::SetEntry(Vector &vector, unique_ptr<Vector> cc) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = make_buffer<VectorListBuffer>();
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	((VectorListBuffer *)vector.auxiliary.get())->SetChild(move(cc));
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	ListVector::Initialize(target);
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	ListVector::Initialize(target);
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, Value &insert) {
	ListVector::Initialize(target);
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.PushBack(insert);
}

} // namespace duckdb
