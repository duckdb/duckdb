#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/fsst.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/types/varint.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "fsst.h"

#include <cstring> // strlen() on Solaris

namespace duckdb {

UnifiedVectorFormat::UnifiedVectorFormat() : sel(nullptr), data(nullptr) {
}

UnifiedVectorFormat::UnifiedVectorFormat(UnifiedVectorFormat &&other) noexcept : sel(nullptr), data(nullptr) {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	if (refers_to_self) {
		sel = &owned_sel;
	}
}

UnifiedVectorFormat &UnifiedVectorFormat::operator=(UnifiedVectorFormat &&other) noexcept {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	if (refers_to_self) {
		sel = &owned_sel;
	}
	return *this;
}

Vector::Vector(LogicalType type_p, bool create_data, bool initialize_to_zero, idx_t capacity)
    : vector_type(VectorType::FLAT_VECTOR), type(std::move(type_p)), data(nullptr), validity(capacity) {
	if (create_data) {
		Initialize(initialize_to_zero, capacity);
	}
}

Vector::Vector(LogicalType type_p, idx_t capacity) : Vector(std::move(type_p), true, false, capacity) {
}

Vector::Vector(LogicalType type_p, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(std::move(type_p)), data(dataptr) {
	if (dataptr && !type.IsValid()) {
		throw InternalException("Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const VectorCache &cache) : type(cache.GetType()) {
	ResetFromCache(cache);
}

Vector::Vector(Vector &other) : type(other.type) {
	Reference(other);
}

Vector::Vector(const Vector &other, const SelectionVector &sel, idx_t count) : type(other.type) {
	Slice(other, sel, count);
}

Vector::Vector(const Vector &other, idx_t offset, idx_t end) : type(other.type) {
	Slice(other, offset, end);
}

Vector::Vector(const Value &value) : type(value.type()) {
	Reference(value);
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(std::move(other.type)), data(other.data),
      validity(std::move(other.validity)), buffer(std::move(other.buffer)), auxiliary(std::move(other.auxiliary)) {
}

void Vector::Reference(const Value &value) {
	D_ASSERT(GetType().id() == value.type().id());
	this->vector_type = VectorType::CONSTANT_VECTOR;
	buffer = VectorBuffer::CreateConstantVector(value.type());
	auto internal_type = value.type().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_uniq<VectorStructBuffer>();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_vectors = struct_buffer->GetChildren();
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto vector =
			    make_uniq<Vector>(value.IsNull() ? Value(child_types[i].second) : StructValue::GetChildren(value)[i]);
			child_vectors.push_back(std::move(vector));
		}
		auxiliary = shared_ptr<VectorBuffer>(struct_buffer.release());
		if (value.IsNull()) {
			SetValue(0, value);
		}
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_uniq<VectorListBuffer>(value.type());
		auxiliary = shared_ptr<VectorBuffer>(list_buffer.release());
		data = buffer->GetData();
		SetValue(0, value);
	} else if (internal_type == PhysicalType::ARRAY) {
		auto array_buffer = make_uniq<VectorArrayBuffer>(value.type());
		auxiliary = shared_ptr<VectorBuffer>(array_buffer.release());
		SetValue(0, value);
	} else {
		auxiliary.reset();
		data = buffer->GetData();
		SetValue(0, value);
	}
}

void Vector::Reference(const Vector &other) {
	if (other.GetType().id() != GetType().id()) {
		throw InternalException("Vector::Reference used on vector of different type");
	}
	D_ASSERT(other.GetType() == GetType());
	Reinterpret(other);
}

void Vector::ReferenceAndSetType(const Vector &other) {
	type = other.GetType();
	Reference(other);
}

void Vector::Reinterpret(const Vector &other) {
	vector_type = other.vector_type;
#ifdef DEBUG
	auto &this_type = GetType();
	auto &other_type = other.GetType();

	auto type_is_same = other_type == this_type;
	bool this_is_nested = this_type.IsNested();
	bool other_is_nested = other_type.IsNested();

	bool not_nested = this_is_nested == false && other_is_nested == false;
	bool type_size_equal = GetTypeIdSize(this_type.InternalType()) == GetTypeIdSize(other_type.InternalType());
	//! Either the types are completely identical, or they are not nested and their physical type size is the same
	//! The reason nested types are not allowed is because copying the auxiliary buffer does not happen recursively
	//! e.g DOUBLE[] to BIGINT[], the type of the LIST would say BIGINT but the child Vector says DOUBLE
	D_ASSERT((not_nested && type_size_equal) || type_is_same);
#endif
	AssignSharedPointer(buffer, other.buffer);
	AssignSharedPointer(auxiliary, other.auxiliary);
	data = other.data;
	validity = other.validity;
}

void Vector::ResetFromCache(const VectorCache &cache) {
	cache.ResetFromCache(*this);
}

void Vector::Slice(const Vector &other, idx_t offset, idx_t end) {
	D_ASSERT(end >= offset);
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	if (other.GetVectorType() != VectorType::FLAT_VECTOR) {
		// we can slice the data directly only for flat vectors
		// for non-flat vectors slice using a selection vector instead
		idx_t count = end - offset;
		SelectionVector sel(count);
		for (idx_t i = 0; i < count; i++) {
			sel.set_index(i, offset + i);
		}
		Slice(other, sel, count);
		return;
	}

	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		Vector new_vector(GetType());
		auto &entries = StructVector::GetEntries(new_vector);
		auto &other_entries = StructVector::GetEntries(other);
		D_ASSERT(entries.size() == other_entries.size());
		for (idx_t i = 0; i < entries.size(); i++) {
			entries[i]->Slice(*other_entries[i], offset, end);
		}
		new_vector.validity.Slice(other.validity, offset, end - offset);
		Reference(new_vector);
	} else if (internal_type == PhysicalType::ARRAY) {
		Vector new_vector(GetType());
		auto &child_vec = ArrayVector::GetEntry(new_vector);
		auto &other_child_vec = ArrayVector::GetEntry(other);
		D_ASSERT(ArrayType::GetSize(GetType()) == ArrayType::GetSize(other.GetType()));
		const auto array_size = ArrayType::GetSize(GetType());
		// We need to slice the child vector with the multiplied offset and end
		child_vec.Slice(other_child_vec, offset * array_size, end * array_size);
		new_vector.validity.Slice(other.validity, offset, end - offset);
		Reference(new_vector);
	} else {
		Reference(other);
		if (offset > 0) {
			data = data + GetTypeIdSize(internal_type) * offset;
			validity.Slice(other.validity, offset, end - offset);
		}
	}
}

void Vector::Slice(const Vector &other, const SelectionVector &sel, idx_t count) {
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
		auto dictionary_size = DictionaryVector::DictionarySize(*this);
		auto dictionary_id = DictionaryVector::DictionaryId(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary));
		if (GetType().InternalType() == PhysicalType::STRUCT) {
			auto &child_vector = DictionaryVector::Child(*this);

			Vector new_child(child_vector);
			new_child.auxiliary = make_buffer<VectorStructBuffer>(new_child, sel, count);
			auxiliary = make_buffer<VectorChildBuffer>(std::move(new_child));
		}
		if (dictionary_size.IsValid()) {
			auto &dict_buffer = buffer->Cast<DictionaryBuffer>();
			dict_buffer.SetDictionarySize(dictionary_size.GetIndex());
			dict_buffer.SetDictionaryId(std::move(dictionary_id));
		}
		return;
	}

	if (GetVectorType() == VectorType::FSST_VECTOR) {
		Flatten(sel, count);
		return;
	}

	Vector child_vector(*this);
	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		child_vector.auxiliary = make_buffer<VectorStructBuffer>(*this, sel, count);
	}
	auto child_ref = make_buffer<VectorChildBuffer>(std::move(child_vector));
	auto dict_buffer = make_buffer<DictionaryBuffer>(sel);
	vector_type = VectorType::DICTIONARY_VECTOR;
	buffer = std::move(dict_buffer);
	auxiliary = std::move(child_ref);
}

void Vector::Dictionary(idx_t dictionary_size, const SelectionVector &sel, idx_t count) {
	Slice(sel, count);
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		buffer->Cast<DictionaryBuffer>().SetDictionarySize(dictionary_size);
	}
}

void Vector::Dictionary(const Vector &dict, idx_t dictionary_size, const SelectionVector &sel, idx_t count) {
	Reference(dict);
	Dictionary(dictionary_size, sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR && GetType().InternalType() != PhysicalType::STRUCT) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto dictionary_size = DictionaryVector::DictionarySize(*this);
		auto dictionary_id = DictionaryVector::DictionaryId(*this);
		auto target_data = current_sel.data();
		auto entry = cache.cache.find(target_data);
		if (entry != cache.cache.end()) {
			// cached entry exists: use that
			this->buffer = make_buffer<DictionaryBuffer>(entry->second->Cast<DictionaryBuffer>().GetSelVector());
			vector_type = VectorType::DICTIONARY_VECTOR;
		} else {
			Slice(sel, count);
			cache.cache[target_data] = this->buffer;
		}
		if (dictionary_size.IsValid()) {
			auto &dict_buffer = buffer->Cast<DictionaryBuffer>();
			dict_buffer.SetDictionarySize(dictionary_size.GetIndex());
			dict_buffer.SetDictionaryId(std::move(dictionary_id));
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(bool initialize_to_zero, idx_t capacity) {
	auxiliary.reset();
	validity.Reset();
	auto &type = GetType();
	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_uniq<VectorStructBuffer>(type, capacity);
		auxiliary = shared_ptr<VectorBuffer>(struct_buffer.release());
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_uniq<VectorListBuffer>(type, capacity);
		auxiliary = shared_ptr<VectorBuffer>(list_buffer.release());
	} else if (internal_type == PhysicalType::ARRAY) {
		auto array_buffer = make_uniq<VectorArrayBuffer>(type, capacity);
		auxiliary = shared_ptr<VectorBuffer>(array_buffer.release());
	}
	auto type_size = GetTypeIdSize(internal_type);
	if (type_size > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, capacity);
		data = buffer->GetData();
		if (initialize_to_zero) {
			memset(data, 0, capacity * type_size);
		}
	}

	if (capacity > validity.Capacity()) {
		validity.Resize(capacity);
	}
}

void Vector::FindResizeInfos(vector<ResizeInfo> &resize_infos, const idx_t multiplier) {

	ResizeInfo resize_info(*this, data, buffer.get(), multiplier);
	resize_infos.emplace_back(resize_info);

	// Base case.
	if (data) {
		return;
	}

	D_ASSERT(auxiliary);
	switch (GetAuxiliary()->GetBufferType()) {
	case VectorBufferType::LIST_BUFFER: {
		auto &vector_list_buffer = auxiliary->Cast<VectorListBuffer>();
		auto &child = vector_list_buffer.GetChild();
		child.FindResizeInfos(resize_infos, multiplier);
		break;
	}
	case VectorBufferType::STRUCT_BUFFER: {
		auto &vector_struct_buffer = auxiliary->Cast<VectorStructBuffer>();
		auto &children = vector_struct_buffer.GetChildren();
		for (auto &child : children) {
			child->FindResizeInfos(resize_infos, multiplier);
		}
		break;
	}
	case VectorBufferType::ARRAY_BUFFER: {
		// We need to multiply the multiplier by the array size because
		// the child vectors of ARRAY types are always child_count * array_size.
		auto &vector_array_buffer = auxiliary->Cast<VectorArrayBuffer>();
		auto new_multiplier = vector_array_buffer.GetArraySize() * multiplier;
		auto &child = vector_array_buffer.GetChild();
		child.FindResizeInfos(resize_infos, new_multiplier);
		break;
	}
	default:
		break;
	}
}

void Vector::Resize(idx_t current_size, idx_t new_size) {
	// The vector does not contain any data.
	if (!buffer) {
		buffer = make_buffer<VectorBuffer>(0);
	}

	// Obtain the resize information for each (nested) vector.
	vector<ResizeInfo> resize_infos;
	FindResizeInfos(resize_infos, 1);

	for (auto &resize_info_entry : resize_infos) {
		// Resize the validity mask.
		auto new_validity_size = new_size * resize_info_entry.multiplier;
		resize_info_entry.vec.validity.Resize(new_validity_size);

		// For nested data types, we only need to resize the validity mask.
		if (!resize_info_entry.data) {
			continue;
		}

		auto type_size = GetTypeIdSize(resize_info_entry.vec.GetType().InternalType());
		auto old_size = current_size * type_size * resize_info_entry.multiplier * sizeof(data_t);
		auto target_size = new_size * type_size * resize_info_entry.multiplier * sizeof(data_t);

		// We have an upper limit of 128GB for a single vector.
		if (target_size > DConstants::MAX_VECTOR_SIZE) {
			throw OutOfRangeException("Cannot resize vector to %s: maximum allowed vector size is %s",
			                          StringUtil::BytesToHumanReadableString(target_size),
			                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
		}

		// Copy the data buffer to a resized buffer.
		auto new_data = make_unsafe_uniq_array_uninitialized<data_t>(target_size);
		memcpy(new_data.get(), resize_info_entry.data, old_size);
		resize_info_entry.buffer->SetData(std::move(new_data));
		resize_info_entry.vec.data = resize_info_entry.buffer->GetData();
	}
}

static bool IsStructOrArrayRecursive(const LogicalType &type) {
	return TypeVisitor::Contains(type, [](const LogicalType &type) {
		auto physical_type = type.InternalType();
		return (physical_type == PhysicalType::STRUCT || physical_type == PhysicalType::ARRAY);
	});
}

void Vector::SetValue(idx_t index, const Value &val) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), val);
	}
	if (!val.IsNull() && val.type() != GetType()) {
		SetValue(index, val.DefaultCastAs(GetType()));
		return;
	}
	D_ASSERT(val.IsNull() || (val.type().InternalType() == GetType().InternalType()));

	validity.Set(index, !val.IsNull());
	auto physical_type = GetType().InternalType();
	if (val.IsNull() && !IsStructOrArrayRecursive(GetType())) {
		// for structs and arrays we still need to set the child-entries to NULL
		// so we do not bail out yet
		return;
	}

	switch (physical_type) {
	case PhysicalType::BOOL:
		reinterpret_cast<bool *>(data)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		reinterpret_cast<int8_t *>(data)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		reinterpret_cast<int16_t *>(data)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		reinterpret_cast<int32_t *>(data)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		reinterpret_cast<int64_t *>(data)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		reinterpret_cast<hugeint_t *>(data)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		reinterpret_cast<uint8_t *>(data)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		reinterpret_cast<uint16_t *>(data)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		reinterpret_cast<uint32_t *>(data)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		reinterpret_cast<uint64_t *>(data)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::UINT128:
		reinterpret_cast<uhugeint_t *>(data)[index] = val.GetValueUnsafe<uhugeint_t>();
		break;
	case PhysicalType::FLOAT:
		reinterpret_cast<float *>(data)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		reinterpret_cast<double *>(data)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		reinterpret_cast<interval_t *>(data)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR: {
		if (!val.IsNull()) {
			reinterpret_cast<string_t *>(data)[index] = StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		}
		break;
	}
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		if (val.IsNull()) {
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				vec_child->SetValue(index, Value());
			}
		} else {
			auto &val_children = StructValue::GetChildren(val);
			D_ASSERT(children.size() == val_children.size());
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				auto &struct_child = val_children[i];
				vec_child->SetValue(index, struct_child);
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		if (val.IsNull()) {
			auto &entry = reinterpret_cast<list_entry_t *>(data)[index];
			ListVector::PushBack(*this, Value());
			entry.length = 1;
			entry.offset = offset;
		} else {
			auto &val_children = ListValue::GetChildren(val);
			if (!val_children.empty()) {
				for (idx_t i = 0; i < val_children.size(); i++) {
					ListVector::PushBack(*this, val_children[i]);
				}
			}
			//! now set the pointer
			auto &entry = reinterpret_cast<list_entry_t *>(data)[index];
			entry.length = val_children.size();
			entry.offset = offset;
		}
		break;
	}
	case PhysicalType::ARRAY: {
		auto array_size = ArrayType::GetSize(GetType());
		auto &child = ArrayVector::GetEntry(*this);
		if (val.IsNull()) {
			for (idx_t i = 0; i < array_size; i++) {
				child.SetValue(index * array_size + i, Value());
			}
		} else {
			auto &val_children = ArrayValue::GetChildren(val);
			for (idx_t i = 0; i < array_size; i++) {
				child.SetValue(index * array_size + i, val_children[i]);
			}
		}
		break;
	}
	default:
		throw InternalException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValueInternal(const Vector &v_p, idx_t index_p) {
	const Vector *vector = &v_p;
	idx_t index = index_p;
	bool finished = false;
	while (!finished) {
		switch (vector->GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			index = 0;
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		case VectorType::FSST_VECTOR:
			finished = true;
			break;
		// dictionary: apply dictionary and forward to child
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel_vector = DictionaryVector::SelVector(*vector);
			auto &child = DictionaryVector::Child(*vector);
			vector = &child;
			index = sel_vector.get_index(index);
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			SequenceVector::GetSequence(*vector, start, increment);
			return Value::Numeric(vector->GetType(), start + increment * NumericCast<int64_t>(index));
		}
		default:
			throw InternalException("Unimplemented vector type for Vector::GetValue");
		}
	}
	auto data = vector->data;
	auto &validity = vector->validity;
	auto &type = vector->GetType();

	if (!validity.RowIsValid(index)) {
		return Value(vector->GetType());
	}

	if (vector->GetVectorType() == VectorType::FSST_VECTOR) {
		if (vector->GetType().InternalType() != PhysicalType::VARCHAR) {
			throw InternalException("FSST Vector with non-string datatype found!");
		}
		auto str_compressed = reinterpret_cast<string_t *>(data)[index];
		auto decoder = FSSTVector::GetDecoder(*vector);
		auto &decompress_buffer = FSSTVector::GetDecompressBuffer(*vector);
		auto string_val = FSSTPrimitives::DecompressValue(decoder, str_compressed.GetData(), str_compressed.GetSize(),
		                                                  decompress_buffer);
		switch (vector->GetType().id()) {
		case LogicalTypeId::VARCHAR:
			return Value(std::move(string_val));
		case LogicalTypeId::BLOB:
			return Value::BLOB_RAW(string_val);
		default:
			throw InternalException("Unsupported vector type for FSST vector");
		}
	}

	switch (vector->GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(reinterpret_cast<bool *>(data)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(reinterpret_cast<int8_t *>(data)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(reinterpret_cast<int16_t *>(data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(reinterpret_cast<int32_t *>(data)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(reinterpret_cast<date_t *>(data)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(reinterpret_cast<dtime_t *>(data)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(reinterpret_cast<dtime_tz_t *>(data)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(reinterpret_cast<int64_t *>(data)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(reinterpret_cast<uint8_t *>(data)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(reinterpret_cast<uint16_t *>(data)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(reinterpret_cast<uint32_t *>(data)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(reinterpret_cast<uint64_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(reinterpret_cast<timestamp_ns_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(reinterpret_cast<timestamp_ms_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(reinterpret_cast<timestamp_sec_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(reinterpret_cast<timestamp_tz_t *>(data)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(reinterpret_cast<hugeint_t *>(data)[index]);
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(reinterpret_cast<uhugeint_t *>(data)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(reinterpret_cast<hugeint_t *>(data)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(reinterpret_cast<int16_t *>(data)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(reinterpret_cast<int32_t *>(data)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(reinterpret_cast<int64_t *>(data)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(reinterpret_cast<hugeint_t *>(data)[index], width, scale);
		default:
			throw InternalException("Physical type '%s' has a width bigger than 38, which is not supported",
			                        TypeIdToString(type.InternalType()));
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(reinterpret_cast<uint8_t *>(data)[index], type);
		case PhysicalType::UINT16:
			return Value::ENUM(reinterpret_cast<uint16_t *>(data)[index], type);
		case PhysicalType::UINT32:
			return Value::ENUM(reinterpret_cast<uint32_t *>(data)[index], type);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
	}
	case LogicalTypeId::POINTER:
		return Value::POINTER(reinterpret_cast<uintptr_t *>(data)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(reinterpret_cast<float *>(data)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(reinterpret_cast<double *>(data)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(reinterpret_cast<interval_t *>(data)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::BLOB: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::VARINT: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::VARINT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::AGGREGATE_STATE: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::AGGREGATE_STATE(vector->GetType(), const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIT: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::MAP: {
		auto offlen = reinterpret_cast<list_entry_t *>(data)[index];
		auto &child_vec = ListVector::GetEntry(*vector);
		duckdb::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::MAP(ListType::GetChildType(type), std::move(children));
	}
	case LogicalTypeId::UNION: {
		// Remember to pass the original index_p here so we dont slice twice when looking up the tag
		// in case this is a dictionary vector
		union_tag_t tag;
		if (UnionVector::TryGetTag(*vector, index_p, tag)) {
			auto value = UnionVector::GetMember(*vector, tag).GetValue(index_p);
			auto members = UnionType::CopyMemberTypes(type);
			return Value::UNION(members, tag, std::move(value));
		} else {
			return Value(vector->GetType());
		}
	}
	case LogicalTypeId::STRUCT: {
		// we can derive the value schema from the vector schema
		auto &child_entries = StructVector::GetEntries(*vector);
		child_list_t<Value> children;
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			auto &struct_child = child_entries[child_idx];
			children.push_back(make_pair(StructType::GetChildName(type, child_idx), struct_child->GetValue(index_p)));
		}
		return Value::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto offlen = reinterpret_cast<list_entry_t *>(data)[index];
		auto &child_vec = ListVector::GetEntry(*vector);
		duckdb::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::LIST(ListType::GetChildType(type), std::move(children));
	}
	case LogicalTypeId::ARRAY: {
		auto stride = ArrayType::GetSize(type);
		auto offset = index * stride;
		auto &child_vec = ArrayVector::GetEntry(*vector);
		duckdb::vector<Value> children;
		for (idx_t i = offset; i < offset + stride; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::ARRAY(ArrayType::GetChildType(type), std::move(children));
	}
	default:
		throw InternalException("Unimplemented type for value access");
	}
}

Value Vector::GetValue(const Vector &v_p, idx_t index_p) {
	auto value = GetValueInternal(v_p, index_p);
	// set the alias of the type to the correct value, if there is a type alias
	if (v_p.GetType().HasAlias()) {
		value.GetTypeMutable().CopyAuxInfo(v_p.GetType());
	}
	if (v_p.GetType().id() != LogicalTypeId::AGGREGATE_STATE && value.type().id() != LogicalTypeId::AGGREGATE_STATE) {

		D_ASSERT(v_p.GetType() == value.type());
	}
	return value;
}

Value Vector::GetValue(idx_t index) const {
	return GetValue(*this, index);
}

// LCOV_EXCL_START
string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::FSST_VECTOR:
		return "FSST";
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
	case VectorType::FSST_VECTOR: {
		for (idx_t i = 0; i < count; i++) {
			string_t compressed_string = reinterpret_cast<string_t *>(data)[i];
			auto decoder = FSSTVector::GetDecoder(*this);
			auto &decompress_buffer = FSSTVector::GetDecompressBuffer(*this);
			Value val = FSSTPrimitives::DecompressValue(decoder, compressed_string.GetData(),
			                                            compressed_string.GetSize(), decompress_buffer);
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
	} break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * UnsafeNumericCast<int64_t>(i)) + (i == count - 1 ? "" : ", ");
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

void Vector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}

// TODO: add the size of validity masks to this
idx_t Vector::GetAllocationSize(idx_t cardinality) const {
	if (!type.IsNested()) {
		auto physical_size = GetTypeIdSize(type.InternalType());
		return cardinality * physical_size;
	}
	auto internal_type = type.InternalType();
	switch (internal_type) {
	case PhysicalType::LIST: {
		auto physical_size = GetTypeIdSize(type.InternalType());
		auto total_size = physical_size * cardinality;

		auto child_cardinality = ListVector::GetListCapacity(*this);
		auto &child_entry = ListVector::GetEntry(*this);
		total_size += (child_entry.GetAllocationSize(child_cardinality));
		return total_size;
	}
	case PhysicalType::ARRAY: {
		auto child_cardinality = ArrayVector::GetTotalSize(*this);

		auto &child_entry = ArrayVector::GetEntry(*this);
		auto total_size = (child_entry.GetAllocationSize(child_cardinality));
		return total_size;
	}
	case PhysicalType::STRUCT: {
		idx_t total_size = 0;
		auto &children = StructVector::GetEntries(*this);
		for (auto &child : children) {
			total_size += child->GetAllocationSize(cardinality);
		}
		return total_size;
	}
	default:
		throw NotImplementedException("Vector::GetAllocationSize not implemented for type: %s", type.ToString());
		break;
	}
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

void Vector::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

template <class T>
static void TemplatedFlattenConstantVector(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = Load<T>(old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Flatten(idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::FSST_VECTOR: {
		// Even though count may only be a part of the vector, we need to flatten the whole thing due to the way
		// ToUnifiedFormat uses flatten
		idx_t total_count = FSSTVector::GetCount(*this);
		// create vector to decompress into
		Vector other(GetType(), total_count);
		// now copy the data of this vector to the other vector, decompressing the strings in the process
		VectorOperations::Copy(*this, other, total_count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType(), count);
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		bool is_null = ConstantVector::IsNull(*this);
		// allocate a new buffer for the vector
		auto old_buffer = std::move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type, MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		if (old_buffer) {
			D_ASSERT(buffer->GetAuxiliaryData() == nullptr);
			// The old buffer might be relying on the auxiliary data, keep it alive
			buffer->MoveAuxiliaryData(*old_buffer);
		}
		data = buffer->GetData();
		vector_type = VectorType::FLAT_VECTOR;
		if (is_null && GetType().InternalType() != PhysicalType::ARRAY) {
			// constant NULL, set nullmask
			validity.EnsureWritable();
			validity.SetAllInvalid(count);
			if (GetType().InternalType() != PhysicalType::STRUCT) {
				// for structs we still need to flatten the child vectors as well
				return;
			}
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
		case PhysicalType::UINT128:
			TemplatedFlattenConstantVector<uhugeint_t>(data, old_data, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenConstantVector<float>(data, old_data, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenConstantVector<double>(data, old_data, count);
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
		case PhysicalType::ARRAY: {
			auto &original_child = ArrayVector::GetEntry(*this);
			auto array_size = ArrayType::GetSize(GetType());
			auto flattened_buffer = make_uniq<VectorArrayBuffer>(GetType(), count);
			auto &new_child = flattened_buffer->GetChild();

			// Fast path: The array is a constant null
			if (is_null) {
				// Invalidate the parent array
				validity.SetAllInvalid(count);
				// Also invalidate the new child array
				new_child.validity.SetAllInvalid(count * array_size);
				// Recurse
				new_child.Flatten(count * array_size);
				// TODO: the fast path should exit here, but the part below it is somehow required for correctness
				// Attach the flattened buffer and return
				// auxiliary = shared_ptr<VectorBuffer>(flattened_buffer.release());
				// return;
			}

			// Now we need to "unpack" the child vector.
			// Basically, do this:
			//
			// | a1 | | 1 |      | a1 | | 1 |
			//        | 2 |      | a2 | | 2 |
			//	             =>    ..   | 1 |
			//                          | 2 |
			// 							 ...

			auto child_vec = make_uniq<Vector>(original_child);
			child_vec->Flatten(count * array_size);

			// Create a selection vector
			SelectionVector sel(count * array_size);
			for (idx_t array_idx = 0; array_idx < count; array_idx++) {
				for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
					auto position = array_idx * array_size + elem_idx;
					// Broadcast the validity
					if (FlatVector::IsNull(*child_vec, elem_idx)) {
						FlatVector::SetNull(new_child, position, true);
					}
					sel.set_index(position, elem_idx);
				}
			}

			// Copy over the data to the new buffer
			VectorOperations::Copy(*child_vec, new_child, sel, count * array_size, 0, 0);
			auxiliary = shared_ptr<VectorBuffer>(flattened_buffer.release());

			break;
		}
		case PhysicalType::STRUCT: {
			auto normalified_buffer = make_uniq<VectorStructBuffer>();

			auto &new_children = normalified_buffer->GetChildren();

			auto &child_entries = StructVector::GetEntries(*this);
			for (auto &child : child_entries) {
				D_ASSERT(child->GetVectorType() == VectorType::CONSTANT_VECTOR);
				auto vector = make_uniq<Vector>(*child);
				vector->Flatten(count);
				new_children.push_back(std::move(vector));
			}
			auxiliary = shared_ptr<VectorBuffer>(normalified_buffer.release());
			break;
		}
		default:
			throw InternalException("Unimplemented type for VectorOperations::Flatten");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment, sequence_count;
		SequenceVector::GetSequence(*this, start, increment, sequence_count);
		auto seq_count = NumericCast<idx_t>(sequence_count);

		buffer = VectorBuffer::CreateStandardVector(GetType(), MaxValue<idx_t>(STANDARD_VECTOR_SIZE, seq_count));
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, seq_count, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify");
	}
}

void Vector::Flatten(const SelectionVector &sel, idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::FSST_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType(), count);
		// copy the data of this vector to the other vector, removing compression and selection vector in the process
		VectorOperations::Copy(*this, other, sel, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) {
	switch (GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		format.owned_sel.Initialize(sel);
		format.sel = &format.owned_sel;

		auto &child = DictionaryVector::Child(*this);
		if (child.GetVectorType() == VectorType::FLAT_VECTOR) {
			format.data = FlatVector::GetData(child);
			format.validity = FlatVector::Validity(child);
		} else {
			// dictionary with non-flat child: create a new reference to the child and flatten it
			Vector child_vector(child);
			child_vector.Flatten(sel, count);
			auto new_aux = make_buffer<VectorChildBuffer>(std::move(child_vector));

			format.data = FlatVector::GetData(new_aux->data);
			format.validity = FlatVector::Validity(new_aux->data);
			this->auxiliary = std::move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
		format.data = ConstantVector::GetData(*this);
		format.validity = ConstantVector::Validity(*this);
		break;
	default:
		Flatten(count);
		format.sel = FlatVector::IncrementalSelectionVector();
		format.data = FlatVector::GetData(*this);
		format.validity = FlatVector::Validity(*this);
		break;
	}
}

void Vector::RecursiveToUnifiedFormat(Vector &input, idx_t count, RecursiveUnifiedVectorFormat &data) {

	input.ToUnifiedFormat(count, data.unified);
	data.logical_type = input.GetType();

	if (input.GetType().InternalType() == PhysicalType::LIST) {
		auto &child = ListVector::GetEntry(input);
		auto child_count = ListVector::GetListSize(input);
		data.children.emplace_back();
		Vector::RecursiveToUnifiedFormat(child, child_count, data.children.back());

	} else if (input.GetType().InternalType() == PhysicalType::ARRAY) {
		auto &child = ArrayVector::GetEntry(input);
		auto array_size = ArrayType::GetSize(input.GetType());
		auto child_count = count * array_size;
		data.children.emplace_back();
		Vector::RecursiveToUnifiedFormat(child, child_count, data.children.back());

	} else if (input.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(input);
		for (idx_t i = 0; i < children.size(); i++) {
			data.children.emplace_back();
		}
		for (idx_t i = 0; i < children.size(); i++) {
			Vector::RecursiveToUnifiedFormat(*children[i], count, data.children[i]);
		}
	}
}

void Vector::Sequence(int64_t start, int64_t increment, idx_t count) {
	this->vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 3);
	auto data = reinterpret_cast<int64_t *>(buffer->GetData());
	data[0] = start;
	data[1] = increment;
	data[2] = int64_t(count);
	validity.Reset();
	auxiliary.reset();
}

// FIXME: This should ideally be const
void Vector::Serialize(Serializer &serializer, idx_t count, bool compressed_serialization) {
	auto &logical_type = GetType();

	UnifiedVectorFormat vdata;

	// serialize compressed vectors to save space, but skip this if serializing into older versions
	if (serializer.ShouldSerialize(5)) {
		if (compressed_serialization) {
			auto vtype = GetVectorType();
			if (vtype == VectorType::DICTIONARY_VECTOR && DictionaryVector::DictionarySize(*this).IsValid()) {
				auto dict = DictionaryVector::Child(*this);
				if (dict.GetVectorType() == VectorType::FLAT_VECTOR) {
					idx_t dict_count = DictionaryVector::DictionarySize(*this).GetIndex();
					auto old_sel = DictionaryVector::SelVector(*this);
					SelectionVector new_sel(count), used_sel(count), map_sel(dict_count);

					// dictionaries may be large (row-group level). A vector may use only a small part.
					// So, restrict dict to the used_sel subset & remap old_sel into new_sel to the new dict positions
					sel_t CODE_UNSEEN = static_cast<sel_t>(dict_count);
					for (sel_t i = 0; i < dict_count; ++i) {
						map_sel[i] = CODE_UNSEEN; // initialize with unused marker
					}
					idx_t used_count = 0;
					for (idx_t i = 0; i < count; ++i) {
						auto pos = old_sel[i];
						if (map_sel[pos] == CODE_UNSEEN) {
							map_sel[pos] = static_cast<sel_t>(used_count);
							used_sel[used_count++] = pos;
						}
						new_sel[i] = map_sel[pos];
					}
					if (used_count * 2 < count) { // only serialize as a dict vector if that makes things smaller
						auto sel_data = reinterpret_cast<data_ptr_t>(new_sel.data());
						dict.Slice(used_sel, used_count);
						serializer.WriteProperty(99, "vector_type", VectorType::DICTIONARY_VECTOR);
						serializer.WriteProperty(100, "sel_vector", sel_data, sizeof(sel_t) * count);
						serializer.WriteProperty(101, "dict_count", used_count);
						return dict.Serialize(serializer, used_count, false);
					}
				}
			} else if (vtype == VectorType::CONSTANT_VECTOR && count >= 1) {
				serializer.WriteProperty(99, "vector_type", VectorType::CONSTANT_VECTOR);
				return Vector::Serialize(serializer, 1, false); // just serialize one value
			} else if (vtype == VectorType::SEQUENCE_VECTOR) {
				serializer.WriteProperty(99, "vector_type", VectorType::SEQUENCE_VECTOR);
				auto data = reinterpret_cast<int64_t *>(buffer->GetData());
				serializer.WriteProperty(100, "seq_start", data[0]);
				serializer.WriteProperty(100, "seq_increment", data[1]);
				return; // for sequence vectors we do not serialize anything else
			} else {
				// TODO: other compressed vector types (FSST)
			}
		}
		serializer.WriteProperty(99, "vector_type", VectorType::FLAT_VECTOR);
	}
	ToUnifiedFormat(count, vdata);

	const bool has_validity_mask = (count > 0) && !vdata.validity.AllValid();
	serializer.WriteProperty(100, "has_validity_mask", has_validity_mask);
	if (has_validity_mask) {
		ValidityMask flat_mask(count);
		flat_mask.Initialize();
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = vdata.sel->get_index(i);
			flat_mask.Set(i, vdata.validity.RowIsValid(row_idx));
		}
		serializer.WriteProperty(101, "validity", const_data_ptr_cast(flat_mask.GetData()),
		                         flat_mask.ValidityMaskSize(count));
	}
	if (TypeIsConstantSize(logical_type.InternalType())) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(logical_type.InternalType()) * count;
		auto ptr = make_unsafe_uniq_array_uninitialized<data_t>(write_size);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteProperty(102, "data", ptr.get(), write_size);
	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);

			// Serialize data as a list
			serializer.WriteList(102, "data", count, [&](Serializer::List &list, idx_t i) {
				auto idx = vdata.sel->get_index(i);
				auto str = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
				list.WriteElement(str);
			});
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);

			// Serialize entries as a list
			serializer.WriteList(103, "children", entries.size(), [&](Serializer::List &list, idx_t i) {
				list.WriteObject([&](Serializer &object) { entries[i]->Serialize(object, count); });
			});
			break;
		}
		case PhysicalType::LIST: {
			auto &child = ListVector::GetEntry(*this);
			auto list_size = ListVector::GetListSize(*this);

			// serialize the list entries in a flat array
			auto entries = make_unsafe_uniq_array_uninitialized<list_entry_t>(count);
			auto source_array = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = source_array[idx];
				if (vdata.validity.RowIsValid(idx)) {
					entries[i].offset = source.offset;
					entries[i].length = source.length;
				} else {
					entries[i].offset = 0;
					entries[i].length = 0;
				}
			}
			serializer.WriteProperty(104, "list_size", list_size);
			serializer.WriteList(105, "entries", count, [&](Serializer::List &list, idx_t i) {
				list.WriteObject([&](Serializer &object) {
					object.WriteProperty(100, "offset", entries[i].offset);
					object.WriteProperty(101, "length", entries[i].length);
				});
			});
			serializer.WriteObject(106, "child", [&](Serializer &object) { child.Serialize(object, list_size); });
			break;
		}
		case PhysicalType::ARRAY: {
			Vector serialized_vector(*this);
			serialized_vector.Flatten(count);

			auto &child = ArrayVector::GetEntry(serialized_vector);
			auto array_size = ArrayType::GetSize(serialized_vector.GetType());
			auto child_size = array_size * count;
			serializer.WriteProperty<uint64_t>(103, "array_size", array_size);
			serializer.WriteObject(104, "child", [&](Serializer &object) { child.Serialize(object, child_size); });
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Serialize!");
		}
	}
}

void Vector::Deserialize(Deserializer &deserializer, idx_t count) {
	auto &logical_type = GetType();
	const auto vtype = // older versions that only supported flat vectors did not serialize vector_type,
	    deserializer.ReadPropertyWithExplicitDefault<VectorType>(99, "vector_type", VectorType::FLAT_VECTOR);

	// first handle deserialization of compressed vector types
	if (vtype == VectorType::CONSTANT_VECTOR) {
		Vector::Deserialize(deserializer, 1); // read a vector of size 1
		Vector::SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	} else if (vtype == VectorType::DICTIONARY_VECTOR) {
		SelectionVector sel(count);
		deserializer.ReadProperty(100, "sel_vector", reinterpret_cast<data_ptr_t>(sel.data()), sizeof(sel_t) * count);
		const auto dict_count = deserializer.ReadProperty<idx_t>(101, "dict_count");
		Vector::Deserialize(deserializer, dict_count); // deserialize the dictionary in this vector
		Vector::Slice(sel, count);                     // will create a dictionary vector
		return;
	} else if (vtype == VectorType::SEQUENCE_VECTOR) {
		const int64_t seq_start = deserializer.ReadProperty<int64_t>(100, "seq_start");
		const int64_t seq_increment = deserializer.ReadProperty<int64_t>(101, "seq_increment");
		Vector::Sequence(seq_start, seq_increment, count);
		return;
	}

	auto &validity = FlatVector::Validity(*this);
	auto validity_count = MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE);
	validity.Reset(validity_count);
	const auto has_validity_mask = deserializer.ReadProperty<bool>(100, "has_validity_mask");
	if (has_validity_mask) {
		validity.Initialize(validity_count);
		deserializer.ReadProperty(101, "validity", data_ptr_cast(validity.GetData()), validity.ValidityMaskSize(count));
	}

	if (TypeIsConstantSize(logical_type.InternalType())) {
		// constant size type: read fixed amount of data
		auto column_size = GetTypeIdSize(logical_type.InternalType()) * count;
		auto ptr = make_unsafe_uniq_array_uninitialized<data_t>(column_size);
		deserializer.ReadProperty(102, "data", ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			deserializer.ReadList(102, "data", [&](Deserializer::List &list, idx_t i) {
				auto str = list.ReadElement<string>();
				if (validity.RowIsValid(i)) {
					strings[i] = StringVector::AddStringOrBlob(*this, str);
				}
			});
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			// Deserialize entries as a list
			deserializer.ReadList(103, "children", [&](Deserializer::List &list, idx_t i) {
				list.ReadObject([&](Deserializer &obj) { entries[i]->Deserialize(obj, count); });
			});
			break;
		}
		case PhysicalType::LIST: {
			// Read the list size
			auto list_size = deserializer.ReadProperty<uint64_t>(104, "list_size");
			ListVector::Reserve(*this, list_size);
			ListVector::SetListSize(*this, list_size);

			// Read the entries
			auto list_entries = FlatVector::GetData<list_entry_t>(*this);
			deserializer.ReadList(105, "entries", [&](Deserializer::List &list, idx_t i) {
				list.ReadObject([&](Deserializer &obj) {
					list_entries[i].offset = obj.ReadProperty<uint64_t>(100, "offset");
					list_entries[i].length = obj.ReadProperty<uint64_t>(101, "length");
				});
			});

			// Read the child vector
			deserializer.ReadObject(106, "child", [&](Deserializer &obj) {
				auto &child = ListVector::GetEntry(*this);
				child.Deserialize(obj, list_size);
			});
			break;
		}
		case PhysicalType::ARRAY: {
			auto array_size = deserializer.ReadProperty<uint64_t>(103, "array_size");
			deserializer.ReadObject(104, "child", [&](Deserializer &obj) {
				auto &child = ArrayVector::GetEntry(*this);
				child.Deserialize(obj, array_size * count);
			});
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Deserialize!");
		}
	}
}

void Vector::SetVectorType(VectorType vector_type_p) {
	vector_type = vector_type_p;
	auto physical_type = GetType().InternalType();
	auto flat_or_const = GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR;
	if (TypeIsConstantSize(physical_type) && flat_or_const) {
		auxiliary.reset();
	}
	if (vector_type == VectorType::CONSTANT_VECTOR && physical_type == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(*this);
		for (auto &entry : entries) {
			entry->SetVectorType(vector_type);
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
	auto flat_sel = FlatVector::IncrementalSelectionVector();

	UTFVerify(*flat_sel, count);
}

void Vector::VerifyMap(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::MAP);
	auto &child = ListType::GetChildType(vector_p.GetType());
	D_ASSERT(StructType::GetChildCount(child) == 2);
	D_ASSERT(StructType::GetChildName(child, 0) == "key");
	D_ASSERT(StructType::GetChildName(child, 1) == "value");

	auto valid_check = MapVector::CheckMapValidity(vector_p, count, sel_p);
	D_ASSERT(valid_check == MapInvalidReason::VALID);
#endif // DEBUG
}

void Vector::VerifyUnion(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG

	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::UNION);
	auto valid_check = UnionVector::CheckUnionValidity(vector_p, count, sel_p);
	if (valid_check != UnionInvalidReason::VALID) {
		throw InternalException("Union not valid, reason: %s", EnumUtil::ToString(valid_check));
	}
#endif // DEBUG
}

void Vector::Verify(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	Vector *vector = &vector_p;
	const SelectionVector *sel = &sel_p;
	SelectionVector owned_sel;
	auto &type = vector->GetType();
	auto vtype = vector->GetVectorType();
	if (vector->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*vector);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*vector);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, count);
		owned_sel.Initialize(new_buffer);
		sel = &owned_sel;
		vector = &child;
		vtype = vector->GetVectorType();
	}
	if (TypeIsConstantSize(type.InternalType()) &&
	    (vtype == VectorType::CONSTANT_VECTOR || vtype == VectorType::FLAT_VECTOR)) {
		D_ASSERT(!vector->auxiliary);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		// verify that the string is correct unicode
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
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

	if (type.id() == LogicalTypeId::VARINT) {
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					Varint::Verify(strings[oidx]);
				}
			}
		} break;
		default:
			break;
		}
	}

	if (type.id() == LogicalTypeId::BIT) {
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					auto buf = strings[oidx].GetData();
					D_ASSERT(idx_t(*buf) < 8);
					Bit::Verify(strings[oidx]);
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (type.InternalType() == PhysicalType::ARRAY) {
		// Arrays have the following invariants
		// 1. if the array vector is a CONSTANT_VECTOR
		//	1.1	The child vector is a FLAT_VECTOR with count = array_size
		//	1.2 OR The child vector is a CONSTANT_VECTOR and must be NULL
		//  1.3 OR The child vector is a CONSTANT_VECTOR and array_size = 1
		// 2. if the array vector is a FLAT_VECTOR, the child vector is a FLAT_VECTOR
		// 	2.2 the count of the child vector is array_size * (parent)count

		auto &child = ArrayVector::GetEntry(*vector);
		auto array_size = ArrayType::GetSize(type);

		if (child.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			D_ASSERT(ConstantVector::IsNull(child));
		} else {
			D_ASSERT(child.GetVectorType() == VectorType::FLAT_VECTOR);
		}

		if (vtype == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*vector)) {
				child.Verify(array_size);
			}
		} else if (vtype == VectorType::FLAT_VECTOR) {
			// Flat vector case
			auto &validity = FlatVector::Validity(*vector);
			idx_t selected_child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					selected_child_count += array_size;
				}
			}

			SelectionVector child_sel(selected_child_count);
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					for (idx_t j = 0; j < array_size; j++) {
						child_sel.set_index(child_count++, oidx * array_size + j);
					}
				}
			}
			Vector::Verify(child, child_sel, child_count);
		}
	}

	if (type.InternalType() == PhysicalType::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		D_ASSERT(!child_types.empty());

		// create a selection vector of the non-null entries of the struct vector
		auto &children = StructVector::GetEntries(*vector);
		D_ASSERT(child_types.size() == children.size());
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			D_ASSERT(children[child_idx]->GetType() == child_types[child_idx].second);
			Vector::Verify(*children[child_idx], sel_p, count);
			if (vtype == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR);
				if (ConstantVector::IsNull(*vector)) {
					D_ASSERT(ConstantVector::IsNull(*children[child_idx]));
				}
			}
			if (vtype != VectorType::FLAT_VECTOR) {
				continue;
			}
			optional_ptr<ValidityMask> child_validity;
			SelectionVector owned_child_sel;
			const SelectionVector *child_sel = &owned_child_sel;
			if (children[child_idx]->GetVectorType() == VectorType::FLAT_VECTOR) {
				child_sel = FlatVector::IncrementalSelectionVector();
				child_validity = &FlatVector::Validity(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &child = DictionaryVector::Child(*children[child_idx]);
				if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
					continue;
				}
				child_validity = &FlatVector::Validity(child);
				child_sel = &DictionaryVector::SelVector(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR) {
				child_sel = ConstantVector::ZeroSelectionVector(count, owned_child_sel);
				child_validity = &ConstantVector::Validity(*children[child_idx]);
			} else {
				continue;
			}
			// for any NULL entry in the struct, the child should be NULL as well
			auto &validity = FlatVector::Validity(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto index = sel->get_index(i);
				if (!validity.RowIsValid(index)) {
					auto child_index = child_sel->get_index(sel_p.get_index(i));
					D_ASSERT(!child_validity->RowIsValid(child_index));
				}
			}
		}

		if (vector->GetType().id() == LogicalTypeId::UNION) {
			// Pass in raw vector
			VerifyUnion(vector_p, sel_p, count);
		}
	}

	if (type.InternalType() == PhysicalType::LIST) {
		if (vtype == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*vector)) {
				auto &child = ListVector::GetEntry(*vector);
				SelectionVector child_sel(ListVector::GetListSize(*vector));
				idx_t child_count = 0;
				auto le = ConstantVector::GetData<list_entry_t>(*vector);
				D_ASSERT(le->offset + le->length <= ListVector::GetListSize(*vector));
				for (idx_t k = 0; k < le->length; k++) {
					child_sel.set_index(child_count++, le->offset + k);
				}
				Vector::Verify(child, child_sel, child_count);
			}
		} else if (vtype == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(*vector);
			auto &child = ListVector::GetEntry(*vector);
			auto child_size = ListVector::GetListSize(*vector);
			auto list_data = FlatVector::GetData<list_entry_t>(*vector);
			idx_t total_size = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					total_size += le.length;
				}
			}
			SelectionVector child_sel(total_size);
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					for (idx_t k = 0; k < le.length; k++) {
						child_sel.set_index(child_count++, le.offset + k);
					}
				}
			}
			Vector::Verify(child, child_sel, child_count);
		}

		if (vector->GetType().id() == LogicalTypeId::MAP) {
			VerifyMap(*vector, *sel, count);
		}
	}
#endif
}

void Vector::Verify(idx_t count) {
	auto flat_sel = FlatVector::IncrementalSelectionVector();
	Verify(*this, *flat_sel, count);
}

void Vector::DebugTransformToDictionary(Vector &vector, idx_t count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		// only supported for flat vectors currently
		return;
	}
	// convert vector to dictionary vector
	// first create an inverted vector of twice the size with NULL values every other value
	// i.e. [1, 2, 3] is converted into [NULL, 3, NULL, 2, NULL, 1]
	idx_t verify_count = count * 2;
	SelectionVector inverted_sel(verify_count);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t current_index = count - i - 1;
		inverted_sel.set_index(offset++, current_index);
		inverted_sel.set_index(offset++, current_index);
	}
	Vector inverted_vector(vector, inverted_sel, verify_count);
	inverted_vector.Flatten(verify_count);
	// now insert the NULL values at every other position
	for (idx_t i = 0; i < count; i++) {
		FlatVector::SetNull(inverted_vector, i * 2, true);
	}
	// construct the selection vector pointing towards the original values
	// we start at the back, (verify_count - 1) and move backwards
	SelectionVector original_sel(count);
	offset = 0;
	for (idx_t i = 0; i < count; i++) {
		original_sel.set_index(offset++, verify_count - 1 - i * 2);
	}
	// now slice the inverted vector with the inverted selection vector
	vector.Slice(inverted_vector, original_sel, count);
	vector.Verify(count);
}

void Vector::DebugShuffleNestedVector(Vector &vector, idx_t count) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::STRUCT: {
		auto &entries = StructVector::GetEntries(vector);
		// recurse into child elements
		for (auto &entry : entries) {
			Vector::DebugShuffleNestedVector(*entry, count);
		}
		break;
	}
	case LogicalTypeId::LIST: {
		if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			break;
		}
		auto list_entries = FlatVector::GetData<list_entry_t>(vector);
		idx_t child_count = 0;
		for (idx_t r = 0; r < count; r++) {
			if (FlatVector::IsNull(vector, r)) {
				continue;
			}
			child_count += list_entries[r].length;
		}
		if (child_count == 0) {
			break;
		}
		auto &child_vector = ListVector::GetEntry(vector);
		// reverse the order of all lists
		SelectionVector child_sel(child_count);
		idx_t position = child_count;
		for (idx_t r = 0; r < count; r++) {
			if (FlatVector::IsNull(vector, r)) {
				continue;
			}
			// move this list to the back
			position -= list_entries[r].length;
			for (idx_t k = 0; k < list_entries[r].length; k++) {
				child_sel.set_index(position + k, list_entries[r].offset + k);
			}
			// adjust the offset to this new position
			list_entries[r].offset = position;
		}
		child_vector.Slice(child_sel, child_count);
		child_vector.Flatten(child_count);
		ListVector::SetListSize(vector, child_count);

		// recurse into child elements
		Vector::DebugShuffleNestedVector(child_vector, child_count);
		break;
	}
	default:
		break;
	}
}

//===--------------------------------------------------------------------===//
// FlatVector
//===--------------------------------------------------------------------===//
void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.validity.Set(idx, !is_null);
	if (!is_null) {
		return;
	}

	auto &type = vector.GetType();
	auto internal_type = type.InternalType();

	// Set all child entries to NULL.
	if (internal_type == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			FlatVector::SetNull(*entry, idx, is_null);
		}
		return;
	}

	// Set all child entries to NULL.
	if (internal_type == PhysicalType::ARRAY) {
		auto &child = ArrayVector::GetEntry(vector);
		auto array_size = ArrayType::GetSize(type);
		auto child_offset = idx * array_size;
		for (idx_t i = 0; i < array_size; i++) {
			FlatVector::SetNull(child, child_offset + i, is_null);
		}
	}
}

//===--------------------------------------------------------------------===//
// ConstantVector
//===--------------------------------------------------------------------===//
void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.validity.Set(0, !is_null);
	if (is_null) {
		auto &type = vector.GetType();
		auto internal_type = type.InternalType();
		if (internal_type == PhysicalType::STRUCT) {
			// set all child entries to null as well
			auto &entries = StructVector::GetEntries(vector);
			for (auto &entry : entries) {
				entry->SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(*entry, is_null);
			}
		} else if (internal_type == PhysicalType::ARRAY) {
			auto &child = ArrayVector::GetEntry(vector);
			D_ASSERT(child.GetVectorType() == VectorType::CONSTANT_VECTOR ||
			         child.GetVectorType() == VectorType::FLAT_VECTOR);
			auto array_size = ArrayType::GetSize(type);
			if (child.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(array_size == 1);
				ConstantVector::SetNull(child, is_null);
			} else {
				for (idx_t i = 0; i < array_size; i++) {
					FlatVector::SetNull(child, i, is_null);
				}
			}
		}
	}
}

const SelectionVector *ConstantVector::ZeroSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return ConstantVector::ZeroSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, 0);
	}
	return &owned_sel;
}

void ConstantVector::Reference(Vector &vector, Vector &source, idx_t position, idx_t count) {
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto list_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(list_index)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
		auto list_entry = list_data[list_index];

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetData<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetEntry(vector);
		child.Reference(ListVector::GetEntry(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	case PhysicalType::ARRAY: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);
		auto source_idx = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(source_idx)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// Reference the child vector
		auto &target_child = ArrayVector::GetEntry(vector);
		auto &source_child = ArrayVector::GetEntry(source);
		target_child.Reference(source_child);

		// Only take the element at the given position
		auto array_size = ArrayType::GetSize(source_type);
		SelectionVector sel(array_size);
		for (idx_t i = 0; i < array_size; i++) {
			sel.set_index(i, array_size * source_idx + i);
		}
		target_child.Slice(sel, array_size);
		target_child.Flatten(array_size); // since its constant we only have to flatten this much

		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		vector.validity.Set(0, true);
		break;
	}
	case PhysicalType::STRUCT: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(*target_entries[i], *source_entries[i], position, count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		vector.validity.Set(0, true);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		auto value = source.GetValue(position);
		vector.Reference(value);
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

//===--------------------------------------------------------------------===//
// StringVector
//===--------------------------------------------------------------------===//
string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t StringVector::AddStringOrBlob(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddStringOrBlob(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, UnsafeNumericCast<uint32_t>(strlen(data))));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), UnsafeNumericCast<uint32_t>(data.size())));
}

VectorStringBuffer &StringVector::GetStringBuffer(Vector &vector) {
	if (vector.GetType().InternalType() != PhysicalType::VARCHAR) {
		throw InternalException("StringVector::GetStringBuffer - vector is not of internal type VARCHAR but of type %s",
		                        vector.GetType());
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	return vector.auxiliary.get()->Cast<VectorStringBuffer>();
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::BIT);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len <= string_t::INLINE_LENGTH) {
		return string_t(UnsafeNumericCast<uint32_t>(len));
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.EmptyString(len);
}

void StringVector::AddHandle(Vector &vector, BufferHandle handle) {
	auto &string_buffer = GetStringBuffer(vector);
	string_buffer.AddHeapReference(make_buffer<ManagedVectorBuffer>(std::move(handle)));
}

void StringVector::AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer) {
	D_ASSERT(buffer.get() != vector.auxiliary.get());
	auto &string_buffer = GetStringBuffer(vector);
	string_buffer.AddHeapReference(std::move(buffer));
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
	StringVector::AddBuffer(vector, other.auxiliary);
}

//===--------------------------------------------------------------------===//
// FSSTVector
//===--------------------------------------------------------------------===//
string_t FSSTVector::AddCompressedString(Vector &vector, const char *data, idx_t len) {
	return FSSTVector::AddCompressedString(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t FSSTVector::AddCompressedString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);
	auto &fsst_string_buffer = vector.auxiliary.get()->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.AddBlob(data);
}

void *FSSTVector::GetDecoder(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		throw InternalException("GetDecoder called on FSST Vector without registered buffer");
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);
	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.GetDecoder();
}

vector<unsigned char> &FSSTVector::GetDecompressBuffer(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		throw InternalException("GetDecompressBuffer called on FSST Vector without registered buffer");
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);
	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.GetDecompressBuffer();
}

void FSSTVector::RegisterDecoder(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder,
                                 const idx_t string_block_limit) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.AddDecoder(duckdb_fsst_decoder, string_block_limit);
}

void FSSTVector::SetCount(Vector &vector, idx_t count) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.SetCount(count);
}

idx_t FSSTVector::GetCount(Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.GetCount();
}

void FSSTVector::DecompressVector(const Vector &src, Vector &dst, idx_t src_offset, idx_t dst_offset, idx_t copy_count,
                                  const SelectionVector *sel) {
	D_ASSERT(src.GetVectorType() == VectorType::FSST_VECTOR);
	D_ASSERT(dst.GetVectorType() == VectorType::FLAT_VECTOR);
	auto dst_mask = FlatVector::Validity(dst);
	auto ldata = FSSTVector::GetCompressedData<string_t>(src);
	auto tdata = FlatVector::GetData<string_t>(dst);
	auto &str_buffer = StringVector::GetStringBuffer(dst);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel->get_index(src_offset + i);
		auto target_idx = dst_offset + i;
		string_t compressed_string = ldata[source_idx];
		if (dst_mask.RowIsValid(target_idx) && compressed_string.GetSize() > 0) {
			auto decoder = FSSTVector::GetDecoder(src);
			tdata[target_idx] = FSSTPrimitives::DecompressValue(decoder, str_buffer, compressed_string.GetData(),
			                                                    compressed_string.GetSize());
		} else {
			tdata[target_idx] = string_t(nullptr, 0);
		}
	}
}

//===--------------------------------------------------------------------===//
// MapVector
//===--------------------------------------------------------------------===//
Vector &MapVector::GetKeys(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return *entries[0];
}
Vector &MapVector::GetValues(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return *entries[1];
}

const Vector &MapVector::GetKeys(const Vector &vector) {
	return GetKeys((Vector &)vector);
}
const Vector &MapVector::GetValues(const Vector &vector) {
	return GetValues((Vector &)vector);
}

MapInvalidReason MapVector::CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel) {

	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);

	// unify the MAP vector, which is a physical LIST vector
	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);
	auto map_entries = UnifiedVectorFormat::GetDataNoConst<list_entry_t>(map_data);
	auto maps_length = ListVector::GetListSize(map);

	// unify the child vector containing the keys
	auto &keys = MapVector::GetKeys(map);
	UnifiedVectorFormat key_data;
	keys.ToUnifiedFormat(maps_length, key_data);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto mapped_row = sel.get_index(row_idx);
		auto map_idx = map_data.sel->get_index(mapped_row);

		if (!map_data.validity.RowIsValid(map_idx)) {
			continue;
		}

		value_set_t unique_keys;
		auto length = map_entries[map_idx].length;
		auto offset = map_entries[map_idx].offset;

		for (idx_t child_idx = 0; child_idx < length; child_idx++) {
			auto key_idx = key_data.sel->get_index(offset + child_idx);

			if (!key_data.validity.RowIsValid(key_idx)) {
				return MapInvalidReason::NULL_KEY;
			}

			auto value = keys.GetValue(key_idx);
			auto unique = unique_keys.insert(value).second;
			if (!unique) {
				return MapInvalidReason::DUPLICATE_KEY;
			}
		}
	}

	return MapInvalidReason::VALID;
}

void MapVector::MapConversionVerify(Vector &vector, idx_t count) {
	auto reason = MapVector::CheckMapValidity(vector, count);
	EvalMapInvalidReason(reason);
}

void MapVector::EvalMapInvalidReason(MapInvalidReason reason) {
	switch (reason) {
	case MapInvalidReason::VALID:
		return;
	case MapInvalidReason::DUPLICATE_KEY:
		throw InvalidInputException("Map keys must be unique.");
	case MapInvalidReason::NULL_KEY:
		throw InvalidInputException("Map keys can not be NULL.");
	case MapInvalidReason::NOT_ALIGNED:
		throw InvalidInputException("The map key list does not align with the map value list.");
	case MapInvalidReason::INVALID_PARAMS:
		throw InvalidInputException("Invalid map argument(s). Valid map arguments are a list of key-value pairs (MAP "
		                            "{'key1': 'val1', ...}), two lists (MAP ([1, 2], [10, 11])), or no arguments.");
	default:
		throw InternalException("MapInvalidReason not implemented");
	}
}

//===--------------------------------------------------------------------===//
// StructVector
//===--------------------------------------------------------------------===//
vector<unique_ptr<Vector>> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::UNION);

	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return StructVector::GetEntries(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.auxiliary->Cast<VectorStructBuffer>().GetChildren();
}

const vector<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

//===--------------------------------------------------------------------===//
// ListVector
//===--------------------------------------------------------------------===//
template <class T>
T &ListVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return vector.auxiliary->template Cast<VectorListBuffer>().GetChild();
}

const Vector &ListVector::GetEntry(const Vector &vector) {
	return GetEntryInternal<const Vector>(vector);
}

Vector &ListVector::GetEntry(Vector &vector) {
	return GetEntryInternal<Vector>(vector);
}

void ListVector::Reserve(Vector &vector, idx_t required_capacity) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = vector.auxiliary->Cast<VectorListBuffer>();
	child_buffer.Reserve(required_capacity);
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetSize();
}

idx_t ListVector::GetListCapacity(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetCapacity();
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
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
		return;
	}
	vec.auxiliary->Cast<VectorListBuffer>().SetSize(size);
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = target.auxiliary.get()->Cast<VectorListBuffer>();
	target_buffer.PushBack(insert);
}

idx_t ListVector::GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count) {

	auto info = ListVector::GetConsecutiveChildListInfo(list, offset, count);
	if (info.needs_slicing) {
		SelectionVector sel(info.child_list_info.length);
		ListVector::GetConsecutiveChildSelVector(list, sel, offset, count);

		result.Slice(sel, info.child_list_info.length);
		result.Flatten(info.child_list_info.length);
	}
	return info.child_list_info.length;
}

ConsecutiveChildListInfo ListVector::GetConsecutiveChildListInfo(Vector &list, idx_t offset, idx_t count) {

	ConsecutiveChildListInfo info;
	UnifiedVectorFormat unified_list_data;
	list.ToUnifiedFormat(offset + count, unified_list_data);
	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(unified_list_data);

	// find the first non-NULL entry
	idx_t first_length = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		info.child_list_info.offset = list_data[idx].offset;
		first_length = list_data[idx].length;
		break;
	}

	// small performance improvement for constant vectors
	// avoids iterating over all their (constant) elements
	if (list.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		info.child_list_info.length = first_length;
		return info;
	}

	// now get the child count and determine whether the children are stored consecutively
	// also determine if a flat vector has pseudo constant values (all offsets + length the same)
	// this can happen e.g. for UNNESTs
	bool is_consecutive = true;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		if (list_data[idx].offset != info.child_list_info.offset || list_data[idx].length != first_length) {
			info.is_constant = false;
		}
		if (list_data[idx].offset != info.child_list_info.offset + info.child_list_info.length) {
			is_consecutive = false;
		}
		info.child_list_info.length += list_data[idx].length;
	}

	if (info.is_constant) {
		info.child_list_info.length = first_length;
	}
	if (!info.is_constant && !is_consecutive) {
		info.needs_slicing = true;
	}

	return info;
}

void ListVector::GetConsecutiveChildSelVector(Vector &list, SelectionVector &sel, idx_t offset, idx_t count) {
	UnifiedVectorFormat unified_list_data;
	list.ToUnifiedFormat(offset + count, unified_list_data);
	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(unified_list_data);

	//	SelectionVector child_sel(info.second.length);
	idx_t entry = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		for (idx_t k = 0; k < list_data[idx].length; k++) {
			//			child_sel.set_index(entry++, list_data[idx].offset + k);
			sel.set_index(entry++, list_data[idx].offset + k);
		}
	}
	//
	//	result.Slice(child_sel, info.second.length);
	//	result.Flatten(info.second.length);
	//	info.second.offset = 0;
}

//===--------------------------------------------------------------------===//
// UnionVector
//===--------------------------------------------------------------------===//
const Vector &UnionVector::GetMember(const Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return *entries[member_index + 1]; // skip the "tag" entry
}

Vector &UnionVector::GetMember(Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return *entries[member_index + 1]; // skip the "tag" entry
}

const Vector &UnionVector::GetTags(const Vector &vector) {
	// the tag vector is always the first struct child.
	return *StructVector::GetEntries(vector)[0];
}

Vector &UnionVector::GetTags(Vector &vector) {
	// the tag vector is always the first struct child.
	return *StructVector::GetEntries(vector)[0];
}

void UnionVector::SetToMember(Vector &union_vector, union_tag_t tag, Vector &member_vector, idx_t count,
                              bool keep_tags_for_null) {
	D_ASSERT(union_vector.GetType().id() == LogicalTypeId::UNION);
	D_ASSERT(tag < UnionType::GetMemberCount(union_vector.GetType()));

	// Set the union member to the specified vector
	UnionVector::GetMember(union_vector, tag).Reference(member_vector);
	auto &tag_vector = UnionVector::GetTags(union_vector);

	if (member_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// if the member vector is constant, we can set the union to constant as well
		union_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<union_tag_t>(tag_vector)[0] = tag;
		if (keep_tags_for_null) {
			ConstantVector::SetNull(union_vector, false);
			ConstantVector::SetNull(tag_vector, false);
		} else {
			ConstantVector::SetNull(union_vector, ConstantVector::IsNull(member_vector));
			ConstantVector::SetNull(tag_vector, ConstantVector::IsNull(member_vector));
		}

	} else {
		// otherwise flatten and set to flatvector
		member_vector.Flatten(count);
		union_vector.SetVectorType(VectorType::FLAT_VECTOR);

		if (member_vector.validity.AllValid()) {
			// if the member vector is all valid, we can set the tag to constant
			tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto tag_data = ConstantVector::GetData<union_tag_t>(tag_vector);
			*tag_data = tag;
		} else {
			tag_vector.SetVectorType(VectorType::FLAT_VECTOR);
			if (keep_tags_for_null) {
				FlatVector::Validity(tag_vector).SetAllValid(count);
				FlatVector::Validity(union_vector).SetAllValid(count);
			} else {
				// ensure the tags have the same validity as the member
				FlatVector::Validity(union_vector) = FlatVector::Validity(member_vector);
				FlatVector::Validity(tag_vector) = FlatVector::Validity(member_vector);
			}

			auto tag_data = FlatVector::GetData<union_tag_t>(tag_vector);
			memset(tag_data, tag, count);
		}
	}

	// Set the non-selected members to constant null vectors
	for (idx_t i = 0; i < UnionType::GetMemberCount(union_vector.GetType()); i++) {
		if (i != tag) {
			auto &member = UnionVector::GetMember(union_vector, i);
			member.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(member, true);
		}
	}
}

bool UnionVector::TryGetTag(const Vector &vector, idx_t index, union_tag_t &result) {
	// the tag vector is always the first struct child.
	auto &tag_vector = *StructVector::GetEntries(vector)[0];
	if (tag_vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(tag_vector);
		auto &dict_sel = DictionaryVector::SelVector(tag_vector);
		auto mapped_idx = dict_sel.get_index(index);
		if (FlatVector::IsNull(child, mapped_idx)) {
			return false;
		} else {
			result = FlatVector::GetData<union_tag_t>(child)[mapped_idx];
			return true;
		}
	}
	if (tag_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(tag_vector)) {
			return false;
		} else {
			result = ConstantVector::GetData<union_tag_t>(tag_vector)[0];
			return true;
		}
	}
	if (FlatVector::IsNull(tag_vector, index)) {
		return false;
	} else {
		result = FlatVector::GetData<union_tag_t>(tag_vector)[index];
		return true;
	}
}

//! Raw selection vector passed in (not merged with any other selection vectors)
UnionInvalidReason UnionVector::CheckUnionValidity(Vector &vector_p, idx_t count, const SelectionVector &sel_p) {
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::UNION);

	// Will contain the (possibly) merged selection vector
	const SelectionVector *sel = &sel_p;
	SelectionVector owned_sel;
	Vector *vector = &vector_p;
	if (vector->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// In the case of a dictionary vector, unwrap the Vector, and merge the selection vectors.
		auto &child = DictionaryVector::Child(*vector);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*vector);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, count);
		owned_sel.Initialize(new_buffer);
		sel = &owned_sel;
		vector = &child;
	} else if (vector->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		sel = ConstantVector::ZeroSelectionVector(count, owned_sel);
	}

	auto member_count = UnionType::GetMemberCount(vector_p.GetType());
	if (member_count == 0) {
		return UnionInvalidReason::NO_MEMBERS;
	}

	UnifiedVectorFormat vector_vdata;
	vector_p.ToUnifiedFormat(count, vector_vdata);

	auto &entries = StructVector::GetEntries(vector_p);
	duckdb::vector<UnifiedVectorFormat> child_vdata(entries.size());
	for (idx_t entry_idx = 0; entry_idx < entries.size(); entry_idx++) {
		auto &child = *entries[entry_idx];
		child.ToUnifiedFormat(count, child_vdata[entry_idx]);
	}

	auto &tag_vdata = child_vdata[0];

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto mapped_idx = sel->get_index(row_idx);

		if (!vector_vdata.validity.RowIsValid(mapped_idx)) {
			continue;
		}

		auto tag_idx = tag_vdata.sel->get_index(sel_p.get_index(row_idx));
		if (!tag_vdata.validity.RowIsValid(tag_idx)) {
			// we can't have NULL tags!
			return UnionInvalidReason::NULL_TAG;
		}
		auto tag = UnifiedVectorFormat::GetData<union_tag_t>(tag_vdata)[tag_idx];
		if (tag >= member_count) {
			return UnionInvalidReason::TAG_OUT_OF_RANGE;
		}

		bool found_valid = false;
		for (idx_t i = 0; i < member_count; i++) {
			auto &member_vdata = child_vdata[1 + i]; // skip the tag
			idx_t member_idx = member_vdata.sel->get_index(sel_p.get_index(row_idx));
			if (!member_vdata.validity.RowIsValid(member_idx)) {
				continue;
			}
			if (found_valid) {
				return UnionInvalidReason::VALIDITY_OVERLAP;
			}
			found_valid = true;
			if (tag != static_cast<union_tag_t>(i)) {
				return UnionInvalidReason::TAG_MISMATCH;
			}
		}
	}

	return UnionInvalidReason::VALID;
}

//===--------------------------------------------------------------------===//
// ArrayVector
//===--------------------------------------------------------------------===//
template <class T>
T &ArrayVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::ARRAY_BUFFER);
	return vector.auxiliary->template Cast<VectorArrayBuffer>().GetChild();
}

const Vector &ArrayVector::GetEntry(const Vector &vector) {
	return GetEntryInternal<const Vector>(vector);
}

Vector &ArrayVector::GetEntry(Vector &vector) {
	return GetEntryInternal<Vector>(vector);
}

idx_t ArrayVector::GetTotalSize(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	D_ASSERT(vector.auxiliary);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetTotalSize(child);
	}
	return vector.auxiliary->Cast<VectorArrayBuffer>().GetChildSize();
}

} // namespace duckdb
