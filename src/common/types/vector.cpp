#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/sequence_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/union_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/fsst.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

enum class VectorConstructorAction { REFERENCE_VECTOR };

Vector::Vector(LogicalType type_p, VectorType vector_type, buffer_ptr<VectorBuffer> buffer_p)
    : type(std::move(type_p)), buffer(std::move(buffer_p)) {
}

Vector::Vector(LogicalType type_p, idx_t capacity, VectorDataInitialization initialize) : type(std::move(type_p)) {
	Initialize(initialize, capacity);
}

Vector::Vector(LogicalType type_p, data_ptr_t dataptr) : type(std::move(type_p)) {
	if (!dataptr) {
		return;
	}
	if (!type.IsValid()) {
		throw InternalException("Cannot create a vector of type INVALID!");
	}
	if (type.IsNested()) {
		throw InternalException("Cannot create a nested vector from a single data pointer");
	}
	if (type.InternalType() == PhysicalType::VARCHAR) {
		buffer = make_buffer<VectorStringBuffer>(dataptr);
	} else {
		buffer = make_buffer<StandardVectorBuffer>(dataptr);
	}
}

Vector::Vector(const VectorCache &cache) : type(cache.GetType()) {
	ResetFromCache(cache);
}

Vector::Vector(const Vector &other, VectorConstructorAction) : type(other.type) {
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

Vector::Vector(Vector &&other) noexcept : type(std::move(other.type)), buffer(std::move(other.buffer)) {
}

Vector Vector::Ref(const Vector &other) {
	return Vector(other, VectorConstructorAction::REFERENCE_VECTOR);
}

void Vector::Reference(const Value &value) {
	D_ASSERT(GetType().id() == value.type().id());
	auto internal_type = value.type().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_buffer<VectorStructBuffer>();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_vectors = struct_buffer->GetChildren();
		for (idx_t i = 0; i < child_types.size(); i++) {
			child_vectors.emplace_back(value.IsNull() ? Value(child_types[i].second)
			                                          : StructValue::GetChildren(value)[i]);
		}
		buffer = std::move(struct_buffer);
		if (value.IsNull()) {
			SetValue(0, value);
		}
	} else if (internal_type == PhysicalType::LIST) {
		buffer = VectorBuffer::CreateConstantVector(value.type());
		SetValue(0, value);
	} else if (internal_type == PhysicalType::ARRAY) {
		buffer = make_buffer<VectorArrayBuffer>(value.type());
		SetValue(0, value);
	} else {
		buffer = VectorBuffer::CreateConstantVector(value.type());
		SetValue(0, value);
	}
	SetVectorType(VectorType::CONSTANT_VECTOR);
}

void Vector::Reference(const Vector &other) {
	if (other.GetType().id() != GetType().id()) {
		throw InternalException("Vector::Reference used on vector of different type (source %s referenced %s)",
		                        GetType(), other.GetType());
	}
	D_ASSERT(other.GetType() == GetType());
	ConstReference(other);
}

void Vector::ReferenceAndSetType(const Vector &other) {
	type = other.GetType();
	Reference(other);
}

void Vector::Reinterpret(const Vector &other) {
	auto &this_type = GetType();
	auto &other_type = other.GetType();
#ifdef DEBUG
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
	ConstReference(other);
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR && other_type != this_type) {
		Vector new_vector(this_type, nullptr);
		new_vector.Reinterpret(DictionaryVector::Child(other));
		auto &old_dict = buffer->Cast<DictionaryBuffer>();
		auto new_entry = make_shared_ptr<DictionaryEntry>(std::move(new_vector));
		buffer = make_buffer<DictionaryBuffer>(old_dict.GetSelVector(), std::move(new_entry));
		auto dict_size = old_dict.GetDictionarySize();
		if (dict_size.IsValid()) {
			buffer->Cast<DictionaryBuffer>().SetDictionarySize(dict_size.GetIndex());
		}
		buffer->Cast<DictionaryBuffer>().SetDictionaryId(old_dict.GetDictionaryId());
	}
}

void Vector::ConstReference(const Vector &other) const {
	AssignSharedPointer(buffer, other.buffer);
}

void Vector::ResetFromCache(const VectorCache &cache) {
	cache.ResetFromCache(*this);
}

void Vector::Slice(const Vector &other, idx_t offset, idx_t end) {
	D_ASSERT(end >= offset);
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR || offset == 0) {
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
	// Keep a reference to the old buffer in case this == &other (self-slice).
	// Without this, replacing 'buffer' (which IS other.buffer when this == &other) before
	// reading other.buffer->GetValidityMask() would lose the old validity.
	auto old_buffer = other.buffer;
	if (internal_type == PhysicalType::STRUCT) {
		Vector new_vector(GetType());
		auto &entries = StructVector::GetEntries(new_vector);
		auto &other_entries = StructVector::GetEntries(other);
		D_ASSERT(entries.size() == other_entries.size());
		for (idx_t i = 0; i < entries.size(); i++) {
			entries[i].Slice(other_entries[i], offset, end);
		}
		new_vector.buffer->GetValidityMask().Slice(old_buffer->GetValidityMask(), offset, end - offset);
		Reference(new_vector);
	} else if (internal_type == PhysicalType::ARRAY) {
		Vector new_vector(GetType());
		auto &child_vec = ArrayVector::GetEntry(new_vector);
		auto &other_child_vec = ArrayVector::GetEntry(other);
		D_ASSERT(ArrayType::GetSize(GetType()) == ArrayType::GetSize(other.GetType()));
		const auto array_size = ArrayType::GetSize(GetType());
		// We need to slice the child vector with the multiplied offset and end
		child_vec.Slice(other_child_vec, offset * array_size, end * array_size);
		new_vector.buffer->GetValidityMask().Slice(old_buffer->GetValidityMask(), offset, end - offset);
		Reference(new_vector);
	} else if (internal_type == PhysicalType::LIST) {
		auto offset_ptr = old_buffer->GetData() + GetTypeIdSize(internal_type) * offset;
		auto &parent = old_buffer->Cast<VectorListBuffer>();
		buffer = make_buffer<VectorListBuffer>(offset_ptr, parent);
		buffer->GetValidityMask().Slice(old_buffer->GetValidityMask(), offset, end - offset);
	} else if (internal_type == PhysicalType::VARCHAR) {
		auto offset_ptr = old_buffer->GetData() + GetTypeIdSize(internal_type) * offset;
		auto string_buffer = make_buffer<VectorStringBuffer>(offset_ptr);
		buffer = std::move(string_buffer);
		StringVector::AddHeapReference(*this, other);
		buffer->GetValidityMask().Slice(old_buffer->GetValidityMask(), offset, end - offset);
	} else {
		auto offset_ptr = old_buffer->GetData() + GetTypeIdSize(internal_type) * offset;
		buffer = make_buffer<StandardVectorBuffer>(offset_ptr);
		buffer->GetValidityMask().Slice(old_buffer->GetValidityMask(), offset, end - offset);
	}
}

void Vector::Slice(const Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (!sel.IsSet() || count == 0) {
		return; // Nothing to do here
	}
	if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	auto internal_type = GetType().InternalType();
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		if (internal_type == PhysicalType::STRUCT) {
			throw InternalException("Struct vectors cannot be dictionary vectors");
		}
		// already a dictionary, slice the current dictionary
		auto &old_dict = buffer->Cast<DictionaryBuffer>();
		auto dictionary_size = DictionaryVector::DictionarySize(*this);
		auto dictionary_id = DictionaryVector::DictionaryId(*this);
		auto sliced_dictionary = old_dict.GetSelVector().Slice(sel, count);
		auto entry = old_dict.GetEntryPtr();
		buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary), std::move(entry));
		if (dictionary_size.IsValid()) {
			auto &dict_buffer = buffer->Cast<DictionaryBuffer>();
			dict_buffer.SetDictionarySize(dictionary_size.GetIndex());
			dict_buffer.SetDictionaryId(std::move(dictionary_id));
		}
		return;
	}

	if (GetVectorType() == VectorType::FSST_VECTOR || GetVectorType() == VectorType::SHREDDED_VECTOR) {
		Flatten(sel, count);
		return;
	}
	if (internal_type == PhysicalType::STRUCT) {
		// structs should not be sliced themselves - only their children are sliced
		buffer = make_buffer<VectorStructBuffer>(*this, sel, count);
		return;
	}

	// move this vector as a child vector in the dictionary
	Vector child_vector(Vector::Ref(*this));
	auto entry = make_shared_ptr<DictionaryEntry>(std::move(child_vector));
	buffer = make_buffer<DictionaryBuffer>(sel, std::move(entry));
}

void Vector::Dictionary(idx_t dictionary_size, const SelectionVector &sel, idx_t count) {
	Slice(sel, count);
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		buffer->Cast<DictionaryBuffer>().SetDictionarySize(dictionary_size);
	}
}

void Vector::Dictionary(Vector &dict, idx_t dictionary_size, const SelectionVector &sel, idx_t count) {
	Reference(dict);
	Dictionary(dictionary_size, sel, count);
}

void Vector::Dictionary(buffer_ptr<DictionaryEntry> reusable_dict, const SelectionVector &sel) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		throw InternalException("Struct vectors cannot be dictionaries");
	}
	D_ASSERT(type == reusable_dict->data.GetType());
	buffer = make_buffer<DictionaryBuffer>(sel, std::move(reusable_dict));
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto dictionary_size = DictionaryVector::DictionarySize(*this);
		auto dictionary_id = DictionaryVector::DictionaryId(*this);
		auto target_data = current_sel.data();
		auto cache_entry = cache.cache.find(target_data);
		if (cache_entry != cache.cache.end()) {
			// cached entry exists: use the cached selection vector with our dictionary entry
			auto &old_dict = this->buffer->Cast<DictionaryBuffer>();
			auto dict_entry = old_dict.GetEntryPtr();
			this->buffer = make_buffer<DictionaryBuffer>(cache_entry->second->Cast<DictionaryBuffer>().GetSelVector(),
			                                             std::move(dict_entry));
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

void Vector::Initialize(VectorDataInitialization data_initialize, idx_t capacity) {
	auto &type = GetType();
	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		buffer = make_buffer<VectorStructBuffer>(type, capacity);
	} else if (internal_type == PhysicalType::LIST) {
		buffer = make_buffer<VectorListBuffer>(capacity, type);
		if (data_initialize == VectorDataInitialization::ZERO_INITIALIZE) {
			auto data = buffer->GetData();
			memset(data, 0, capacity * sizeof(list_entry_t));
		}
	} else if (internal_type == PhysicalType::ARRAY) {
		buffer = make_buffer<VectorArrayBuffer>(type, capacity);
	} else {
		auto type_size = GetTypeIdSize(internal_type);
		if (type_size == 0) {
			throw InternalException("Trying to create buffer for zero-length type");
		}
		buffer = VectorBuffer::CreateStandardVector(type, capacity);
		if (data_initialize == VectorDataInitialization::ZERO_INITIALIZE) {
			auto data = buffer->GetData();
			memset(data, 0, capacity * type_size);
		}
	}
}

void Vector::FindResizeInfos(vector<ResizeInfo> &resize_infos, const idx_t multiplier) {
	const auto type_size = GetTypeIdSize(type.InternalType());
	auto buffer_ptr = type_size ? buffer.get() : nullptr;
	ResizeInfo resize_info(*this, buffer_ptr, multiplier);
	resize_infos.emplace_back(resize_info);

	if (!buffer) {
		return;
	}

	switch (buffer->GetBufferType()) {
	case VectorBufferType::ARRAY_BUFFER: {
		// We need to multiply the multiplier by the array size because
		// the child vectors of ARRAY types are always child_count * array_size.
		auto &vector_array_buffer = buffer->Cast<VectorArrayBuffer>();
		auto new_multiplier = vector_array_buffer.GetArraySize() * multiplier;
		auto &child = vector_array_buffer.GetChild();
		child.FindResizeInfos(resize_infos, new_multiplier);
		break;
	}
	case VectorBufferType::STRUCT_BUFFER: {
		auto &vector_struct_buffer = buffer->Cast<VectorStructBuffer>();
		auto &children = vector_struct_buffer.GetChildren();
		for (auto &child : children) {
			child.FindResizeInfos(resize_infos, multiplier);
		}
		break;
	}
	default:
		break;
	}
}

void Vector::AddAuxiliaryData(unique_ptr<AuxiliaryDataHolder> data) {
	buffer->AddAuxiliaryData(std::move(data));
}

void Vector::AddHeapReference(const Vector &other) {
	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		AddHeapReference(DictionaryVector::Child(other));
		return;
	}
	auto &auxiliary_data = other.buffer->GetAuxiliaryData();
	if (!auxiliary_data) {
		return;
	}
	AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(auxiliary_data));
}

void Vector::Resize(idx_t current_size, idx_t new_size) {
	// The vector does not contain any data.
	if (!buffer) {
		auto internal_type = GetType().InternalType();
		if (internal_type == PhysicalType::LIST) {
			throw InternalException("Resize for empty list not supported");
		}
		if (internal_type == PhysicalType::VARCHAR) {
			buffer = make_buffer<VectorStringBuffer>(idx_t(0));
		} else {
			buffer = make_buffer<StandardVectorBuffer>(0, GetTypeIdSize(internal_type));
		}
	}

	// Obtain the resize information for each (nested) vector.
	vector<ResizeInfo> resize_infos;
	FindResizeInfos(resize_infos, 1);

	for (auto &resize_info_entry : resize_infos) {
		// Resize the validity mask.
		auto new_validity_size = new_size * resize_info_entry.multiplier;
		resize_info_entry.vec.buffer->GetValidityMask().Resize(new_validity_size);

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
		auto stored_allocator = resize_info_entry.buffer->GetAllocator();
		auto &allocator = stored_allocator ? *stored_allocator : Allocator::DefaultAllocator();
		auto new_data = allocator.Allocate(target_size);
		memcpy(new_data.get(), resize_info_entry.data, old_size);
		// Save the resized validity mask before replacing the buffer.
		auto resized_validity = std::move(resize_info_entry.vec.buffer->GetValidityMask());
		buffer_ptr<VectorBuffer> new_buffer;
		if (resize_info_entry.vec.GetType().InternalType() == PhysicalType::LIST) {
			auto &old_buffer = resize_info_entry.vec.buffer->Cast<VectorListBuffer>();
			new_buffer = make_buffer<VectorListBuffer>(std::move(new_data), old_buffer);
		} else if (resize_info_entry.vec.GetType().InternalType() == PhysicalType::VARCHAR) {
			auto &old_buffer = resize_info_entry.vec.buffer->Cast<VectorStringBuffer>();
			new_buffer = make_buffer<VectorStringBuffer>(std::move(new_data), old_buffer);
		} else {
			new_buffer = make_buffer<StandardVectorBuffer>(std::move(new_data));
		}
		// Restore the resized validity mask into the new buffer.
		new_buffer->GetValidityMask() = std::move(resized_validity);
		resize_info_entry.buffer = new_buffer.get();
		resize_info_entry.vec.buffer = std::move(new_buffer);
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

	buffer->GetValidityMask().Set(index, !val.IsNull());
	auto physical_type = GetType().InternalType();
	if (val.IsNull() && !IsStructOrArrayRecursive(GetType())) {
		// for structs and arrays we still need to set the child-entries to NULL
		// so we do not bail out yet
		return;
	}
	switch (physical_type) {
	case PhysicalType::BOOL:
		FlatVector::GetDataMutable<bool>(*this)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		FlatVector::GetDataMutable<int8_t>(*this)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		FlatVector::GetDataMutable<int16_t>(*this)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		FlatVector::GetDataMutable<int32_t>(*this)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		FlatVector::GetDataMutable<int64_t>(*this)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		FlatVector::GetDataMutable<hugeint_t>(*this)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		FlatVector::GetDataMutable<uint8_t>(*this)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		FlatVector::GetDataMutable<uint16_t>(*this)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		FlatVector::GetDataMutable<uint32_t>(*this)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		FlatVector::GetDataMutable<uint64_t>(*this)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::UINT128:
		FlatVector::GetDataMutable<uhugeint_t>(*this)[index] = val.GetValueUnsafe<uhugeint_t>();
		break;
	case PhysicalType::FLOAT:
		FlatVector::GetDataMutable<float>(*this)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		FlatVector::GetDataMutable<double>(*this)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		FlatVector::GetDataMutable<interval_t>(*this)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR: {
		if (!val.IsNull()) {
			FlatVector::GetDataMutable<string_t>(*this)[index] =
			    StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		}
		break;
	}
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		if (val.IsNull()) {
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				vec_child.SetValue(index, Value());
			}
		} else {
			auto &val_children = StructValue::GetChildren(val);
			D_ASSERT(children.size() == val_children.size());
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				auto &struct_child = val_children[i];
				vec_child.SetValue(index, struct_child);
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		if (val.IsNull()) {
			auto &entry = FlatVector::GetDataMutable<list_entry_t>(*this)[index];
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
			auto &entry = FlatVector::GetDataMutable<list_entry_t>(*this)[index];
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
	const_reference<Vector> current_vector_ref(v_p);
	idx_t index = index_p;
	bool finished = false;
	while (!finished) {
		auto &current_vector = current_vector_ref.get();
		switch (current_vector.GetVectorType()) {
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
			auto &sel_vector = DictionaryVector::SelVector(current_vector);
			auto &child = DictionaryVector::Child(current_vector);
			current_vector_ref = child;
			index = sel_vector.get_index(index);
			break;
		}
		case VectorType::SHREDDED_VECTOR: {
			// FIXME: this is extremely inefficient
			Vector copy(LogicalType::VARIANT());
			SelectionVector sel(1);
			sel.set_index(0, index);
			auto &shredded = ShreddedVector::GetShreddedVector(current_vector);
			auto &unshredded = ShreddedVector::GetUnshreddedVector(current_vector);

			Vector sliced_shredded(shredded, sel, 1);
			Vector sliced_unshredded(unshredded, sel, 1);
			sliced_shredded.Flatten(1);
			sliced_unshredded.Flatten(1);

			child_list_t<LogicalType> shredded_subtypes;
			shredded_subtypes.push_back(make_pair("unshredded", unshredded.GetType()));
			shredded_subtypes.push_back(make_pair("shredded", shredded.GetType()));
			Vector new_shredded(LogicalType::STRUCT(std::move(shredded_subtypes)));
			StructVector::GetEntries(new_shredded)[0].Reference(sliced_unshredded);
			StructVector::GetEntries(new_shredded)[1].Reference(sliced_shredded);

			copy.Shred(new_shredded);
			copy.Flatten(1);
			return copy.GetValue(0);
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			SequenceVector::GetSequence(current_vector, start, increment);
			return Value::Numeric(current_vector.GetType(),
			                      start + static_cast<int64_t>(static_cast<uint64_t>(increment) * index));
		}
		default:
			throw InternalException("Unimplemented vector type for Vector::GetValue");
		}
	}
	auto &vector = current_vector_ref.get();
	auto &validity = vector.buffer->GetValidityMask();
	auto &type = vector.GetType();
	if (!validity.RowIsValid(index)) {
		return Value(vector.GetType());
	}

	if (vector.GetVectorType() == VectorType::FSST_VECTOR) {
		if (vector.GetType().InternalType() != PhysicalType::VARCHAR) {
			throw InternalException("FSST Vector with non-string datatype found!");
		}
		auto str_compressed = FSSTVector::GetCompressedData(vector)[index];
		auto decoder = FSSTVector::GetDecoder(vector);
		auto &decompress_buffer = FSSTVector::GetDecompressBuffer(vector);
		auto string_val = FSSTPrimitives::DecompressValue(decoder, str_compressed.GetData(), str_compressed.GetSize(),
		                                                  decompress_buffer);
		switch (type.id()) {
		case LogicalTypeId::VARCHAR:
			return Value(std::move(string_val));
		case LogicalTypeId::BLOB:
			return Value::BLOB_RAW(string_val);
		default:
			throw InternalException("Unsupported vector type for FSST vector");
		}
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(FlatVector::GetData<bool>(vector)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(FlatVector::GetData<int8_t>(vector)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(FlatVector::GetData<int16_t>(vector)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(FlatVector::GetData<int32_t>(vector)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(FlatVector::GetData<date_t>(vector)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(FlatVector::GetData<dtime_t>(vector)[index]);
	case LogicalTypeId::TIME_NS:
		return Value::TIME_NS(FlatVector::GetData<dtime_ns_t>(vector)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(FlatVector::GetData<dtime_tz_t>(vector)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(FlatVector::GetData<int64_t>(vector)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(FlatVector::GetData<uint8_t>(vector)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(FlatVector::GetData<uint16_t>(vector)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(FlatVector::GetData<uint32_t>(vector)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(FlatVector::GetData<uint64_t>(vector)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(FlatVector::GetData<timestamp_t>(vector)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(FlatVector::GetData<timestamp_ns_t>(vector)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(FlatVector::GetData<timestamp_ms_t>(vector)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(FlatVector::GetData<timestamp_sec_t>(vector)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(FlatVector::GetData<timestamp_tz_t>(vector)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(FlatVector::GetData<hugeint_t>(vector)[index]);
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(FlatVector::GetData<uhugeint_t>(vector)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(FlatVector::GetData<hugeint_t>(vector)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(FlatVector::GetData<int16_t>(vector)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(FlatVector::GetData<int32_t>(vector)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(FlatVector::GetData<int64_t>(vector)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(FlatVector::GetData<hugeint_t>(vector)[index], width, scale);
		default:
			throw InternalException("Physical type '%s' has a width bigger than 38, which is not supported",
			                        TypeIdToString(type.InternalType()));
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(FlatVector::GetData<uint8_t>(vector)[index], type);
		case PhysicalType::UINT16:
			return Value::ENUM(FlatVector::GetData<uint16_t>(vector)[index], type);
		case PhysicalType::UINT32:
			return Value::ENUM(FlatVector::GetData<uint32_t>(vector)[index], type);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
	}
	case LogicalTypeId::POINTER:
		return Value::POINTER(FlatVector::GetData<uintptr_t>(vector)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(FlatVector::GetData<float>(vector)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(FlatVector::GetData<double>(vector)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(FlatVector::GetData<interval_t>(vector)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = FlatVector::GetData<string_t>(vector)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::BLOB: {
		auto str = FlatVector::GetData<string_t>(vector)[index];
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIGNUM: {
		auto str = FlatVector::GetData<bignum_t>(vector)[index];
		return Value::BIGNUM(const_data_ptr_cast(str.data.GetData()), str.data.GetSize());
	}
	case LogicalTypeId::GEOMETRY: {
		auto str = FlatVector::GetData<string_t>(vector)[index];
		if (GeoType::HasCRS(type)) {
			return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize(), GeoType::GetCRS(type));
		}
		return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::LEGACY_AGGREGATE_STATE: {
		auto str = FlatVector::GetData<string_t>(vector)[index];
		return Value::LEGACY_AGGREGATE_STATE(type, const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIT: {
		auto str = FlatVector::GetData<string_t>(vector)[index];
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::SQLNULL: {
		return Value();
	}
	case LogicalTypeId::MAP: {
		auto offlen = FlatVector::GetData<list_entry_t>(vector)[index];
		auto &child_vec = ListVector::GetEntry(vector);
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
		if (UnionVector::TryGetTag(vector, index_p, tag)) {
			auto value = UnionVector::GetMember(vector, tag).GetValue(index_p);
			auto members = UnionType::CopyMemberTypes(type);
			return Value::UNION(members, tag, std::move(value));
		} else {
			return Value(type);
		}
	}
	case LogicalTypeId::VARIANT: {
		duckdb::vector<Value> children;
		children.emplace_back(VariantVector::GetKeys(vector).GetValue(index_p));
		children.emplace_back(VariantVector::GetChildren(vector).GetValue(index_p));
		children.emplace_back(VariantVector::GetValues(vector).GetValue(index_p));
		children.emplace_back(VariantVector::GetData(vector).GetValue(index_p));
		return Value::VARIANT(children);
	}
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::STRUCT: {
		// we can derive the value schema from the vector schema
		auto &child_entries = StructVector::GetEntries(vector);
		duckdb::vector<Value> children;
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			auto &struct_child = child_entries[child_idx];
			children.push_back(struct_child.GetValue(index_p));
		}

		if (type.id() == LogicalTypeId::AGGREGATE_STATE) {
			// We could also just call `Value::STRUCT` as it has the same implementation, but this is implementation
			// details, so for consistency and bullet-proof implementation we keep those constructors separate.
			return Value::AGGREGATE_STATE(type, std::move(children));
		}
		return Value::STRUCT(type, std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto offlen = FlatVector::GetData<list_entry_t>(vector)[index];
		auto &child_vec = ListVector::GetEntry(vector);
		duckdb::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::LIST(ListType::GetChildType(type), std::move(children));
	}
	case LogicalTypeId::ARRAY: {
		auto stride = ArrayType::GetSize(type);
		auto offset = index * stride;
		auto &child_vec = ArrayVector::GetEntry(vector);
		duckdb::vector<Value> children;
		for (idx_t i = offset; i < offset + stride; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::ARRAY(ArrayType::GetChildType(type), std::move(children));
	}
	case LogicalTypeId::TYPE: {
		auto blob = FlatVector::GetData<string_t>(vector)[index];
		return Value::TYPE(blob);
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
	if (v_p.GetType().id() != LogicalTypeId::LEGACY_AGGREGATE_STATE &&
	    value.type().id() != LogicalTypeId::LEGACY_AGGREGATE_STATE) {
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
	case VectorType::SHREDDED_VECTOR:
		return "SHREDDED";
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
		auto compressed_data = FSSTVector::GetCompressedData(*this);
		for (idx_t i = 0; i < count; i++) {
			string_t compressed_string = compressed_data[i];
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
	case VectorType::SHREDDED_VECTOR: {
		auto &shredded_vector = ShreddedVector::GetShreddedVector(*this);
		auto &unshredded_vector = ShreddedVector::GetUnshreddedVector(*this);
		retval += "Shredded: " + shredded_vector.ToString(count);
		retval += ", Unshredded: " + unshredded_vector.ToString(count);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + static_cast<int64_t>(static_cast<uint64_t>(increment) * i)) +
			          (i == count - 1 ? "" : ", ");
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

idx_t Vector::GetAllocationSize(idx_t) const {
	return GetAllocationSize();
}

idx_t Vector::GetAllocationSize() const {
	if (!buffer) {
		return 0;
	}
	return buffer->GetAllocationSize();
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

void Vector::Flatten(idx_t count) const {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		switch (GetType().InternalType()) {
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			for (auto &entry : entries) {
				entry.Flatten(count);
			}
			break;
		}
		case PhysicalType::LIST: {
			auto &entry = ListVector::GetEntry(*this);
			entry.Flatten(ListVector::GetListSize(*this));
			break;
		}
		case PhysicalType::ARRAY: {
			auto &entry = ArrayVector::GetEntry(*this);
			entry.Flatten(ArrayVector::GetTotalSize(*this));
			break;
		}
		default:
			break;
		}
		break;
	case VectorType::SHREDDED_VECTOR: {
		// unshred the vector
		ShreddedVector::Unshred(*this, count);
		Flatten(count);
		break;
	}
	case VectorType::FSST_VECTOR: {
		// Even though count may only be a part of the vector, we need to flatten the whole thing due to the way
		// ToUnifiedFormat uses flatten
		idx_t total_count = FSSTVector::GetCount(*this);
		// create vector to decompress into
		Vector other(GetType(), total_count);
		// now copy the data of this vector to the other vector, decompressing the strings in the process
		VectorOperations::Copy(*this, other, total_count, 0, 0);
		// create a reference to the data in the other vector
		ConstReference(other);
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType(), count);
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		ConstReference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		Vector flattened_vector(GetType(), MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		ConstantVector::Flatten(*this, flattened_vector, count);
		ConstReference(flattened_vector);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment, sequence_count;
		SequenceVector::GetSequence(*this, start, increment, sequence_count);
		auto seq_count = NumericCast<idx_t>(sequence_count);

		Vector flattened_vector(GetType(), MaxValue<idx_t>(STANDARD_VECTOR_SIZE, seq_count));
		VectorOperations::GenerateSequence(flattened_vector, seq_count, start, increment);
		ConstReference(flattened_vector);
		break;
	}
	default:
		throw InternalException("Unimplemented type for flatten");
	}
}

void Vector::Flatten(const SelectionVector &sel, idx_t count) const {
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
		ConstReference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		Vector flattened_vector(GetType(), MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		ConstantVector::Flatten(*this, flattened_vector, count);
		ConstReference(flattened_vector);
		break;
	}
	case VectorType::SHREDDED_VECTOR: {
		// unshred the shredded vector
		ShreddedVector::Unshred(*this, sel, count);
		Flatten(sel, count);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		Vector flattened_vector(GetType(), MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		VectorOperations::GenerateSequence(flattened_vector, count, sel, start, increment);
		ConstReference(flattened_vector);
		break;
	}
	default:
		throw InternalException("Unimplemented type for flatten with selection vector");
	}
}

void Vector::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	format.physical_type = GetType().InternalType();
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
			Vector child_vector(Vector::Ref(child));
			child_vector.Flatten(sel, count);
			auto new_entry = make_buffer<DictionaryEntry>(std::move(child_vector));
			auto &dict_entry = *new_entry;
			auto &old_dict_buffer = this->buffer->Cast<DictionaryBuffer>();
			auto new_dict_buffer = make_buffer<DictionaryBuffer>(old_dict_buffer.GetSelVector(), std::move(new_entry));
			this->buffer = std::move(new_dict_buffer);

			format.data = FlatVector::GetData(dict_entry.data);
			format.validity = FlatVector::Validity(dict_entry.data);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
		format.data = ConstantVector::GetData(*this);
		format.validity = ConstantVector::Validity(*this);
		break;
	case VectorType::SHREDDED_VECTOR:
		// unshred and call ToUnifiedFormat recursively
		ShreddedVector::Unshred(*this, count);
		ToUnifiedFormat(count, format);
		break;
	default:
		Flatten(count);
		format.sel = FlatVector::IncrementalSelectionVector();
		format.data = FlatVector::GetData(*this);
		format.validity = FlatVector::Validity(*this);
		break;
	}
}

void Vector::RecursiveToUnifiedFormat(const Vector &input, idx_t count, RecursiveUnifiedVectorFormat &data) {
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
			Vector::RecursiveToUnifiedFormat(children[i], count, data.children[i]);
		}
	}
}

void Vector::Sequence(int64_t start, int64_t increment, idx_t count) {
	this->buffer = make_buffer<SequenceBuffer>(start, increment, static_cast<int64_t>(count));
}

void Vector::Shred(Vector &shredded_data) {
	if (GetType().id() != LogicalTypeId::VARIANT) {
		throw InternalException("Vector::Shred can only be used on variant vectors");
	}
	auto &shredded_type = shredded_data.GetType();
	if (shredded_type.id() != LogicalTypeId::STRUCT || StructType::GetChildCount(shredded_type) != 2) {
		throw InternalException("Vector::Shred parameter must be a struct with two children");
	}
	this->buffer = make_buffer<ShreddedVectorBuffer>(shredded_data);
}

// FIXME: This should ideally be const
void Vector::Serialize(Serializer &serializer, idx_t count, bool compressed_serialization) {
	auto &logical_type = GetType();

	UnifiedVectorFormat vdata;

	// serialize compressed vectors to save space, but skip this if serializing into older versions
	if (!serializer.ShouldSerialize(5)) {
		compressed_serialization = false;
	}
	if (compressed_serialization) {
		auto vtype = GetVectorType();
		if (vtype == VectorType::DICTIONARY_VECTOR && DictionaryVector::DictionarySize(*this).IsValid()) {
			auto dict = Vector::Ref(DictionaryVector::Child(*this));
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
					serializer.WriteProperty(90, "vector_type", VectorType::DICTIONARY_VECTOR);
					serializer.WriteProperty(91, "sel_vector", sel_data, sizeof(sel_t) * count);
					serializer.WriteProperty(92, "dict_count", used_count);
					return dict.Serialize(serializer, used_count, false);
				}
			}
		} else if (vtype == VectorType::CONSTANT_VECTOR && count >= 1) {
			serializer.WriteProperty(90, "vector_type", VectorType::CONSTANT_VECTOR);
			return Vector::Serialize(serializer, 1, false); // just serialize one value
		} else if (vtype == VectorType::SEQUENCE_VECTOR) {
			serializer.WriteProperty(90, "vector_type", VectorType::SEQUENCE_VECTOR);
			auto &sequence = buffer->Cast<SequenceBuffer>();
			serializer.WriteProperty(91, "seq_start", sequence.start);
			serializer.WriteProperty(92, "seq_increment", sequence.increment);
			return; // for sequence vectors we do not serialize anything else
		} else {
			// TODO: other compressed vector types (SHREDDED, FSST)
		}
	}
	ToUnifiedFormat(count, vdata);

	if (logical_type.id() == LogicalTypeId::GEOMETRY && serializer.ShouldSerialize(Geometry::VERSION_ADDED)) {
		serializer.WriteProperty<GeometryStorageType>(99, "geometry_format", GeometryStorageType::WKB);
	}

	const bool has_validity_mask = (count > 0) && vdata.validity.CanHaveNull();
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
	} else if (logical_type.id() == LogicalTypeId::GEOMETRY) {
		auto geoms = UnifiedVectorFormat::GetData<string_t>(vdata);

		// Are we targeting an older serialization version?
		if (!serializer.ShouldSerialize(7)) {
			// Serialize data as old-style SPATIAL format
			string blob;
			serializer.WriteList(102, "data", count, [&](Serializer::List &list, idx_t i) {
				auto idx = vdata.sel->get_index(i);
				if (!vdata.validity.RowIsValid(idx)) {
					list.WriteElement(NullValue<string_t>());
				} else {
					Geometry::ToSpatialGeometry(geoms[idx], blob);
					list.WriteElement(blob);
				}
			});
		} else {
			// Serialize as WKB format
			serializer.WriteList(102, "data", count, [&](Serializer::List &list, idx_t i) {
				auto idx = vdata.sel->get_index(i);
				auto wkb = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : geoms[idx];
				list.WriteElement(wkb);
			});
		}
	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
			// new way to serialize strings, two blobs, first lengths, then string bytes
			if (serializer.ShouldSerialize(8)) {
				// we write all the lengths whether the string is null or not. lets not pull a parquet.
				auto length_data_length = sizeof(uint32_t) * count;
				auto length_data = make_unsafe_uniq_array_uninitialized<data_t>(length_data_length);
				idx_t byte_data_length = 0;

				// write all the lengths
				auto lenghs_write_ptr = reinterpret_cast<uint32_t *>(length_data.get());
				for (idx_t i = 0; i < count; i++) {
					auto idx = vdata.sel->get_index(i);
					auto this_length = vdata.validity.RowIsValid(idx) ? strings[idx].GetSize() : 0;
					lenghs_write_ptr[i] = UnsafeNumericCast<uint32_t>(this_length);
					byte_data_length += this_length;
				}

				// write the bytes
				auto byte_data = make_unsafe_uniq_array_uninitialized<data_t>(byte_data_length);
				// write the non-null strings
				auto string_write_ptr = byte_data.get();
				for (idx_t i = 0; i < count; i++) {
					auto idx = vdata.sel->get_index(i);
					if (!vdata.validity.RowIsValid(idx)) {
						continue;
					}
					auto this_length = strings[idx].GetSize();
					memcpy(string_write_ptr, strings[idx].GetData(), this_length);
					string_write_ptr += this_length;
				}
				serializer.WriteProperty(107, "byte_data_length", optional_idx(byte_data_length));
				// we do not encode length_data_length because its not required
				serializer.WriteProperty(108, "length_data", length_data.get(), length_data_length);
				serializer.WriteProperty(109, "byte_data", byte_data.get(), byte_data_length);
			} else { // old and slow way: list
				serializer.WriteList(102, "data", count, [&](Serializer::List &list, idx_t i) {
					auto idx = vdata.sel->get_index(i);
					auto str = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
					list.WriteElement(str);
				});
			}

			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);

			// Serialize entries as a list
			serializer.WriteList(103, "children", entries.size(), [&](Serializer::List &list, idx_t i) {
				list.WriteObject(
				    [&](Serializer &object) { entries[i].Serialize(object, count, compressed_serialization); });
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
			serializer.WriteObject(106, "child", [&](Serializer &object) {
				child.Serialize(object, list_size, compressed_serialization);
			});
			break;
		}
		case PhysicalType::ARRAY: {
			Vector serialized_vector(Vector::Ref(*this));
			serialized_vector.Flatten(count);

			auto &child = ArrayVector::GetEntry(serialized_vector);
			auto array_size = ArrayType::GetSize(serialized_vector.GetType());
			auto child_size = array_size * count;
			serializer.WriteProperty<uint64_t>(103, "array_size", array_size);
			serializer.WriteObject(104, "child", [&](Serializer &object) {
				child.Serialize(object, child_size, compressed_serialization);
			});
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Serialize!");
		}
	}
}

class StringDeserializeHolder : public AuxiliaryDataHolder {
public:
	explicit StringDeserializeHolder(idx_t size) {
		data = unique_ptr<data_t[]>(new data_t[size]);
	}
	unique_ptr<data_t[]> data;
};

void Vector::Deserialize(Deserializer &deserializer, idx_t count) {
	auto &logical_type = GetType();

	const auto vtype = // older versions that only supported flat vectors did not serialize vector_type,
	    deserializer.ReadPropertyWithExplicitDefault<VectorType>(90, "vector_type", VectorType::FLAT_VECTOR);

	// first handle deserialization of compressed vector types
	if (vtype == VectorType::CONSTANT_VECTOR) {
		Vector::Deserialize(deserializer, 1); // read a vector of size 1
		Vector::SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	} else if (vtype == VectorType::DICTIONARY_VECTOR) {
		SelectionVector sel(count);
		deserializer.ReadProperty(91, "sel_vector", reinterpret_cast<data_ptr_t>(sel.data()), sizeof(sel_t) * count);
		const auto dict_count = deserializer.ReadProperty<idx_t>(92, "dict_count");
		Vector::Deserialize(deserializer, dict_count); // deserialize the dictionary in this vector
		Vector::Slice(sel, count);                     // will create a dictionary vector
		return;
	} else if (vtype == VectorType::SEQUENCE_VECTOR) {
		const int64_t seq_start = deserializer.ReadProperty<int64_t>(91, "seq_start");
		const int64_t seq_increment = deserializer.ReadProperty<int64_t>(92, "seq_increment");
		Vector::Sequence(seq_start, seq_increment, count);
		return;
	}

	auto geometry_format = GeometryStorageType::WKB;
	if (logical_type.id() == LogicalTypeId::GEOMETRY) {
		// Try to read the geometry format, but default to the old SPATIAL format for older versions that did not
		// serialize this property
		geometry_format = deserializer.ReadPropertyWithExplicitDefault<GeometryStorageType>(
		    99, "geometry_format", GeometryStorageType::SPATIAL);
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
	} else if (logical_type.id() == LogicalTypeId::GEOMETRY) {
		auto blobs = FlatVector::GetDataMutable<string_t>(*this);

		if (geometry_format == GeometryStorageType::WKB) {
			deserializer.ReadList(102, "data", [&](Deserializer::List &list, idx_t i) {
				auto geom = list.ReadElement<string>();
				if (validity.RowIsValid(i)) {
					blobs[i] = StringVector::AddStringOrBlob(*this, geom);
				}
			});
		} else if (geometry_format == GeometryStorageType::SPATIAL) {
			// Try to read old SPATIAL format and convert to new GEOMETRY format
			deserializer.ReadList(102, "data", [&](Deserializer::List &list, idx_t i) {
				auto blob = list.ReadElement<string>();
				if (validity.RowIsValid(i)) {
					Geometry::FromSpatialGeometry(blob, blobs[i], *this);
				}
			});
		} else {
			throw InternalException("Unsupported geometry format in vector serialization");
		}

	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = FlatVector::GetDataMutable<string_t>(*this);
			auto byte_data_length =
			    deserializer.ReadPropertyWithExplicitDefault<optional_idx>(107, "byte_data_length", optional_idx());
			if (byte_data_length.IsValid()) { // new serialization
				auto length_data_length = count * sizeof(uint32_t);
				auto length_data = make_unsafe_uniq_array_uninitialized<data_t>(length_data_length);
				deserializer.ReadProperty(108, "length_data", length_data.get(), length_data_length);

				auto byte_data_buffer = make_uniq<StringDeserializeHolder>(byte_data_length.GetIndex());
				// directly read into a string buffer we can glue to the vector
				deserializer.ReadProperty(109, "byte_data", byte_data_buffer->data.get(), byte_data_length.GetIndex());
				auto lengths_read_ptr = reinterpret_cast<uint32_t *>(length_data.get());
				auto byte_read_ptr = reinterpret_cast<const char *>(byte_data_buffer->data.get());
				StringVector::AddAuxiliaryData(*this, std::move(byte_data_buffer));

				for (idx_t i = 0; i < count; ++i) {
					if (!validity.RowIsValid(i)) {
						continue;
					}
					strings[i] = string_t(byte_read_ptr, lengths_read_ptr[i]);
					byte_read_ptr += lengths_read_ptr[i];
				}
			} else { // this is ye olde way of string serialization
				deserializer.ReadList(102, "data", [&](Deserializer::List &list, idx_t i) {
					auto str = list.ReadElement<string>();
					if (validity.RowIsValid(i)) {
						strings[i] = StringVector::AddStringOrBlob(*this, str);
					}
				});
			}
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			// Deserialize entries as a list
			deserializer.ReadList(103, "children", [&](Deserializer::List &list, idx_t i) {
				list.ReadObject([&](Deserializer &obj) { entries[i].Deserialize(obj, count); });
			});
			break;
		}
		case PhysicalType::LIST: {
			// Read the list size
			auto list_size = deserializer.ReadProperty<uint64_t>(104, "list_size");
			ListVector::Reserve(*this, list_size);
			ListVector::SetListSize(*this, list_size);

			// Read the entries
			auto list_entries = FlatVector::GetDataMutable<list_entry_t>(*this);
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

VectorType Vector::GetVectorType() const {
	if (!buffer) {
		return VectorType::FLAT_VECTOR;
	}
	return buffer->GetVectorType();
}

void Vector::SetVectorType(VectorType new_vector_type) {
	if (new_vector_type != VectorType::FLAT_VECTOR && new_vector_type != VectorType::CONSTANT_VECTOR) {
		throw InternalException("SetVectorType can only be used with FLAT / CONSTANT vectors");
	}
	if (buffer) {
		// FIXME: should we allow vectors without a buffer?
		buffer->SetVectorType(new_vector_type);
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
			auto &flat_validity = FlatVector::Validity(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (flat_validity.RowIsValid(oidx)) {
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

void Vector::VerifyVariant(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG

	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::VARIANT);
	if (!VariantUtils::Verify(vector_p, sel_p, count)) {
		throw InternalException("Variant not valid");
	}
#endif // DEBUG
}

void Vector::Verify(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (vector_p.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &shredded = ShreddedVector::GetShreddedVector(vector_p);
		auto &unshredded = ShreddedVector::GetUnshreddedVector(vector_p);
		Verify(shredded, sel_p, count);
		Verify(unshredded, sel_p, count);
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

	if (type.id() == LogicalTypeId::BIGNUM) {
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					Bignum::Verify(static_cast<bignum_t>(strings[oidx]));
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
		// struct vectors cannot be dictionary vectors
		D_ASSERT(vector->GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &child_types = StructType::GetChildTypes(type);
		D_ASSERT(!child_types.empty());

		// create a selection vector of the non-null entries of the struct vector
		auto &children = StructVector::GetEntries(*vector);
		D_ASSERT(child_types.size() == children.size());
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			D_ASSERT(children[child_idx].GetType() == child_types[child_idx].second);
			Vector::Verify(children[child_idx], sel_p, count);
			if (vtype == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(children[child_idx].GetVectorType() == VectorType::CONSTANT_VECTOR);
				if (ConstantVector::IsNull(*vector)) {
					D_ASSERT(ConstantVector::IsNull(children[child_idx]));
				}
			}
			if (vtype != VectorType::FLAT_VECTOR) {
				continue;
			}
			optional_ptr<ValidityMask> child_validity;
			SelectionVector owned_child_sel;
			const SelectionVector *child_sel = &owned_child_sel;
			if (children[child_idx].GetVectorType() == VectorType::FLAT_VECTOR) {
				child_sel = FlatVector::IncrementalSelectionVector();
				child_validity = &FlatVector::Validity(children[child_idx]);
			} else if (children[child_idx].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &child = DictionaryVector::Child(children[child_idx]);
				if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
					continue;
				}
				child_validity = &FlatVector::Validity(child);
				child_sel = &DictionaryVector::SelVector(children[child_idx]);
			} else if (children[child_idx].GetVectorType() == VectorType::CONSTANT_VECTOR) {
				child_sel = ConstantVector::ZeroSelectionVector(count, owned_child_sel);
				child_validity = &ConstantVector::Validity(children[child_idx]);
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
		if (vector->GetType().id() == LogicalTypeId::VARIANT) {
			VerifyVariant(vector_p, sel_p, count);
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
	auto reusable_dict = DictionaryVector::CreateReusableDictionary(vector.type, verify_count);
	auto &inverted_vector = reusable_dict->data;
	inverted_vector.Slice(vector, inverted_sel, verify_count);
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
	if (vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// Reusable dictionary API does not work for STRUCT
		vector.Dictionary(inverted_vector, verify_count, original_sel, count);
	} else {
		vector.Dictionary(reusable_dict, original_sel);
	}
	vector.Verify(count);
}

void Vector::DebugShuffleNestedVector(Vector &vector, idx_t count) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::STRUCT: {
		auto &entries = StructVector::GetEntries(vector);
		// recurse into child elements
		for (auto &entry : entries) {
			Vector::DebugShuffleNestedVector(entry, count);
		}
		break;
	}
	case LogicalTypeId::LIST: {
		if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			break;
		}
		auto list_entries = FlatVector::GetDataMutable<list_entry_t>(vector);
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

} // namespace duckdb
