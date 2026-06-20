#include "reader/variant/parquet_variant_iterator.hpp"

#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "reader/uuid_column_reader.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <cmath>
#include <set>

namespace duckdb {

namespace {

//! Read a little-endian unsigned integer of 'size' bytes (without advancing)
idx_t ReadVarLE(idx_t size, const_data_ptr_t ptr) {
	D_ASSERT(size <= sizeof(idx_t));
	idx_t result = 0;
	memcpy(&result, ptr, size);
	return result;
}

//! Lazy reader over a Spark variant-encoded OBJECT (the value starts at its header byte)
struct BinaryObjectReader {
	BinaryObjectReader(const VariantMetadata &metadata, const_data_ptr_t value_start) : metadata(metadata) {
		auto value_metadata = VariantValueMetadata::FromHeaderByte(value_start[0]);
		D_ASSERT(value_metadata.basic_type == VariantBasicType::OBJECT);
		field_id_size = value_metadata.field_id_size;
		field_offset_size = value_metadata.field_offset_size;
		auto data = value_start + 1;
		if (value_metadata.is_large) {
			count = Load<uint32_t>(data);
			data += sizeof(uint32_t);
		} else {
			count = Load<uint8_t>(data);
			data += sizeof(uint8_t);
		}
		field_ids = data;
		field_offsets = data + (count * field_id_size);
		values = field_offsets + (NumericCast<idx_t>(count + 1) * field_offset_size);
	}

	string_t Key(idx_t i) const {
		auto field_id = ReadVarLE(field_id_size, field_ids + (i * field_id_size));
		auto &key = metadata.strings[field_id];
		return string_t(key.c_str(), NumericCast<uint32_t>(key.size()));
	}
	const_data_ptr_t Child(idx_t i) const {
		auto offset = ReadVarLE(field_offset_size, field_offsets + (i * field_offset_size));
		return values + offset;
	}

	const VariantMetadata &metadata;
	const_data_ptr_t field_ids;
	const_data_ptr_t field_offsets;
	const_data_ptr_t values;
	uint32_t field_id_size;
	uint32_t field_offset_size;
	idx_t count;
};

//! Lazy reader over a Spark variant-encoded ARRAY (the value starts at its header byte)
struct BinaryArrayReader {
	explicit BinaryArrayReader(const_data_ptr_t value_start) {
		auto value_metadata = VariantValueMetadata::FromHeaderByte(value_start[0]);
		D_ASSERT(value_metadata.basic_type == VariantBasicType::ARRAY);
		field_offset_size = value_metadata.field_offset_size;
		auto data = value_start + 1;
		if (value_metadata.is_large) {
			count = Load<uint32_t>(data);
			data += sizeof(uint32_t);
		} else {
			count = Load<uint8_t>(data);
			data += sizeof(uint8_t);
		}
		field_offsets = data;
		values = field_offsets + (NumericCast<idx_t>(count + 1) * field_offset_size);
	}

	const_data_ptr_t Element(idx_t i) const {
		auto offset = ReadVarLE(field_offset_size, field_offsets + (i * field_offset_size));
		return values + offset;
	}

	const_data_ptr_t field_offsets;
	const_data_ptr_t values;
	uint32_t field_offset_size;
	idx_t count;
};

//! The VariantLogicalType of a (valid) shredded leaf at 'typed_idx'. Mirrors the writer's type mapping;
//! note Parquet emits BINARY as a base64 VARCHAR (matching its JSON-serialization choice).
VariantLogicalType ShreddedLeafTypeId(const ShreddedGroupView &view, idx_t typed_idx) {
	switch (view.typed_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return UnifiedVectorFormat::GetData<bool>(view.typed_format)[typed_idx] ? VariantLogicalType::BOOL_TRUE
		                                                                        : VariantLogicalType::BOOL_FALSE;
	case LogicalTypeId::TINYINT:
		return VariantLogicalType::INT8;
	case LogicalTypeId::SMALLINT:
		return VariantLogicalType::INT16;
	case LogicalTypeId::INTEGER:
		return VariantLogicalType::INT32;
	case LogicalTypeId::BIGINT:
		return VariantLogicalType::INT64;
	case LogicalTypeId::FLOAT:
		return VariantLogicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return VariantLogicalType::DOUBLE;
	case LogicalTypeId::DECIMAL:
		return VariantLogicalType::DECIMAL;
	case LogicalTypeId::DATE:
		return VariantLogicalType::DATE;
	case LogicalTypeId::TIME:
		return VariantLogicalType::TIME_MICROS;
	case LogicalTypeId::TIMESTAMP_TZ:
		return VariantLogicalType::TIMESTAMP_MICROS_TZ;
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return VariantLogicalType::TIMESTAMP_NANOS_TZ;
	case LogicalTypeId::TIMESTAMP:
		return VariantLogicalType::TIMESTAMP_MICROS;
	case LogicalTypeId::TIMESTAMP_NS:
		return VariantLogicalType::TIMESTAMP_NANOS;
	case LogicalTypeId::BLOB:
		//! Parquet emits binary as base64 VARCHAR
		return VariantLogicalType::VARCHAR;
	case LogicalTypeId::VARCHAR:
		return VariantLogicalType::VARCHAR;
	case LogicalTypeId::UUID:
		return VariantLogicalType::UUID;
	default:
		throw NotImplementedException("Variant shredding on type: '%s' is not implemented", view.typed_type.ToString());
	}
}

//! The VariantLogicalType of a Spark variant-encoded value (the value starts at its header byte)
VariantLogicalType BinaryTypeId(const_data_ptr_t data) {
	auto value_metadata = VariantValueMetadata::FromHeaderByte(data[0]);
	switch (value_metadata.basic_type) {
	case VariantBasicType::SHORT_STRING:
		return VariantLogicalType::VARCHAR;
	case VariantBasicType::OBJECT:
		return VariantLogicalType::OBJECT;
	case VariantBasicType::ARRAY:
		return VariantLogicalType::ARRAY;
	case VariantBasicType::PRIMITIVE:
		break;
	default:
		throw InternalException("Unexpected VariantBasicType");
	}
	switch (value_metadata.primitive_type) {
	case VariantPrimitiveType::NULL_TYPE:
		return VariantLogicalType::VARIANT_NULL;
	case VariantPrimitiveType::BOOLEAN_TRUE:
		return VariantLogicalType::BOOL_TRUE;
	case VariantPrimitiveType::BOOLEAN_FALSE:
		return VariantLogicalType::BOOL_FALSE;
	case VariantPrimitiveType::INT8:
		return VariantLogicalType::INT8;
	case VariantPrimitiveType::INT16:
		return VariantLogicalType::INT16;
	case VariantPrimitiveType::INT32:
		return VariantLogicalType::INT32;
	case VariantPrimitiveType::INT64:
		return VariantLogicalType::INT64;
	case VariantPrimitiveType::DOUBLE:
		return VariantLogicalType::DOUBLE;
	case VariantPrimitiveType::FLOAT:
		return VariantLogicalType::FLOAT;
	case VariantPrimitiveType::DECIMAL4:
	case VariantPrimitiveType::DECIMAL8:
	case VariantPrimitiveType::DECIMAL16:
		return VariantLogicalType::DECIMAL;
	case VariantPrimitiveType::DATE:
		return VariantLogicalType::DATE;
	case VariantPrimitiveType::TIMESTAMP_MICROS:
		return VariantLogicalType::TIMESTAMP_MICROS_TZ;
	case VariantPrimitiveType::TIMESTAMP_NTZ_MICROS:
		return VariantLogicalType::TIMESTAMP_MICROS;
	case VariantPrimitiveType::BINARY:
		//! Parquet emits binary as base64 VARCHAR
		return VariantLogicalType::VARCHAR;
	case VariantPrimitiveType::STRING:
		return VariantLogicalType::VARCHAR;
	case VariantPrimitiveType::TIME_NTZ_MICROS:
		return VariantLogicalType::TIME_MICROS;
	case VariantPrimitiveType::TIMESTAMP_NANOS:
		return VariantLogicalType::TIMESTAMP_NANOS_TZ;
	case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS:
		return VariantLogicalType::TIMESTAMP_NANOS;
	case VariantPrimitiveType::UUID:
		return VariantLogicalType::UUID;
	default:
		throw NotImplementedException("Variant PrimitiveType (%d) is not supported",
		                              static_cast<uint8_t>(value_metadata.primitive_type));
	}
}

//! The physical storage type of a DECIMAL of the given width (matches VariantDecimalData::GetPhysicalType)
PhysicalType DecimalPhysicalType(uint32_t width) {
	if (width > DecimalWidth<int64_t>::max) {
		return PhysicalType::INT128;
	} else if (width > DecimalWidth<int32_t>::max) {
		return PhysicalType::INT64;
	} else if (width > DecimalWidth<int16_t>::max) {
		return PhysicalType::INT32;
	}
	return PhysicalType::INT16;
}

//! The implied precision of a decimal value: floor(log10(val)) + 1
template <class T>
uint8_t ComputeDecimalWidth(T value) {
	if (value == 0) {
		return 1;
	}
	auto abs_val = value;
	if (abs_val < 0) {
		abs_val = -abs_val;
	}
	return static_cast<uint8_t>(floor(log10(static_cast<double>(abs_val))) + 1);
}

//! Re-encode a decimal 'value' at the physical type implied by 'width' and own it in the row's arena
VariantDecimalData ArenaDecimal(const ParquetVariantIterator &state, uint8_t width, uint8_t scale, int64_t value) {
	switch (DecimalPhysicalType(width)) {
	case PhysicalType::INT16: {
		auto narrowed = NumericCast<int16_t>(value);
		return VariantDecimalData(width, scale, state.StoreBytes(const_data_ptr_cast(&narrowed), sizeof(int16_t)));
	}
	case PhysicalType::INT32: {
		auto narrowed = NumericCast<int32_t>(value);
		return VariantDecimalData(width, scale, state.StoreBytes(const_data_ptr_cast(&narrowed), sizeof(int32_t)));
	}
	case PhysicalType::INT64:
		return VariantDecimalData(width, scale, state.StoreBytes(const_data_ptr_cast(&value), sizeof(int64_t)));
	default: {
		hugeint_t widened(value);
		return VariantDecimalData(width, scale, state.StoreBytes(const_data_ptr_cast(&widened), sizeof(hugeint_t)));
	}
	}
}

} // namespace

//===--------------------------------------------------------------------===//
// ShreddedGroupView
//===--------------------------------------------------------------------===//
void ShreddedGroupView::Build(Vector &group) {
	D_ASSERT(group.GetType().id() == LogicalTypeId::STRUCT);
	auto &entries = StructVector::GetEntries(group);
	auto &child_types = StructType::GetChildTypes(group.GetType());
	D_ASSERT(entries.size() == child_types.size());

	//! From the spec: the Parquet columns storing variant metadata and values must be accessed by name
	optional_ptr<Vector> value_vec;
	optional_ptr<Vector> typed_vec;
	for (idx_t i = 0; i < entries.size(); i++) {
		auto &name = child_types[i].first;
		if (name == "value") {
			value_vec = entries[i];
		} else if (name == "typed_value") {
			typed_vec = entries[i];
		} else {
			throw InvalidInputException("Variant group can only contain 'value'/'typed_value', not: %s", name);
		}
	}
	if (!value_vec) {
		throw InvalidInputException("Required column 'value' not found in Variant group");
	}

	value_vec->ToUnifiedFormat(value_format);
	value_data = UnifiedVectorFormat::GetData<string_t>(value_format);

	if (!typed_vec) {
		has_typed_value = false;
		return;
	}
	has_typed_value = true;
	typed_type = typed_vec->GetType();
	typed_vec->ToUnifiedFormat(typed_format);

	switch (typed_type.id()) {
	case LogicalTypeId::STRUCT: {
		kind = ParquetGroupKind::OBJECT;
		auto &fields_meta = StructType::GetChildTypes(typed_type);
		auto &field_entries = StructVector::GetEntries(*typed_vec);
		for (idx_t i = 0; i < field_entries.size(); i++) {
			field_names.push_back(fields_meta[i].first.GetIdentifierName());
			auto field_view = make_uniq<ShreddedGroupView>();
			field_view->Build(field_entries[i]);
			fields.push_back(std::move(field_view));
		}
		break;
	}
	case LogicalTypeId::LIST: {
		kind = ParquetGroupKind::ARRAY;
		list_data = UnifiedVectorFormat::GetData<list_entry_t>(typed_format);
		element = make_uniq<ShreddedGroupView>();
		element->Build(ListVector::GetChildMutable(*typed_vec));
		break;
	}
	default:
		kind = ParquetGroupKind::LEAF;
		break;
	}
}

//===--------------------------------------------------------------------===//
// ParquetVariantIterator
//===--------------------------------------------------------------------===//
ParquetVariantIterator::ParquetVariantIterator(Vector &metadata, Vector &group) {
	metadata.ToUnifiedFormat(metadata_format);
	metadata_data = UnifiedVectorFormat::GetData<string_t>(metadata_format);
	root_view.Build(group);
}

void ParquetVariantIterator::BeginRow(idx_t row) {
	current_row = row;
	current_metadata.reset();
	byte_arena.clear();
}

const VariantMetadata &ParquetVariantIterator::GetMetadata() const {
	if (!current_metadata) {
		auto &blob = metadata_data[metadata_format.sel->get_index(current_row)];
		current_metadata = make_uniq<VariantMetadata>(blob);
	}
	return *current_metadata;
}

const_data_ptr_t ParquetVariantIterator::StoreBytes(const_data_ptr_t data, idx_t size) const {
	byte_arena.push_back(make_uniq<string>(const_char_ptr_cast(data), size));
	return const_data_ptr_cast(byte_arena.back()->data());
}

string_t ParquetVariantIterator::StoreString(string str) const {
	byte_arena.push_back(make_uniq<string>(std::move(str)));
	auto &owned = *byte_arena.back();
	return string_t(owned.data(), NumericCast<uint32_t>(owned.size()));
}

ParquetVariantNode ParquetVariantIterator::ResolveGroup(const ShreddedGroupView &view, idx_t index) const {
	if (view.has_typed_value) {
		auto typed_idx = view.typed_format.sel->get_index(index);
		if (view.typed_format.validity.RowIsValid(typed_idx)) {
			if (view.kind == ParquetGroupKind::OBJECT) {
				//! (Partially) shredded object - the binary 'value', if present, holds the leftover fields
				const_data_ptr_t overlay = nullptr;
				auto value_idx = view.value_format.sel->get_index(index);
				if (view.value_format.validity.RowIsValid(value_idx)) {
					auto overlay_data = const_data_ptr_cast(view.value_data[value_idx].GetData());
					if (VariantValueMetadata::FromHeaderByte(overlay_data[0]).basic_type != VariantBasicType::OBJECT) {
						throw InvalidInputException(
						    "Partially shredded objects have to encode Object Variants in the 'value'");
					}
					overlay = overlay_data;
				}
				return ParquetVariantNode::MakeShredded(*this, view, index, overlay);
			}
			//! LEAF or ARRAY - the binary 'value' is irrelevant (a leaf is never partially shredded)
			return ParquetVariantNode::MakeShredded(*this, view, index);
		}
	}

	//! No (valid) shredded value - fall back to the binary 'value'
	auto value_idx = view.value_format.sel->get_index(index);
	if (view.value_format.validity.RowIsValid(value_idx)) {
		auto data = const_data_ptr_cast(view.value_data[value_idx].GetData());
		if (view.has_typed_value && view.kind == ParquetGroupKind::OBJECT &&
		    VariantValueMetadata::FromHeaderByte(data[0]).basic_type == VariantBasicType::OBJECT) {
			throw InvalidInputException(
			    "When 'typed_value' for a shredded Object is NULL, 'value' can not contain an Object value");
		}
		return ParquetVariantNode::MakeBinary(*this, data);
	}
	return ParquetVariantNode::MakeMissing();
}

ParquetVariantNode ParquetVariantIterator::Root(idx_t row) const {
	auto root = ResolveGroup(root_view, row);
	//! A root value is never "missing" - treat any such case as a SQL NULL
	return root.IsMissing() ? ParquetVariantNode::MakeNull() : root;
}

//===--------------------------------------------------------------------===//
// ParquetVariantNode
//===--------------------------------------------------------------------===//
VariantLogicalType ParquetVariantNode::GetTypeId() const {
	switch (kind) {
	case Kind::NULL_VALUE:
		return VariantLogicalType::VARIANT_NULL;
	case Kind::SHREDDED:
		switch (view->kind) {
		case ParquetGroupKind::OBJECT:
			return VariantLogicalType::OBJECT;
		case ParquetGroupKind::ARRAY:
			return VariantLogicalType::ARRAY;
		default:
			return ShreddedLeafTypeId(*view, view->typed_format.sel->get_index(index));
		}
	case Kind::BINARY:
		return BinaryTypeId(binary);
	default:
		throw InternalException("ParquetVariantNode::GetTypeId on a MISSING value");
	}
}

const_data_ptr_t ParquetVariantNode::GetDataPointer() const {
	if (kind == Kind::SHREDDED) {
		auto typed_idx = view->typed_format.sel->get_index(index);
		auto type_size = GetTypeIdSize(view->typed_format.physical_type);
		return view->typed_format.data + (typed_idx * type_size);
	}
	D_ASSERT(kind == Kind::BINARY);
	auto value_metadata = VariantValueMetadata::FromHeaderByte(binary[0]);
	auto payload = binary + 1;
	if (value_metadata.primitive_type == VariantPrimitiveType::UUID) {
		//! The Parquet UUID byte order differs from the canonical (hugeint) layout - materialize it
		auto uuid = UUIDValueConversion::ReadParquetUUID(payload);
		return state->StoreBytes(const_data_ptr_cast(&uuid), sizeof(hugeint_t));
	}
	//! Fixed-width primitives are already stored in the canonical little-endian layout
	return payload;
}

string_t ParquetVariantNode::GetString() const {
	if (kind == Kind::SHREDDED) {
		auto typed_idx = view->typed_format.sel->get_index(index);
		auto str = UnifiedVectorFormat::GetData<string_t>(view->typed_format)[typed_idx];
		if (view->typed_type.id() == LogicalTypeId::BLOB) {
			return state->StoreString(Blob::ToBase64(str));
		}
		if (!Utf8Proc::IsValid(str.GetData(), str.GetSize())) {
			throw InternalException("Can't decode Variant string, it isn't valid UTF8");
		}
		return str;
	}
	D_ASSERT(kind == Kind::BINARY);
	auto value_metadata = VariantValueMetadata::FromHeaderByte(binary[0]);
	auto payload = binary + 1;
	if (value_metadata.basic_type == VariantBasicType::SHORT_STRING) {
		auto string_data = const_char_ptr_cast(payload);
		if (!Utf8Proc::IsValid(string_data, value_metadata.string_size)) {
			throw InternalException("Can't decode Variant short-string, string isn't valid UTF8");
		}
		return string_t(string_data, value_metadata.string_size);
	}
	auto size = Load<uint32_t>(payload);
	auto string_data = const_char_ptr_cast(payload + sizeof(uint32_t));
	if (value_metadata.primitive_type == VariantPrimitiveType::BINARY) {
		//! Follow the JSON serialization guide by converting BINARY to base64
		return state->StoreString(Blob::ToBase64(string_t(string_data, size)));
	}
	if (!Utf8Proc::IsValid(string_data, size)) {
		throw InternalException("Can't decode Variant string, it isn't valid UTF8");
	}
	return string_t(string_data, size);
}

VariantDecimalData ParquetVariantNode::GetDecimal() const {
	if (kind == Kind::SHREDDED) {
		uint8_t width;
		uint8_t scale;
		view->typed_type.GetDecimalProperties(width, scale);
		auto typed_idx = view->typed_format.sel->get_index(index);
		auto type_size = GetTypeIdSize(view->typed_format.physical_type);
		return VariantDecimalData(width, scale, view->typed_format.data + (typed_idx * type_size));
	}
	D_ASSERT(kind == Kind::BINARY);
	auto value_metadata = VariantValueMetadata::FromHeaderByte(binary[0]);
	auto payload = binary + 1;
	uint8_t scale = Load<uint8_t>(payload);
	auto value_data = payload + sizeof(uint8_t);

	//! The binary value stores the integer at the encoded width (4/8/16 bytes); the canonical layout
	//! stores it at the physical type implied by the (computed) precision, so re-encode it into the arena.
	switch (value_metadata.primitive_type) {
	case VariantPrimitiveType::DECIMAL4: {
		int64_t value = Load<int32_t>(value_data);
		return ArenaDecimal(*state, ComputeDecimalWidth<int64_t>(value), scale, value);
	}
	case VariantPrimitiveType::DECIMAL8: {
		int64_t value = Load<int64_t>(value_data);
		return ArenaDecimal(*state, ComputeDecimalWidth<int64_t>(value), scale, value);
	}
	default: {
		D_ASSERT(value_metadata.primitive_type == VariantPrimitiveType::DECIMAL16);
		hugeint_t value;
		value.lower = Load<uint64_t>(value_data);
		value.upper = Load<int64_t>(value_data + sizeof(uint64_t));
		return VariantDecimalData(DecimalWidth<hugeint_t>::max, scale,
		                          state->StoreBytes(const_data_ptr_cast(&value), sizeof(hugeint_t)));
	}
	}
}

ParquetObjectIterator ParquetVariantNode::GetObjectChildren(VariantIterationOrder order) const {
	(void)order;
	if (kind == Kind::SHREDDED) {
		return ParquetObjectIterator(*state, *view, index, binary);
	}
	D_ASSERT(kind == Kind::BINARY);
	return ParquetObjectIterator(*state, state->GetMetadata(), binary);
}

ParquetArrayIterator ParquetVariantNode::GetArrayChildren() const {
	if (kind == Kind::SHREDDED) {
		return ParquetArrayIterator(*state, *view, index);
	}
	D_ASSERT(kind == Kind::BINARY);
	return ParquetArrayIterator(*state, state->GetMetadata(), binary);
}

//===--------------------------------------------------------------------===//
// ParquetObjectIterator
//===--------------------------------------------------------------------===//
void ParquetObjectIterator::Finalize() {
	std::sort(ordered_entries.begin(), ordered_entries.end(),
	          [](const ParquetObjectEntry &a, const ParquetObjectEntry &b) { return a.key < b.key; });
}

ParquetObjectIterator::ParquetObjectIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view,
                                             idx_t index, const_data_ptr_t overlay) {
	//! Typed (shredded) fields - skipping the ones that are missing for this row
	std::set<string> typed_keys;
	for (idx_t i = 0; i < view.fields.size(); i++) {
		auto node = state.ResolveGroup(*view.fields[i], index);
		if (node.IsMissing()) {
			continue;
		}
		auto &name = view.field_names[i];
		typed_keys.insert(name);
		ordered_entries.push_back(
		    ParquetObjectEntry {string_t(name.c_str(), NumericCast<uint32_t>(name.size())), node});
	}
	//! Leftover (overlay) fields from the binary 'value' - typed fields win on key collisions
	if (overlay) {
		BinaryObjectReader reader(state.GetMetadata(), overlay);
		for (idx_t i = 0; i < reader.count; i++) {
			auto key = reader.Key(i);
			if (typed_keys.count(key.GetString())) {
				continue;
			}
			ordered_entries.push_back(ParquetObjectEntry {key, ParquetVariantNode::MakeBinary(state, reader.Child(i))});
		}
	}
	Finalize();
}

ParquetObjectIterator::ParquetObjectIterator(const ParquetVariantIterator &state, const VariantMetadata &metadata,
                                             const_data_ptr_t data) {
	BinaryObjectReader reader(metadata, data);
	for (idx_t i = 0; i < reader.count; i++) {
		ordered_entries.push_back(
		    ParquetObjectEntry {reader.Key(i), ParquetVariantNode::MakeBinary(state, reader.Child(i))});
	}
	Finalize();
}

//===--------------------------------------------------------------------===//
// ParquetArrayIterator
//===--------------------------------------------------------------------===//
ParquetArrayIterator::ParquetArrayIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view,
                                           idx_t index)
    : state(state), shredded(true), element(view.element.get()) {
	auto &entry = view.list_data[view.typed_format.sel->get_index(index)];
	base = entry.offset;
	length = entry.length;
}

ParquetArrayIterator::ParquetArrayIterator(const ParquetVariantIterator &state, const VariantMetadata &metadata,
                                           const_data_ptr_t data)
    : state(state), shredded(false) {
	(void)metadata;
	BinaryArrayReader reader(data);
	length = reader.count;
	field_offsets = reader.field_offsets;
	values = reader.values;
	field_offset_size = reader.field_offset_size;
}

ParquetVariantNode ParquetArrayIterator::operator[](idx_t i) const {
	if (shredded) {
		return state.get().ResolveGroup(*element, base + i);
	}
	auto offset = ReadVarLE(field_offset_size, field_offsets + (i * field_offset_size));
	return ParquetVariantNode::MakeBinary(state.get(), values + offset);
}

//===--------------------------------------------------------------------===//
// ParquetVariantIteratorSource
//===--------------------------------------------------------------------===//
bool ParquetVariantIteratorSource::Emit(idx_t row, VariantBuilder &builder) {
	iterator.BeginRow(row);
	auto root = iterator.Root(row);
	if (root.IsNull()) {
		return true;
	}
	//! A root that resolves to a (variant) NULL is a genuine SQL NULL row
	if (root.GetTypeId() == VariantLogicalType::VARIANT_NULL) {
		return true;
	}
	EmitIterator(root, builder);
	return false;
}

//===--------------------------------------------------------------------===//
// ParquetVariantConversion
//===--------------------------------------------------------------------===//
void ParquetVariantConversion::Convert(Vector &metadata, Vector &group, Vector &result, idx_t count) {
	ParquetVariantIterator iterator(metadata, group);
	ParquetVariantIteratorSource source(iterator);
	BuildVariant(source, count, result);
}

} // namespace duckdb
