#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/serializer/varint.hpp"

namespace duckdb {

//! Indices into the shredded "STRUCT(typed_value, untyped_value_index)" wrapper
static constexpr idx_t TYPED_VALUE_INDEX = 0;
static constexpr idx_t UNTYPED_VALUE_INDEX = 1;

//! Child indices into the unshredded layout (see UnshreddedVariantLayout)
static constexpr idx_t KEYS_INDEX = 0;
static constexpr idx_t CHILDREN_INDEX = 1;
static constexpr idx_t VALUES_INDEX = 2;
static constexpr idx_t DATA_INDEX = 3;
//! Child indices within the children / values structs
static constexpr idx_t KEY_ID_INDEX = 0;
static constexpr idx_t VALUE_ID_INDEX = 1;
static constexpr idx_t TYPE_ID_INDEX = 0;
static constexpr idx_t BYTE_OFFSET_INDEX = 1;

namespace {

//! Decode the (length-prefixed) string payload of a value at the given byte offset in the blob
string_t DecodeStringData(const string_t &blob, uint32_t byte_offset) {
	auto ptr = const_data_ptr_cast(blob.GetData()) + byte_offset;
	auto length = VarintDecode<uint32_t>(ptr);
	return string_t(reinterpret_cast<const char *>(ptr), length);
}

//! Decode the (width, scale, value pointer) of a DECIMAL value at the given byte offset
VariantDecimalData DecodeDecimalData(const string_t &blob, uint32_t byte_offset) {
	auto ptr = const_data_ptr_cast(blob.GetData()) + byte_offset;
	auto width = VarintDecode<uint32_t>(ptr);
	auto scale = VarintDecode<uint32_t>(ptr);
	return VariantDecimalData(width, scale, ptr);
}

//! Decode the (child_count, children_idx) of an OBJECT/ARRAY value at the given byte offset
VariantNestedData DecodeNestedData(const string_t &blob, uint32_t byte_offset) {
	auto ptr = const_data_ptr_cast(blob.GetData()) + byte_offset;
	VariantNestedData result;
	result.child_count = VarintDecode<uint32_t>(ptr);
	result.children_idx = result.child_count ? VarintDecode<uint32_t>(ptr) : 0;
	return result;
}

} // namespace

//===--------------------------------------------------------------------===//
// VariantIteratorState
//===--------------------------------------------------------------------===//
VariantIteratorState::VariantIteratorState(const Vector &variant)
    //! The unshredded ("core") source is the variant itself, or the unshredded component of a shredded vector
    : unshredded(variant.GetVectorType() == VectorType::SHREDDED_VECTOR ? ShreddedVector::GetUnshreddedVector(variant)
                                                                        : variant) {
	if (variant.GetVectorType() != VectorType::SHREDDED_VECTOR) {
		return;
	}
	is_shredded = true;

	//! Flatten the shredded component so the tree can be navigated directly. Flattening these
	//! (regular, typed) vectors is cheap and - unlike flattening the SHREDDED_VECTOR itself - does
	//! not reconstruct the unshredded representation.
	auto &shredded_vec = ShreddedVector::GetShreddedVector(variant);
	shredded_root = make_uniq<Vector>(Vector::Ref(shredded_vec));
	shredded_root->Flatten();

	//! The row validity lives on the parent STRUCT(unshredded, shredded)
	auto &struct_vec = variant.GetBufferRef()->Cast<ShreddedVectorBuffer>().GetChild();
	row_format = make_uniq<UnifiedVectorFormat>();
	struct_vec.ToUnifiedFormat(*row_format);
}

//===--------------------------------------------------------------------===//
// UnshreddedVariantIterator
//===--------------------------------------------------------------------===//
UnshreddedVariantIterator::UnshreddedVariantIterator(const Vector &unshredded) : data(unshredded) {
}

bool UnshreddedVariantIterator::RowIsValid(idx_t row) const {
	return data[row].IsValid();
}

VariantLogicalType UnshreddedVariantIterator::GetTypeId(idx_t row, idx_t value_index) const {
	auto raw = data[row].GetChildValue<VALUES_INDEX>().GetChildValue(value_index).GetChildValue<TYPE_ID_INDEX>();
	return static_cast<VariantLogicalType>(raw.GetValueUnsafe());
}

uint32_t UnshreddedVariantIterator::GetByteOffset(idx_t row, idx_t value_index) const {
	return data[row]
	    .GetChildValue<VALUES_INDEX>()
	    .GetChildValue(value_index)
	    .GetChildValue<BYTE_OFFSET_INDEX>()
	    .GetValueUnsafe();
}

const string_t &UnshreddedVariantIterator::GetBlob(idx_t row) const {
	return data[row].GetChildValue<DATA_INDEX>().GetValueUnsafe();
}

string_t UnshreddedVariantIterator::GetKey(idx_t row, idx_t key_index) const {
	return data[row].GetChildValue<KEYS_INDEX>().GetChildValue(key_index).GetValueUnsafe();
}

uint32_t UnshreddedVariantIterator::GetKeysIndex(idx_t row, idx_t child_index) const {
	return data[row]
	    .GetChildValue<CHILDREN_INDEX>()
	    .GetChildValue(child_index)
	    .GetChildValue<KEY_ID_INDEX>()
	    .GetValueUnsafe();
}

uint32_t UnshreddedVariantIterator::GetValuesIndex(idx_t row, idx_t child_index) const {
	return data[row]
	    .GetChildValue<CHILDREN_INDEX>()
	    .GetChildValue(child_index)
	    .GetChildValue<VALUE_ID_INDEX>()
	    .GetValueUnsafe();
}

//===--------------------------------------------------------------------===//
// Root / row validity
//===--------------------------------------------------------------------===//
VariantIterator VariantIteratorState::Root(idx_t row) const {
	if (is_shredded) {
		//! A SQL-NULL row has both components NULL - the resolution below already yields a NULL cursor in
		//! that case, but we short-circuit on the row validity to avoid touching the child vectors
		if (!row_format->validity.RowIsValid(row_format->sel->get_index(row))) {
			return VariantIterator::MakeNull(*this);
		}
		auto root = VariantIterator::ResolveShredded(*this, *shredded_root, row, row);
		//! a root value is never "missing" - treat any such case as a SQL NULL
		return root.IsMissing() ? VariantIterator::MakeNull(*this) : root;
	}
	if (!unshredded.RowIsValid(row)) {
		return VariantIterator::MakeNull(*this);
	}
	//! The unshredded root value lives at values[0]
	return VariantIterator::MakeUnshredded(*this, row, 0);
}

bool VariantIteratorState::RowIsValid(idx_t row) const {
	//! A VARIANT is never NULL at the root via a VARIANT_NULL value (that is reserved for nested values) -
	//! a root that resolves to NULL is a genuine SQL NULL. This matches the semantics of unshredding,
	//! where a shredded value whose typed leaf is NULL (with no unshredded leftover) becomes a SQL NULL.
	return !Root(row).IsNull();
}

//===--------------------------------------------------------------------===//
// VariantIterator - factory helpers
//===--------------------------------------------------------------------===//
VariantIterator VariantIterator::MakeNull(const VariantIteratorState &state) {
	VariantIterator result;
	result.state = &state;
	result.kind = Kind::NULL_VALUE;
	return result;
}

VariantIterator VariantIterator::MakeMissing(const VariantIteratorState &state) {
	VariantIterator result;
	result.state = &state;
	result.kind = Kind::MISSING;
	return result;
}

VariantIterator VariantIterator::MakeUnshredded(const VariantIteratorState &state, idx_t row, uint32_t value_index) {
	VariantIterator result;
	result.state = &state;
	result.kind = Kind::UNSHREDDED;
	result.row = row;
	result.value_index = value_index;
	return result;
}

VariantIterator VariantIterator::MakeShredded(const VariantIteratorState &state, const Vector &content, idx_t index,
                                              idx_t row, uint32_t overlay_value_index) {
	VariantIterator result;
	result.state = &state;
	result.kind = Kind::SHREDDED;
	result.row = row;
	result.shredded_content = content;
	result.shredded_index = index;
	result.overlay_value_index = overlay_value_index;
	return result;
}

//===--------------------------------------------------------------------===//
// Shredded resolution
//===--------------------------------------------------------------------===//
VariantIterator VariantIterator::ResolveShredded(const VariantIteratorState &state, const Vector &node, idx_t index,
                                                 idx_t row) {
	if (node.GetType().id() != LogicalTypeId::STRUCT) {
		//! A flattened (fully-consistent) primitive - a NULL here represents a VARIANT_NULL value
		if (FlatVector::IsNull(node, index)) {
			return MakeNull(state);
		}
		return MakeShredded(state, node, index, row, 0);
	}

	//! A "STRUCT(typed_value, [untyped_value_index])" wrapper
	auto &entries = StructVector::GetEntries(node);
	auto &typed_value = entries[TYPED_VALUE_INDEX];

	bool overlay_valid = false;
	uint32_t overlay_value_index = 0;
	if (entries.size() > 1) {
		auto &untyped_value_index = entries[UNTYPED_VALUE_INDEX];
		if (!FlatVector::IsNull(untyped_value_index, index)) {
			overlay_valid = true;
			overlay_value_index = FlatVector::GetData<uint32_t>(untyped_value_index)[index];
		}
	}

	if (!FlatVector::IsNull(typed_value, index)) {
		//! The value is (at least partially) shredded
		if (typed_value.GetType().id() == LogicalTypeId::LIST) {
			//! ARRAY values are not partially shredded - ignore any overlay
			return MakeShredded(state, typed_value, index, row, 0);
		}
		//! Only OBJECT values merge a leftover (overlay) object
		auto overlay = typed_value.GetType().id() == LogicalTypeId::STRUCT ? overlay_value_index : 0;
		if (!overlay_valid) {
			overlay = 0;
		}
		return MakeShredded(state, typed_value, index, row, overlay);
	}

	//! The value did not fit the shredded schema - it lives entirely in the unshredded component
	if (!overlay_valid) {
		//! No shredded and no unshredded value -> VARIANT_NULL
		return MakeNull(state);
	}
	if (overlay_value_index == 0) {
		//! 0 is reserved to indicate a missing value
		return MakeMissing(state);
	}
	//! The unshredded component stores 1-based value indices (0 == missing)
	return MakeUnshredded(state, row, overlay_value_index - 1);
}

//===--------------------------------------------------------------------===//
// Type resolution
//===--------------------------------------------------------------------===//
static VariantLogicalType ShreddedTypeId(const Vector &content, idx_t index) {
	auto &type = content.GetType();
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return VariantLogicalType::OBJECT;
	case LogicalTypeId::LIST:
		return VariantLogicalType::ARRAY;
	case LogicalTypeId::BOOLEAN:
		return FlatVector::GetData<bool>(content)[index] ? VariantLogicalType::BOOL_TRUE
		                                                 : VariantLogicalType::BOOL_FALSE;
	case LogicalTypeId::TINYINT:
		return VariantLogicalType::INT8;
	case LogicalTypeId::SMALLINT:
		return VariantLogicalType::INT16;
	case LogicalTypeId::INTEGER:
		return VariantLogicalType::INT32;
	case LogicalTypeId::BIGINT:
		return VariantLogicalType::INT64;
	case LogicalTypeId::HUGEINT:
		return VariantLogicalType::INT128;
	case LogicalTypeId::UTINYINT:
		return VariantLogicalType::UINT8;
	case LogicalTypeId::USMALLINT:
		return VariantLogicalType::UINT16;
	case LogicalTypeId::UINTEGER:
		return VariantLogicalType::UINT32;
	case LogicalTypeId::UBIGINT:
		return VariantLogicalType::UINT64;
	case LogicalTypeId::UHUGEINT:
		return VariantLogicalType::UINT128;
	case LogicalTypeId::FLOAT:
		return VariantLogicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return VariantLogicalType::DOUBLE;
	case LogicalTypeId::DECIMAL:
		return VariantLogicalType::DECIMAL;
	case LogicalTypeId::VARCHAR:
		return VariantLogicalType::VARCHAR;
	case LogicalTypeId::BLOB:
		return VariantLogicalType::BLOB;
	case LogicalTypeId::UUID:
		return VariantLogicalType::UUID;
	case LogicalTypeId::DATE:
		return VariantLogicalType::DATE;
	case LogicalTypeId::TIME:
		return VariantLogicalType::TIME_MICROS;
	case LogicalTypeId::TIME_NS:
		return VariantLogicalType::TIME_NANOS;
	case LogicalTypeId::TIME_TZ:
		return VariantLogicalType::TIME_MICROS_TZ;
	case LogicalTypeId::TIMESTAMP_SEC:
		return VariantLogicalType::TIMESTAMP_SEC;
	case LogicalTypeId::TIMESTAMP_MS:
		return VariantLogicalType::TIMESTAMP_MILIS;
	case LogicalTypeId::TIMESTAMP:
		return VariantLogicalType::TIMESTAMP_MICROS;
	case LogicalTypeId::TIMESTAMP_NS:
		return VariantLogicalType::TIMESTAMP_NANOS;
	case LogicalTypeId::TIMESTAMP_TZ:
		return VariantLogicalType::TIMESTAMP_MICROS_TZ;
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return VariantLogicalType::TIMESTAMP_NANOS_TZ;
	case LogicalTypeId::INTERVAL:
		return VariantLogicalType::INTERVAL;
	case LogicalTypeId::BIGNUM:
		return VariantLogicalType::BIGNUM;
	case LogicalTypeId::BIT:
		return VariantLogicalType::BITSTRING;
	case LogicalTypeId::GEOMETRY:
		return VariantLogicalType::GEOMETRY;
	default:
		throw NotImplementedException("Shredded VARIANT type '%s' is not supported by VariantIterator",
		                              type.ToString());
	}
}

VariantLogicalType VariantIterator::GetTypeId() const {
	switch (kind) {
	case Kind::NULL_VALUE:
		return VariantLogicalType::VARIANT_NULL;
	case Kind::UNSHREDDED:
		return state->unshredded.GetTypeId(row, value_index);
	case Kind::SHREDDED:
		return ShreddedTypeId(*shredded_content, shredded_index);
	default:
		throw InternalException("VariantIterator::GetTypeId called on a MISSING value");
	}
}

//===--------------------------------------------------------------------===//
// Primitive accessors
//===--------------------------------------------------------------------===//
const_data_ptr_t VariantIterator::GetData() const {
	if (kind == Kind::UNSHREDDED) {
		auto &blob = state->unshredded.GetBlob(row);
		return const_data_ptr_cast(blob.GetData()) + state->unshredded.GetByteOffset(row, value_index);
	}
	D_ASSERT(kind == Kind::SHREDDED);
	auto &content = *shredded_content;
	auto type_size = GetTypeIdSize(content.GetType().InternalType());
	return FlatVector::GetData(content) + shredded_index * type_size;
}

string_t VariantIterator::GetString() const {
	if (kind == Kind::UNSHREDDED) {
		return DecodeStringData(state->unshredded.GetBlob(row), state->unshredded.GetByteOffset(row, value_index));
	}
	D_ASSERT(kind == Kind::SHREDDED);
	return FlatVector::GetData<string_t>(*shredded_content)[shredded_index];
}

VariantDecimalData VariantIterator::GetDecimal() const {
	if (kind == Kind::UNSHREDDED) {
		return DecodeDecimalData(state->unshredded.GetBlob(row), state->unshredded.GetByteOffset(row, value_index));
	}
	D_ASSERT(kind == Kind::SHREDDED);
	auto &type = shredded_content->GetType();
	auto width = DecimalType::GetWidth(type);
	auto scale = DecimalType::GetScale(type);
	auto type_size = GetTypeIdSize(type.InternalType());
	auto value_ptr = FlatVector::GetData(*shredded_content) + shredded_index * type_size;
	return VariantDecimalData(width, scale, value_ptr);
}

//===--------------------------------------------------------------------===//
// Nested accessors
//===--------------------------------------------------------------------===//
vector<pair<string_t, VariantIterator>> VariantIterator::GetObjectChildren() const {
	vector<pair<string_t, VariantIterator>> result;
	if (kind == Kind::UNSHREDDED) {
		auto nested_data =
		    DecodeNestedData(state->unshredded.GetBlob(row), state->unshredded.GetByteOffset(row, value_index));
		result.reserve(nested_data.child_count);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto child_idx = nested_data.children_idx + i;
			auto key_idx = state->unshredded.GetKeysIndex(row, child_idx);
			auto child_values_idx = state->unshredded.GetValuesIndex(row, child_idx);
			result.emplace_back(state->unshredded.GetKey(row, key_idx), MakeUnshredded(*state, row, child_values_idx));
		}
		return result;
	}
	D_ASSERT(kind == Kind::SHREDDED);
	auto &content = *shredded_content;
	auto &child_types = StructType::GetChildTypes(content.GetType());
	auto &child_entries = StructVector::GetEntries(content);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		auto child = ResolveShredded(*state, child_entries[i], shredded_index, row);
		if (child.IsMissing()) {
			//! The field is absent for this row
			continue;
		}
		auto &name = child_types[i].first;
		result.emplace_back(string_t(name.c_str(), NumericCast<uint32_t>(name.size())), child);
	}
	if (overlay_value_index != 0) {
		//! Merge the leftover fields that did not fit the shredded schema
		auto overlay = MakeUnshredded(*state, row, overlay_value_index - 1);
		auto overlay_children = overlay.GetObjectChildren();
		for (auto &entry : overlay_children) {
			result.emplace_back(std::move(entry));
		}
	}
	return result;
}

vector<VariantIterator> VariantIterator::GetArrayChildren() const {
	vector<VariantIterator> result;
	if (kind == Kind::UNSHREDDED) {
		auto nested_data =
		    DecodeNestedData(state->unshredded.GetBlob(row), state->unshredded.GetByteOffset(row, value_index));
		result.reserve(nested_data.child_count);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto child_values_idx = state->unshredded.GetValuesIndex(row, nested_data.children_idx + i);
			result.emplace_back(MakeUnshredded(*state, row, child_values_idx));
		}
		return result;
	}
	D_ASSERT(kind == Kind::SHREDDED);
	auto &content = *shredded_content;
	auto list_data = FlatVector::GetData<list_entry_t>(content);
	auto &element = ListVector::GetChild(content);
	auto &entry = list_data[shredded_index];
	result.reserve(entry.length);
	for (idx_t i = 0; i < entry.length; i++) {
		result.emplace_back(ResolveShredded(*state, element, entry.offset + i, row));
	}
	return result;
}

} // namespace duckdb
