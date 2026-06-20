#include "reader/variant/parquet_variant_iterator.hpp"

#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <set>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Shredded leaf conversion (typed_value -> VariantValue)
//===--------------------------------------------------------------------===//
template <class T>
struct ConvertShreddedValue {
	static VariantValue Convert(T val);
	static VariantValue ConvertDecimal(T val, uint8_t width, uint8_t scale) {
		throw InternalException("ConvertShreddedValue::ConvertDecimal not implemented for type");
	}
	static VariantValue ConvertBlob(T val) {
		throw InternalException("ConvertShreddedValue::ConvertBlob not implemented for type");
	}
};

//! boolean
template <>
VariantValue ConvertShreddedValue<bool>::Convert(bool val) {
	return VariantValue(Value::BOOLEAN(val));
}
//! int8
template <>
VariantValue ConvertShreddedValue<int8_t>::Convert(int8_t val) {
	return VariantValue(Value::TINYINT(val));
}
//! int16
template <>
VariantValue ConvertShreddedValue<int16_t>::Convert(int16_t val) {
	return VariantValue(Value::SMALLINT(val));
}
//! int32
template <>
VariantValue ConvertShreddedValue<int32_t>::Convert(int32_t val) {
	return VariantValue(Value::INTEGER(val));
}
//! int64
template <>
VariantValue ConvertShreddedValue<int64_t>::Convert(int64_t val) {
	return VariantValue(Value::BIGINT(val));
}
//! float
template <>
VariantValue ConvertShreddedValue<float>::Convert(float val) {
	return VariantValue(Value::FLOAT(val));
}
//! double
template <>
VariantValue ConvertShreddedValue<double>::Convert(double val) {
	return VariantValue(Value::DOUBLE(val));
}
//! NOTE: decimal2 - not in the spec, but some writers create this regardless
template <>
VariantValue ConvertShreddedValue<int16_t>::ConvertDecimal(int16_t val, uint8_t width, uint8_t scale) {
	return VariantValue(Value::DECIMAL(val, width, scale));
}
//! decimal4/decimal8/decimal16
template <>
VariantValue ConvertShreddedValue<int32_t>::ConvertDecimal(int32_t val, uint8_t width, uint8_t scale) {
	return VariantValue(Value::DECIMAL(val, width, scale));
}
template <>
VariantValue ConvertShreddedValue<int64_t>::ConvertDecimal(int64_t val, uint8_t width, uint8_t scale) {
	return VariantValue(Value::DECIMAL(val, width, scale));
}
template <>
VariantValue ConvertShreddedValue<hugeint_t>::ConvertDecimal(hugeint_t val, uint8_t width, uint8_t scale) {
	return VariantValue(Value::DECIMAL(val, width, scale));
}
//! date
template <>
VariantValue ConvertShreddedValue<date_t>::Convert(date_t val) {
	return VariantValue(Value::DATE(val));
}
//! time
template <>
VariantValue ConvertShreddedValue<dtime_t>::Convert(dtime_t val) {
	return VariantValue(Value::TIME(val));
}
//! timestamptz(6)
template <>
VariantValue ConvertShreddedValue<timestamp_tz_t>::Convert(timestamp_tz_t val) {
	return VariantValue(Value::TIMESTAMPTZ(val));
}
//! timestamptz(9)
template <>
VariantValue ConvertShreddedValue<timestamp_tz_ns_t>::Convert(timestamp_tz_ns_t val) {
	return VariantValue(Value::TIMESTAMPTZNS(val));
}
//! timestampntz(6)
template <>
VariantValue ConvertShreddedValue<timestamp_t>::Convert(timestamp_t val) {
	return VariantValue(Value::TIMESTAMP(val));
}
//! timestampntz(9)
template <>
VariantValue ConvertShreddedValue<timestamp_ns_t>::Convert(timestamp_ns_t val) {
	return VariantValue(Value::TIMESTAMPNS(val));
}
//! binary
template <>
VariantValue ConvertShreddedValue<string_t>::ConvertBlob(string_t val) {
	return VariantValue(Value(Blob::ToBase64(val)));
}
//! string
template <>
VariantValue ConvertShreddedValue<string_t>::Convert(string_t val) {
	if (!Utf8Proc::IsValid(val.GetData(), val.GetSize())) {
		throw InternalException("Can't decode Variant string, it isn't valid UTF8");
	}
	return VariantValue(Value(val.GetString()));
}
//! uuid
template <>
VariantValue ConvertShreddedValue<hugeint_t>::Convert(hugeint_t val) {
	return VariantValue(Value::UUID(val));
}

template <class T>
static T LeafData(const ShreddedGroupView &view, idx_t typed_idx) {
	return UnifiedVectorFormat::GetData<T>(view.typed_format)[typed_idx];
}

//! Convert the (valid) shredded leaf 'view' at logical position 'index' into a VariantValue
static VariantValue ConvertLeaf(const ShreddedGroupView &view, idx_t index) {
	auto typed_idx = view.typed_format.sel->get_index(index);
	auto &type = view.typed_type;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return ConvertShreddedValue<bool>::Convert(LeafData<bool>(view, typed_idx));
	case LogicalTypeId::TINYINT:
		return ConvertShreddedValue<int8_t>::Convert(LeafData<int8_t>(view, typed_idx));
	case LogicalTypeId::SMALLINT:
		return ConvertShreddedValue<int16_t>::Convert(LeafData<int16_t>(view, typed_idx));
	case LogicalTypeId::INTEGER:
		return ConvertShreddedValue<int32_t>::Convert(LeafData<int32_t>(view, typed_idx));
	case LogicalTypeId::BIGINT:
		return ConvertShreddedValue<int64_t>::Convert(LeafData<int64_t>(view, typed_idx));
	case LogicalTypeId::FLOAT:
		return ConvertShreddedValue<float>::Convert(LeafData<float>(view, typed_idx));
	case LogicalTypeId::DOUBLE:
		return ConvertShreddedValue<double>::Convert(LeafData<double>(view, typed_idx));
	case LogicalTypeId::DECIMAL: {
		uint8_t width;
		uint8_t scale;
		type.GetDecimalProperties(width, scale);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			//! NOTE: This is not spec compliant, but some writers shred DECIMAL2
			return ConvertShreddedValue<int16_t>::ConvertDecimal(LeafData<int16_t>(view, typed_idx), width, scale);
		case PhysicalType::INT32:
			return ConvertShreddedValue<int32_t>::ConvertDecimal(LeafData<int32_t>(view, typed_idx), width, scale);
		case PhysicalType::INT64:
			return ConvertShreddedValue<int64_t>::ConvertDecimal(LeafData<int64_t>(view, typed_idx), width, scale);
		case PhysicalType::INT128:
			return ConvertShreddedValue<hugeint_t>::ConvertDecimal(LeafData<hugeint_t>(view, typed_idx), width, scale);
		default:
			throw NotImplementedException("Decimal with PhysicalType (%s) not implemented for shredded Variant",
			                              EnumUtil::ToString(type.InternalType()));
		}
	}
	case LogicalTypeId::DATE:
		return ConvertShreddedValue<date_t>::Convert(LeafData<date_t>(view, typed_idx));
	case LogicalTypeId::TIME:
		return ConvertShreddedValue<dtime_t>::Convert(LeafData<dtime_t>(view, typed_idx));
	case LogicalTypeId::TIMESTAMP_TZ:
		return ConvertShreddedValue<timestamp_tz_t>::Convert(LeafData<timestamp_tz_t>(view, typed_idx));
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return ConvertShreddedValue<timestamp_tz_ns_t>::Convert(LeafData<timestamp_tz_ns_t>(view, typed_idx));
	case LogicalTypeId::TIMESTAMP:
		return ConvertShreddedValue<timestamp_t>::Convert(LeafData<timestamp_t>(view, typed_idx));
	case LogicalTypeId::TIMESTAMP_NS:
		return ConvertShreddedValue<timestamp_ns_t>::Convert(LeafData<timestamp_ns_t>(view, typed_idx));
	case LogicalTypeId::BLOB:
		return ConvertShreddedValue<string_t>::ConvertBlob(LeafData<string_t>(view, typed_idx));
	case LogicalTypeId::VARCHAR:
		return ConvertShreddedValue<string_t>::Convert(LeafData<string_t>(view, typed_idx));
	case LogicalTypeId::UUID:
		return ConvertShreddedValue<hugeint_t>::Convert(LeafData<hugeint_t>(view, typed_idx));
	default:
		throw NotImplementedException("Variant shredding on type: '%s' is not implemented", type.ToString());
	}
}

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
	arena.clear();
}

const VariantMetadata &ParquetVariantIterator::GetMetadata() const {
	if (!current_metadata) {
		auto &blob = metadata_data[metadata_format.sel->get_index(current_row)];
		current_metadata = make_uniq<VariantMetadata>(blob);
	}
	return *current_metadata;
}

const VariantValue *ParquetVariantIterator::ArenaAdd(VariantValue value) const {
	arena.push_back(make_uniq<VariantValue>(std::move(value)));
	return arena.back().get();
}

VariantValue ParquetVariantIterator::DecodeBinary(const ShreddedGroupView &view, idx_t value_index) const {
	auto binary = view.value_data[value_index].GetData();
	return VariantBinaryDecoder::Decode(GetMetadata(), const_data_ptr_cast(binary));
}

ParquetVariantNode ParquetVariantIterator::ResolveGroup(const ShreddedGroupView &view, idx_t index) const {
	if (view.has_typed_value) {
		auto typed_idx = view.typed_format.sel->get_index(index);
		if (view.typed_format.validity.RowIsValid(typed_idx)) {
			switch (view.kind) {
			case ParquetGroupKind::LEAF:
				//! A leaf is never partially shredded, so the binary 'value' is irrelevant here
				return ParquetVariantNode::MakeValue(ArenaAdd(ConvertLeaf(view, index)));
			case ParquetGroupKind::ARRAY:
				return ParquetVariantNode::MakeArray(*this, view, index);
			case ParquetGroupKind::OBJECT: {
				//! (Partially) shredded object - the binary 'value', if present, holds the leftover fields
				const VariantValue *overlay = nullptr;
				auto value_idx = view.value_format.sel->get_index(index);
				if (view.value_format.validity.RowIsValid(value_idx)) {
					auto decoded = DecodeBinary(view, value_idx);
					if (decoded.value_type != VariantValueType::OBJECT) {
						throw InvalidInputException(
						    "Partially shredded objects have to encode Object Variants in the 'value'");
					}
					overlay = ArenaAdd(std::move(decoded));
				}
				return ParquetVariantNode::MakeObject(*this, view, index, overlay);
			}
			}
		}
	}

	//! No (valid) shredded value - fall back to the binary 'value'
	auto value_idx = view.value_format.sel->get_index(index);
	if (view.value_format.validity.RowIsValid(value_idx)) {
		auto decoded = DecodeBinary(view, value_idx);
		if (view.has_typed_value && view.kind == ParquetGroupKind::OBJECT &&
		    decoded.value_type == VariantValueType::OBJECT) {
			throw InvalidInputException(
			    "When 'typed_value' for a shredded Object is NULL, 'value' can not contain an Object value");
		}
		return ParquetVariantNode::MakeValue(ArenaAdd(std::move(decoded)));
	}
	return ParquetVariantNode::MakeMissing();
}

ParquetVariantNode ParquetVariantIterator::Root(idx_t row) const {
	auto root = ResolveGroup(root_view, row);
	//! A root value is never "missing" - treat any such case as a SQL NULL
	return root.IsMissing() ? ParquetVariantNode::MakeNull() : root;
}

//===--------------------------------------------------------------------===//
// ParquetVariantNode - nested accessors
//===--------------------------------------------------------------------===//
ParquetObjectIterator ParquetVariantNode::GetObjectChildren(VariantIterationOrder order) const {
	D_ASSERT(kind == Kind::OBJECT);
	(void)order;
	return ParquetObjectIterator(*state, *view, index, overlay);
}

ParquetArrayIterator ParquetVariantNode::GetArrayChildren() const {
	D_ASSERT(kind == Kind::ARRAY);
	return ParquetArrayIterator(*state, *view, index);
}

//===--------------------------------------------------------------------===//
// ParquetObjectIterator
//===--------------------------------------------------------------------===//
ParquetObjectIterator::ParquetObjectIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view,
                                             idx_t index, const VariantValue *overlay) {
	//! Typed (shredded) fields - skipping the ones that are missing for this row
	std::set<string> typed_keys;
	for (idx_t i = 0; i < view.fields.size(); i++) {
		auto node = state.ResolveGroup(*view.fields[i], index);
		if (node.IsMissing()) {
			continue;
		}
		auto &name = view.field_names[i];
		typed_keys.insert(name);
		ordered_entries.push_back(ParquetObjectEntry {string_t(name), node});
	}
	//! Leftover (overlay) fields from the binary 'value' - typed fields win on key collisions
	if (overlay) {
		for (auto &entry : overlay->ObjectChildren()) {
			if (typed_keys.count(entry.first)) {
				continue;
			}
			ordered_entries.push_back(
			    ParquetObjectEntry {string_t(entry.first), ParquetVariantNode::MakeValue(&entry.second)});
		}
	}
	std::sort(ordered_entries.begin(), ordered_entries.end(),
	          [](const ParquetObjectEntry &a, const ParquetObjectEntry &b) { return a.key < b.key; });
}

//===--------------------------------------------------------------------===//
// ParquetArrayIterator
//===--------------------------------------------------------------------===//
ParquetArrayIterator::ParquetArrayIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view,
                                           idx_t index)
    : state(state), element(*view.element) {
	auto &entry = view.list_data[view.typed_format.sel->get_index(index)];
	base = entry.offset;
	length = entry.length;
}

ParquetVariantNode ParquetArrayIterator::operator[](idx_t i) const {
	return state.get().ResolveGroup(element.get(), base + i);
}

//===--------------------------------------------------------------------===//
// ParquetVariantIteratorSource
//===--------------------------------------------------------------------===//
bool ParquetVariantIteratorSource::Emit(idx_t row, VariantBuilder &builder) {
	iterator.BeginRow(row);
	auto root = iterator.Root(row);
	if (root.IsNull() || root.IsMissing()) {
		return true;
	}
	//! A top-level variant-null (a binary value decoding to NULL) is a genuine SQL NULL row
	auto value = root.AsValue();
	if (value && (value->IsNull() || value->IsMissing())) {
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
