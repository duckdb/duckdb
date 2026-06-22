#include "reader/variant/parquet_variant_iterator.hpp"

#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/types/blob.hpp"

#include <cstring>

namespace duckdb {

namespace {

//! A shredded node is either a "picked off" bare primitive (fully convertible - referenced directly), or a
//! wrapper STRUCT("typed_value" <T>, "untyped_value_index" UINTEGER) (see SetShreddedType in
//! variant_shredding.cpp). The (row-local, 1-based) untyped_value_index points at the leftover value in the
//! row's unshredded pool; NULL means "no leftover" (VARIANT_NULL / missing -> see is_object_field).
constexpr idx_t SHRED_TYPED_VALUE = 0;
constexpr idx_t SHRED_UNTYPED_INDEX = 1;

//===--------------------------------------------------------------------===//
// Analysis: which subtrees can be "picked off" (fully shredded -> flattened + referenced)
//===--------------------------------------------------------------------===//
//! Whether the group's binary 'value' column is entirely NULL (no leftover anywhere in the chunk)
bool ValueAllNull(const ShreddedGroupView &view, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		if ((*view.value)[i].IsValid()) {
			return false;
		}
	}
	return true;
}

//! A plan mirroring the group tree: 'flat' marks a primitive leaf that is fully convertible and is emitted
//! as a bare (referenced) column instead of a wrapper.
struct ShredPlan {
	bool flat = false;
	vector<ShredPlan> fields; //! OBJECT
	unique_ptr<ShredPlan> element;
};

ShredPlan AnalyzeShred(const ShreddedGroupView &view, idx_t count, bool is_object_field) {
	ShredPlan plan;
	if (!view.has_typed_value) {
		return plan; //! everything is leftover -> wrapper
	}
	switch (view.kind) {
	case ParquetGroupKind::LEAF: {
		//! A leaf is fully convertible if there is no binary leftover; an OBJECT field additionally must never
		//! be missing (a NULL typed_value of a flat field would read as VARIANT_NULL, not "missing")
		bool no_leftover = ValueAllNull(view, count);
		bool no_missing = !is_object_field || view.leaf_format.validity.CheckAllValid(count);
		plan.flat = no_leftover && no_missing;
		break;
	}
	case ParquetGroupKind::OBJECT: {
		plan.fields.reserve(view.fields.size());
		for (auto &field : view.fields) {
			plan.fields.push_back(AnalyzeShred(*field, count, true));
		}
		break;
	}
	default: {
		D_ASSERT(view.kind == ParquetGroupKind::ARRAY);
		auto child_count = ListVector::GetListSize(*view.typed_value_vec);
		plan.element = make_uniq<ShredPlan>(AnalyzeShred(*view.element, child_count, false));
		break;
	}
	}
	return plan;
}

//===--------------------------------------------------------------------===//
// Stage 1: derive the shred type
//===--------------------------------------------------------------------===//
LogicalType DeriveShredNodeType(const ShreddedGroupView &view, const ShredPlan &plan);

//! The 'typed_value' (<T>) of a node: a primitive, a STRUCT of field nodes, or a LIST of an element node.
LogicalType DeriveTypedValueType(const ShreddedGroupView &view, const ShredPlan &plan) {
	if (!view.has_typed_value) {
		//! Nothing is shredded here - a placeholder all-NULL column, everything goes to the unshredded pool
		return LogicalType::INTEGER;
	}
	switch (view.kind) {
	case ParquetGroupKind::LEAF:
		//! Parquet emits binary as base64 VARCHAR, so a BLOB leaf is converted (not referenced) - keep the
		//! shredded type VARCHAR so it unshreds to the same logical value
		if (view.typed_type.id() == LogicalTypeId::BLOB) {
			return LogicalType::VARCHAR;
		}
		return view.typed_type;
	case ParquetGroupKind::OBJECT: {
		child_list_t<LogicalType> fields;
		for (idx_t i = 0; i < view.fields.size(); i++) {
			fields.emplace_back(view.field_names[i], DeriveShredNodeType(*view.fields[i], plan.fields[i]));
		}
		return LogicalType::STRUCT(std::move(fields));
	}
	default:
		D_ASSERT(view.kind == ParquetGroupKind::ARRAY);
		return LogicalType::LIST(DeriveShredNodeType(*view.element, *plan.element));
	}
}

LogicalType DeriveShredNodeType(const ShreddedGroupView &view, const ShredPlan &plan) {
	if (plan.flat) {
		//! picked off: a bare primitive column (no untyped_value_index needed)
		return DeriveTypedValueType(view, plan);
	}
	child_list_t<LogicalType> children;
	children.emplace_back("typed_value", DeriveTypedValueType(view, plan));
	children.emplace_back("untyped_value_index", LogicalType::UINTEGER);
	return LogicalType::STRUCT(std::move(children));
}

//===--------------------------------------------------------------------===//
// Stage 2a: fill the typed_value tree (referencing Parquet leaves where possible)
//===--------------------------------------------------------------------===//
//! Reference (or, for BLOB, base64-convert) the Parquet leaf into 'out'
void FillLeafTypedValue(const ShreddedGroupView &view, Vector &out, idx_t count) {
	if (view.typed_type.id() == LogicalTypeId::BLOB) {
		//! base64-encode the binary leaf into the VARCHAR typed_value
		auto blob_data = UnifiedVectorFormat::GetData<string_t>(view.leaf_format);
		auto out_data = FlatVector::GetDataMutable<string_t>(out);
		auto &out_validity = FlatVector::ValidityMutable(out);
		for (idx_t i = 0; i < count; i++) {
			auto idx = view.leaf_format.sel->get_index(i);
			if (!view.leaf_format.validity.RowIsValid(idx)) {
				out_validity.SetInvalid(i);
				continue;
			}
			out_data[i] = StringVector::AddString(out, Blob::ToBase64(blob_data[idx]));
		}
	} else {
		//! zero-copy: the Parquet leaf maps exactly to the shredded type
		out.Reference(*view.typed_value_vec);
	}
}

//! Captures the (per-node) untyped_value_index target so the leftover pass can wire it up. A "flat" node has
//! no untyped_value_index (untyped_index_data stays null) and never contributes a leftover.
struct ShredNodeWriter {
	uint32_t *untyped_index_data = nullptr;
	ValidityMask *untyped_index_validity = nullptr;
	vector<unique_ptr<ShredNodeWriter>> fields; //! OBJECT
	unique_ptr<ShredNodeWriter> element;        //! ARRAY
};

//! 'is_object_field': whether this node is a field of an OBJECT. For object fields the "no leftover" default
//! is 0 (== a missing field), since a NULL untyped_value_index means a present VARIANT_NULL value. For the
//! root / array elements (no notion of "missing") the default is NULL (== VARIANT_NULL).
void FillShredNode(const ShreddedGroupView &view, const ShredPlan &plan, Vector &node, idx_t count,
                   ShredNodeWriter &writer, bool is_object_field) {
	if (plan.flat) {
		//! picked off: 'node' is the bare primitive column - reference it, no untyped_value_index
		FillLeafTypedValue(view, node, count);
		return;
	}

	auto &entries = StructVector::GetEntries(node);
	auto &typed_value = entries[SHRED_TYPED_VALUE];
	auto &untyped_index = entries[SHRED_UNTYPED_INDEX];

	writer.untyped_index_data = FlatVector::GetDataMutable<uint32_t>(untyped_index);
	writer.untyped_index_validity = &FlatVector::ValidityMutable(untyped_index);
	if (is_object_field) {
		memset(writer.untyped_index_data, 0, count * sizeof(uint32_t));
	} else {
		writer.untyped_index_validity->SetAllInvalid(count);
	}

	if (!view.has_typed_value) {
		FlatVector::ValidityMutable(typed_value).SetAllInvalid(count);
		return;
	}

	switch (view.kind) {
	case ParquetGroupKind::LEAF:
		FillLeafTypedValue(view, typed_value, count);
		break;
	case ParquetGroupKind::OBJECT: {
		//! typed_value is a STRUCT of field nodes; its validity is the object's shredded-ness
		auto &dst_validity = FlatVector::ValidityMutable(typed_value);
		for (idx_t i = 0; i < count; i++) {
			if (!view.typed_validity->IsValid(i)) {
				dst_validity.SetInvalid(i);
			}
		}
		auto &field_entries = StructVector::GetEntries(typed_value);
		writer.fields.resize(view.fields.size());
		for (idx_t i = 0; i < view.fields.size(); i++) {
			writer.fields[i] = make_uniq<ShredNodeWriter>();
			FillShredNode(*view.fields[i], plan.fields[i], field_entries[i], count, *writer.fields[i], true);
		}
		break;
	}
	default: {
		D_ASSERT(view.kind == ParquetGroupKind::ARRAY);
		//! typed_value is a LIST of element nodes; copy the Parquet list entries (the element child is built
		//! 1:1, so the offsets align) and recurse into the element node over the list child
		auto out_data = FlatVector::GetDataMutable<list_entry_t>(typed_value);
		auto &out_validity = FlatVector::ValidityMutable(typed_value);
		for (idx_t i = 0; i < count; i++) {
			auto entry = (*view.list)[i];
			if (!entry.IsValid()) {
				out_validity.SetInvalid(i);
				out_data[i] = list_entry_t(0, 0);
			} else {
				out_data[i] = entry.GetValueUnsafe();
			}
		}
		auto child_count = ListVector::GetListSize(*view.typed_value_vec);
		ListVector::Reserve(typed_value, child_count);
		ListVector::SetListSize(typed_value, child_count);
		writer.element = make_uniq<ShredNodeWriter>();
		FillShredNode(*view.element, *plan.element, ListVector::GetChildMutable(typed_value), child_count,
		              *writer.element, false);
		break;
	}
	}
}

//===--------------------------------------------------------------------===//
// Stage 2b: build the unshredded pool + wire untyped_value_index (per row)
//===--------------------------------------------------------------------===//
bool TypedValid(const ShreddedGroupView &view, idx_t index) {
	switch (view.kind) {
	case ParquetGroupKind::LEAF:
		return view.leaf_format.validity.RowIsValid(view.leaf_format.sel->get_index(index));
	case ParquetGroupKind::ARRAY:
		return (*view.list)[index].IsValid();
	default:
		D_ASSERT(view.kind == ParquetGroupKind::OBJECT);
		return view.typed_validity->IsValid(index);
	}
}

//! A root that resolves to a (variant) NULL / missing is a genuine SQL NULL row (matches the unshredded path)
bool IsRowNull(const ParquetVariantNode &root) {
	return root.IsNull() || root.GetTypeId() == VariantLogicalType::VARIANT_NULL;
}

//! Initializes an all-NULL unshredded pool for a fully picked-off (flat root) column - it is never accessed
struct EmptyUnshreddedSource {
	bool Emit(idx_t row, VariantBuilder &builder) {
		return true;
	}
};

struct ShreddedLeftoverSource {
	ParquetVariantIterator &iterator;
	ShredNodeWriter &root_writer;
	ValidityMask &shredded_validity;

	bool Emit(idx_t row, VariantBuilder &builder) {
		iterator.BeginRow(row);
		auto root = iterator.Root(row);
		if (IsRowNull(root)) {
			shredded_validity.SetInvalid(row);
			return true;
		}
		EmitNode(iterator.GetRootView(), root_writer, row, builder);
		return false;
	}

	//! Emit the binary value (one subtree) into the unshredded pool, recording its 1-based row-local index in
	//! this node's untyped_value_index
	void EmitLeftover(const string_t &value, ShredNodeWriter &writer, idx_t index, VariantBuilder &builder) {
		auto data = const_data_ptr_cast(value.GetData());
		auto local = builder.LocalValue();
		iterator.EmitBinary(data, builder);
		writer.untyped_index_data[index] = local + 1;
		writer.untyped_index_validity->SetValid(index);
	}

	void EmitNode(const ShreddedGroupView &view, ShredNodeWriter &writer, idx_t index, VariantBuilder &builder) {
		auto value_entry = (*view.value)[index];
		bool value_present = value_entry.IsValid();
		bool typed_valid = view.has_typed_value && TypedValid(view, index);

		if (typed_valid) {
			if (view.kind == ParquetGroupKind::OBJECT) {
				//! A partially-shredded object: the 'value' blob holds the leftover (overlay) fields
				if (value_present) {
					EmitLeftover(value_entry.GetValueUnsafe(), writer, index, builder);
				}
				for (idx_t i = 0; i < view.fields.size(); i++) {
					EmitNode(*view.fields[i], *writer.fields[i], index, builder);
				}
			} else if (view.kind == ParquetGroupKind::ARRAY) {
				auto entry = (*view.list)[index].GetValueUnsafe();
				for (idx_t j = 0; j < entry.length; j++) {
					EmitNode(*view.element, *writer.element, entry.offset + j, builder);
				}
			}
			//! LEAF (incl. picked-off): fully shredded, no leftover
		} else if (value_present) {
			//! Not shredded - the whole value is a leftover (do not recurse: the subtree lives in this value)
			EmitLeftover(value_entry.GetValueUnsafe(), writer, index, builder);
		}
		//! else: missing / VARIANT_NULL - leave untyped_value_index at its default
	}
};

} // namespace

void ParquetVariantConversion::ConvertToShredded(Vector &metadata, Vector &group, Vector &result, idx_t count) {
	ParquetVariantIterator iterator(metadata, group);
	auto &root_view = iterator.GetRootView();

	//! Analysis + Stage 1: pick off fully-convertible subtrees, derive STRUCT("unshredded", "shredded")
	auto root_plan = AnalyzeShred(root_view, count, false);
	child_list_t<LogicalType> shredded_data_children;
	shredded_data_children.emplace_back("unshredded", VariantShredding::GetUnshreddedType());
	shredded_data_children.emplace_back("shredded", DeriveShredNodeType(root_view, root_plan));
	Vector shredded_data(LogicalType::STRUCT(std::move(shredded_data_children)), count);

	auto &shredded_data_entries = StructVector::GetEntries(shredded_data);
	auto &unshredded = shredded_data_entries[0];
	auto &shredded = shredded_data_entries[1];

	if (root_plan.flat) {
		//! The whole column is a fully-shredded primitive: reference it; the unshredded pool is unused
		FillLeafTypedValue(root_view, shredded, count);
		EmptyUnshreddedSource empty;
		BuildVariant(empty, count, unshredded);
	} else {
		//! Stage 2a: fill the typed_value tree (referencing leaves that map exactly)
		ShredNodeWriter root_writer;
		FillShredNode(root_view, root_plan, shredded, count, root_writer, false);
		//! Stage 2b: build the unshredded pool from leftovers, wire untyped_value_index, set row validity
		ShreddedLeftoverSource source {iterator, root_writer, FlatVector::ValidityMutable(shredded)};
		BuildVariant(source, count, unshredded);
	}

	FlatVector::SetSize(shredded_data, count_t(count));
	result.Shred(shredded_data, count);
}

} // namespace duckdb
