#include "parquet_column_kv.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

ChildColumnKV::ChildColumnKV() : children(make_uniq<case_insensitive_map_t<ColumnKV>>()) {
}

ChildColumnKV ChildColumnKV::Copy() const {
	ChildColumnKV result;
	for (const auto &child : *children) {
		result.children->emplace(child.first, child.second.Copy());
	}
	return result;
}

optional_ptr<const ColumnKV> ChildColumnKV::GetChild(const string &name) const {
	auto it = children->find(name);
	if (it == children->end()) {
		return nullptr;
	}
	return it->second;
}

ColumnKV::ColumnKV() {
}

ColumnKV ColumnKV::Copy() const {
	ColumnKV result;
	result.metadata = metadata;
	result.children = children.Copy();
	return result;
}

static case_insensitive_map_t<LogicalType> GetChildNameToTypeMap(const LogicalType &type) {
	case_insensitive_map_t<LogicalType> name_to_type_map;
	switch (type.id()) {
	case LogicalTypeId::LIST:
		name_to_type_map.emplace("element", ListType::GetChildType(type));
		break;
	case LogicalTypeId::ARRAY:
		name_to_type_map.emplace("element", ArrayType::GetChildType(type));
		break;
	case LogicalTypeId::MAP:
		name_to_type_map.emplace("key", MapType::KeyType(type));
		name_to_type_map.emplace("value", MapType::ValueType(type));
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			name_to_type_map.emplace(child_type);
		}
		break;
	default: // LCOV_EXCL_START
		throw InternalException("Unexpected type in GetChildNameToTypeMap");
	} // LCOV_EXCL_STOP
	return name_to_type_map;
}

//! Nested types that COLUMN_KV_METADATA can recurse into (and that GetChildNameToTypeMap handles). This is
//! intentionally narrower than LogicalType::IsNested(): VARIANT/UNION/AGGREGATE_STATE are nested but expose no
//! stable user-facing child surface to attach key/value metadata to, so they are rejected rather than recursed.
static bool IsRecursableNestedType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return true;
	default:
		return false;
	}
}

static string ColumnKVValueToString(const Value &value, const string &col_name) {
	if (value.IsNull()) {
		throw BinderException("COLUMN_KV_METADATA: NULL key/value not allowed for column \"%s\"", col_name);
	}
	const auto type_id = value.type().id();
	if (type_id != LogicalTypeId::VARCHAR && type_id != LogicalTypeId::BLOB) {
		throw BinderException("COLUMN_KV_METADATA: key/value for column \"%s\" must be VARCHAR or BLOB, found \"%s\"",
		                      col_name, value.type().ToString());
	}
	// VARCHAR and BLOB are both string-backed; StringValue::Get preserves raw bytes (including embedded NULs).
	return StringValue::Get(value);
}

void ColumnKV::GetColumnKV(const Value &kv_value, ChildColumnKV &kv,
                           const case_insensitive_map_t<LogicalType> &name_to_type_map) {
	const auto &struct_type = kv_value.type();
	if (struct_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("COLUMN_KV_METADATA must be a STRUCT of column names, e.g., {col1: MAP {'k': 'v'}, col2: "
		                      "{nested_col: MAP {'k': 'v'}}}");
	}
	if (kv_value.IsNull()) {
		throw BinderException("COLUMN_KV_METADATA must not be NULL");
	}
	const auto &struct_children = StructValue::GetChildren(kv_value);
	D_ASSERT(StructType::GetChildTypes(struct_type).size() == struct_children.size());
	for (idx_t i = 0; i < struct_children.size(); i++) {
		const auto &col_name = StructType::GetChildName(struct_type, i);

		auto it = name_to_type_map.find(col_name);
		if (it == name_to_type_map.end()) {
			string names;
			for (const auto &name : name_to_type_map) {
				if (!names.empty()) {
					names += ", ";
				}
				names += name.first;
			}
			throw BinderException("Column name \"%s\" specified in COLUMN_KV_METADATA not found. Consider using "
			                      "WRITE_PARTITION_COLUMNS if this column is a partition column. Available column "
			                      "names: [%s]",
			                      col_name, names);
		}
		const auto &col_type = it->second;

		const auto &child_value = struct_children[i];
		const auto &child_type = child_value.type();
		if (child_value.IsNull()) {
			throw BinderException("COLUMN_KV_METADATA: specification for column \"%s\" must not be NULL", col_name);
		}

		ColumnKV column_kv;
		if (child_type.id() == LogicalTypeId::MAP) {
			// Leaf KV: the column must be a primitive (non-nested) type. VARIANT/UNION/AGGREGATE_STATE are nested
			// (PhysicalType::STRUCT) and have no leaf column chunk to carry KV, so they are rejected here rather
			// than silently dropped at write time.
			if (col_type.IsNested()) {
				throw BinderException("COLUMN_KV_METADATA: column \"%s\" with type \"%s\" is a nested type; specify "
				                      "key/value metadata on its leaf columns",
				                      col_name, LogicalTypeIdToString(col_type.id()));
			}
			const auto &entries = MapValue::GetChildren(child_value);
			for (const auto &entry : entries) {
				const auto &entry_children = StructValue::GetChildren(entry);
				D_ASSERT(entry_children.size() == 2);
				auto key = ColumnKVValueToString(entry_children[0], col_name);
				auto value = ColumnKVValueToString(entry_children[1], col_name);
				column_kv.metadata.emplace_back(std::move(key), std::move(value));
			}
		} else if (child_type.id() == LogicalTypeId::STRUCT) {
			// Nesting: the column must be a nested type we can recurse into (LIST/ARRAY/MAP/STRUCT).
			if (!IsRecursableNestedType(col_type)) {
				throw BinderException("COLUMN_KV_METADATA: column \"%s\" with type \"%s\" does not support nested "
				                      "key/value metadata",
				                      col_name, LogicalTypeIdToString(col_type.id()));
			}
			GetColumnKV(child_value, column_kv.children, GetChildNameToTypeMap(col_type));
		} else {
			throw BinderException("COLUMN_KV_METADATA: \"%s\" must be a STRUCT (nesting) or MAP (leaf)", col_name);
		}

		// Case-insensitive duplicate column names are already rejected by the STRUCT literal binder.
		kv.children->emplace(col_name, std::move(column_kv));
	}
}

} // namespace duckdb
