#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName *type_name) {
	auto name = (reinterpret_cast<duckdb_libpgquery::PGValue *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	LogicalType base_type = TransformStringToLogicalType(name);

	if (base_type == LogicalTypeId::STRUCT) {
		D_ASSERT(type_name->typmods && type_name->typmods->length > 0);
		child_list_t<LogicalType> children;
		unordered_set<string> name_collision_set;

		for (auto node = type_name->typmods->head; node; node = node->next) {
			auto &type_val = *((duckdb_libpgquery::PGList *)node->data.ptr_value);
			D_ASSERT(type_val.length == 2);

			auto entry_name_node = (duckdb_libpgquery::PGValue *)(type_val.head->data.ptr_value);
			D_ASSERT(entry_name_node->type == duckdb_libpgquery::T_PGString);
			auto entry_type_node = (duckdb_libpgquery::PGValue *)(type_val.tail->data.ptr_value);
			D_ASSERT(entry_type_node->type == duckdb_libpgquery::T_PGTypeName);

			auto entry_name = string(entry_name_node->val.str);
			D_ASSERT(!entry_name.empty());

			if (name_collision_set.find(entry_name) != name_collision_set.end()) {
				throw ParserException("Duplicate struct entry name \"%s\"", entry_name);
			}
			name_collision_set.insert(entry_name);

			auto entry_type = TransformTypeName((duckdb_libpgquery::PGTypeName *)entry_type_node);
			children.push_back(make_pair(entry_name, entry_type));
		}
		D_ASSERT(!children.empty());
		return LogicalType(base_type.id(), children);
	}

	if (base_type == LogicalTypeId::MAP) {
		//! We transform MAP<TYPE_KEY, TYPE_VALUE> to STRUCT<LIST<key: TYPE_KEY>, LIST<value: TYPE_VALUE>>

		if (!type_name->typmods || type_name->typmods->length != 2) {
			throw ParserException("Map type needs exactly two entries, key and value type");
		}
		child_list_t<LogicalType> children;

		child_list_t<LogicalType> key_type, value_type;
		key_type.push_back(
		    {"", TransformTypeName((duckdb_libpgquery::PGTypeName *)type_name->typmods->head->data.ptr_value)});
		value_type.push_back(
		    {"", TransformTypeName((duckdb_libpgquery::PGTypeName *)type_name->typmods->tail->data.ptr_value)});

		children.push_back({"key", {LogicalTypeId::LIST, key_type}});
		children.push_back({"value", {LogicalTypeId::LIST, value_type}});

		D_ASSERT(children.size() == 2);

		return LogicalType(LogicalTypeId::MAP, children);
	}

	int8_t width = base_type.width(), scale = base_type.scale();
	// check any modifiers
	int modifier_idx = 0;
	if (type_name->typmods) {
		for (auto node = type_name->typmods->head; node; node = node->next) {
			auto &const_val = *((duckdb_libpgquery::PGAConst *)node->data.ptr_value);
			if (const_val.type != duckdb_libpgquery::T_PGAConst ||
			    const_val.val.type != duckdb_libpgquery::T_PGInteger) {
				throw ParserException("Expected an integer constant as type modifier");
			}
			if (const_val.val.val.ival < 0) {
				throw ParserException("Negative modifier not supported");
			}
			if (modifier_idx == 0) {
				width = const_val.val.val.ival;
			} else if (modifier_idx == 1) {
				scale = const_val.val.val.ival;
			} else {
				throw ParserException("A maximum of two modifiers is supported");
			}
			modifier_idx++;
		}
	}
	switch (base_type.id()) {
	case LogicalTypeId::VARCHAR:
		if (modifier_idx > 1) {
			throw ParserException("VARCHAR only supports a single modifier");
		}
		// FIXME: create CHECK constraint based on varchar width
		width = 0;
		break;
	case LogicalTypeId::DECIMAL:
		if (modifier_idx == 1) {
			// only width is provided: set scale to 0
			scale = 0;
		}
		if (width <= 0 || width > Decimal::MAX_WIDTH_DECIMAL) {
			throw ParserException("Width must be between 1 and %d!", (int)Decimal::MAX_WIDTH_DECIMAL);
		}
		if (scale > width) {
			throw ParserException("Scale cannot be bigger than width");
		}
		break;
	case LogicalTypeId::INTERVAL:
		if (modifier_idx > 1) {
			throw ParserException("INTERVAL only supports a single modifier");
		}
		width = 0;
		break;
	default:
		if (modifier_idx > 0) {
			throw ParserException("Type %s does not support any modifiers!", base_type.ToString());
		}
	}

	return LogicalType(base_type.id(), width, scale);
}

} // namespace duckdb
