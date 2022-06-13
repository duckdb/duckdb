#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName *type_name) {
	if (!type_name || type_name->type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();

	auto name = (reinterpret_cast<duckdb_libpgquery::PGValue *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	LogicalTypeId base_type = TransformStringToLogicalTypeId(name);

	LogicalType result_type;
	if (base_type == LogicalTypeId::STRUCT) {
		if (!type_name->typmods || type_name->typmods->length == 0) {
			throw ParserException("Struct needs a name and entries");
		}
		child_list_t<LogicalType> children;
		unordered_set<string> name_collision_set;

		for (auto node = type_name->typmods->head; node; node = node->next) {
			auto &type_val = *((duckdb_libpgquery::PGList *)node->data.ptr_value);
			if (type_val.length != 2) {
				throw ParserException("Struct entry needs an entry name and a type name");
			}

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
		result_type = LogicalType::STRUCT(move(children));
	} else if (base_type == LogicalTypeId::MAP) {
		//! We transform MAP<TYPE_KEY, TYPE_VALUE> to STRUCT<LIST<key: TYPE_KEY>, LIST<value: TYPE_VALUE>>

		if (!type_name->typmods || type_name->typmods->length != 2) {
			throw ParserException("Map type needs exactly two entries, key and value type");
		}
		child_list_t<LogicalType> children;
		auto key_type = TransformTypeName((duckdb_libpgquery::PGTypeName *)type_name->typmods->head->data.ptr_value);
		auto value_type = TransformTypeName((duckdb_libpgquery::PGTypeName *)type_name->typmods->tail->data.ptr_value);

		children.push_back({"key", LogicalType::LIST(key_type)});
		children.push_back({"value", LogicalType::LIST(value_type)});

		D_ASSERT(children.size() == 2);

		result_type = LogicalType::MAP(move(children));
	} else {
		int8_t width, scale;
		if (base_type == LogicalTypeId::DECIMAL) {
			// default decimal width/scale
			width = 18;
			scale = 3;
		} else {
			width = 0;
			scale = 0;
		}
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
		switch (base_type) {
		case LogicalTypeId::VARCHAR:
			if (modifier_idx > 1) {
				throw ParserException("VARCHAR only supports a single modifier");
			}
			// FIXME: create CHECK constraint based on varchar width
			width = 0;
			result_type = LogicalType::VARCHAR;
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
			result_type = LogicalType::DECIMAL(width, scale);
			break;
		case LogicalTypeId::INTERVAL:
			if (modifier_idx > 1) {
				throw ParserException("INTERVAL only supports a single modifier");
			}
			width = 0;
			result_type = LogicalType::INTERVAL;
			break;
		case LogicalTypeId::USER: {
			string user_type_name {name};
			result_type = LogicalType::USER(user_type_name);
			break;
		}
		default:
			if (modifier_idx > 0) {
				throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
			}
			result_type = LogicalType(base_type);
			break;
		}
	}
	if (type_name->arrayBounds) {
		// array bounds: turn the type into a list
		idx_t extra_stack = 0;
		for (auto cell = type_name->arrayBounds->head; cell != nullptr; cell = cell->next) {
			result_type = LogicalType::LIST(move(result_type));
			StackCheck(extra_stack++);
		}
	}
	return result_type;
}

} // namespace duckdb
