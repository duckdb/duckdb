#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();

	auto name = PGPointerCast<duckdb_libpgquery::PGValue>(type_name.names->tail->data.ptr_value)->val.str;
	// transform it to the SQL type
	LogicalTypeId base_type = TransformStringToLogicalTypeId(name);

	LogicalType result_type;
	if (base_type == LogicalTypeId::LIST) {
		throw ParserException("LIST is not valid as a stand-alone type");
	} else if (base_type == LogicalTypeId::ENUM) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Enum needs a set of entries");
		}
		Vector enum_vector(LogicalType::VARCHAR, type_name.typmods->length);
		auto string_data = FlatVector::GetData<string_t>(enum_vector);
		idx_t pos = 0;
		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto constant_value = PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
			if (constant_value->type != duckdb_libpgquery::T_PGAConst ||
			    constant_value->val.type != duckdb_libpgquery::T_PGString) {
				throw ParserException("Enum type requires a set of strings as type modifiers");
			}
			string_data[pos++] = StringVector::AddString(enum_vector, constant_value->val.val.str);
		}
		return LogicalType::ENUM(enum_vector, type_name.typmods->length);
	} else if (base_type == LogicalTypeId::STRUCT) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Struct needs a name and entries");
		}
		child_list_t<LogicalType> children;
		case_insensitive_set_t name_collision_set;

		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &type_val = *PGPointerCast<duckdb_libpgquery::PGList>(node->data.ptr_value);
			if (type_val.length != 2) {
				throw ParserException("Struct entry needs an entry name and a type name");
			}

			auto entry_name_node = PGPointerCast<duckdb_libpgquery::PGValue>(type_val.head->data.ptr_value);
			D_ASSERT(entry_name_node->type == duckdb_libpgquery::T_PGString);
			auto entry_type_node = PGPointerCast<duckdb_libpgquery::PGTypeName>(type_val.tail->data.ptr_value);
			D_ASSERT(entry_type_node->type == duckdb_libpgquery::T_PGTypeName);

			auto entry_name = string(entry_name_node->val.str);
			D_ASSERT(!entry_name.empty());

			if (name_collision_set.find(entry_name) != name_collision_set.end()) {
				throw ParserException("Duplicate struct entry name \"%s\"", entry_name);
			}
			name_collision_set.insert(entry_name);
			auto entry_type = TransformTypeName(*entry_type_node);

			children.push_back(make_pair(entry_name, entry_type));
		}
		D_ASSERT(!children.empty());
		result_type = LogicalType::STRUCT(children);

	} else if (base_type == LogicalTypeId::MAP) {
		if (!type_name.typmods || type_name.typmods->length != 2) {
			throw ParserException("Map type needs exactly two entries, key and value type");
		}
		auto key_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->head->data.ptr_value));
		auto value_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->tail->data.ptr_value));

		result_type = LogicalType::MAP(std::move(key_type), std::move(value_type));
	} else if (base_type == LogicalTypeId::UNION) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Union type needs at least one member");
		}
		if (type_name.typmods->length > (int)UnionType::MAX_UNION_MEMBERS) {
			throw ParserException("Union types can have at most %d members", UnionType::MAX_UNION_MEMBERS);
		}

		child_list_t<LogicalType> children;
		case_insensitive_set_t name_collision_set;

		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &type_val = *PGPointerCast<duckdb_libpgquery::PGList>(node->data.ptr_value);
			if (type_val.length != 2) {
				throw ParserException("Union type member needs a tag name and a type name");
			}

			auto entry_name_node = PGPointerCast<duckdb_libpgquery::PGValue>(type_val.head->data.ptr_value);
			D_ASSERT(entry_name_node->type == duckdb_libpgquery::T_PGString);
			auto entry_type_node = PGPointerCast<duckdb_libpgquery::PGTypeName>(type_val.tail->data.ptr_value);
			D_ASSERT(entry_type_node->type == duckdb_libpgquery::T_PGTypeName);

			auto entry_name = string(entry_name_node->val.str);
			D_ASSERT(!entry_name.empty());

			if (name_collision_set.find(entry_name) != name_collision_set.end()) {
				throw ParserException("Duplicate union type tag name \"%s\"", entry_name);
			}

			name_collision_set.insert(entry_name);

			auto entry_type = TransformTypeName(*entry_type_node);
			children.push_back(make_pair(entry_name, entry_type));
		}
		D_ASSERT(!children.empty());
		result_type = LogicalType::UNION(std::move(children));
	} else {
		int64_t width, scale;
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
		if (type_name.typmods) {
			for (auto node = type_name.typmods->head; node; node = node->next) {
				auto &const_val = *PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
				if (const_val.type != duckdb_libpgquery::T_PGAConst ||
				    const_val.val.type != duckdb_libpgquery::T_PGInteger) {
					throw ParserException("Expected an integer constant as type modifier");
				}
				if (const_val.val.val.ival < 0) {
					throw ParserException("Negative modifier not supported");
				}
				if (modifier_idx == 0) {
					width = const_val.val.val.ival;
					if (base_type == LogicalTypeId::BIT && const_val.location != -1) {
						width = 0;
					}
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
		case LogicalTypeId::BIT: {
			if (!width && type_name.typmods) {
				throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
			}
			result_type = LogicalType(base_type);
			break;
		}
		case LogicalTypeId::TIMESTAMP:
			if (modifier_idx == 0) {
				result_type = LogicalType::TIMESTAMP;
			} else {
				if (modifier_idx > 1) {
					throw ParserException("TIMESTAMP only supports a single modifier");
				}
				if (width > 10) {
					throw ParserException("TIMESTAMP only supports until nano-second precision (9)");
				}
				if (width == 0) {
					result_type = LogicalType::TIMESTAMP_S;
				} else if (width <= 3) {
					result_type = LogicalType::TIMESTAMP_MS;
				} else if (width <= 6) {
					result_type = LogicalType::TIMESTAMP;
				} else {
					result_type = LogicalType::TIMESTAMP_NS;
				}
			}
			break;
		default:
			if (modifier_idx > 0) {
				throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
			}
			result_type = LogicalType(base_type);
			break;
		}
	}
	if (type_name.arrayBounds) {
		// array bounds: turn the type into a list
		idx_t extra_stack = 0;
		for (auto cell = type_name.arrayBounds->head; cell != nullptr; cell = cell->next) {
			result_type = LogicalType::LIST(result_type);
			StackCheck(extra_stack++);
		}
	}
	return result_type;
}

} // namespace duckdb
