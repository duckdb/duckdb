#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

struct SizeModifiers {
	int64_t width = 0;
	int64_t scale = 0;
	// How many modifiers were found
	idx_t count = 0;
};

static SizeModifiers GetSizeModifiers(duckdb_libpgquery::PGTypeName &type_name, LogicalTypeId base_type) {

	SizeModifiers result;

	if (base_type == LogicalTypeId::DECIMAL) {
		// Defaults for DECIMAL
		result.width = 18;
		result.scale = 3;
	}

	if (type_name.typmods) {
		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &const_val = *Transformer::PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
			if (const_val.type != duckdb_libpgquery::T_PGAConst ||
			    const_val.val.type != duckdb_libpgquery::T_PGInteger) {
				throw ParserException("Expected an integer constant as type modifier");
			}
			if (const_val.val.val.ival < 0) {
				throw ParserException("Negative modifier not supported");
			}
			if (result.count == 0) {
				result.width = const_val.val.val.ival;
				if (base_type == LogicalTypeId::BIT && const_val.location != -1) {
					result.width = 0;
				}
			} else if (result.count == 1) {
				result.scale = const_val.val.val.ival;
			} else {
				throw ParserException("A maximum of two modifiers is supported");
			}
			result.count++;
		}
	}
	return result;
}

vector<Value> Transformer::TransformTypeModifiers(duckdb_libpgquery::PGTypeName &type_name) {
	vector<Value> type_mods;
	if (type_name.typmods) {
		for (auto node = type_name.typmods->head; node; node = node->next) {
			if (type_mods.size() > 9) {
				const auto &name =
				    *PGPointerCast<duckdb_libpgquery::PGValue>(type_name.names->tail->data.ptr_value)->val.str;
				throw ParserException("'%s': a maximum of 9 type modifiers is allowed", name);
			}
			const auto &const_val = *PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
			if (const_val.type != duckdb_libpgquery::T_PGAConst) {
				throw ParserException("Expected a constant as type modifier");
			}
			const auto const_expr = TransformValue(const_val.val);
			type_mods.push_back(std::move(const_expr->value));
		}
	}
	return type_mods;
}

LogicalType Transformer::TransformTypeNameInternal(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.names->length > 1) {
		// qualified typename
		vector<string> names;
		for (auto cell = type_name.names->head; cell; cell = cell->next) {
			names.push_back(PGPointerCast<duckdb_libpgquery::PGValue>(cell->data.ptr_value)->val.str);
		}
		vector<Value> type_mods = TransformTypeModifiers(type_name);
		switch (type_name.names->length) {
		case 2: {
			return LogicalType::USER(INVALID_CATALOG, std::move(names[0]), std::move(names[1]), std::move(type_mods));
		}
		case 3: {
			return LogicalType::USER(std::move(names[0]), std::move(names[1]), std::move(names[2]),
			                         std::move(type_mods));
		}
		default:
			throw ParserException(
			    "Too many qualifications for type name - expected [catalog.schema.name] or [schema.name]");
		}
	}

	auto name = PGPointerCast<duckdb_libpgquery::PGValue>(type_name.names->tail->data.ptr_value)->val.str;
	// transform it to the SQL type
	LogicalTypeId base_type = TransformStringToLogicalTypeId(name);

	if (base_type == LogicalTypeId::LIST) {
		throw ParserException("LIST is not valid as a stand-alone type");
	}
	if (base_type == LogicalTypeId::ENUM) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Enum needs a set of entries");
		}
		Vector enum_vector(LogicalType::VARCHAR, NumericCast<idx_t>(type_name.typmods->length));
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
		return LogicalType::ENUM(enum_vector, NumericCast<idx_t>(type_name.typmods->length));
	}
	if (base_type == LogicalTypeId::STRUCT) {
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
		return LogicalType::STRUCT(children);
	}
	if (base_type == LogicalTypeId::MAP) {
		if (!type_name.typmods || type_name.typmods->length != 2) {
			throw ParserException("Map type needs exactly two entries, key and value type");
		}
		auto key_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->head->data.ptr_value));
		auto value_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->tail->data.ptr_value));

		return LogicalType::MAP(std::move(key_type), std::move(value_type));
	}
	if (base_type == LogicalTypeId::UNION) {
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
		return LogicalType::UNION(std::move(children));
	}
	if (base_type == LogicalTypeId::USER) {
		string user_type_name {name};
		vector<Value> type_mods = TransformTypeModifiers(type_name);
		return LogicalType::USER(user_type_name, type_mods);
	}

	SizeModifiers modifiers = GetSizeModifiers(type_name, base_type);
	switch (base_type) {
	case LogicalTypeId::VARCHAR:
		if (modifiers.count > 1) {
			throw ParserException("VARCHAR only supports a single modifier");
		}
		// FIXME: create CHECK constraint based on varchar width
		modifiers.width = 0;
		return LogicalType::VARCHAR;
	case LogicalTypeId::DECIMAL:
		if (modifiers.count > 2) {
			throw ParserException("DECIMAL only supports a maximum of two modifiers");
		}
		if (modifiers.count == 1) {
			// only width is provided: set scale to 0
			modifiers.scale = 0;
		}
		if (modifiers.width <= 0 || modifiers.width > Decimal::MAX_WIDTH_DECIMAL) {
			throw ParserException("Width must be between 1 and %d!", (int)Decimal::MAX_WIDTH_DECIMAL);
		}
		if (modifiers.scale > modifiers.width) {
			throw ParserException("Scale cannot be bigger than width");
		}
		return LogicalType::DECIMAL(NumericCast<uint8_t>(modifiers.width), NumericCast<uint8_t>(modifiers.scale));
	case LogicalTypeId::INTERVAL:
		if (modifiers.count > 1) {
			throw ParserException("INTERVAL only supports a single modifier");
		}
		modifiers.width = 0;
		return LogicalType::INTERVAL;
	case LogicalTypeId::BIT:
		if (!modifiers.width && type_name.typmods) {
			throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
		}
		return LogicalType(base_type);
	case LogicalTypeId::TIMESTAMP:
		if (modifiers.count == 0) {
			return LogicalType::TIMESTAMP;
		}
		if (modifiers.count > 1) {
			throw ParserException("TIMESTAMP only supports a single modifier");
		}
		if (modifiers.width > 10) {
			throw ParserException("TIMESTAMP only supports until nano-second precision (9)");
		}
		if (modifiers.width == 0) {
			return LogicalType::TIMESTAMP_S;
		}
		if (modifiers.width <= 3) {
			return LogicalType::TIMESTAMP_MS;
		}
		if (modifiers.width <= 6) {
			return LogicalType::TIMESTAMP;
		}
		return LogicalType::TIMESTAMP_NS;
	default:
		if (modifiers.count > 0) {
			throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
		}
		return LogicalType(base_type);
	}
}

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();
	auto result_type = TransformTypeNameInternal(type_name);
	if (type_name.arrayBounds) {
		// array bounds: turn the type into a list
		idx_t extra_stack = 0;
		for (auto cell = type_name.arrayBounds->head; cell != nullptr; cell = cell->next) {
			StackCheck(extra_stack++);
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(cell->data.ptr_value);
			if (val->type != duckdb_libpgquery::T_PGInteger) {
				throw ParserException("Expected integer value as array bound");
			}
			auto array_size = val->val.ival;
			if (array_size < 0) {
				// -1 if bounds are empty
				result_type = LogicalType::LIST(result_type);
			} else if (array_size == 0) {
				// Empty arrays are not supported
				throw ParserException("Arrays must have a size of at least 1");
			} else if (array_size > static_cast<int64_t>(ArrayType::MAX_ARRAY_SIZE)) {
				throw ParserException("Arrays must have a size of at most %d", ArrayType::MAX_ARRAY_SIZE);
			} else {
				result_type = LogicalType::ARRAY(result_type, NumericCast<idx_t>(array_size));
			}
		}
	}
	return result_type;
}

} // namespace duckdb
