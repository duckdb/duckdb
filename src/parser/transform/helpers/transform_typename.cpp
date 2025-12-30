#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/type_parameter.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

LogicalType Transformer::TransformTypeNameInternal(duckdb_libpgquery::PGTypeName &type_name) {
	// Parse typename/any qualifications

	string unbound_name;
	string schema_name;
	string catalog_name;

	if (type_name.names->length == 0) {
		throw ParserException("Type name cannot be empty");
	}
	if (type_name.names->length == 1) {
		auto unbound_name_cell = type_name.names->head;

		unbound_name = PGPointerCast<duckdb_libpgquery::PGValue>(unbound_name_cell->data.ptr_value)->val.str;
	}
	if (type_name.names->length == 2) {
		auto schema_name_cell = type_name.names->head;
		auto unbound_name_cell = schema_name_cell->next;

		schema_name = PGPointerCast<duckdb_libpgquery::PGValue>(schema_name_cell->data.ptr_value)->val.str;
		unbound_name = PGPointerCast<duckdb_libpgquery::PGValue>(unbound_name_cell->data.ptr_value)->val.str;
	}
	if (type_name.names->length == 3) {
		auto catalog_name_cell = type_name.names->head;
		auto schema_name_cell = catalog_name_cell->next;
		auto unbound_name_cell = schema_name_cell->next;

		catalog_name = PGPointerCast<duckdb_libpgquery::PGValue>(catalog_name_cell->data.ptr_value)->val.str;
		schema_name = PGPointerCast<duckdb_libpgquery::PGValue>(schema_name_cell->data.ptr_value)->val.str;
		unbound_name = PGPointerCast<duckdb_libpgquery::PGValue>(unbound_name_cell->data.ptr_value)->val.str;
	}
	if (type_name.names->length > 4) {
		throw ParserException(
		    "Too many qualifications for type name - expected [catalog.schema.name] or [schema.name]");
	}

	// Parse type modifiers
	vector<unique_ptr<TypeParameter>> type_params;
	for (auto typemod = type_name.typmods ? type_name.typmods->head : nullptr; typemod; typemod = typemod->next) {
		// Type mods are always a list of (name, node) pairs

		string name_str;
		auto typemod_node = PGPointerCast<duckdb_libpgquery::PGNode>(typemod->data.ptr_value);

		if (typemod_node->type == duckdb_libpgquery::T_PGList) {
			auto &typemod_pair = *PGPointerCast<duckdb_libpgquery::PGList>(typemod->data.ptr_value);
			if (typemod_pair.length != 2) {
				throw ParserException("Expected type modifier to be a pair of (name, value)");
			}

			// This is the actual argument node
			typemod_node = PGPointerCast<duckdb_libpgquery::PGNode>(typemod_pair.tail->data.ptr_value);

			// Extract name of the type modifier (optional)
			auto name_node = PGPointerCast<duckdb_libpgquery::PGNode>(typemod_pair.head->data.ptr_value);
			if (name_node) {
				if (name_node->type != duckdb_libpgquery::T_PGString) {
					throw ParserException("Expected a constant as type modifier name");
				}
				name_str = PGPointerCast<duckdb_libpgquery::PGValue>(name_node.get())->val.str;
			}
		}

		// Extract value of the type modifier
		// This is either:
		// 1. A constant value
		// 2. A expression
		// 3. A type name

		switch (typemod_node->type) {
		case duckdb_libpgquery::T_PGAConst: {
			// Constant value
			auto const_node = PGPointerCast<duckdb_libpgquery::PGAConst>(typemod_node.get());
			type_params.push_back(TypeParameter::EXPRESSION(std::move(name_str), TransformValue(const_node->val)));
		} break;
		case duckdb_libpgquery::T_PGAExpr:
		case duckdb_libpgquery::T_PGFuncCall: {
			// Expression
			auto expr = TransformExpression(*typemod_node);
			type_params.push_back(TypeParameter::EXPRESSION(std::move(name_str), std::move(expr)));
		} break;
		case duckdb_libpgquery::T_PGTypeName: {
			// Type name
			auto type = TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(typemod_node.get()));
			type_params.push_back(TypeParameter::TYPE(std::move(name_str), std::move(type)));
		} break;
		default:
			throw ParserException("Expected a constant, expression or type name as type modifier value");
		}
	}

	return LogicalType::UNBOUND(catalog_name, schema_name, unbound_name, std::move(type_params));
}

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();

	auto result_type = TransformTypeNameInternal(type_name);

	if (type_name.arrayBounds) {
		// For both arrays and lists, the inner type is stored as the first type parameter
		vector<unique_ptr<TypeParameter>> type_params;
		type_params.push_back(TypeParameter::TYPE("", std::move(result_type)));

		idx_t extra_stack = 0;
		for (auto cell = type_name.arrayBounds->head; cell != nullptr; cell = cell->next) {
			StackCheck(extra_stack++);
			auto arg = PGPointerCast<duckdb_libpgquery::PGNode>(cell->data.ptr_value);

			// If the bound is NULL, we have an dynamically sized list
			if (!arg) {
				result_type = LogicalType::UNBOUND("list", std::move(type_params));
				continue;
			}

			// If the bound is an expression, we have a fixed-size array
			if (arg->type == duckdb_libpgquery::T_PGAExpr || arg->type == duckdb_libpgquery::T_PGAConst ||
			    arg->type == duckdb_libpgquery::T_PGFuncCall) {
				auto bound_expr = TransformExpression(*arg);
				type_params.push_back(TypeParameter::EXPRESSION("", std::move(bound_expr)));
				result_type = LogicalType::UNBOUND("array", std::move(type_params));
				continue;
			}

			throw ParserException("ARRAY bounds must only contain expressions");
		}
	}
	return result_type;
}

} // namespace duckdb
