#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformTypeExpressionInternal(duckdb_libpgquery::PGTypeName &type_name) {
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
	if (type_name.names->length >= 4) {
		throw ParserException(
		    "Too many qualifications for type name - expected [catalog.schema.name] or [schema.name]");
	}

	D_ASSERT(!unbound_name.empty());

	// The postgres parser emits a bunch of strange type names - we want to normalize them here so that the alias for
	// columns from expressions containing these types actually use the DuckDB type name.
	// Eventually we should make the parser emit the correct names directly.
	auto known_type_id = TransformStringToLogicalTypeId(unbound_name);
	if (known_type_id != LogicalTypeId::UNBOUND) {
		unbound_name = LogicalTypeIdToString(known_type_id);
	}

	// Parse type modifiers
	vector<unique_ptr<ParsedExpression>> type_params;
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

		if (typemod_node->type == duckdb_libpgquery::T_PGTypeName) {
			auto type_node = *PGPointerCast<duckdb_libpgquery::PGTypeName>(typemod_node.get());
			auto type_expr = TransformTypeExpression(type_node);
			type_expr->SetAlias(std::move(name_str));
			type_params.push_back(std::move(type_expr));
		} else {
			// Expression
			auto expr = TransformExpression(*typemod_node);

			// TODO: Allow arbitrary expressions in the future
			if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
				throw ParserException("Expected a constant as type modifier");
			}

			expr->SetAlias(std::move(name_str));
			type_params.push_back(std::move(expr));
		}
	}

	auto result = make_uniq<TypeExpression>(catalog_name, schema_name, unbound_name, std::move(type_params));

	// Assign query location
	if (type_name.location >= 0) {
		result->query_location = NumericCast<idx_t>(type_name.location);
	}

	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformTypeExpression(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();

	auto result = TransformTypeExpressionInternal(type_name);

	if (type_name.arrayBounds) {
		// For both arrays and lists, the inner type is stored as the first type parameter

		idx_t extra_stack = 0;
		for (auto cell = type_name.arrayBounds->head; cell != nullptr; cell = cell->next) {
			StackCheck(extra_stack++);
			auto arg = PGPointerCast<duckdb_libpgquery::PGNode>(cell->data.ptr_value);

			if (arg->type == duckdb_libpgquery::T_PGInteger) {
				auto int_node = PGPointerCast<duckdb_libpgquery::PGValue>(arg.get());
				auto size = int_node->val.ival;

				vector<unique_ptr<ParsedExpression>> type_params;
				type_params.push_back(std::move(result));

				if (size == -1) {
					// LIST type
					result = make_uniq<TypeExpression>("list", std::move(type_params));
				} else {
					// ARRAY type
					type_params.push_back(make_uniq<ConstantExpression>(Value::BIGINT(int_node->val.ival)));
					result = make_uniq<TypeExpression>("array", std::move(type_params));
				}

				continue;
			}

			throw ParserException("ARRAY bounds must only contain expressions");
		}
	}

	// Assign query location
	if (type_name.location >= 0) {
		result->query_location = NumericCast<idx_t>(type_name.location);
	}

	return result;
}

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName &type_name) {
	auto type_expr = TransformTypeExpression(type_name);
	return LogicalType::UNBOUND(std::move(type_expr));
}

} // namespace duckdb
