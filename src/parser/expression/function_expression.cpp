#include "duckdb/parser/expression/function_expression.hpp"

#include <utility>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

FunctionExpression::FunctionExpression() : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION) {
}

FunctionExpression::FunctionExpression(Identifier catalog, Identifier schema, const Identifier &function_name,
                                       vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys_p,
                                       bool distinct, bool is_operator, bool export_state_p)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), catalog(std::move(catalog)),
      schema(std::move(schema)), function_name(StringUtil::Lower(function_name.GetIdentifierName())),
      is_operator(is_operator), distinct(distinct), filter(std::move(filter)), order_bys(std::move(order_bys_p)),
      export_state(export_state_p) {
	arguments.reserve(children_p.size());
	for (auto &child : children_p) {
		arguments.emplace_back(std::move(child));
	}
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		order_bys = make_uniq<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const Identifier &function_name, vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state_p)
    : FunctionExpression(Identifier::InvalidCatalog(), Identifier::InvalidSchema(), function_name,
                         std::move(children_p), std::move(filter), std::move(order_bys), distinct, is_operator,
                         export_state_p) {
}

FunctionExpression::FunctionExpression(Identifier catalog_name, Identifier schema_name, const Identifier &function_name,
                                       vector<FunctionArgument> children, unique_ptr<ParsedExpression> filter,
                                       unique_ptr<OrderModifier> order_bys_p, bool distinct, bool is_operator,
                                       bool export_state)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), catalog(std::move(catalog_name)),
      schema(std::move(schema_name)), function_name(StringUtil::Lower(function_name.GetIdentifierName())),
      is_operator(is_operator), arguments(std::move(children)), distinct(distinct), filter(std::move(filter)),
      order_bys(std::move(order_bys_p)), export_state(export_state) {
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		this->order_bys = make_uniq<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const Identifier &function_name, vector<FunctionArgument> children,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state)
    : FunctionExpression(Identifier::InvalidCatalog(), Identifier::InvalidSchema(), function_name, std::move(children),
                         std::move(filter), std::move(order_bys), distinct, is_operator, export_state) {
}

string FunctionExpression::ToString() const {
	if (is_operator) {
		// built-in operator
		D_ASSERT(!distinct);
		if (arguments.size() == 1) {
			if (StringUtil::Contains(function_name.GetIdentifierName(), "__postfix")) {
				return "((" + arguments[0].ToString() + ")" +
				       StringUtil::Replace(function_name.GetIdentifierName(), "__postfix", "") + ")";
			}
			return function_name.GetIdentifierName() + "(" + arguments[0].ToString() + ")";
		}
		if (arguments.size() == 2) {
			return StringUtil::Format("(%s %s %s)", arguments[0].ToString(), function_name.GetIdentifierName(),
			                          arguments[1].ToString());
		}
	}
	// standard function call
	string result;
	if (!catalog.empty()) {
		result += SQLIdentifier(catalog) + ".";
	}
	if (!schema.empty()) {
		result += SQLIdentifier(schema) + ".";
	}
	result += SQLIdentifier(function_name);
	result += "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	result += StringUtil::Join(arguments, arguments.size(), ", ",
	                           [&](const FunctionArgument &child) { return child.ToString(); });
	// ordered aggregate
	if (order_bys && !order_bys->orders.empty()) {
		if (arguments.empty()) {
			result += ") WITHIN GROUP (";
		}
		result += " ORDER BY ";
		for (idx_t i = 0; i < order_bys->orders.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += order_bys->orders[i].ToString();
		}
	}
	result += ")";

	// filtered aggregate
	if (filter) {
		result += " FILTER (WHERE " + filter->ToString() + ")";
	}

	if (export_state) {
		result += " EXPORT_STATE";
	}

	return result;
}

void FunctionExpression::Verify() const {
	D_ASSERT(!function_name.empty());
}

optional_ptr<ParsedExpression> FunctionExpression::IsLambdaFunction() {
	// Ignore the ->> operator (JSON extension).
	if (function_name == "->>") {
		return nullptr;
	}
	// Check the children for lambda expressions.
	for (auto &child : arguments) {
		if (child.GetExpression().GetExpressionClass() == ExpressionClass::LAMBDA) {
			return *child.GetExpressionMutable();
		}
	}
	return nullptr;
}

void FunctionExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<Identifier>(200, "function_name", function_name);
	serializer.WritePropertyWithDefault<Identifier>(201, "schema", schema);

	if (!serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		// Legacy serialization.
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &arg : arguments) {
			auto copy = arg.GetExpression().Copy();
			copy->SetAlias(arg.GetName());
			children.push_back(std::move(copy));
		}
		serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(202, "children", children);
	}

	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(203, "filter", filter);
	serializer.WritePropertyWithDefault<unique_ptr<OrderModifier>>(204, "order_bys", order_bys);
	serializer.WritePropertyWithDefault<bool>(205, "distinct", distinct);
	serializer.WritePropertyWithDefault<bool>(206, "is_operator", is_operator);
	serializer.WritePropertyWithDefault<bool>(207, "export_state", export_state);
	serializer.WritePropertyWithDefault<Identifier>(208, "catalog", catalog);

	if (serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		serializer.WritePropertyWithDefault<vector<FunctionArgument>>(209, "arguments", arguments);
	}
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<FunctionExpression>(new FunctionExpression());
	deserializer.ReadPropertyWithDefault<Identifier>(200, "function_name", result->function_name);
	deserializer.ReadPropertyWithDefault<Identifier>(201, "schema", result->schema);

	// Legacy children deserialization
	vector<unique_ptr<ParsedExpression>> children;
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(202, "children", children);
	if (!children.empty()) {
		result->arguments.reserve(children.size());
		for (auto &child : children) {
			auto alias = child->GetAlias();
			result->arguments.emplace_back(alias, std::move(child));
		}

		// Mark this function expression as a legacy function call, so that the binder can handle it accordingly.
		result->is_legacy_function_call = true;
	}

	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(203, "filter", result->filter);
	auto order_bys = deserializer.ReadPropertyWithDefault<unique_ptr<ResultModifier>>(204, "order_bys");
	result->order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(std::move(order_bys));
	deserializer.ReadPropertyWithDefault<bool>(205, "distinct", result->distinct);
	deserializer.ReadPropertyWithDefault<bool>(206, "is_operator", result->is_operator);
	deserializer.ReadPropertyWithDefault<bool>(207, "export_state", result->export_state);
	deserializer.ReadPropertyWithDefault<Identifier>(208, "catalog", result->catalog);

	// New children deserialization
	if (children.empty()) {
		deserializer.ReadPropertyWithDefault<vector<FunctionArgument>>(209, "arguments", result->arguments);
	}

	return std::move(result);
}

hash_t FunctionArgument::Hash() const {
	hash_t result = duckdb::Hash<const char *>(name.c_str());
	if (expression) {
		result = CombineHash(result, expression->Hash());
	}
	return result;
}

} // namespace duckdb
