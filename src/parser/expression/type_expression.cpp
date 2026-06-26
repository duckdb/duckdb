#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/hash.hpp"
namespace duckdb {

TypeExpression::TypeExpression(Identifier catalog, Identifier schema, Identifier type_name,
                               vector<unique_ptr<ParsedExpression>> children_p)
    : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE), qualified_name {std::move(catalog),
                                                                                     std::move(schema),
                                                                                     std::move(type_name)},
      children(std::move(children_p)) {
	D_ASSERT(!qualified_name.Name().empty());
}

TypeExpression::TypeExpression(Identifier type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(Identifier::InvalidCatalog(), Identifier::InvalidSchema(), std::move(type_name),
                     std::move(children)) {
}

TypeExpression::TypeExpression(const string &type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(Identifier(type_name), std::move(children)) {
}

TypeExpression::TypeExpression() : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE) {
}

string TypeExpression::ToString() const {
	string result;
	auto &type_name = qualified_name.Name();
	if (!qualified_name.Catalog().empty()) {
		result += SQLIdentifier(qualified_name.Catalog()) + ".";
	}
	if (!qualified_name.Schema().empty()) {
		result += SQLIdentifier(qualified_name.Schema()) + ".";
	}

	auto &params = children;

	// LIST and ARRAY have special syntax
	if (result.empty() && type_name == "LIST" && params.size() == 1) {
		return params[0]->ToString() + "[]";
	}
	if (result.empty() && type_name == "ARRAY" && params.size() == 2) {
		auto &type_param = params[0];
		auto &size_param = params[1];
		return type_param->ToString() + "[" + size_param->ToString() + "]";
	}
	// So does STRUCT, MAP and UNION
	if (result.empty() && type_name == "STRUCT") {
		if (params.empty()) {
			return "STRUCT";
		}
		string struct_result = "STRUCT(";
		for (idx_t i = 0; i < params.size(); i++) {
			struct_result += SQLIdentifier(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				struct_result += ", ";
			}
		}
		struct_result += ")";
		return struct_result;
	}
	if (result.empty() && type_name == "UNION") {
		if (params.empty()) {
			return "UNION";
		}
		string union_result = "UNION(";
		for (idx_t i = 0; i < params.size(); i++) {
			union_result += SQLIdentifier(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				union_result += ", ";
			}
		}
		union_result += ")";
		return union_result;
	}

	if (result.empty() && type_name == "MAP" && params.size() == 2) {
		return "MAP(" + params[0]->ToString() + ", " + params[1]->ToString() + ")";
	}

	if (result.empty() && type_name == "VARCHAR" && !params.empty()) {
		if (params.back()->HasAlias() && params.back()->GetAlias() == "collation") {
			// Special case for VARCHAR with collation
			auto collate_expr = params.back()->Cast<ConstantExpression>();
			return StringUtil::Format("VARCHAR COLLATE %s", SQLIdentifier(StringValue::Get(collate_expr.GetValue())));
		}
	}

	if (result.empty() && type_name == "INTERVAL" && !params.empty()) {
		// We ignore interval types parameters.
		return "INTERVAL";
	}

	auto type_id = TransformStringToLogicalTypeId(type_name.GetIdentifierName());
	if (type_id != LogicalTypeId::UNBOUND && type_id != LogicalTypeId::SQLNULL) {
		// Built-in type name
		result += type_name.GetIdentifierName();
	} else {
		result += SQLIdentifier(type_name);
	}

	if (!params.empty()) {
		result += "(";
		for (idx_t i = 0; i < params.size(); i++) {
			result += params[i]->ToString();
			if (i < params.size() - 1) {
				result += ", ";
			}
		}
		result += ")";
	}
	return result;
}

void TypeExpression::Verify() const {
	D_ASSERT(!qualified_name.Name().empty());
}

} // namespace duckdb
