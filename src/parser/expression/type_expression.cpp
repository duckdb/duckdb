#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/hash.hpp"
namespace duckdb {

TypeExpression::TypeExpression(string catalog, string schema, string type_name,
                               vector<unique_ptr<ParsedExpression>> children_p)
    : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE), catalog(std::move(catalog)),
      schema(std::move(schema)), type_name(std::move(type_name)), children(std::move(children_p)) {
	D_ASSERT(!this->type_name.empty());
}

TypeExpression::TypeExpression(string type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(INVALID_CATALOG, INVALID_SCHEMA, std::move(type_name), std::move(children)) {
}

TypeExpression::TypeExpression() : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE) {
}

string TypeExpression::ToString() const {
	string result;
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}

	auto &params = children;

	// LIST and ARRAY have special syntax
	if (result.empty() && StringUtil::CIEquals(type_name, "LIST") && params.size() == 1) {
		return params[0]->ToString() + "[]";
	}
	if (result.empty() && StringUtil::CIEquals(type_name, "ARRAY") && params.size() == 2) {
		auto &type_param = params[0];
		auto &size_param = params[1];
		return type_param->ToString() + "[" + size_param->ToString() + "]";
	}
	// So does STRUCT, MAP and UNION
	if (result.empty() && StringUtil::CIEquals(type_name, "STRUCT")) {
		if (params.empty()) {
			return "STRUCT";
		}
		string struct_result = "STRUCT(";
		for (idx_t i = 0; i < params.size(); i++) {
			struct_result += KeywordHelper::WriteOptionallyQuoted(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				struct_result += ", ";
			}
		}
		struct_result += ")";
		return struct_result;
	}
	if (result.empty() && StringUtil::CIEquals(type_name, "UNION")) {
		if (params.empty()) {
			return "UNION";
		}
		string union_result = "UNION(";
		for (idx_t i = 0; i < params.size(); i++) {
			union_result += KeywordHelper::WriteOptionallyQuoted(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				union_result += ", ";
			}
		}
		union_result += ")";
		return union_result;
	}

	if (result.empty() && StringUtil::CIEquals(type_name, "MAP") && params.size() == 2) {
		return "MAP(" + params[0]->ToString() + ", " + params[1]->ToString() + ")";
	}

	if (result.empty() && StringUtil::CIEquals(type_name, "VARCHAR") && !params.empty()) {
		if (params.back()->HasAlias() && StringUtil::CIEquals(params.back()->GetAlias(), "collation")) {
			// Special case for VARCHAR with collation
			auto collate_expr = params.back()->Cast<ConstantExpression>();
			return StringUtil::Format("VARCHAR COLLATE %s", SQLIdentifier(StringValue::Get(collate_expr.value)));
		}
	}

	if (result.empty() && StringUtil::CIEquals(type_name, "INTERVAL") && !params.empty()) {
		// We ignore interval types parameters.
		return "INTERVAL";
	}

	result += KeywordHelper::WriteOptionallyQuoted(type_name, '"', true, KeywordCategory::KEYWORD_COL_NAME);

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

unique_ptr<ParsedExpression> TypeExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	copy_children.reserve(children.size());
	for (const auto &child : children) {
		copy_children.push_back(child->Copy());
	}

	auto copy = make_uniq<TypeExpression>(catalog, schema, type_name, std::move(copy_children));
	copy->CopyProperties(*this);

	return std::move(copy);
}

bool TypeExpression::Equal(const TypeExpression &a, const TypeExpression &b) {
	if (a.catalog != b.catalog || a.schema != b.schema || a.type_name != b.type_name) {
		return false;
	}
	return ParsedExpression::ListEquals(a.children, b.children);
}

void TypeExpression::Verify() const {
	D_ASSERT(!type_name.empty());
}

hash_t TypeExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(catalog.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(type_name.c_str()));
	return result;
}

} // namespace duckdb
