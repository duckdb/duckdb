#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/hash.hpp"
namespace duckdb {

TypeExpression::TypeExpression(QualifiedName qualified_name_p, vector<unique_ptr<ParsedExpression>> children_p)
    : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE), qualified_name(std::move(qualified_name_p)),
      children(std::move(children_p)) {
	D_ASSERT(!qualified_name.Name().empty());
}

TypeExpression::TypeExpression(Identifier type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(QualifiedName(std::move(type_name)), std::move(children)) {
}

TypeExpression::TypeExpression(const string &type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(Identifier(type_name), std::move(children)) {
}

TypeExpression::TypeExpression() : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE) {
}

string TypeExpression::ToString() const {
	auto &type_name = qualified_name.Name();
	// render the full qualification - the schema path can be nested (e.g. s1.s2.s3.my_type)
	string result = qualified_name.QualificationToString();

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

//===--------------------------------------------------------------------===//
// LogicalType -> TypeExpression
//===--------------------------------------------------------------------===//

namespace {

unique_ptr<ParsedExpression> TypeChild(const LogicalType &type, const Identifier &name) {
	auto child = TypeExpression::FromLogicalType(type);
	if (!name.empty()) {
		child->SetAlias(name);
	}
	return std::move(child);
}

unique_ptr<ParsedExpression> ValueChild(Value value, const char *label = nullptr) {
	auto child = make_uniq_base<ParsedExpression, ConstantExpression>(std::move(value));
	if (label) {
		child->SetAlias(Identifier(label));
	}
	return child;
}

//! The name a built-in type is spelled with. Must be a name DefaultTypeGenerator knows.
const char *BuiltinTypeName(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::UHUGEINT:
		return "UHUGEINT";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::BIGNUM:
		return "BIGNUM";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIME_NS:
		return "TIME_NS";
	case LogicalTypeId::TIME_TZ:
		return "TIMETZ";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP_US";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "TIMESTAMPTZ_NS";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::BIT:
		return "BIT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::TYPE:
		return "TYPE";
	case LogicalTypeId::VARIANT:
		return "VARIANT";
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::TUPLE:
		return "TUPLE";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::UNION:
		return "UNION";
	default:
		return nullptr;
	}
}

} // namespace

unique_ptr<TypeExpression> TypeExpression::FromLogicalType(const LogicalType &type) {
	if (type.IsUnbound()) {
		auto &expr = UnboundType::GetTypeExpression(type);
		if (expr->GetExpressionClass() != ExpressionClass::TYPE) {
			throw InternalException("Unbound type does not wrap a type expression");
		}
		return unique_ptr_cast<ParsedExpression, TypeExpression>(expr->Copy());
	}

	vector<unique_ptr<ParsedExpression>> children;

	// A type that carries an alias is a user-defined type: name it, and let the binder resolve it again.
	auto alias = type.GetAlias();
	if (!alias.empty()) {
		if (type.HasExtensionInfo()) {
			for (auto &modifier : type.GetExtensionInfo()->modifiers) {
				children.push_back(ValueChild(modifier.value));
			}
		}
		return make_uniq<TypeExpression>(Identifier(alias), std::move(children));
	}

	auto name = BuiltinTypeName(type.id());
	if (!name) {
		// only the internal placeholder ids (ANY, UNKNOWN, INVALID, ...) have no name, and none of them is a
		// type a user can name in the first place
		throw InternalException("Type '%s' cannot be expressed as a type expression", type.ToString());
	}

	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		children.push_back(ValueChild(Value::UTINYINT(DecimalType::GetWidth(type))));
		children.push_back(ValueChild(Value::UTINYINT(DecimalType::GetScale(type))));
		break;
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(type);
		if (!collation.empty()) {
			children.push_back(ValueChild(Value(collation), "collation"));
		}
		break;
	}
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(type)) {
			children.push_back(ValueChild(Value(GeoType::GetCRS(type).GetDefinition())));
		}
		break;
	case LogicalTypeId::ENUM:
		for (idx_t i = 0; i < EnumType::GetSize(type); i++) {
			children.push_back(ValueChild(Value(EnumType::GetString(type, i).GetString())));
		}
		break;
	case LogicalTypeId::LIST:
		children.push_back(TypeChild(ListType::GetChildType(type), Identifier()));
		break;
	case LogicalTypeId::ARRAY:
		children.push_back(TypeChild(ArrayType::GetChildType(type), Identifier()));
		if (!ArrayType::IsAnySize(type)) {
			children.push_back(ValueChild(Value::UBIGINT(ArrayType::GetSize(type))));
		}
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child : StructType::GetChildTypes(type)) {
			children.push_back(TypeChild(child.second, child.first));
		}
		break;
	case LogicalTypeId::TUPLE:
		for (auto &child : StructType::GetChildTypes(type)) {
			children.push_back(TypeChild(child.second, Identifier()));
		}
		break;
	case LogicalTypeId::MAP:
		children.push_back(TypeChild(MapType::KeyType(type), Identifier()));
		children.push_back(TypeChild(MapType::ValueType(type), Identifier()));
		break;
	case LogicalTypeId::UNION:
		for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
			children.push_back(TypeChild(UnionType::GetMemberType(type, i), UnionType::GetMemberName(type, i)));
		}
		break;
	default:
		break;
	}

	return make_uniq<TypeExpression>(Identifier(name), std::move(children));
}

void TypeExpression::Verify() const {
	D_ASSERT(!qualified_name.Name().empty());
}

} // namespace duckdb
