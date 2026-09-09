#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

unique_ptr<ConstantExpression> ConstantExpression::FromLiteral(Literal literal) {
	return make_uniq<ConstantExpression>(std::move(literal));
}

unique_ptr<ConstantExpression> ConstantExpression::Null() {
	return FromLiteral(Literal::Null());
}

unique_ptr<ConstantExpression> ConstantExpression::Boolean(bool value) {
	return FromLiteral(Literal::Boolean(value));
}

unique_ptr<ConstantExpression> ConstantExpression::Integer(int64_t value) {
	return FromLiteral(Literal::Integer(value));
}

unique_ptr<ConstantExpression> ConstantExpression::Number(string text) {
	return FromLiteral(Literal::Number(std::move(text)));
}

unique_ptr<ConstantExpression> ConstantExpression::String(string text) {
	return FromLiteral(Literal::String(std::move(text)));
}

unique_ptr<ConstantExpression> ConstantExpression::Hex(string text) {
	return FromLiteral(Literal::Hex(std::move(text)));
}

unique_ptr<ConstantExpression> ConstantExpression::Bit(string text) {
	return FromLiteral(Literal::Bit(std::move(text)));
}

//! Whether binding the literal yields exactly this value (same type, same contents)
static bool RoundTrips(const Literal &literal, const Value &value) {
	auto bound = literal.ToValue();
	return bound.type() == value.type() && !ValueOperations::DistinctFrom(bound, value);
}

static unique_ptr<ParsedExpression> CastTo(const LogicalType &type, unique_ptr<ParsedExpression> child) {
	return make_uniq<CastExpression>(type, std::move(child));
}

static unique_ptr<ParsedExpression> LiteralOrCast(Literal literal, const Value &value) {
	if (RoundTrips(literal, value)) {
		return ConstantExpression::FromLiteral(std::move(literal));
	}
	return CastTo(value.type(), ConstantExpression::FromLiteral(std::move(literal)));
}

static unique_ptr<ParsedExpression> StringCast(const Value &value) {
	return CastTo(value.type(), ConstantExpression::String(value.ToString()));
}

static unique_ptr<ParsedExpression> ListValueExpression(vector<unique_ptr<ParsedExpression>> children) {
	return make_uniq<FunctionExpression>("list_value", std::move(children));
}

static vector<unique_ptr<ParsedExpression>> ChildExpressions(const vector<Value> &values) {
	vector<unique_ptr<ParsedExpression>> result;
	result.reserve(values.size());
	for (auto &child : values) {
		result.push_back(ConstantExpression::FromValue(child));
	}
	return result;
}

static unique_ptr<ParsedExpression> NamedArgument(const Identifier &name, const Value &value,
                                                  const char *function_name) {
	// the parser sets both the argument name and the alias, and the binders read the alias
	auto child = ConstantExpression::FromValue(value);
	child->SetAlias(name);
	vector<FunctionArgument> arguments;
	arguments.emplace_back(name, std::move(child));
	return make_uniq<FunctionExpression>(Identifier(function_name), std::move(arguments));
}

static unique_ptr<ParsedExpression> StructExpression(const Value &value) {
	auto &type = value.type();
	auto &children = StructValue::GetChildren(value);
	if (StructType::IsUnnamed(type)) {
		return make_uniq<FunctionExpression>("row", ChildExpressions(children));
	}
	vector<FunctionArgument> arguments;
	for (idx_t i = 0; i < children.size(); i++) {
		auto &name = StructType::GetChildName(type, i);
		auto child = ConstantExpression::FromValue(children[i]);
		child->SetAlias(name);
		arguments.emplace_back(name, std::move(child));
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(arguments));
}

static unique_ptr<ParsedExpression> MapExpression(const Value &value) {
	vector<unique_ptr<ParsedExpression>> keys;
	vector<unique_ptr<ParsedExpression>> values;
	for (auto &entry : MapValue::GetChildren(value)) {
		auto &pair = StructValue::GetChildren(entry);
		keys.push_back(ConstantExpression::FromValue(pair[0]));
		values.push_back(ConstantExpression::FromValue(pair[1]));
	}
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(ListValueExpression(std::move(keys)));
	arguments.push_back(ListValueExpression(std::move(values)));
	// the map constructor cannot reproduce empty or NULL-only key/value types
	return CastTo(value.type(), make_uniq<FunctionExpression>("map", std::move(arguments)));
}

unique_ptr<ParsedExpression> ConstantExpression::FromValue(const Value &value) {
	auto &type = value.type();
	if (value.IsNull()) {
		if (type.id() == LogicalTypeId::SQLNULL) {
			return Null();
		}
		return CastTo(type, Null());
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return LiteralOrCast(Literal::Boolean(value.GetValue<bool>()), value);
	case LogicalTypeId::VARCHAR:
		return LiteralOrCast(Literal::String(StringValue::Get(value)), value);
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE: {
		auto dbl = value.GetValue<double>();
		if (Value::IsNan(dbl)) {
			return StringCast(value);
		}
		if (!Value::DoubleIsFinite(dbl)) {
			// an out-of-range number literal is the SQL spelling of infinity
			return LiteralOrCast(Literal::Number(dbl < 0 ? "-1e1000" : "1e1000"), value);
		}
		return LiteralOrCast(Literal::Number(value.ToString()), value);
	}
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::DECIMAL:
		return LiteralOrCast(Literal::Number(value.ToString()), value);
	case LogicalTypeId::BLOB: {
		auto &bytes = StringValue::Get(value);
		string hex;
		hex.reserve(bytes.size() * 2);
		for (auto byte : bytes) {
			hex += StringUtil::Format("%02X", static_cast<uint8_t>(byte));
		}
		return LiteralOrCast(Literal::Hex(std::move(hex)), value);
	}
	case LogicalTypeId::BIT:
		return LiteralOrCast(Literal::Bit(value.ToString()), value);
	case LogicalTypeId::STRUCT:
		return StructExpression(value);
	case LogicalTypeId::LIST: {
		auto &children = ListValue::GetChildren(value);
		if (children.empty()) {
			return CastTo(type, ListValueExpression({}));
		}
		return ListValueExpression(ChildExpressions(children));
	}
	case LogicalTypeId::ARRAY: {
		auto &children = ArrayValue::GetChildren(value);
		if (children.empty()) {
			return CastTo(type, ListValueExpression({}));
		}
		return CastTo(type, make_uniq<FunctionExpression>("array_value", ChildExpressions(children)));
	}
	case LogicalTypeId::MAP:
		return MapExpression(value);
	case LogicalTypeId::TYPE:
		return TypeExpression::FromLogicalType(TypeValue::GetType(value));
	case LogicalTypeId::POINTER:
		return FromLiteral(Literal::Pointer(value.GetPointer()));
	case LogicalTypeId::UNION: {
		auto tag = UnionValue::GetTag(value);
		auto &name = UnionType::GetMemberName(type, tag);
		return CastTo(type, NamedArgument(name, UnionValue::GetValue(value), "union_value"));
	}
	default:
		return StringCast(value);
	}
}

} // namespace duckdb
