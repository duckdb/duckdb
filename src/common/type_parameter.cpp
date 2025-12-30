#include "duckdb/common/type_parameter.hpp"

namespace duckdb {

namespace {

class TypeTypeParameter final : public TypeParameter {
public:
	TypeTypeParameter(string name_p, LogicalType type_p)
	    : TypeParameter(TypeParameterKind::TYPE, std::move(name_p)), type(std::move(type_p)) {
	}

	unique_ptr<TypeParameter> Copy() const override {
		return make_uniq<TypeTypeParameter>(GetName(), type);
	}

	bool Equals(const TypeParameter &other) const override {
		if (!other.IsType()) {
			return false;
		}
		if (other.GetName() != GetName()) {
			return false;
		}
		return type == other.GetType();
	}

	string ToString() const override {
		if (HasName()) {
			return GetName() + " " + type.ToString();
		}
		return type.ToString();
	}

	const LogicalType &GetType() const override {
		return type;
	}

	const unique_ptr<ParsedExpression> &GetExpression() const override {
		throw InternalException("Cannot get expression from TypeTypeParameter");
	}

private:
	LogicalType type;
};

class ExpressionTypeParameter final : public TypeParameter {
public:
	ExpressionTypeParameter(string name_p, unique_ptr<ParsedExpression> expr_p)
	    : TypeParameter(TypeParameterKind::EXPRESSION, std::move(name_p)), expr(std::move(expr_p)) {
	}

	unique_ptr<TypeParameter> Copy() const override {
		return make_uniq<ExpressionTypeParameter>(GetName(), expr->Copy());
	}

	bool Equals(const TypeParameter &other) const override {
		if (!other.IsExpression()) {
			return false;
		}
		if (other.GetName() != GetName()) {
			return false;
		}
		return ParsedExpression::Equals(expr, other.GetExpression());
	}

	string ToString() const override {
		if (HasName()) {
			return GetName() + " (" + expr->ToString() + ")";
		}
		return expr->ToString();
	}

	const LogicalType &GetType() const override {
		throw InternalException("Cannot get type from ExpressionTypeParameter");
	}

	const unique_ptr<ParsedExpression> &GetExpression() const override {
		return expr;
	}

private:
	unique_ptr<ParsedExpression> expr;
};

} // namespace

unique_ptr<TypeParameter> TypeParameter::TYPE(string name, LogicalType type) {
	return make_uniq<TypeTypeParameter>(std::move(name), std::move(type));
}

unique_ptr<TypeParameter> TypeParameter::EXPRESSION(string name, unique_ptr<ParsedExpression> expr) {
	return make_uniq<ExpressionTypeParameter>(std::move(name), std::move(expr));
}

void TypeParameter::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault(100, "kind", kind, TypeParameterKind::EXPRESSION);
	serializer.WritePropertyWithDefault(101, "name", name, string());

	switch (kind) {
	case TypeParameterKind::EXPRESSION:
		serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression", GetExpression());
		break;
	case TypeParameterKind::TYPE:
		serializer.WritePropertyWithDefault<LogicalType>(201, "type", GetType(), LogicalType::INVALID);
		break;
	default:
		throw SerializationException("Unsupported TypeParameterKind for serialization");
	}
}

unique_ptr<TypeParameter> TypeParameter::Deserialize(Deserializer &deserializer) {
	auto kind =
	    deserializer.ReadPropertyWithExplicitDefault<TypeParameterKind>(100, "kind", TypeParameterKind::EXPRESSION);

	auto name = deserializer.ReadPropertyWithExplicitDefault<string>(101, "name", string());

	switch (kind) {
	case TypeParameterKind::EXPRESSION: {
		auto expr = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression");
		return TypeParameter::EXPRESSION(std::move(name), std::move(expr));
	}
	case TypeParameterKind::TYPE: {
		auto type = deserializer.ReadPropertyWithExplicitDefault<LogicalType>(201, "type", LogicalType::INVALID);
		return TypeParameter::TYPE(std::move(name), std::move(type));
	}
	default:
		throw SerializationException("Unsupported TypeParameterKind for deserialization");
	}
}

} // namespace duckdb
