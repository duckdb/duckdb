#pragma once

namespace duckdb {

enum class TypeParameterKind : uint8_t {
	EXPRESSION,
	TYPE,
};

class TypeParameter {
public:
	virtual ~TypeParameter() = default;

	static unique_ptr<TypeParameter> EXPRESSION(unique_ptr<ParsedExpression> expr);
	static unique_ptr<TypeParameter> TYPE(LogicalType type);

	TypeParameterKind GetKind() const {
		return kind;
	}
	bool IsExpression() const {
		return kind == TypeParameterKind::EXPRESSION;
	}
	bool IsType() const {
		return kind == TypeParameterKind::TYPE;
	}

	virtual const unique_ptr<ParsedExpression> &GetExpression() const = 0;
	virtual const LogicalType &GetType() const = 0;

	virtual unique_ptr<TypeParameter> Copy() const = 0;
	virtual bool Equals(const TypeParameter &other) const = 0;
	virtual string ToString() const = 0;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<TypeParameter> Deserialize(Deserializer &deserializer);

protected:
	TypeParameter(TypeParameterKind kind_p) : kind(kind_p) {
	}

private:
	TypeParameterKind kind;
};

} // namespace duckdb
