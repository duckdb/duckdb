#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class TypeParameterKind : uint8_t {
	EXPRESSION,
	TYPE,
};

class Serializer;
class Deserializer;

class TypeParameter {
public:
	virtual ~TypeParameter() = default;

	static unique_ptr<TypeParameter> EXPRESSION(string name, unique_ptr<ParsedExpression> expr);
	static unique_ptr<TypeParameter> TYPE(string name, LogicalType type);

	TypeParameterKind GetKind() const {
		return kind;
	}
	bool IsExpression() const {
		return kind == TypeParameterKind::EXPRESSION;
	}
	bool IsType() const {
		return kind == TypeParameterKind::TYPE;
	}
	const string &GetName() const {
		return name;
	}
	bool HasName() const {
		return !name.empty();
	}

	virtual const unique_ptr<ParsedExpression> &GetExpression() const = 0;
	virtual const LogicalType &GetType() const = 0;

	virtual unique_ptr<TypeParameter> Copy() const = 0;
	virtual bool Equals(const TypeParameter &other) const = 0;
	virtual string ToString() const = 0;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<TypeParameter> Deserialize(Deserializer &deserializer);

protected:
	TypeParameter(TypeParameterKind kind_p, string name_p) : kind(kind_p), name(std::move(name_p)) {
	}

private:
	TypeParameterKind kind;
	string name;
};

} // namespace duckdb
