//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/positional_reference_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

class PositionalReferenceExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::POSITIONAL_REFERENCE;

public:
	DUCKDB_API explicit PositionalReferenceExpression(idx_t index);

	idx_t index;

public:
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	static bool Equal(const PositionalReferenceExpression &a, const PositionalReferenceExpression &b);
	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	PositionalReferenceExpression();
};
} // namespace duckdb
