#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(duckdb_libpgquery::PGTypeCast &root) {
	// get the type to cast to
	auto type_name = root.typeName;
	LogicalType target_type = TransformTypeName(*type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (!root.tryCast && target_type == LogicalType::BLOB && root.arg->type == duckdb_libpgquery::T_PGAConst) {
		auto c = PGPointerCast<duckdb_libpgquery::PGAConst>(root.arg);
		if (c->val.type == duckdb_libpgquery::T_PGString) {
			return make_uniq<ConstantExpression>(Value::BLOB(string(c->val.val.str)));
		}
	}
	// transform the expression node
	auto expression = TransformExpression(root.arg);
	bool try_cast = root.tryCast;

	// now create a cast operation
	return make_uniq<CastExpression>(target_type, std::move(expression), try_cast);
}

} // namespace duckdb
