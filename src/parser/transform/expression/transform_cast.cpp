#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(PGTypeCast *root) {
	if (!root) {
		return nullptr;
	}
	// get the type to cast to
	auto type_name = root->typeName;
	LogicalType target_type = TransformTypeName(type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (target_type == LogicalType::BLOB && root->arg->type == T_PGAConst) {
		PGAConst *c = reinterpret_cast<PGAConst *>(root->arg);
		if (c->val.type == T_PGString) {
			return make_unique<ConstantExpression>(Value::BLOB(string(c->val.val.str)));
		}
	}
	// transform the expression node
	auto expression = TransformExpression(root->arg);

	// now create a cast operation
	return make_unique<CastExpression>(target_type, move(expression));
}

} // namespace duckdb
