#include "common/limits.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(TypeCast *root) {
	if (!root) {
		return nullptr;
	}
	// get the type to cast to
	TypeName *type_name = root->typeName;
	SQLType target_type = TransformTypeName(type_name);

	// transform the expression node
	auto expression = TransformExpression(root->arg);
	// now create a cast operation
	return make_unique<CastExpression>(target_type, move(expression));
}
