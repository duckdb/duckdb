#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(PGTypeCast *root) {
	if (!root) {
		return nullptr;
	}
	// get the type to cast to
	auto type_name = root->typeName;
	SQLType target_type = TransformTypeName(type_name);

	//check by a constant BLOB value, then change PGAConstant value type to T_PGBitString
	if(target_type == SQLType::BLOB && root->arg->type == T_PGAConst) {
		PGAConst *c = reinterpret_cast<PGAConst *>(root->arg);
		c->val.type = T_PGBitString;
	}

	// transform the expression node
	auto expression = TransformExpression(root->arg);

//	//check by a constant VARCHAR value
//	if(expression->type == ExpressionType::VALUE_CONSTANT && target_type == SQLType::VARCHAR) {
//		auto constant_expression_ptr = static_cast<ConstantExpression*>(expression.get());
//		//validating VARCHAR and scape BLOB strings
//		constant_expression_ptr->value.ValidateString();
//	}
	// now create a cast operation
	return make_unique<CastExpression>(target_type, move(expression));
}
