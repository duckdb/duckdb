#include "common/limits.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformTypeCast(TypeCast *root) {
	if (!root) {
		return nullptr;
	}
	// get the type to cast to
	TypeName *type_name = root->typeName;
	char *name = (reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)->val.str);
	TypeId target_type = TransformStringToTypeId(name);

	if (root->arg->type == T_A_Const) {
		// cast a constant value
		// get the original constant value
		auto constant = TransformConstant(reinterpret_cast<A_Const *>(root->arg));
		Value &source_value = reinterpret_cast<ConstantExpression *>(constant.get())->value;

		if (!source_value.is_null && TypeIsIntegral(source_value.type) && TypeIsIntegral(target_type)) {
			// properly handle numeric overflows
			target_type = std::max(MinimalType(source_value.GetNumericValue()), target_type);
		}

		// perform the cast and substitute the expression
		Value new_value = source_value.CastAs(target_type);

		return make_unique<ConstantExpression>(new_value);
	} else {
		// transform the expression node
		auto expression = TransformExpression(root->arg);
		// now create a cast operation
		return make_unique<CastExpression>(target_type, move(expression));
	}
}
