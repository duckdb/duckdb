#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static BoundCastInfo BindCastFunction(ClientContext &context, const LogicalType &source, const LogicalType &target) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput input(context);
	return cast_functions.GetCastFunction(source, target, input);
}

BoundCastExpression::BoundCastExpression(unique_ptr<Expression> child_p, LogicalType target_type_p,
                                         BoundCastInfo bound_cast_p, bool try_cast_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, std::move(target_type_p)),
      child(std::move(child_p)), try_cast(try_cast_p), bound_cast(std::move(bound_cast_p)) {
}

BoundCastExpression::BoundCastExpression(ClientContext &context, unique_ptr<Expression> child_p,
                                         LogicalType target_type_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, std::move(target_type_p)),
      child(std::move(child_p)), try_cast(false),
      bound_cast(BindCastFunction(context, child->return_type, return_type)) {
}

unique_ptr<Expression> AddCastExpressionInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                                 BoundCastInfo bound_cast, bool try_cast) {
	if (ExpressionBinder::GetExpressionReturnType(*expr) == target_type) {
		return expr;
	}
	auto &expr_type = expr->return_type;
	if (target_type.id() == LogicalTypeId::LIST && expr_type.id() == LogicalTypeId::LIST) {
		auto &target_list = ListType::GetChildType(target_type);
		auto &expr_list = ListType::GetChildType(expr_type);
		if (target_list.id() == LogicalTypeId::ANY || expr_list == target_list) {
			return expr;
		}
	}
	auto result = make_uniq<BoundCastExpression>(std::move(expr), target_type, std::move(bound_cast), try_cast);
	result->query_location = result->child->query_location;
	return std::move(result);
}

unique_ptr<Expression> AddCastToTypeInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                             CastFunctionSet &cast_functions, GetCastFunctionInput &get_input,
                                             bool try_cast) {
	D_ASSERT(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = expr->Cast<BoundParameterExpression>();
		if (!target_type.IsValid()) {
			// invalidate the parameter
			parameter.parameter_data->return_type = LogicalType::INVALID;
			parameter.return_type = target_type;
			return expr;
		}
		if (parameter.parameter_data->return_type.id() == LogicalTypeId::INVALID) {
			// we don't know the type of this parameter
			parameter.return_type = target_type;
			return expr;
		}
		if (parameter.parameter_data->return_type.id() == LogicalTypeId::UNKNOWN) {
			// prepared statement parameter cast - but there is no type, convert the type
			parameter.parameter_data->return_type = target_type;
			parameter.return_type = target_type;
			return expr;
		}
		// prepared statement parameter already has a type
		if (parameter.parameter_data->return_type == target_type) {
			// this type! we are done
			parameter.return_type = parameter.parameter_data->return_type;
			return expr;
		}
		// invalidate the type
		parameter.parameter_data->return_type = LogicalType::INVALID;
		parameter.return_type = target_type;
		return expr;
	} else if (expr->expression_class == ExpressionClass::BOUND_DEFAULT) {
		D_ASSERT(target_type.IsValid());
		auto &def = expr->Cast<BoundDefaultExpression>();
		def.return_type = target_type;
	}
	if (!target_type.IsValid()) {
		return expr;
	}

	auto cast_function = cast_functions.GetCastFunction(expr->return_type, target_type, get_input);
	return AddCastExpressionInternal(std::move(expr), target_type, std::move(cast_function), try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddDefaultCastToType(unique_ptr<Expression> expr,
                                                                 const LogicalType &target_type, bool try_cast) {
	CastFunctionSet default_set;
	GetCastFunctionInput get_input;
	get_input.query_location = expr->query_location;
	return AddCastToTypeInternal(std::move(expr), target_type, default_set, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(ClientContext &context, unique_ptr<Expression> expr,
                                                          const LogicalType &target_type, bool try_cast) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput get_input(context);
	get_input.query_location = expr->query_location;
	return AddCastToTypeInternal(std::move(expr), target_type, cast_functions, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddArrayCastToList(ClientContext &context, unique_ptr<Expression> expr) {
	if (expr->return_type.id() != LogicalTypeId::ARRAY) {
		return expr;
	}
	auto &child_type = ArrayType::GetChildType(expr->return_type);
	return BoundCastExpression::AddCastToType(context, std::move(expr), LogicalType::LIST(child_type));
}

bool BoundCastExpression::CastIsInvertible(const LogicalType &source_type, const LogicalType &target_type) {
	D_ASSERT(source_type.IsValid() && target_type.IsValid());
	if (source_type.id() == LogicalTypeId::BOOLEAN || target_type.id() == LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::FLOAT || target_type.id() == LogicalTypeId::FLOAT) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::DOUBLE || target_type.id() == LogicalTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::DECIMAL || target_type.id() == LogicalTypeId::DECIMAL) {
		uint8_t source_width, target_width;
		uint8_t source_scale, target_scale;
		// cast to or from decimal
		// cast is only invertible if the cast is strictly widening
		if (!source_type.GetDecimalProperties(source_width, source_scale)) {
			return false;
		}
		if (!target_type.GetDecimalProperties(target_width, target_scale)) {
			return false;
		}
		if (target_scale < source_scale) {
			return false;
		}
		return true;
	}
	switch (source_type.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		switch (target_type.id()) {
			// see types.hpp to see timestamp ranking
		case LogicalTypeId::TIMESTAMP:
			return source_type.id() <= LogicalTypeId::TIMESTAMP;
		case LogicalTypeId::TIMESTAMP_SEC:
			return source_type.id() <= LogicalTypeId::TIMESTAMP_SEC;
		case LogicalTypeId::TIMESTAMP_MS:
			return source_type.id() <= LogicalTypeId::TIMESTAMP_MS;
		case LogicalTypeId::TIMESTAMP_NS:
			return source_type.id() <= LogicalTypeId::TIMESTAMP_NS;
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			return false;
		case LogicalTypeId::TIMESTAMP_TZ:
			return source_type.id() == LogicalTypeId::TIMESTAMP_TZ;
		default:
			break;
		}
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::TIME_TZ:
		return false;
	default:
		break;
	}
	if (target_type.id() == LogicalTypeId::VARCHAR) {
		switch (source_type.id()) {
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::TIMESTAMP_TZ:
			return true;
		default:
			return false;
		}
	}
	return true;
}

string BoundCastExpression::ToString() const {
	return (try_cast ? "TRY_CAST(" : "CAST(") + child->GetName() + " AS " + return_type.ToString() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundCastExpression>();
	if (!Expression::Equals(*child, *other.child)) {
		return false;
	}
	if (try_cast != other.try_cast) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() const {
	auto copy = make_uniq<BoundCastExpression>(child->Copy(), return_type, bound_cast.Copy(), try_cast);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
