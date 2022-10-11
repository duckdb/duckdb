#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

BoundCastExpression::BoundCastExpression(unique_ptr<Expression> child_p, LogicalType target_type_p,
                                         BoundCastInfo bound_cast_p, bool try_cast_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, move(target_type_p)), child(move(child_p)),
      try_cast(try_cast_p), bound_cast(move(bound_cast_p)) {
}

unique_ptr<Expression> AddCastExpressionInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                                 BoundCastInfo bound_cast, bool try_cast) {
	if (expr->return_type == target_type) {
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
	return make_unique<BoundCastExpression>(move(expr), target_type, move(bound_cast), try_cast);
}

static BoundCastInfo BindCastFunction(ClientContext &context, const LogicalType &source, const LogicalType &target) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput input(context);
	return cast_functions.GetCastFunction(source, target, input);
}

unique_ptr<Expression> AddCastToTypeInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                             CastFunctionSet &cast_functions, GetCastFunctionInput &get_input,
                                             bool try_cast) {
	D_ASSERT(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*expr;
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
		auto &def = (BoundDefaultExpression &)*expr;
		def.return_type = target_type;
	}
	if (!target_type.IsValid()) {
		return expr;
	}

	auto cast_function = cast_functions.GetCastFunction(expr->return_type, target_type, get_input);
	return AddCastExpressionInternal(move(expr), target_type, move(cast_function), try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddDefaultCastToType(unique_ptr<Expression> expr,
                                                                 const LogicalType &target_type, bool try_cast) {
	CastFunctionSet default_set;
	GetCastFunctionInput get_input;
	return AddCastToTypeInternal(move(expr), target_type, default_set, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(ClientContext &context, unique_ptr<Expression> expr,
                                                          const LogicalType &target_type, bool try_cast) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput get_input(context);
	return AddCastToTypeInternal(move(expr), target_type, cast_functions, get_input, try_cast);
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
	if (source_type.id() == LogicalTypeId::TIMESTAMP || source_type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		switch (target_type.id()) {
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			return false;
		default:
			break;
		}
	}
	if (source_type.id() == LogicalTypeId::VARCHAR) {
		switch (target_type.id()) {
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

bool BoundCastExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundCastExpression *)other_p;
	if (!Expression::Equals(child.get(), other->child.get())) {
		return false;
	}
	if (try_cast != other->try_cast) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_unique<BoundCastExpression>(child->Copy(), return_type, bound_cast.Copy(), try_cast);
	copy->CopyProperties(*this);
	return move(copy);
}

void BoundCastExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*child);
	writer.WriteSerializable(return_type);
	writer.WriteField(try_cast);
}

unique_ptr<Expression> BoundCastExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	auto child = reader.ReadRequiredSerializable<Expression>(state.gstate);
	auto target_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto try_cast = reader.ReadRequired<bool>();
	auto cast_function = BindCastFunction(state.gstate.context, child->return_type, target_type);
	return make_unique<BoundCastExpression>(move(child), move(target_type), move(cast_function), try_cast);
}

} // namespace duckdb
