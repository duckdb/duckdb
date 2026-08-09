#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static unique_ptr<Expression> AddCastExpressionInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                                        BoundCastInfo bound_cast, bool try_cast) {
	if (ExpressionBinder::GetExpressionReturnType(*expr) == target_type) {
		return expr;
	}
	auto &expr_type = expr->GetReturnType();
	if (target_type.id() == LogicalTypeId::LIST && expr_type.id() == LogicalTypeId::LIST) {
		auto &target_list = ListType::GetChildType(target_type);
		auto &expr_list = ListType::GetChildType(expr_type);
		if (target_list.id() == LogicalTypeId::ANY || expr_list == target_list) {
			return expr;
		}
	}
	return BoundCastExpression::Create(std::move(expr), target_type, std::move(bound_cast), try_cast);
}

static unique_ptr<Expression> AddCastToTypeInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                                    CastFunctionSet &cast_functions, GetCastFunctionInput &get_input,
                                                    bool try_cast) {
	D_ASSERT(expr);
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = expr->Cast<BoundParameterExpression>();
		if (!target_type.IsValid()) {
			// invalidate the parameter
			parameter.ParameterData()->return_type = LogicalType::INVALID;
			parameter.SetReturnType(target_type);
			return expr;
		}
		if (parameter.ParameterData()->return_type.id() == LogicalTypeId::INVALID) {
			// we don't know the type of this parameter
			parameter.SetReturnType(target_type);
			return expr;
		}
		if (parameter.ParameterData()->return_type.id() == LogicalTypeId::UNKNOWN) {
			// prepared statement parameter cast - but there is no type, convert the type
			parameter.ParameterData()->return_type = target_type;
			parameter.SetReturnType(target_type);
			return expr;
		}
		// prepared statement parameter already has a type
		if (parameter.ParameterData()->return_type == target_type) {
			// this type! we are done
			parameter.SetReturnType(parameter.ParameterData()->return_type);
			return expr;
		}
		// If this occurrence's own return_type still matches parameter_data->return_type, the
		// parameter was pinned by an inner cast on this same occurrence (e.g. CAST(CAST($1 AS A) AS B)).
		// Add a regular cast on top instead of invalidating.
		if (parameter.GetReturnType() == parameter.ParameterData()->return_type) {
			auto cast_function = cast_functions.GetCastFunction(parameter.GetReturnType(), target_type, get_input);
			return AddCastExpressionInternal(std::move(expr), target_type, std::move(cast_function), try_cast);
		}
		// invalidate the type
		parameter.ParameterData()->return_type = LogicalType::INVALID;
		parameter.SetReturnType(target_type);
		return expr;
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_DEFAULT) {
		D_ASSERT(target_type.IsValid());
		auto &def = expr->Cast<BoundDefaultExpression>();
		def.SetReturnType(target_type);
	}
	if (!target_type.IsValid()) {
		return expr;
	}

	auto cast_function = cast_functions.GetCastFunction(expr->GetReturnType(), target_type, get_input);
	return AddCastExpressionInternal(std::move(expr), target_type, std::move(cast_function), try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddDefaultCastToType(unique_ptr<Expression> expr,
                                                                 const LogicalType &target_type, bool try_cast) {
	CastFunctionSet default_set;
	GetCastFunctionInput get_input;
	get_input.query_location = expr->GetQueryLocation();
	return AddCastToTypeInternal(std::move(expr), target_type, default_set, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(ClientContext &context, unique_ptr<Expression> expr,
                                                          const LogicalType &target_type, bool try_cast) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput get_input(context);
	get_input.query_location = expr->GetQueryLocation();
	return AddCastToTypeInternal(std::move(expr), target_type, cast_functions, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddArrayCastToList(ClientContext &context, unique_ptr<Expression> expr) {
	if (expr->GetReturnType().id() != LogicalTypeId::ARRAY) {
		return expr;
	}
	auto &child_type = ArrayType::GetChildType(expr->GetReturnType());
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
	if (source_type.id() == LogicalTypeId::VARIANT || target_type.id() == LogicalTypeId::VARIANT) {
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
	case LogicalTypeId::TIMESTAMP_TZ_NS:
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
		case LogicalTypeId::TIMESTAMP_TZ_NS:
			return LogicalTypeId::TIMESTAMP_TZ <= source_type.id() &&
			       source_type.id() <= LogicalTypeId::TIMESTAMP_TZ_NS;
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
		case LogicalTypeId::TIME_NS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_TZ_NS:
			return true;
		default:
			return false;
		}
	}
	if (source_type.IsSigned() && target_type.IsUnsigned()) {
		return false;
	}
	return true;
}

//! The child type of a LIST or ARRAY
static const LogicalType &ListOrArrayChildType(const LogicalType &type) {
	return type.id() == LogicalTypeId::ARRAY ? ArrayType::GetChildType(type) : ListType::GetChildType(type);
}

//! Whether a cast of a nested type can throw a runtime error
//! Nested casts are executed on the child types, so they throw if any of the child casts throws, or if the
//! structure of the source does not fit into the structure of the target
static bool NestedCastCanThrow(const LogicalType &source_type, const LogicalType &target_type) {
	// Casts to and from UNION validate the union tags, which can throw
	if (source_type.id() == LogicalTypeId::UNION || target_type.id() == LogicalTypeId::UNION) {
		return true;
	}
	switch (source_type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		if (target_type.id() == LogicalTypeId::ARRAY) {
			// Casting into a fixed-size ARRAY throws whenever the source length differs
			if (source_type.id() != LogicalTypeId::ARRAY || ArrayType::IsAnySize(source_type) ||
			    ArrayType::IsAnySize(target_type) ||
			    ArrayType::GetSize(source_type) != ArrayType::GetSize(target_type)) {
				return true;
			}
		} else if (target_type.id() != LogicalTypeId::LIST) {
			return true;
		}
		return BoundCastExpression::CastCanThrow(ListOrArrayChildType(source_type), ListOrArrayChildType(target_type),
		                                         false);
	case LogicalTypeId::STRUCT: {
		if (target_type.id() != LogicalTypeId::STRUCT) {
			return true;
		}
		auto &source_children = StructType::GetChildTypes(source_type);
		auto &target_children = StructType::GetChildTypes(target_type);
		if (source_children.size() != target_children.size()) {
			return true;
		}
		for (idx_t child_idx = 0; child_idx < source_children.size(); child_idx++) {
			if (BoundCastExpression::CastCanThrow(source_children[child_idx].second, target_children[child_idx].second,
			                                      false)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::MAP:
		if (target_type.id() != LogicalTypeId::MAP) {
			return true;
		}
		// Map keys cannot be NULL, so a failing key cast throws even when the value cast would default to NULL
		return BoundCastExpression::CastCanThrow(MapType::KeyType(source_type), MapType::KeyType(target_type), false) ||
		       BoundCastExpression::CastCanThrow(MapType::ValueType(source_type), MapType::ValueType(target_type),
		                                         false);
	default:
		return true;
	}
}

bool BoundCastExpression::CastCanThrow(const LogicalType &source_type, const LogicalType &target_type, bool try_cast) {
	if (source_type == target_type) {
		return false;
	}
	// Casts involving custom types are implemented by extensions, so we cannot reason about them - they can throw
	// even for a try_cast, e.g. casting JSON into a MAP throws when a key fails to cast, since map keys cannot be NULL
	if (source_type.HasAlias() || target_type.HasAlias()) {
		return true;
	}
	if (try_cast) {
		// try_cast turns conversion errors into NULL values instead of throwing
		return false;
	}
	// Every value has a string representation, so casting to VARCHAR never throws. VARCHAR is not the maximum
	// of the coercion lattice (you do not implicitly widen to it), so the max-type check below would miss this.
	if (target_type.id() == LogicalTypeId::VARCHAR) {
		return false;
	}
	// Casting from a string requires parsing it, which throws whenever the string is not a valid representation of
	// the target type. Types that sit above VARCHAR in the coercion lattice (e.g. UUID, BLOB, BIT, nested types)
	// would otherwise be considered safe by the max-type check below.
	// ENUM values are cast through their string value, so casting from an ENUM parses a string as well
	if (source_type.id() == LogicalTypeId::VARCHAR || source_type.id() == LogicalTypeId::ENUM) {
		return true;
	}
	// BIGNUM has arbitrary precision, so its values do not necessarily fit in any other type, e.g. BIGNUM -> DOUBLE
	// overflows for large values, even though BIGNUM implicitly casts to DOUBLE
	if (source_type.id() == LogicalTypeId::BIGNUM) {
		return true;
	}
	// Casts of nested types are executed on the child types - recurse into them
	if (source_type.IsNested() || target_type.IsNested()) {
		return NestedCastCanThrow(source_type, target_type);
	}
	// Casting to DECIMAL overflows whenever the value does not fit in the width and scale, e.g. 100 -> DECIMAL(38,37)
	if (target_type.id() == LogicalTypeId::DECIMAL) {
		return true;
	}
	// Casting to a TIMESTAMP overflows for values outside of the timestamp range, even when widening the type,
	// e.g. a DATE far in the future does not fit in a TIMESTAMP
	if (LogicalType::TypeIsTimestamp(target_type) || target_type.id() == LogicalTypeId::TIMESTAMP_TZ_NS) {
		return true;
	}
	// A cast only succeeds for every input if the target type can represent every value of the source type,
	// i.e. if the target is the maximum of the two types. Narrowing casts can overflow, and casts between types
	// that have no ordering in the lattice can fail to convert - or not be implemented at all, e.g. INTEGER -> DATE
	LogicalType max_type;
	if (!LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(source_type, target_type, max_type)) {
		return true;
	}
	return max_type != target_type;
}

} // namespace duckdb
