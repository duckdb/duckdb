#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/function_serialization.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

static const LogicalType &GetFunctionReturnType(const BoundScalarFunction &function) {
	return function.GetReturnType();
}

static bool ScalarSQLExportChildrenMatchArguments(const vector<unique_ptr<Expression>> &children,
                                                  const vector<LogicalType> &arguments) {
	if (children.size() != arguments.size()) {
		return false;
	}
	for (idx_t child_index = 0; child_index < children.size(); child_index++) {
		if (!children[child_index] || children[child_index]->GetReturnType() != arguments[child_index]) {
			return false;
		}
	}
	return true;
}

BoundFunctionExpression::BoundFunctionExpression(BoundScalarFunction bound_function,
                                                 vector<unique_ptr<Expression>> arguments,
                                                 unique_ptr<FunctionData> bind_info_p, bool is_operator)
    : Expression(GetFunctionExpressionType(bound_function, arguments, bind_info_p.get()),
                 ExpressionClass::BOUND_FUNCTION, GetFunctionReturnType(bound_function)),
      function(std::move(bound_function)), children(std::move(arguments)), bind_info(std::move(bind_info_p)),
      is_operator(is_operator) {
	D_ASSERT(!function.GetName().empty());
}

void BoundFunctionExpression::SetCatalogSQLExportRecipe(QualifiedName name, vector<LogicalType> arguments,
                                                        LogicalType return_type, scalar_function_sql_export_t callback,
                                                        bool requires_callback) {
	sql_export_recipe = BoundScalarFunctionSQLExportRecipe {BoundFunctionSQLExportType::CATALOG,
	                                                        std::move(name),
	                                                        std::move(arguments),
	                                                        std::move(return_type),
	                                                        callback,
	                                                        requires_callback};
}

void BoundFunctionExpression::SetStructuralSQLExportRecipe(BoundFunctionSQLExportType type) {
	D_ASSERT(type != BoundFunctionSQLExportType::CATALOG);
	sql_export_recipe = BoundScalarFunctionSQLExportRecipe {type, QualifiedName(), {}, LogicalType(), nullptr, false};
}

ExpressionType BoundFunctionExpression::GetFunctionExpressionType(const BoundScalarFunction &bound_function,
                                                                  const vector<unique_ptr<Expression>> &arguments,
                                                                  optional_ptr<FunctionData> bind_info_p) {
	FunctionToStringInput input(bound_function, bind_info_p.get(), arguments);
	return bound_function.GetExpressionType(input);
}

bool BoundFunctionExpression::RequiresOrderedExecution() const {
	if (function.RequiresOrderedExecution()) {
		return true;
	}
	bool has_value = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (child.GetExpressionType() != ExpressionType::BOUND_FUNCTION) {
			return;
		}
		auto &child_function = child.Cast<BoundFunctionExpression>().Function();
		has_value |= child_function.RequiresOrderedExecution();
	});
	return has_value;
}

bool BoundFunctionExpression::IsVolatile() const {
	return function.GetStability() == FunctionStability::VOLATILE ? true : Expression::IsVolatile();
}

bool BoundFunctionExpression::IsConsistent() const {
	return function.GetStability() != FunctionStability::CONSISTENT ? false : Expression::IsConsistent();
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	if (function.HasBindLambdaCallback()) {
		// This is a lambda function
		D_ASSERT(bind_info);
		auto &lambda_bind_data = bind_info->Cast<LambdaFunctionData>();
		auto lambda_expr = lambda_bind_data.GetLambdaExpression();
		if (lambda_expr && lambda_expr->IsVolatile()) {
			return false;
		}
	}
	return function.GetStability() == FunctionStability::VOLATILE ? false : Expression::IsFoldable();
}

bool BoundFunctionExpression::CanThrow() const {
	if (function.GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		return true;
	}
	return Expression::CanThrow();
}

string BoundFunctionExpression::ToString() const {
	if (function.HasToStringCallback()) {
		FunctionToStringInput input(function, bind_info.get(), children);
		return function.FunctionToString(input);
	}
	auto &function_name = function.GetName().GetIdentifierName();

	if (is_operator) {
		// built-in operator
		if (children.size() == 1) {
			if (StringUtil::Contains(function_name, "__postfix")) {
				return "((" + children[0]->ToString() + ")" + StringUtil::Replace(function_name, "__postfix", "") + ")";
			}
			return function_name + "(" + children[0]->ToString() + ")";
		}
		if (children.size() == 2) {
			return StringUtil::Format("(%s %s %s)", children[0]->ToString(), function_name, children[1]->ToString());
		}
	}

	// standard function call
	string result;
	result += SQLIdentifier(function_name);
	result += "(";

	result += StringUtil::Join(children, children.size(), ", ",
	                           [&](const unique_ptr<Expression> &child) { return child->ToString(); });

	result += ")";
	return result;
}

bool BoundFunctionExpression::PropagatesNullValues() const {
	return function.GetNullHandling() == FunctionNullHandling::SPECIAL_HANDLING ? false
	                                                                            : Expression::PropagatesNullValues();
}

hash_t BoundFunctionExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, function.Hash());
}

bool BoundFunctionExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundFunctionExpression>();
	if (other.function != function) {
		return false;
	}
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	if (!FunctionData::Equals(bind_info.get(), other.bind_info.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() const {
	vector<unique_ptr<Expression>> new_children;
	new_children.reserve(children.size());
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	unique_ptr<FunctionData> new_bind_info = bind_info ? bind_info->Copy() : nullptr;

	auto copy =
	    make_uniq<BoundFunctionExpression>(function, std::move(new_children), std::move(new_bind_info), is_operator);
	copy->CopyProperties(*this);
	copy->sql_export_recipe = sql_export_recipe;
	return std::move(copy);
}

void BoundFunctionExpression::Verify() const {
	D_ASSERT(!function.GetName().empty());
	D_ASSERT(function.GetDefinition());
}

void BoundFunctionExpression::Serialize(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(StorageVersion::V2_0_0) && function.HasLegacySerializeCallback()) {
		// serialize legacy expression for backwards compatibility
		FunctionToStringInput input(function, bind_info.get(), children);
		auto legacy_expr = function.GetLegacySerializeCallback()(input);
		legacy_expr->Serialize(serializer);
		return;
	}

	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	FunctionSerializer::Serialize(serializer, function, bind_info.get());
	serializer.WriteProperty(202, "is_operator", is_operator);
	serializer.WriteProperty(203, "has_sql_export_recipe", sql_export_recipe.has_value());
	if (sql_export_recipe) {
		serializer.WriteProperty<uint8_t>(204, "sql_export_type", static_cast<uint8_t>(sql_export_recipe->type));
		if (sql_export_recipe->type == BoundFunctionSQLExportType::CATALOG) {
			serializer.WriteProperty(205, "sql_export_name", sql_export_recipe->name);
			serializer.WriteProperty(206, "sql_export_arguments", sql_export_recipe->arguments);
			serializer.WriteProperty(207, "sql_export_return_type", sql_export_recipe->return_type);
		}
	}
}

namespace {

//! Plans serialized before the lambda expression was kept in the children do not contain it. Recover it from
//! the bind data, so that the children line up with the function's arguments either way.
void RestoreErasedLambdaChild(const BoundScalarFunction &function, optional_ptr<FunctionData> bind_info,
                              vector<unique_ptr<Expression>> &children) {
	if (!function.HasBindLambdaCallback() || !bind_info) {
		return;
	}
	auto &arguments = function.GetArguments();
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() != LogicalTypeId::LAMBDA) {
			continue;
		}
		if (i < children.size() && children[i]->GetReturnType().id() == LogicalTypeId::LAMBDA) {
			// the lambda is already where it belongs
			return;
		}
		auto lambda_child = bind_info->Cast<LambdaFunctionData>().RecoverLambdaChild();
		if (lambda_child && i <= children.size()) {
			children.insert(children.begin() + NumericCast<int64_t>(i), std::move(lambda_child));
		}
		return;
	}
}

} // namespace

unique_ptr<Expression> BoundFunctionExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");

	auto entry = FunctionSerializer::Deserialize<BoundScalarFunction, ScalarFunctionCatalogEntry>(
	    deserializer, CatalogType::SCALAR_FUNCTION_ENTRY, children, return_type);

	auto is_operator = deserializer.ReadProperty<bool>(202, "is_operator");
	auto has_sql_export_recipe = deserializer.ReadPropertyWithDefault<bool>(203, "has_sql_export_recipe");
	optional<BoundScalarFunctionSQLExportRecipe> sql_export_recipe;
	if (has_sql_export_recipe) {
		auto export_type =
		    static_cast<BoundFunctionSQLExportType>(deserializer.ReadProperty<uint8_t>(204, "sql_export_type"));
		if (export_type == BoundFunctionSQLExportType::CATALOG) {
			auto name = deserializer.ReadProperty<QualifiedName>(205, "sql_export_name");
			auto arguments = deserializer.ReadProperty<vector<LogicalType>>(206, "sql_export_arguments");
			auto recipe_return_type = deserializer.ReadProperty<LogicalType>(207, "sql_export_return_type");
			sql_export_recipe = BoundScalarFunctionSQLExportRecipe {
			    export_type, std::move(name), std::move(arguments), std::move(recipe_return_type), nullptr, false};
		} else if (export_type == BoundFunctionSQLExportType::CAST ||
		           export_type == BoundFunctionSQLExportType::COMPARISON ||
		           export_type == BoundFunctionSQLExportType::BETWEEN) {
			sql_export_recipe =
			    BoundScalarFunctionSQLExportRecipe {export_type, QualifiedName(), {}, LogicalType(), nullptr, false};
		} else {
			throw SerializationException("Bound scalar function has an invalid SQL export recipe type");
		}
	}

	RestoreErasedLambdaChild(entry.first, entry.second.get(), children);

	if (entry.first.HasBindExpressionCallback()) {
		// bind the function expression
		auto &context = deserializer.Get<ClientContext &>();
		auto bind_input = FunctionBindExpressionInput(context, entry.first, entry.second, children);
		// replace the function expression with the bound expression
		auto bound_expression = entry.first.GetBindExpressionCallback()(bind_input);
		if (bound_expression) {
			return bound_expression;
		}
		// Otherwise, fall through and continue on normally
	}
	auto result =
	    make_uniq<BoundFunctionExpression>(std::move(entry.first), std::move(children), std::move(entry.second));
	result->is_operator = is_operator;
	if (sql_export_recipe) {
		switch (sql_export_recipe->type) {
		case BoundFunctionSQLExportType::CATALOG: {
			auto &definition = result->function.GetDefinition();
			if (!definition ||
			    sql_export_recipe->name !=
			        QualifiedName(definition->GetCatalogName(), definition->GetSchemaName(), definition->GetName()) ||
			    !ScalarSQLExportChildrenMatchArguments(result->children, sql_export_recipe->arguments) ||
			    sql_export_recipe->return_type != result->GetReturnType()) {
				throw SerializationException(
				    "Bound scalar function SQL export recipe does not match the live catalog definition");
			}
			result->SetCatalogSQLExportRecipe(
			    std::move(sql_export_recipe->name), std::move(sql_export_recipe->arguments),
			    std::move(sql_export_recipe->return_type), definition->GetSQLExportCallback(),
			    definition->HasBindCallback() || definition->GetCaptureArgumentAliases());
			break;
		}
		case BoundFunctionSQLExportType::CAST:
			if (result->GetExpressionType() != ExpressionType::OPERATOR_CAST ||
			    !BoundCastExpression::HasValidBindData(*result)) {
				throw SerializationException("Bound cast SQL export recipe does not match the deserialized expression");
			}
			result->SetStructuralSQLExportRecipe(BoundFunctionSQLExportType::CAST);
			break;
		case BoundFunctionSQLExportType::COMPARISON:
			if (!BoundComparisonExpression::IsComparison(*result) || result->BindInfo()) {
				throw SerializationException(
				    "Bound comparison SQL export recipe does not match the deserialized expression");
			}
			result->SetStructuralSQLExportRecipe(BoundFunctionSQLExportType::COMPARISON);
			break;
		case BoundFunctionSQLExportType::BETWEEN:
			if (result->GetExpressionType() != ExpressionType::COMPARE_BETWEEN ||
			    !BoundBetweenExpression::HasValidBindData(*result)) {
				throw SerializationException(
				    "Bound BETWEEN SQL export recipe does not match the deserialized expression");
			}
			result->SetStructuralSQLExportRecipe(BoundFunctionSQLExportType::BETWEEN);
			break;
		}
	}
	if (result->return_type != return_type) {
		// return type mismatch - push a cast
		auto &context = deserializer.Get<ClientContext &>();
		return BoundCastExpression::AddCastToType(context, std::move(result), return_type);
	}
	return std::move(result);
}

} // namespace duckdb
