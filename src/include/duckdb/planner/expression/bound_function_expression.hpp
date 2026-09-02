//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_sql_export.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;
class FunctionBinder;
struct BoundBetweenExpression;
struct BoundCastExpression;
struct BoundComparisonExpression;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_FUNCTION;

public:
	BoundFunctionExpression(BoundScalarFunction bound_function, vector<unique_ptr<Expression>> arguments,
	                        unique_ptr<FunctionData> bind_info, bool is_operator = false);

public:
	const BoundScalarFunction &Function() const {
		return function;
	}
	BoundScalarFunction &FunctionMutable() {
		sql_export_recipe.reset();
		return function;
	}
	void SetExecutionFunction(scalar_function_t callback) {
		function.SetFunctionCallback(std::move(callback));
	}
	const vector<unique_ptr<Expression>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<Expression>> &GetChildrenMutable() {
		return children;
	}
	const unique_ptr<FunctionData> &BindInfo() const {
		return bind_info;
	}
	unique_ptr<FunctionData> &BindInfoMutable() {
		sql_export_recipe.reset();
		return bind_info;
	}
	optional_ptr<const BoundScalarFunctionSQLExportRecipe> GetSQLExportRecipe() const {
		return sql_export_recipe ? &*sql_export_recipe : nullptr;
	}
	bool IsOperator() const {
		return is_operator;
	}
	bool &IsOperatorMutable() {
		return is_operator;
	}
	bool RequiresOrderedExecution() const;

	bool IsVolatile() const override;
	bool IsConsistent() const override;
	bool IsFoldable() const override;
	bool CanThrow() const override;
	string ToString() const override;
	bool PropagatesNullValues() const override;
	hash_t Hash() const override;
	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;
	void Verify() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	void SetCatalogSQLExportRecipe(QualifiedName name, vector<LogicalType> arguments, LogicalType return_type,
	                               scalar_function_sql_export_t callback, bool requires_callback);
	void SetStructuralSQLExportRecipe(BoundFunctionSQLExportType type);
	unique_ptr<FunctionData> &BindInfoForStructuralMutation() {
		return bind_info;
	}

	friend class FunctionBinder;
	friend struct BoundBetweenExpression;
	friend struct BoundCastExpression;
	friend struct BoundComparisonExpression;

	static ExpressionType GetFunctionExpressionType(const BoundScalarFunction &bound_function,
	                                                const vector<unique_ptr<Expression>> &arguments,
	                                                optional_ptr<FunctionData> bind_info);

private:
	//! The bound function expression
	BoundScalarFunction function;
	//! List of child-expressions of the function
	vector<unique_ptr<Expression>> children;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;
	//! Authenticated logical SQL identity, independent of the current execution implementation
	optional<BoundScalarFunctionSQLExportRecipe> sql_export_recipe;
};

} // namespace duckdb
