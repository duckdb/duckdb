#include "duckdb/optimizer/regex_range_filter.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/value.hpp"

#include <regex>

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LikeOptimizer::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->type != LogicalOperatorType::FILTER) {
		return op;
	}

	for (auto &expr : op->expressions) {
		if (expr->type == ExpressionType::BOUND_FUNCTION) {
			auto &func = (BoundFunctionExpression &)*expr.get();
			if (func.function.name != "~~" || func.children.size() != 2) {
				continue;
			}
			Value pattern_str = ExpressionExecutor::EvaluateScalar(*func.children[1]);
			if (!pattern_str.is_null && pattern_str.type == TypeId::VARCHAR) {

				string patt_str = string(((string_t)pattern_str.str_value).GetData());

				if( std::regex_match(patt_str, std::regex("[^%_]*[%]+")) ) {
					// Prefix LIKE pattern : [^%_]*[%]+, ignoring undescore
					auto prefix_func = GetScalarFunction("prefix");
					// replace LIKE by prefix function
					func.function = prefix_func;

					// removing "%" from the prefix pattern
					patt_str.erase(std::remove(patt_str.begin(), patt_str.end(), '%'), patt_str.end());

					auto &const_expr = (BoundConstantExpression &)*func.children[1].get();
					// set the new pattern without "%"
					const_expr.value = Value(patt_str);
				} else if( std::regex_match(patt_str, std::regex("[%]+[^%_]*")) ) {
					// Suffix LIKE pattern: [%]+[^%_]*, ignoring undescore
					auto suffix_func = GetScalarFunction("suffix");
					// replace LIKE by suffix function
					func.function = suffix_func;

					// removing "%" from the suffix pattern
					patt_str.erase(std::remove(patt_str.begin(), patt_str.end(), '%'), patt_str.end());

					auto &const_expr = (BoundConstantExpression &)*func.children[1].get();
					// set the new pattern without "%"
					const_expr.value = Value(patt_str);
				} else if( std::regex_match(patt_str, std::regex("[%]+[^%_]*[%]+")) ) {
					// Contains LIKE pattern: [%]+[^%_]*[%]+, ignoring undescore
					auto contains_func = GetScalarFunction("contains");
					func.function = contains_func;

					// removing "%" from the contains pattern
					patt_str.erase(std::remove(patt_str.begin(), patt_str.end(), '%'), patt_str.end());

					auto &const_expr = (BoundConstantExpression &)*func.children[1].get();
					// set the new pattern without "%"
					const_expr.value = Value(patt_str);
				}
			}
		}
	}

	return op;
}
/**
 * \brief This method finds the builtin function from the catalog
 * \param func_name The function name
 * \return unique_ptr<BoundFunctionExpression> Returns the scalar function case
 * it was already cataloged, otherwise, throws a CatalogException
 */
ScalarFunction LikeOptimizer::GetScalarFunction(string func_name) {

	auto func_entry = (ScalarFunctionCatalogEntry *)optimizer.context.catalog.GetEntry(optimizer.context,
																					   CatalogType::SCALAR_FUNCTION,
																					   DEFAULT_SCHEMA,
																					   func_name);
	if(func_entry == nullptr || func_entry->functions.size() == 0) {
		throw CatalogException("The scalar function \"%s\" was not registered!", func_name);
	}
	return func_entry->functions[0];
}
