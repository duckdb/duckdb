#include "duckdb/planner/named_parameter_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

NamedParameterBinder::NamedParameterBinder(Binder &binder, QueryErrorContext error_context) : binder(binder), error_context(error_context) {
}

void NamedParameterBinder::EvaluateInputParameters(vector<LogicalType> &arguments, vector<Value> &parameters, unordered_map<string, Value> &named_parameters, vector<unique_ptr<ParsedExpression>> &children, string func_type) {
	for (auto &child : children) {
		string parameter_name;

		ConstantBinder constant_binder(binder, binder.context, func_type + " FUNCTION parameter");
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = (ComparisonExpression &)*child;
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = (ColumnRefExpression &)*comp.left;
				if (colref.table_name.empty()) {
					parameter_name = colref.column_name;
					child = move(comp.right);
				}
			}
		}
		LogicalType sql_type;
		auto expr = constant_binder.Bind(child, &sql_type);
		if (!expr->IsFoldable()) {
			throw BinderException(error_context.FormatError(func_type + " function requires a constant parameter"));
		}
		auto constant = ExpressionExecutor::EvaluateScalar(*expr);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (named_parameters.size() > 0) {
				throw BinderException(error_context.FormatError("Unnamed parameters cannot come after named parameters"));
			}
			arguments.push_back(sql_type);
			parameters.push_back(move(constant));
		} else {
			named_parameters[parameter_name] = move(constant);
		}
	}
}

void NamedParameterBinder::CheckNamedParameters(unordered_map<string, LogicalType> named_parameter_types, unordered_map<string, Value> named_parameter_values, string func_name) {
	for (auto &kv : named_parameter_values) {
		auto entry = named_parameter_types.find(kv.first);
		if (entry == named_parameter_types.end()) {
			// create a list of named parameters for the error
			string named_params;
			for(auto &kv : named_parameter_types) {
				named_params += "    " + kv.first + " " + kv.second.ToString() + "\n";
			}
			if (named_params.empty()) {
				named_params = "Function does not accept any named parameters.";
			} else {
				named_params = "Candidates: " + named_params;
			}
			throw BinderException(error_context.FormatError("Invalid named parameter \"%s\" for function %s\n%s", kv.first, func_name, named_params));
		}
		kv.second = kv.second.CastAs(entry->second);
	}
}

} // namespace duckdb
