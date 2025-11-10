#include "duckdb/function/function_binder.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context_p) : binder(nullptr), context(context_p) {
}
FunctionBinder::FunctionBinder(Binder &binder_p) : binder(&binder_p), context(binder_p.context) {
}

optional_idx FunctionBinder::BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
	if (arguments.size() < func.arguments.size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return optional_idx();
	}
	idx_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		LogicalType arg_type = i < func.arguments.size() ? func.arguments[i] : func.varargs;
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, arguments[i], arg_type);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += idx_t(cast_cost);
		} else {
			// we can't implicitly cast: throw an error
			return optional_idx();
		}
	}
	return cost;
}

optional_idx FunctionBinder::BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
	if (func.HasVarArgs()) {
		// special case varargs function
		return BindVarArgsFunctionCost(func, arguments);
	}
	if (func.arguments.size() != arguments.size()) {
		// invalid argument count: check the next function
		return optional_idx();
	}
	idx_t cost = 0;
	bool has_parameter = false;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() == LogicalTypeId::UNKNOWN) {
			has_parameter = true;
			continue;
		}
		int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, arguments[i], func.arguments[i]);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += idx_t(cast_cost);
		} else {
			// we can't implicitly cast: throw an error
			return optional_idx();
		}
	}
	if (has_parameter) {
		// all arguments are implicitly castable and there is a parameter - return 0 as cost
		return 0;
	}
	return cost;
}

template <class T>
vector<idx_t> FunctionBinder::BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
                                                         const vector<LogicalType> &arguments, ErrorData &error) {
	optional_idx best_function;
	idx_t lowest_cost = NumericLimits<idx_t>::Maximum();
	vector<idx_t> candidate_functions;
	for (idx_t f_idx = 0; f_idx < functions.functions.size(); f_idx++) {
		auto &func = functions.functions[f_idx];
		// check the arguments of the function
		auto bind_cost = BindFunctionCost(func, arguments);
		if (!bind_cost.IsValid()) {
			// auto casting was not possible
			continue;
		}
		auto cost = bind_cost.GetIndex();
		if (cost == lowest_cost) {
			candidate_functions.push_back(f_idx);
			continue;
		}
		if (cost > lowest_cost) {
			continue;
		}
		candidate_functions.clear();
		lowest_cost = cost;
		best_function = f_idx;
	}
	if (!best_function.IsValid()) {
		// no matching function was found, throw an error
		vector<string> candidates;
		string catalog_name;
		string schema_name;
		for (auto &f : functions.functions) {
			if (catalog_name.empty() && !f.catalog_name.empty()) {
				catalog_name = f.catalog_name;
			}
			if (schema_name.empty() && !f.schema_name.empty()) {
				schema_name = f.schema_name;
			}
			candidates.push_back(f.ToString());
		}
		error = ErrorData(BinderException::NoMatchingFunction(catalog_name, schema_name, name, arguments, candidates));
		return candidate_functions;
	}
	candidate_functions.push_back(best_function.GetIndex());
	return candidate_functions;
}

template <class T>
optional_idx FunctionBinder::MultipleCandidateException(const string &catalog_name, const string &schema_name,
                                                        const string &name, FunctionSet<T> &functions,
                                                        vector<idx_t> &candidate_functions,
                                                        const vector<LogicalType> &arguments, ErrorData &error) {
	D_ASSERT(functions.functions.size() > 1);
	// there are multiple possible function definitions
	// throw an exception explaining which overloads are there
	string call_str = Function::CallToString(catalog_name, schema_name, name, arguments);
	string candidate_str;
	for (auto &conf : candidate_functions) {
		T f = functions.GetFunctionByOffset(conf);
		candidate_str += "\t" + f.ToString() + "\n";
	}
	error = ErrorData(
	    ExceptionType::BINDER,
	    StringUtil::Format("Could not choose a best candidate function for the function call \"%s\". In order to "
	                       "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
	                       call_str, candidate_str));
	return optional_idx();
}

template <class T>
optional_idx FunctionBinder::BindFunctionFromArguments(const string &name, FunctionSet<T> &functions,
                                                       const vector<LogicalType> &arguments, ErrorData &error) {
	auto candidate_functions = BindFunctionsFromArguments<T>(name, functions, arguments, error);
	if (candidate_functions.empty()) {
		// No candidates, return an invalid index.
		return optional_idx();
	}
	if (candidate_functions.size() > 1) {
		// Multiple candidates, check if there are any unknown arguments.
		for (auto &arg_type : arguments) {
			if (arg_type.IsUnknown()) {
				// We cannot resolve the parameters to a function.
				throw ParameterNotResolvedException();
			}
		}
		auto catalog_name = functions.functions.size() > 0 ? functions.functions[0].catalog_name : "";
		auto schema_name = functions.functions.size() > 0 ? functions.functions[0].schema_name : "";
		return MultipleCandidateException(catalog_name, schema_name, name, functions, candidate_functions, arguments,
		                                  error);
	}
	return candidate_functions[0];
}

optional_idx FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, PragmaFunctionSet &functions, vector<Value> &parameters,
                                          ErrorData &error) {
	vector<LogicalType> types;
	for (auto &value : parameters) {
		types.push_back(value.type());
	}
	auto entry = BindFunctionFromArguments(name, functions, types, error);
	if (!entry.IsValid()) {
		error.Throw();
	}
	auto candidate_function = functions.GetFunctionByOffset(entry.GetIndex());
	// cast the input parameters
	for (idx_t i = 0; i < parameters.size(); i++) {
		auto target_type =
		    i < candidate_function.arguments.size() ? candidate_function.arguments[i] : candidate_function.varargs;
		parameters[i] = parameters[i].CastAs(context, target_type);
	}
	return entry;
}

vector<LogicalType> FunctionBinder::GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments) {
	vector<LogicalType> types;
	types.reserve(arguments.size());
	for (auto &argument : arguments) {
		types.push_back(ExpressionBinder::GetExpressionReturnType(*argument));
	}
	return types;
}

optional_idx FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

enum class LogicalTypeComparisonResult : uint8_t { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

static LogicalTypeComparisonResult RequiresCast(const LogicalType &source_type, const LogicalType &target_type) {
	if (target_type.id() == LogicalTypeId::ANY) {
		return LogicalTypeComparisonResult::TARGET_IS_ANY;
	}
	if (source_type == target_type) {
		return LogicalTypeComparisonResult::IDENTICAL_TYPE;
	}
	if (source_type.id() == LogicalTypeId::LIST && target_type.id() == LogicalTypeId::LIST) {
		return RequiresCast(ListType::GetChildType(source_type), ListType::GetChildType(target_type));
	}
	if (source_type.id() == LogicalTypeId::ARRAY && target_type.id() == LogicalTypeId::ARRAY) {
		return RequiresCast(ArrayType::GetChildType(source_type), ArrayType::GetChildType(target_type));
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

static bool TypeRequiresPrepare(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return true;
	}
	if (type.id() == LogicalTypeId::LIST) {
		return TypeRequiresPrepare(ListType::GetChildType(type));
	}
	return false;
}

static LogicalType PrepareTypeForCastRecursive(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return AnyType::GetTargetType(type);
	}
	if (type.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(PrepareTypeForCastRecursive(ListType::GetChildType(type)));
	}
	return type;
}

static void PrepareTypeForCast(LogicalType &type) {
	if (!TypeRequiresPrepare(type)) {
		return;
	}
	type = PrepareTypeForCastRecursive(type);
}

void FunctionBinder::CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children) {
	for (auto &arg : function.arguments) {
		PrepareTypeForCast(arg);
	}
	PrepareTypeForCast(function.varargs);

	for (idx_t i = 0; i < children.size(); i++) {
		auto target_type = i < function.arguments.size() ? function.arguments[i] : function.varargs;
		if (target_type.id() == LogicalTypeId::STRING_LITERAL || target_type.id() == LogicalTypeId::INTEGER_LITERAL) {
			throw InternalException(
			    "Function %s returned a STRING_LITERAL or INTEGER_LITERAL type - return an explicit type instead",
			    function.name);
		}
		target_type.Verify();
		// don't cast lambda children, they get removed before execution
		if (children[i]->return_type.id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->return_type, target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			children[i] = BoundCastExpression::AddCastToType(context, std::move(children[i]), target_type);
		}
	}
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const string &schema, const string &name,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto &function = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(context, schema, name);
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function, std::move(children), error, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto best_function = BindFunction(func.name, func.functions, children, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}

	// found a matching function!
	auto bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// If any of the parameters are NULL, the function will just be replaced with a NULL constant.
	// We try to give the NULL constant the correct type, but we have to do this without binding the function,
	// because functions with DEFAULT_NULL_HANDLING should not have to deal with NULL inputs in their bind code.
	// Some functions may have an invalid default return type, as they must be bound to infer the return type.
	// In those cases, we default to SQLNULL.
	const auto return_type_if_null =
	    bound_function.GetReturnType().IsComplete() ? bound_function.GetReturnType() : LogicalType::SQLNULL;
	if (bound_function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		for (auto &child : children) {
			if (child->return_type == LogicalTypeId::SQLNULL) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
			if (!child->IsFoldable()) {
				continue;
			}
			Value result;
			if (!ExpressionExecutor::TryEvaluateScalar(context, *child, result)) {
				continue;
			}
			if (result.IsNull()) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
		}
	}
	return BindScalarFunction(bound_function, std::move(children), is_operator, binder);
}

static bool RequiresCollationPropagation(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && !type.HasAlias();
}

static string ExtractCollation(const vector<unique_ptr<Expression>> &children) {
	string collation;
	for (auto &arg : children) {
		if (!RequiresCollationPropagation(arg->return_type)) {
			// not a varchar column
			continue;
		}
		auto child_collation = StringType::GetCollation(arg->return_type);
		if (collation.empty()) {
			collation = child_collation;
		} else if (!child_collation.empty() && collation != child_collation) {
			throw BinderException("Cannot combine types with different collation!");
		}
	}
	return collation;
}

static void PropagateCollations(ClientContext &, ScalarFunction &bound_function,
                                vector<unique_ptr<Expression>> &children) {
	if (!RequiresCollationPropagation(bound_function.GetReturnType())) {
		// we only need to propagate if the function returns a varchar
		return;
	}
	auto collation = ExtractCollation(children);
	if (collation.empty()) {
		// no collation to propagate
		return;
	}
	// propagate the collation to the return type
	auto collation_type = LogicalType::VARCHAR_COLLATION(std::move(collation));
	bound_function.SetReturnType(std::move(collation_type));
}

static void PushCollations(ClientContext &context, ScalarFunction &bound_function,
                           vector<unique_ptr<Expression>> &children, CollationType type) {
	auto collation = ExtractCollation(children);
	if (collation.empty()) {
		// no collation to push
		return;
	}
	// push collation into the return type if required
	auto collation_type = LogicalType::VARCHAR_COLLATION(std::move(collation));
	if (RequiresCollationPropagation(bound_function.GetReturnType())) {
		bound_function.SetReturnType(collation_type);
	}
	// push collations to the children
	for (auto &arg : children) {
		if (RequiresCollationPropagation(arg->return_type)) {
			// if this is a varchar type - propagate the collation
			arg->return_type = collation_type;
		}
		// now push the actual collation handling
		ExpressionBinder::PushCollation(context, arg, arg->return_type, type);
	}
}

static void HandleCollations(ClientContext &context, ScalarFunction &bound_function,
                             vector<unique_ptr<Expression>> &children) {
	switch (bound_function.GetCollationHandling()) {
	case FunctionCollationHandling::IGNORE_COLLATIONS:
		// explicitly ignoring collation handling
		break;
	case FunctionCollationHandling::PROPAGATE_COLLATIONS:
		PropagateCollations(context, bound_function, children);
		break;
	case FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS:
		// first propagate, then push collations to the children
		PushCollations(context, bound_function, children, CollationType::COMBINABLE_COLLATIONS);
		break;
	default:
		throw InternalException("Unrecognized collation handling");
	}
}

static void InferTemplateType(ClientContext &context, const LogicalType &source, const LogicalType &target,
                              case_insensitive_map_t<vector<LogicalType>> &bindings, const Expression &current_expr,
                              const BaseScalarFunction &function) {
	if (target.id() == LogicalTypeId::UNKNOWN || target.id() == LogicalTypeId::SQLNULL) {
		// If the actual type is unknown, we cannot infer anything more.
		// Therefore, we map all remaining templates in the source to UNKNOWN or SQLNULL, if not already inferred to
		// something else

		// This might seem a bit strange, why not just not set the binding and error out later when we try to substitute
		// all templates? Well, this is how bindings for most nested functions already work, they simply propagate the
		// UNKNOWN/SQLNULL. The binder will later check for UNKNOWN/SQLNULL in return types and if it finds one, insert
		// a dummy cast to INT32 so that the function can be executed without errors (and just return NULLs).

		TypeVisitor::Contains(source, [&](const LogicalType &child) {
			if (child.id() == LogicalTypeId::TEMPLATE) {
				const auto index = TemplateType::GetName(child);
				if (bindings.find(index) == bindings.end()) {
					// not found, add the binding
					bindings[index] = {target.id()};
				}
			}
			return false; // continue visiting
		});
		return;
	}

	// If the source is a template type, we bind it, or try to unify its existing binding with the target type.
	if (source.id() == LogicalTypeId::TEMPLATE) {
		const auto &index = TemplateType::GetName(source);
		auto it = bindings.find(index);
		if (it == bindings.end()) {
			// not found, add the binding
			bindings[index] = {target};
			return;
		}
		if (it->second.back() == target) {
			// already bound to the same type
			return;
		}

		// Try to unify (promote) the type candidates
		LogicalType result;
		if (LogicalType::TryGetMaxLogicalType(context, it->second.back(), target, result)) {
			// Type unification was successful
			if (it->second.back() != result) {
				// update the binding
				it->second.push_back(target);
				it->second.push_back(std::move(result)); // Push the new promoted type
			}
			return;
		}

		// If we reach here, it means the types are incompatible
		string msg =
		    StringUtil::Format("Cannot deduce template type '%s' in function: '%s'\nType '%s' was inferred to be:\n",
		                       TemplateType::GetName(source), function.ToString(), TemplateType::GetName(source));
		const auto &steps = it->second;

		for (idx_t i = 0; i < steps.size(); i += 2) {
			if (i == 0) {
				// Normalize the first step to ensure it is a valid type
				msg += StringUtil::Format(" - '%s', from first occurrence\n", steps[i].ToString());
			} else {
				msg += StringUtil::Format(" - '%s', by promoting '%s' + '%s'\n", steps[i].ToString(),
				                          steps[i - 2].ToString(), steps[i - 1]);
			}
		}
		msg += StringUtil::Format(" - '%s', which is incompatible with previously inferred type!", target.ToString());
		throw BinderException(current_expr.GetQueryLocation(), msg);
	}

	// Otherwise, recurse downwards into nested types, and try to infer nested type members
	// This only works if the source and target types are completely defined (excluding templates),
	// i.e. they have aux info.
	if (!(source.IsNested() && target.IsNested() && source.AuxInfo() && target.AuxInfo())) {
		return;
	}

	switch (source.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		if ((source.id() == LogicalTypeId::ARRAY || source.id() == LogicalTypeId::LIST) &&
		    (target.id() == LogicalTypeId::LIST || target.id() == LogicalTypeId::ARRAY)) {
			const auto &source_child =
			    source.id() == LogicalTypeId::LIST ? ListType::GetChildType(source) : ArrayType::GetChildType(source);
			const auto &target_child =
			    target.id() == LogicalTypeId::LIST ? ListType::GetChildType(target) : ArrayType::GetChildType(target);
			InferTemplateType(context, source_child, target_child, bindings, current_expr, function);
		}
	} break;
	case LogicalTypeId::MAP: {
		// Map is only implicitly castable to map, so we only need to handle this case here/
		if (target.id() == LogicalTypeId::MAP) {
			const auto &source_key = MapType::KeyType(source);
			const auto &source_val = MapType::ValueType(source);
			const auto &target_key = MapType::KeyType(target);
			const auto &target_val = MapType::ValueType(target);

			InferTemplateType(context, source_key, target_key, bindings, current_expr, function);
			InferTemplateType(context, source_val, target_val, bindings, current_expr, function);
		}
	} break;
	case LogicalTypeId::UNION: {
		// TODO: Support union types with template member types.
		throw NotImplementedException("Union types cannot infer templated member types yet!");
	} break;
	case LogicalTypeId::STRUCT: {
		// Structs are only implicitly castable to structs, so we only need to handle this case here.
		if (target.id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(source)) {
			const auto &source_children = StructType::GetChildTypes(source);
			const auto &target_children = StructType::GetChildTypes(target);

			const auto common_children = MinValue(source_children.size(), target_children.size());
			for (idx_t i = 0; i < common_children; i++) {
				const auto &source_child_type = source_children[i].second;
				const auto &target_child_type = target_children[i].second;
				InferTemplateType(context, source_child_type, target_child_type, bindings, current_expr, function);
			}
		} else {
			// TODO: Support named structs with template child types.
			throw NotImplementedException("Named structs cannot infer templated child types yet!");
		}
	} break;
	default:
		break; // no template type to infer
	}
}

static void SubstituteTemplateType(LogicalType &type, case_insensitive_map_t<vector<LogicalType>> &bindings,
                                   const string &function_name) {
	// Replace all template types in with their bound concrete types.
	type = TypeVisitor::VisitReplace(type, [&](const LogicalType &t) -> LogicalType {
		if (t.id() == LogicalTypeId::TEMPLATE) {
			const auto index = TemplateType::GetName(t);
			auto it = bindings.find(index);
			if (it != bindings.end()) {
				// found a binding, return the concrete type
				return LogicalType::NormalizeType(it->second.back());
			}

			// If we reach here, the template type was not bound to any concrete type.
			// We dont throw an error here, but give users a chance to handle unresolved template type later in the
			// "bind_scalar_function_t" callback. We then throw an error if the template type is still not bound
			// in the "CheckTemplateTypesResolved" method afterwards.
		}
		return t;
	});
}

void FunctionBinder::ResolveTemplateTypes(BaseScalarFunction &bound_function,
                                          const vector<unique_ptr<Expression>> &children) {
	case_insensitive_map_t<vector<LogicalType>> bindings;
	vector<reference<LogicalType>> to_substitute;

	// First, we need to infer the template types from the children.
	for (idx_t i = 0; i < bound_function.arguments.size(); i++) {
		auto &param = bound_function.arguments[i];

		// If the parameter is not templated, we can skip it.
		if (param.IsTemplated()) {
			auto actual = ExpressionBinder::GetExpressionReturnType(*children[i]);
			InferTemplateType(context, param, actual, bindings, *children[i], bound_function);

			to_substitute.emplace_back(param);
		}
	}

	// If the function has a templated varargs, we need to infer its type too
	if (bound_function.varargs.IsTemplated()) {
		// All remaining children are considered varargs.
		for (idx_t i = bound_function.arguments.size(); i < children.size(); i++) {
			auto actual = ExpressionBinder::GetExpressionReturnType(*children[i]);
			InferTemplateType(context, bound_function.varargs, actual, bindings, *children[i], bound_function);
		}
		to_substitute.emplace_back(bound_function.varargs);
	}

	// If the return type is templated, we need to subsitute it as well
	if (bound_function.GetReturnType().IsTemplated()) {
		to_substitute.emplace_back(bound_function.GetReturnType());
	}

	// Finally, substitute all template types in the bound function with their concrete types.
	for (auto &templated_type : to_substitute) {
		SubstituteTemplateType(templated_type, bindings, bound_function.name);
	}
}

static void VerifyTemplateType(const LogicalType &type, const string &function_name) {
	TypeVisitor::Contains(type, [&](const LogicalType &type) {
		if (type.id() == LogicalTypeId::TEMPLATE) {
			const auto msg =
			    "Function '%s' has a template parameter type '%s' that could not be resolved to a concrete type";
			throw BinderException(msg, function_name, TemplateType::GetName(type));
		}
		return false; // continue visiting
	});
}

// Verify that all template types are bound to concrete types.
void FunctionBinder::CheckTemplateTypesResolved(const BaseScalarFunction &bound_function) {
	for (const auto &arg : bound_function.arguments) {
		VerifyTemplateType(arg, bound_function.name);
	}
	VerifyTemplateType(bound_function.varargs, bound_function.name);
	VerifyTemplateType(bound_function.GetReturnType(), bound_function.name);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(ScalarFunction bound_function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	// Attempt to resolve template types, before we call the "Bind" callback.
	ResolveTemplateTypes(bound_function, children);

	unique_ptr<FunctionData> bind_info;

	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
	} else if (bound_function.bind_extended) {
		if (!binder) {
			throw InternalException("Function '%s' has a 'bind_extended' but the FunctionBinder was created without "
			                        "a reference to a Binder",
			                        bound_function.name);
		}
		ScalarFunctionBindInput bind_input(*binder);
		bind_info = bound_function.bind_extended(bind_input, bound_function, children);
	}

	// After the "bind" callback, we verify that all template types are bound to concrete types.
	CheckTemplateTypesResolved(bound_function);

	if (bound_function.get_modified_databases && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties);
		bound_function.get_modified_databases(context, input);
	}

	HandleCollations(context, bound_function, children);

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	auto return_type = bound_function.GetReturnType();
	unique_ptr<Expression> result;
	auto result_func = make_uniq<BoundFunctionExpression>(std::move(return_type), std::move(bound_function),
	                                                      std::move(children), std::move(bind_info), is_operator);
	if (result_func->function.bind_expression) {
		// if a bind_expression callback is registered - call it and emit the resulting expression
		FunctionBindExpressionInput input(context, result_func->bind_info.get(), result_func->children);
		result = result_func->function.bind_expression(input);
	}
	if (!result) {
		result = std::move(result_func);
	}
	return result;
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(AggregateFunction bound_function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	ResolveTemplateTypes(bound_function, children);

	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.arguments.size(), children.size()));
	}

	CheckTemplateTypesResolved(bound_function);

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	return make_uniq<BoundAggregateExpression>(std::move(bound_function), std::move(children), std::move(filter),
	                                           std::move(bind_info), aggr_type);
}

} // namespace duckdb
