#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context_p) : binder(nullptr), context(context_p) {
}
FunctionBinder::FunctionBinder(Binder &binder_p) : binder(&binder_p), context(binder_p.context) {
}

// Split the full (maybe-named) argument list into positional types + named (name, type) pairs.
// Returns false if a positional argument follows a named one, which is not allowed.
static bool TrySplitArgumentTypes(const vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
                                  vector<LogicalType> &positional, vector<pair<Identifier, LogicalType>> &named) {
	for (auto &arg : arguments) {
		auto type = ExpressionBinder::GetExpressionReturnType(*arg.second);
		if (!arg.first.empty()) {
			named.emplace_back(arg.first, std::move(type));
			continue;
		}
		if (named.empty()) {
			positional.push_back(std::move(type));
			continue;
		}
		// a positional argument cannot follow a named one
		return false;
	}
	return true;
}

// Split the full (maybe-named) argument list into the positional + named (keyword) children.
static auto SplitArguments(vector<pair<Identifier, unique_ptr<Expression>>> arguments)
    -> pair<vector<unique_ptr<Expression>>, vector<pair<Identifier, unique_ptr<Expression>>>> {
	vector<unique_ptr<Expression>> regular_args;
	vector<pair<Identifier, unique_ptr<Expression>>> keyword_args;

	for (auto &arg : arguments) {
		if (!arg.first.empty()) {
			keyword_args.push_back(std::move(arg));
			continue;
		}
		if (keyword_args.empty()) {
			regular_args.push_back(std::move(arg.second));
			continue;
		}
		// Defensive: a positional argument following a named one should already have been rejected during binding
		// (see TrySplitArgumentTypes / BindFunctionWithImplicitNaming).
		throw BinderException(arg.second->GetQueryLocation(),
		                      "Positional argument '%s' cannot follow named arguments in a function call.",
		                      arg.second->ToString());
	}

	return {std::move(regular_args), std::move(keyword_args)};
}

static int64_t ImplicitCastCost(optional_ptr<ClientContext> context, const LogicalType &source,
                                const LogicalType &target) {
	if (context) {
		return CastFunctionSet::ImplicitCastCost(*context, source, target);
	}
	// without a context we can only consider the built-in cast rules - no registered casts are reachable
	return CastRules::ImplicitCast(source, target);
}

static bool InferTemplateType(optional_ptr<ClientContext> context, const LogicalType &source, const LogicalType &target,
                              case_insensitive_map_t<vector<LogicalType>> &bindings, FailedFunctionCandidate &failure) {
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
		return true;
	}

	// If the source is a template type, we bind it, or try to unify its existing binding with the target type.
	if (source.id() == LogicalTypeId::TEMPLATE) {
		const auto &index = TemplateType::GetName(source);
		auto it = bindings.find(index);
		if (it == bindings.end()) {
			// not found, add the binding
			bindings[index] = {target};
			return true;
		}
		if (it->second.back() == target) {
			// already bound to the same type
			return true;
		}

		// Try to unify (promote) the type candidates - without a client context, with the built-in casts only
		LogicalType result;
		const bool unified = context
		                         ? LogicalType::TryGetMaxLogicalType(*context, it->second.back(), target, result)
		                         : LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(it->second.back(), target, result);
		if (unified) {
			// Type unification was successful
			if (it->second.back() != result) {
				// update the binding
				it->second.push_back(target);
				it->second.push_back(std::move(result)); // Push the new promoted type
			}
			return true;
		}

		// the types are incompatible
		failure.reason = FunctionMismatchReason::TEMPLATE_DEDUCTION;
		failure.template_name = TemplateType::GetName(source);
		failure.inferred_types = it->second;
		failure.inferred_types.push_back(target);
		return false;
	}

	// Otherwise, recurse downwards into nested types, and try to infer nested type members
	// This only works if the source and target types are completely defined (excluding templates),
	// i.e. they have aux info.
	if (!(source.IsNested() && target.IsNested() && source.HasParameters() && target.HasParameters())) {
		return true;
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
			if (!InferTemplateType(context, source_child, target_child, bindings, failure)) {
				return false;
			}
		}
	} break;
	case LogicalTypeId::MAP: {
		// Map is only implicitly castable to map, so we only need to handle this case here/
		if (target.id() == LogicalTypeId::MAP) {
			const auto &source_key = MapType::KeyType(source);
			const auto &source_val = MapType::ValueType(source);
			const auto &target_key = MapType::KeyType(target);
			const auto &target_val = MapType::ValueType(target);

			if (!InferTemplateType(context, source_key, target_key, bindings, failure)) {
				return false;
			}
			if (!InferTemplateType(context, source_val, target_val, bindings, failure)) {
				return false;
			}
		}
	} break;
	case LogicalTypeId::UNION: {
		// TODO: Support union types with template member types.
		throw NotImplementedException("Union types cannot infer templated member types yet!");
	} break;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		// Structs are only implicitly castable to structs, so we only need to handle this case here.
		if (StructType::IsStruct(target) && StructType::IsUnnamed(source)) {
			const auto &source_children = StructType::GetChildTypes(source);
			const auto &target_children = StructType::GetChildTypes(target);

			const auto common_children = MinValue(source_children.size(), target_children.size());
			for (idx_t i = 0; i < common_children; i++) {
				const auto &source_child_type = source_children[i].second;
				const auto &target_child_type = target_children[i].second;
				if (!InferTemplateType(context, source_child_type, target_child_type, bindings, failure)) {
					return false;
				}
			}
		} else {
			// TODO: Support named structs with template child types.
			throw NotImplementedException("Named structs cannot infer templated child types yet!");
		}
	} break;
	default:
		break; // no template type to infer
	}
	return true;
}

//! The name of a template a type still contains, empty if it contains none
static string FindTemplate(const LogicalType &type) {
	string name;
	TypeVisitor::Contains(type, [&](const LogicalType &child) {
		if (child.id() != LogicalTypeId::TEMPLATE) {
			return false;
		}
		name = TemplateType::GetName(child);
		return true;
	});
	return name;
}

static void SubstituteTemplateType(LogicalType &type, case_insensitive_map_t<vector<LogicalType>> &bindings,
                                   const Identifier &function_name) {
	// Replace all template types in with their bound concrete types.
	type = TypeVisitor::VisitReplace(type, [&](const LogicalType &t) -> LogicalType {
		if (t.id() == LogicalTypeId::TEMPLATE) {
			const auto index = TemplateType::GetName(t);
			auto it = bindings.find(index);
			if (it != bindings.end()) {
				// found a binding, return the concrete type
				return LogicalType::NormalizeType(it->second.back());
			}
		}
		return t;
	});
}

bool FunctionBinder::MatchOverload(optional_ptr<ClientContext> context, const SimpleFunction &func,
                                   const vector<LogicalType> &arguments,
                                   const vector<pair<Identifier, LogicalType>> &named_arguments, bool explicit_casts,
                                   FunctionBinding &binding, FailedFunctionCandidate &failure) {
	// every mismatch is counted towards the distance, the first one is the reason
	idx_t mismatches = 0;
	auto mismatch = [&](FunctionMismatchReason reason, optional_idx position, idx_t count = 1) {
		if (mismatches == 0) {
			failure.reason = reason;
			failure.position = position;
		}
		mismatches += count;
	};
	const auto &sig = func.GetSignature();

	// Compute total number of arguments passed
	const auto received_arg_count = static_cast<idx_t>(arguments.size() + named_arguments.size());

	// And the maximum number of arguments the function can accept
	const auto positional_count = sig.GetPositionalParameterCount();
	const auto args_param = sig.GetArgs();
	const auto kwargs_param = sig.GetKwargs();

	idx_t maximum_arg_count = NumericLimits<idx_t>::Maximum();
	if (!args_param && !kwargs_param) {
		maximum_arg_count = sig.GetParameterCount();
	}

	// Every parameter without a default has to be filled by the call, either from its position or by its name.
	// Counting the arguments instead would let a named argument stand in for a required positional parameter, so
	// that f(a, *, opt := NULL) matched a call f(opt := 1) that cannot fill "a" just as well as f(*, opt := NULL).
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		auto &param = sig.GetParameter(i);
		if (param.IsVariadic() || param.HasDefaultValue()) {
			continue;
		}
		if (param.AcceptsPosition() && i < arguments.size()) {
			continue;
		}
		bool filled = false;
		if (param.AcceptsName()) {
			for (auto &named_arg : named_arguments) {
				if (named_arg.first == param.GetName()) {
					filled = true;
					break;
				}
			}
		}
		if (!filled) {
			// This parameter cannot be filled by the call, so this function cannot be a match.
			if (mismatches == 0) {
				failure.parameter = i;
			}
			mismatch(FunctionMismatchReason::MISSING_PARAMETER, optional_idx());
		}
	}

	if (received_arg_count > maximum_arg_count) {
		// We have more arguments than the function can take, so this function cannot be a match.
		mismatch(FunctionMismatchReason::TOO_MANY_ARGUMENTS, maximum_arg_count, received_arg_count - maximum_arg_count);
	} else if (arguments.size() > positional_count && !args_param) {
		// There is no "*args" to receive the extra positional arguments
		mismatch(FunctionMismatchReason::TOO_MANY_ARGUMENTS, positional_count, arguments.size() - positional_count);
	}

	for (idx_t i = 0; i < named_arguments.size(); i++) {
		auto param_idx = sig.GetParameterIndexByName(named_arguments[i].first);
		if (!param_idx.IsValid()) {
			if (!kwargs_param) {
				// no parameter with this name, and no "**kwargs" to receive it
				mismatch(FunctionMismatchReason::UNKNOWN_NAMED_ARGUMENT, arguments.size() + i);
			}
		} else if (sig.GetParameter(param_idx.GetIndex()).AcceptsPosition() &&
		           param_idx.GetIndex() < arguments.size()) {
			mismatch(FunctionMismatchReason::ALREADY_PASSED_BY_POSITION, arguments.size() + i);
		}
	}

	if (mismatches > 0) {
		// the wrong shape is further away than arguments that cannot be cast, of which there are at most this many
		failure.distance = received_arg_count + mismatches;
		return false;
	}

	// the shape matches, so every argument has a parameter that receives it: its position, "*args", its name or
	// "**kwargs"
	optional_idx args_idx;
	optional_idx kwargs_idx;
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		switch (sig.GetParameter(i).GetKind()) {
		case FunctionParameterKind::VAR_POSITIONAL:
			args_idx = i;
			break;
		case FunctionParameterKind::VAR_KEYWORD:
			kwargs_idx = i;
			break;
		default:
			break;
		}
	}

	idx_t cost = 0;
	bool has_parameter = false;
	vector<bool> filled(sig.GetParameterCount(), false);
	for (idx_t i = 0; i < received_arg_count; i++) {
		const bool is_named = i >= arguments.size();
		auto &arg_type = is_named ? named_arguments[i - arguments.size()].second : arguments[i];
		idx_t param_idx;
		if (is_named) {
			auto by_name = sig.GetParameterIndexByName(named_arguments[i - arguments.size()].first);
			param_idx = by_name.IsValid() ? by_name.GetIndex() : kwargs_idx.GetIndex();
		} else {
			param_idx = i < positional_count ? i : args_idx.GetIndex();
		}
		binding.argument_parameters.push_back(param_idx);
		filled[param_idx] = true;

		if (!is_named && arg_type.id() == LogicalTypeId::UNKNOWN) {
			has_parameter = true;
			continue;
		}
		if (explicit_casts) {
			continue;
		}
		int64_t cast_cost = ImplicitCastCost(context, arg_type, sig.GetParameter(param_idx).GetType());
		if (cast_cost < 0) {
			mismatch(FunctionMismatchReason::INCOMPATIBLE_ARGUMENT, i);
			continue;
		}
		cost += static_cast<idx_t>(cast_cost);
	}
	if (mismatches > 0) {
		failure.distance = mismatches;
		return false;
	}

	// substitute the templates the arguments and the defaults of the parameters they leave out determine
	for (auto &param : sig.GetParameters()) {
		binding.parameter_types.push_back(param.GetType());
	}
	// the shape and the casts match, so a template that cannot be deduced is the closest miss there is
	failure.distance = 0;
	case_insensitive_map_t<vector<LogicalType>> bindings;
	for (idx_t i = 0; i < received_arg_count; i++) {
		auto &param_type = binding.parameter_types[binding.argument_parameters[i]];
		auto &arg_type = i < arguments.size() ? arguments[i] : named_arguments[i - arguments.size()].second;
		if (param_type.IsTemplated() && !InferTemplateType(context, param_type, arg_type, bindings, failure)) {
			failure.position = i;
			return false;
		}
	}
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		auto &param = sig.GetParameter(i);
		if (!filled[i] && param.HasDefaultValue() && param.GetType().IsTemplated() &&
		    !InferTemplateType(context, param.GetType(), param.GetDefaultValue()->type(), bindings, failure)) {
			return false;
		}
	}
	// only the templated types are rebuilt, the others keep their aliases and extension info
	binding.return_type = sig.GetReturnType();
	for (auto &type : binding.parameter_types) {
		if (type.IsTemplated()) {
			SubstituteTemplateType(type, bindings, func.GetName());
		}
	}
	if (binding.return_type.IsTemplated()) {
		SubstituteTemplateType(binding.return_type, bindings, func.GetName());
	}

	// every template of the types the call uses has to be determined by the arguments and defaults
	for (idx_t i = 0; i <= sig.GetParameterCount(); i++) {
		const bool is_return = i == sig.GetParameterCount();
		if (!is_return && sig.GetParameter(i).IsVariadic() && !filled[i]) {
			continue;
		}
		failure.template_name = FindTemplate(is_return ? binding.return_type : binding.parameter_types[i]);
		if (!failure.template_name.empty()) {
			failure.reason = FunctionMismatchReason::TEMPLATE_UNRESOLVED;
			return false;
		}
	}

	// all arguments are implicitly castable - with a parameter of an unknown type, the cost is 0
	binding.cost = has_parameter ? 0 : cost;
	return true;
}

//! Why the templates of an overload cannot be deduced, with the types the template was inferred to be
static string TemplateDeductionMessage(const SimpleFunction &function, const FailedFunctionCandidate &failure) {
	auto &name = failure.template_name;
	auto msg = StringUtil::Format("Cannot deduce template type '%s' in function: '%s'\n", name, function.ToString());
	if (failure.reason == FunctionMismatchReason::TEMPLATE_UNRESOLVED) {
		return msg + StringUtil::Format("Type '%s' is not determined by the arguments", name);
	}
	// the inferred types are the first one, then pairs of a type and what the template is promoted to with it
	auto &steps = failure.inferred_types;
	msg += StringUtil::Format("Type '%s' was inferred to be:\n", name);
	msg += StringUtil::Format(" - '%s', from first occurrence\n", steps[0].ToString());
	for (idx_t i = 2; i + 1 < steps.size(); i += 2) {
		msg += StringUtil::Format(" - '%s', by promoting '%s' + '%s'\n", steps[i].ToString(), steps[i - 2].ToString(),
		                          steps[i - 1].ToString());
	}
	msg += StringUtil::Format(" - '%s', which is incompatible with previously inferred type!", steps.back().ToString());
	return msg;
}

//! A template that cannot be deduced is reported on its own, it says more about the call than a list of overloads
static bool IsTemplateFailure(const FailedFunctionCandidate &failure) {
	return failure.reason == FunctionMismatchReason::TEMPLATE_DEDUCTION ||
	       failure.reason == FunctionMismatchReason::TEMPLATE_UNRESOLVED;
}

//! Why a call does not match an overload, e.g. "argument 1 of type DATE cannot be implicitly cast to VARCHAR"
static string DescribeMismatch(const SimpleFunction &function, const FailedFunctionCandidate &failure,
                               const vector<LogicalType> &arguments,
                               const vector<pair<Identifier, LogicalType>> &named_arguments) {
	auto &sig = function.GetSignature();
	switch (failure.reason) {
	case FunctionMismatchReason::MISSING_PARAMETER: {
		auto &param_name = sig.GetParameter(failure.parameter.GetIndex()).GetName();
		if (param_name.empty()) {
			return StringUtil::Format("parameter %llu is not passed", failure.parameter.GetIndex() + 1);
		}
		return StringUtil::Format("parameter \"%s\" is not passed", param_name.GetIdentifierName());
	}
	case FunctionMismatchReason::TOO_MANY_ARGUMENTS:
		if (!sig.GetArgs() && arguments.size() > sig.GetPositionalParameterCount()) {
			return StringUtil::Format("it takes at most %llu positional arguments", sig.GetPositionalParameterCount());
		}
		return StringUtil::Format("it takes at most %llu arguments", sig.GetParameterCount());
	case FunctionMismatchReason::UNKNOWN_NAMED_ARGUMENT:
		return StringUtil::Format(
		    "it has no parameter named \"%s\"",
		    named_arguments[failure.position.GetIndex() - arguments.size()].first.GetIdentifierName());
	case FunctionMismatchReason::ALREADY_PASSED_BY_POSITION:
		return StringUtil::Format(
		    "parameter \"%s\" has already been provided as a positional argument",
		    named_arguments[failure.position.GetIndex() - arguments.size()].first.GetIdentifierName());
	case FunctionMismatchReason::INCOMPATIBLE_ARGUMENT: {
		auto position = failure.position.GetIndex();
		if (position < arguments.size()) {
			auto &param_type = position < sig.GetPositionalParameterCount() ? sig.GetParameter(position).GetType()
			                                                                : sig.GetArgs()->GetType();
			return StringUtil::Format("argument %llu of type %s cannot be implicitly cast to %s", position + 1,
			                          arguments[position].ToString(), param_type.ToString());
		}
		auto &named_arg = named_arguments[position - arguments.size()];
		auto param_idx = sig.GetParameterIndexByName(named_arg.first);
		auto &param_type =
		    param_idx.IsValid() ? sig.GetParameter(param_idx.GetIndex()).GetType() : sig.GetKwargs()->GetType();
		return StringUtil::Format("argument \"%s\" of type %s cannot be implicitly cast to %s",
		                          named_arg.first.GetIdentifierName(), named_arg.second.ToString(),
		                          param_type.ToString());
	}
	case FunctionMismatchReason::TEMPLATE_DEDUCTION:
	case FunctionMismatchReason::TEMPLATE_UNRESOLVED:
		return TemplateDeductionMessage(function, failure);
	default:
		throw InternalException("Unrecognized FunctionMismatchReason");
	}
}

//! An overload with why a call does not match it
static string DescribeCandidate(const SimpleFunction &function, const FailedFunctionCandidate &failure,
                                const vector<LogicalType> &arguments,
                                const vector<pair<Identifier, LogicalType>> &named_arguments) {
	return function.ToString() + " (" + DescribeMismatch(function, failure, arguments, named_arguments) + ")";
}

//! The error of a call that no overload matches - it lists every overload of the set, and the closest one if a single
//! overload is closer than all others
static ErrorData NoMatchingFunctionError(const BaseFunctionSet &functions, const vector<LogicalType> &arguments,
                                         const vector<pair<Identifier, LogicalType>> &named_arguments,
                                         optional_ptr<const FailedFunctionCandidate> closest) {
	vector<string> candidates;
	Identifier catalog_name;
	Identifier schema_name;
	for (idx_t f_idx = 0; f_idx < functions.GetOverloadCount(); f_idx++) {
		auto &f = functions.GetOverload(f_idx);
		if (catalog_name.empty() && !f.GetCatalogName().empty()) {
			catalog_name = f.GetCatalogName();
		}
		if (schema_name.empty() && !f.GetSchemaName().empty()) {
			schema_name = f.GetSchemaName();
		}
		candidates.push_back(f.ToString());
	}
	string closest_candidate;
	if (closest) {
		closest_candidate =
		    DescribeCandidate(functions.GetOverload(closest->GetOverloadIndex()), *closest, arguments, named_arguments);
	}
	return ErrorData(BinderException::NoMatchingFunction(catalog_name, schema_name, functions.GetName(), arguments,
	                                                     named_arguments, candidates, closest_candidate));
}

//! The error of a call that several overloads match at the lowest cost - it lists those overloads. The candidates are
//! sorted by cost
static ErrorData AmbiguousFunctionError(const BaseFunctionSet &functions, const vector<FunctionBinding> &candidates,
                                        const vector<LogicalType> &arguments,
                                        const vector<pair<Identifier, LogicalType>> &named_arguments) {
	auto &first = functions.GetOverload(0);
	auto &catalog_name = first.GetCatalogName();
	auto &schema_name = first.GetSchemaName();
	string call_str =
	    Function::CallToString(catalog_name, schema_name, functions.GetName(), arguments, named_arguments);
	string candidate_str;
	for (auto &candidate : candidates) {
		if (candidate.cost == candidates[0].cost) {
			candidate_str += "\t" + functions.GetOverload(candidate.GetOverloadIndex()).ToString() + "\n";
		}
	}
	return ErrorData(
	    ExceptionType::BINDER,
	    StringUtil::Format("Could not choose a best candidate function for the function call \"%s\". In order to "
	                       "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
	                       call_str, candidate_str));
}

//! The first named argument that repeats the name of an earlier one, if any
static optional_idx FindDuplicateNamedArgument(const vector<pair<Identifier, LogicalType>> &named_arguments) {
	identifier_set_t seen_names;
	for (idx_t i = 0; i < named_arguments.size(); i++) {
		if (!seen_names.insert(named_arguments[i].first).second) {
			return i;
		}
	}
	return optional_idx();
}

static BinderException DuplicateNamedArgumentError(const Identifier &name, const Identifier &argument_name) {
	return BinderException("Duplicate named argument %s in function call to %s", argument_name, name);
}

//! The argument at a position - the positional arguments, followed by the named ones
static const Expression &GetArgument(const vector<unique_ptr<Expression>> &arguments,
                                     const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                                     idx_t position) {
	return position < arguments.size() ? *arguments[position] : *named_arguments[position - arguments.size()].second;
}

FunctionSelection FunctionBinder::SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
                                                 const vector<LogicalType> &arguments,
                                                 const vector<pair<Identifier, LogicalType>> &named_arguments,
                                                 bool explicit_casts) {
	FunctionSelection selection;
	auto duplicate = FindDuplicateNamedArgument(named_arguments);
	if (duplicate.IsValid()) {
		// no overload can take the same argument twice
		selection.error = DuplicateNamedArgumentError(functions.GetName(), named_arguments[duplicate.GetIndex()].first);
		selection.error_argument = arguments.size() + duplicate.GetIndex();
		return selection;
	}
	auto &candidates = selection.candidates;
	auto &failed = selection.failed_candidates;

	// Try to select each function
	for (idx_t function_idx = 0; function_idx < functions.GetOverloadCount(); function_idx++) {
		FunctionBinding binding;
		FailedFunctionCandidate failure;
		binding.function_index = function_idx;
		failure.function_index = function_idx;

		// Try to match the overload
		if (!MatchOverload(context, functions.GetOverload(function_idx), arguments, named_arguments, explicit_casts,
		                   binding, failure)) {
			// If not, record the failure
			failed.push_back(std::move(failure));
			continue;
		}

		// Push the successful binding
		candidates.push_back(std::move(binding));
	}

	// Sort candidates by best cost
	std::stable_sort(candidates.begin(), candidates.end(),
	                 [](const FunctionBinding &a, const FunctionBinding &b) { return a.cost < b.cost; });

	// Sort failures by closest distance
	std::stable_sort(
	    failed.begin(), failed.end(),
	    [](const FailedFunctionCandidate &a, const FailedFunctionCandidate &b) { return a.distance < b.distance; });

	if (candidates.empty()) {
		selection.result = FunctionSelectionResult::NO_MATCH;
		// the closest miss is only named if no other overload is as close
		optional_ptr<const FailedFunctionCandidate> closest;
		if (failed.size() == 1 || (failed.size() > 1 && failed[0].distance < failed[1].distance)) {
			closest = failed[0];
			selection.error_argument = failed[0].position;
		}
		if (!failed.empty() && IsTemplateFailure(failed[0])) {
			selection.error =
			    ErrorData(ExceptionType::BINDER,
			              TemplateDeductionMessage(functions.GetOverload(failed[0].GetOverloadIndex()), failed[0]));
			selection.error_argument = failed[0].position;
		} else {
			selection.error = NoMatchingFunctionError(functions, arguments, named_arguments, closest);
		}
	} else if (candidates.size() > 1 && candidates[1].cost == candidates[0].cost) {
		// several overloads match at the lowest cost
		for (auto &arg_type : arguments) {
			if (arg_type.IsUnknown()) {
				// the types of the prepared statement parameters may decide between them
				throw ParameterNotResolvedException();
			}
		}
		selection.result = FunctionSelectionResult::AMBIGUOUS;
		selection.error = AmbiguousFunctionError(functions, candidates, arguments, named_arguments);
	} else {
		selection.result = FunctionSelectionResult::SUCCESS;
	}
	return selection;
}

FunctionSelection
FunctionBinder::SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
                               const vector<unique_ptr<Expression>> &arguments,
                               const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                               bool explicit_casts) {
	auto [types, named_types] = GetArgumentsFromExpressions(arguments, named_arguments);
	auto selection = SelectFunction(context, functions, types, named_types, explicit_casts);
	if (selection.GetErrorArgument().IsValid()) {
		auto &argument = GetArgument(arguments, named_arguments, selection.GetErrorArgument().GetIndex());
		selection.AddErrorLocation(argument.GetQueryLocation());
	}
	return selection;
}

template <class T>
FunctionBinding FunctionBinder::MatchFunction(const shared_ptr<const T> &function,
                                              const vector<unique_ptr<Expression>> &arguments,
                                              const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	// the callers build the named arguments themselves, a positional argument cannot follow them
	for (auto &named_argument : named_arguments) {
		D_ASSERT(!named_argument.first.empty());
		(void)named_argument;
	}
	FunctionSet<T> functions(function->GetName());
	functions.AddFunction(function);
	auto selection = SelectFunction(context, functions, arguments, named_arguments, true);
	if (!selection.IsSuccess()) {
		selection.GetError().Throw();
	}
	return selection.GetBinding();
}

static bool AnyOverloadSupportsImplicitArgumentNames(const BaseFunctionSet &functions) {
	for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
		auto properties = functions.GetOverload(i).GetFunctionProperties();
		if (properties && properties->GetCaptureArgumentAliases()) {
			return true;
		}
	}
	return false;
}

//! Points the error of a selection at the argument it is about - the split keeps the arguments in their order
static void AddErrorArgumentLocation(FunctionSelection &selection,
                                     const vector<pair<Identifier, unique_ptr<Expression>>> &arguments) {
	if (selection.GetErrorArgument().IsValid()) {
		selection.AddErrorLocation(arguments[selection.GetErrorArgument().GetIndex()].second->GetQueryLocation());
	}
}

static ErrorData PositionalAfterNamedArgumentError(const vector<pair<Identifier, unique_ptr<Expression>>> &arguments) {
	bool seen_named = false;
	for (const auto &[name, expr] : arguments) {
		if (!name.empty()) {
			seen_named = true;
			continue;
		}
		if (seen_named) {
			return ErrorData(BinderException(expr->GetQueryLocation(),
			                                 "Positional argument '%s' cannot follow named arguments in function call.",
			                                 expr->ToString()));
		}
	}
	throw InternalException("ThrowPositionalAfterNamed called without a positional-after-named argument");
}

FunctionSelection
FunctionBinder::SelectFunctionFromArguments(const BaseFunctionSet &functions,
                                            vector<pair<Identifier, unique_ptr<Expression>>> &arguments) {
	// First, attempt a regular bind, splitting the arguments into positional + named just once for all overloads.
	vector<LogicalType> positional;
	vector<pair<Identifier, LogicalType>> named;

	// Does this call have a valid positional -> named argument shape?
	const auto is_valid_call = TrySplitArgumentTypes(arguments, positional, named);
	FunctionSelection result;
	if (is_valid_call) {
		// Yes. Let's try to bind
		result = SelectFunction(context, functions, positional, named);
		AddErrorArgumentLocation(result, arguments);

		// We failed binding - does any other overload support implicit argument names?
		// If not, this is a hard failure.
		if (result.IsSuccess() || !AnyOverloadSupportsImplicitArgumentNames(functions)) {
			return result;
		}
	}

	// Either a positional argument follows a named one, or no overload takes the positional arguments.
	// Check if there is any overload that supports implicit argument names.
	if (AnyOverloadSupportsImplicitArgumentNames(functions)) {
		// If so, we can attempt to salvage the call by implicitly naming the positional arguments and retrying again
		for (auto &[name, expr] : arguments) {
			if (name.empty()) {
				name = expr->GetAlias();
			}
		}

		positional.clear();
		named.clear();

		if (TrySplitArgumentTypes(arguments, positional, named)) {
			auto retry_result = SelectFunction(context, functions, positional, named);
			AddErrorArgumentLocation(retry_result, arguments);
			if (retry_result.IsSuccess() || !is_valid_call) {
				return retry_result;
			}
		}
	}
	if (is_valid_call) {
		// the call only failed to match an overload, which is reported in the error
		return result;
	}

	// No overload could rescue the positional-after-named call, give a clear error.
	FunctionSelection positional_after_named;
	positional_after_named.error = PositionalAfterNamedArgumentError(arguments);
	return positional_after_named;
}

//! Reject a named argument that no overload of the set accepts, naming the options that were available. Overload
//! selection would otherwise report only that nothing matched the call, which hides which name was wrong
static void VerifyNamedArgumentsAccepted(const BaseFunctionSet &functions,
                                         const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	for (auto &named_argument : named_arguments) {
		bool accepted = false;
		for (idx_t i = 0; i < functions.GetOverloadCount() && !accepted; i++) {
			auto &signature = functions.GetOverload(i).GetSignature();
			// the options of a "**kwargs" are checked once the overload is chosen
			accepted =
			    signature.GetParameterIndexByName(named_argument.first).IsValid() || signature.GetKwargs() != nullptr;
		}
		if (accepted) {
			continue;
		}
		// list every option any overload accepts, in name order
		map<string, string> candidates;
		for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
			for (auto &param : functions.GetOverload(i).GetSignature().GetParameters()) {
				if (!param.IsVariadic() && param.HasDefaultValue()) {
					candidates[param.GetName().GetIdentifierName()] = param.GetType().ToString();
				}
			}
		}
		string candidate_list;
		for (auto &candidate : candidates) {
			candidate_list += "    " + candidate.first + " " + candidate.second + "\n";
		}
		throw BinderException("Invalid named parameter %s for function %s\n%s", named_argument.first,
		                      functions.GetName().GetIdentifierName(),
		                      candidate_list.empty() ? "Function does not accept any named parameters."
		                                             : "Candidates:\n" + candidate_list);
	}
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
		auto target_size = ArrayType::GetSize(target_type);
		if (target_size != 0 && target_size != ArrayType::GetSize(source_type)) {
			return LogicalTypeComparisonResult::DIFFERENT_TYPES;
		}
		return RequiresCast(ArrayType::GetChildType(source_type), ArrayType::GetChildType(target_type));
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

//! Resolve any ANY in the target type by substituting the matching part of the source, so we never cast a value to a
//! type that still contains ANY - ANY has no physical representation, so building such a vector throws. This happens
//! e.g. when an ARRAY argument fills a LIST(ANY) parameter (the file list of the multi-file readers).
static LogicalType ResolveAnyInParameterType(const LogicalType &target, const LogicalType &source) {
	if (target.id() == LogicalTypeId::ANY) {
		return source;
	}
	if (target.id() == LogicalTypeId::LIST &&
	    (source.id() == LogicalTypeId::LIST || source.id() == LogicalTypeId::ARRAY)) {
		auto &source_child =
		    source.id() == LogicalTypeId::LIST ? ListType::GetChildType(source) : ArrayType::GetChildType(source);
		return LogicalType::LIST(ResolveAnyInParameterType(ListType::GetChildType(target), source_child));
	}
	return target;
}

Value FunctionBinder::CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type) {
	if (RequiresCast(value.type(), parameter_type) == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
		return value.CastAs(context, ResolveAnyInParameterType(parameter_type, value.type()));
	}
	return value;
}

//! Fold an argument to the constant it was bound to. A constant expression carries its value directly - evaluating
//! one is both wasteful and impossible for the types that have no vector representation, such as TABLE
static Value FoldArgument(ClientContext &context, Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return expr.Cast<BoundConstantExpression>().GetValue();
	}
	return ExpressionExecutor::EvaluateScalar(context, expr, true);
}

//! Fold an option to a constant and cast it to its type like an explicit cast - overload selection never checked it.
//! A failed cast names the option, as the cast alone cannot tell which one it was
static Value PlaceOption(ClientContext &context, Expression &expr, const TypedKwarg &option) {
	auto value = FoldArgument(context, expr);
	if (RequiresCast(value.type(), option.type) != LogicalTypeComparisonResult::DIFFERENT_TYPES) {
		return value;
	}
	string error_message;
	auto result = value.TryCastAs(context, option.type, &error_message);
	if (!result) {
		throw InvalidInputException(expr, "Could not cast value %s to named parameter %s of type %s: %s",
		                            value.ToSQLString(), option.name, option.type.ToString(), error_message);
	}
	return std::move(*result);
}

//! The arguments of a call in the order of the signature: [standard | *args | keyword-only | **kwargs], with the
//! defaults of the parameters the call leaves out inserted
struct ResolvedArguments {
	vector<unique_ptr<Expression>> arguments;
	//! The type of the parameter each argument fills, with the templates substituted
	vector<LogicalType> types;
	//! The leading arguments that are passed by position, and the names of the rest - keyword-only or "**kwargs"
	idx_t positional_count = 0;
	vector<Identifier> named_argument_names;
};

//! Drain the arguments of a call into the order of the signature of the function it binds to
static ResolvedArguments ResolveArguments(const SimpleFunction &function, const FunctionBinding &binding,
                                          vector<unique_ptr<Expression>> &arguments,
                                          vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	const auto &sig = function.GetSignature();
	const auto passed_count = arguments.size();

	// the arguments every parameter receives, in the order they were passed
	vector<vector<idx_t>> received(sig.GetParameterCount());
	for (idx_t i = 0; i < binding.argument_parameters.size(); i++) {
		received[binding.argument_parameters[i]].push_back(i);
	}

	ResolvedArguments result;
	for (idx_t param_idx = 0; param_idx < sig.GetParameterCount(); param_idx++) {
		const auto &param = sig.GetParameter(param_idx);
		auto &param_type = binding.parameter_types[param_idx];
		for (auto i : received[param_idx]) {
			if (param.GetKind() == FunctionParameterKind::VAR_KEYWORD) {
				result.named_argument_names.push_back(named_arguments[i - passed_count].first);
			}
			auto &argument = i < passed_count ? arguments[i] : named_arguments[i - passed_count].second;
			result.arguments.push_back(std::move(argument));
			result.types.push_back(param_type);
		}
		if (!param.IsVariadic() && received[param_idx].empty()) {
			// the call matches the function, so every parameter it leaves out has a default
			auto default_arg = make_uniq<BoundConstantExpression>(*param.GetDefaultValue());
			default_arg->SetAlias(param.GetName());
			result.arguments.push_back(std::move(default_arg));
			result.types.push_back(param_type);
		}
		if (param.GetKind() == FunctionParameterKind::KEYWORD_ONLY) {
			result.named_argument_names.push_back(param.GetName());
		}
	}
	result.positional_count = result.arguments.size() - result.named_argument_names.size();
	arguments.clear();
	named_arguments.clear();
	return result;
}

//! Fold the arguments of a table or pragma function to the constants it is called with. The arguments passed by
//! position fill "parameters", so "range(1, col1 := 5)" reaches the callback exactly as "range(1, 5)" does. The
//! keyword-only and "**kwargs" arguments go into "named_parameters" - an option "**kwargs" declares by the name of the
//! option, cast to its type like an explicit cast, as it takes no part in selection
static void FoldArguments(ClientContext &context, const SimpleFunction &function, ResolvedArguments &resolved,
                          vector<Value> &parameters, named_argument_map_t &named_parameters) {
	auto &signature = function.GetSignature();
	auto option_schema = signature.GetTypedKwargs();
	for (idx_t i = 0; i < resolved.arguments.size(); i++) {
		auto &argument = *resolved.arguments[i];
		if (i < resolved.positional_count) {
			// A TABLE parameter is filled by the subquery itself rather than by a value, so its slot holds a plain
			// NULL. A NULL of type TABLE would not do: TABLE has no physical type, so it cannot be deserialized
			parameters.push_back(resolved.types[i].id() == LogicalTypeId::TABLE ? Value()
			                                                                    : FoldArgument(context, argument));
			continue;
		}
		auto &name = resolved.named_argument_names[i - resolved.positional_count];
		if (!option_schema || signature.GetParameterIndexByName(name).IsValid()) {
			named_parameters.insert(make_pair(name, FoldArgument(context, argument)));
			continue;
		}
		auto option_ptr = option_schema->Find(name);
		if (!option_ptr) {
			// only the options that resemble the name - listing every option buries the one that was meant
			const double similarity_threshold = 0.8;
			auto lower_name = StringUtil::Lower(name.GetIdentifierName());
			vector<pair<string, double>> scores;
			for (auto &option_name : option_schema->GetNames()) {
				auto score = StringUtil::SimilarityRating(option_name.GetIdentifierName(), lower_name);
				if (score >= similarity_threshold) {
					scores.emplace_back(option_name.GetIdentifierName(), score);
				}
			}
			auto similar = StringUtil::TopNStrings(scores, 3, similarity_threshold);
			throw BinderException(argument.GetQueryLocation(), "Invalid named parameter %s for function %s%s", name,
			                      function.GetName().GetIdentifierName(),
			                      StringUtil::CandidatesMessage(similar, "Did you mean"));
		}
		auto &option = *option_ptr;
		if (named_parameters.find(option.name) != named_parameters.end()) {
			throw BinderException(argument.GetQueryLocation(),
			                      "Named parameter %s was passed more than once in function call to %s", option.name,
			                      function.GetName().GetIdentifierName());
		}
		named_parameters.insert(make_pair(option.name, PlaceOption(context, argument, option)));
	}
}

unique_ptr<ResolvedTableFunction>
FunctionBinder::BindTableFunction(const TableFunctionSet &functions,
                                  vector<unique_ptr<Expression>> &positional_arguments,
                                  vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(context, functions, positional_arguments, named_arguments);
	if (!selection.IsSuccess()) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto &definition = functions.GetFunctionByOffset(binding.GetOverloadIndex());
	auto resolved = ResolveArguments(*definition, binding, positional_arguments, named_arguments);
	CastToFunctionArguments(definition->GetName(), resolved.types, resolved.arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FoldArguments(context, *definition, resolved, parameters, named_parameters);
	// copied out of the set: binding the table function fills in the bound return types
	return make_uniq<ResolvedTableFunction>(BoundTableFunction(definition), std::move(parameters),
	                                        std::move(named_parameters));
}

unique_ptr<BoundPragmaInfo> FunctionBinder::BindPragmaFunction(
    const PragmaFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
    vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(context, functions, positional_arguments, named_arguments);
	if (!selection.IsSuccess()) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto &definition = functions.GetFunctionByOffset(binding.GetOverloadIndex());
	auto resolved = ResolveArguments(*definition, binding, positional_arguments, named_arguments);
	CastToFunctionArguments(definition->GetName(), resolved.types, resolved.arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FoldArguments(context, *definition, resolved, parameters, named_parameters);
	return make_uniq<BoundPragmaInfo>(*definition, std::move(parameters), std::move(named_parameters));
}

pair<vector<LogicalType>, vector<pair<Identifier, LogicalType>>>
FunctionBinder::GetArgumentsFromExpressions(const vector<unique_ptr<Expression>> &regular_args,
                                            const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
	vector<LogicalType> regular_arg_types;
	vector<pair<Identifier, LogicalType>> keyword_arg_types;

	for (auto &arg : regular_args) {
		regular_arg_types.push_back(ExpressionBinder::GetExpressionReturnType(*arg));
	}
	for (auto &kwarg : keyword_args) {
		keyword_arg_types.emplace_back(kwarg.first, ExpressionBinder::GetExpressionReturnType(*kwarg.second));
	}
	return {std::move(regular_arg_types), std::move(keyword_arg_types)};
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

//! Whether the type has no concrete type information (e.g. DECIMAL without width / scale) - ANY, UNKNOWN and INVALID
//! are excluded, as casts to these have special meaning (e.g. for prepared statement parameters)
static bool IsIncompleteCastTarget(const LogicalType &type) {
	if (type.IsComplete()) {
		return false;
	}
	return !TypeVisitor::Contains(type, [](const LogicalType &child) {
		return child.id() == LogicalTypeId::ANY || child.id() == LogicalTypeId::UNKNOWN ||
		       child.id() == LogicalTypeId::INVALID;
	});
}

void FunctionBinder::CastToFunctionArguments(const Identifier &function_name, vector<LogicalType> &argument_types,
                                             vector<unique_ptr<Expression>> &children) {
	for (auto &arg : argument_types) {
		PrepareTypeForCast(arg);
	}

	// Varargs should be expanded by this point
	// If not, the function has somehow added more argument expressions during binding, which is not allowed.
	// There is one exception, count(*) which can be invoked internally with fewer arguments than the function
	// definition. That should be fixed separately though.
	D_ASSERT(children.size() <= argument_types.size());

	for (idx_t i = 0; i < children.size(); i++) {
		auto &target_type = argument_types[i];
		if (target_type.id() == LogicalTypeId::STRING_LITERAL || target_type.id() == LogicalTypeId::INTEGER_LITERAL) {
			throw InternalException(
			    "Function %s returned a STRING_LITERAL or INTEGER_LITERAL type - return an explicit type instead",
			    function_name);
		}
		target_type.Verify();
		// don't cast lambda children, their slot only ever holds a placeholder
		if (children[i]->GetReturnType().id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		const bool target_has_any = TypeVisitor::Contains(target_type, LogicalTypeId::ANY);
		if (target_has_any && children[i]->GetReturnType().id() == LogicalTypeId::ARRAY &&
		    target_type.id() == LogicalTypeId::LIST) {
			// an array passed to a LIST(ANY) parameter is cast to a list of its child type
			children[i] = BoundCastExpression::AddArrayCastToList(context, std::move(children[i]));
		}
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->GetReturnType(), target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			if (target_has_any && children[i]->GetReturnType().id() != LogicalTypeId::UNKNOWN) {
				// a type that is not concrete cannot be cast to (e.g. a NULL or a VARIANT passed to LIST(ANY)) - the
				// function has to handle the argument as-is. Unresolved parameters are still assigned the type.
				continue;
			}
			if (IsIncompleteCastTarget(target_type)) {
				throw InternalException("Function %s has incomplete argument type %s - concrete argument types must "
				                        "be resolved in the resolve_types callback",
				                        function_name, target_type);
			}
			children[i] = BoundCastExpression::AddCastToType(context, std::move(children[i]), target_type);
		}
	}
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const Identifier &schema, const Identifier &name,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto &function = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(
	    context, QualifiedName(Catalog::GetSystemCatalog(context).GetName(), schema, name));
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function, std::move(children), error, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const Identifier &schema, const Identifier &name,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	ErrorData error;
	auto result = BindScalarFunction(schema, name, std::move(children), error, is_operator, binder);
	if (!result) {
		error.Throw();
	}
	return result;
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.emplace_back(string(), std::move(child));
	}
	return BindScalarFunction(func, std::move(arguments), error, is_operator, binder);
}

//! Returns true if any argument is an untyped NULL, i.e. has type SQLNULL
static bool HasUntypedNullArgument(const vector<unique_ptr<Expression>> &regular_args,
                                   const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
	for (auto &child : regular_args) {
		if (child->GetReturnType() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	for (auto &child : keyword_args) {
		if (child.second->GetReturnType() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	return false;
}

//! Evaluates a constant argument and returns true if it is NULL. A non-NULL result replaces the argument with a
//! constant: the argument has been evaluated anyway, and keeping the result means that it is not evaluated again by
//! every enclosing function call - which made binding nested constant expressions quadratic in their depth.
static bool FoldConstantArgument(ClientContext &context, unique_ptr<Expression> &child) {
	if (child->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return child->Cast<BoundConstantExpression>().GetValue().IsNull();
	}
	// a lambda has no value of its own, the function it is passed to executes its body
	if (child->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA || !child->IsFoldable()) {
		return false;
	}
	Value result;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *child, result)) {
		// the argument cannot be evaluated: leave it in place so that the error surfaces at execution
		return false;
	}
	if (result.IsNull()) {
		return true;
	}
	auto constant = make_uniq<BoundConstantExpression>(std::move(result));
	constant->SetAlias(child->GetAlias());
	constant->SetQueryLocation(child->GetQueryLocation());
	child = std::move(constant);
	return false;
}

//! Returns true if any argument is a constant expression that evaluates to NULL. Arguments that evaluate to a non-NULL
//! value are folded to constants along the way.
static bool HasNullConstantArgument(ClientContext &context, vector<unique_ptr<Expression>> &regular_args,
                                    vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
	for (auto &child : regular_args) {
		if (FoldConstantArgument(context, child)) {
			return true;
		}
	}
	for (auto &child : keyword_args) {
		if (FoldConstantArgument(context, child.second)) {
			return true;
		}
	}
	return false;
}

static void PropagateReturnCollation(BoundSimpleFunction &bound_function, const FunctionProperties &props,
                                     const vector<unique_ptr<Expression>> &children, const string &pushed_collation);

//! The type of the NULL constant a function call is folded to - SQLNULL if the return type could not be determined
static LogicalType NullReturnType(const LogicalType &return_type) {
	return return_type.IsComplete() ? return_type : LogicalType::SQLNULL;
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> arguments,
                                                          ErrorData &error, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	auto best_function = SelectFunctionFromArguments(func.functions, arguments);
	if (!best_function.IsSuccess()) {
		error = best_function.GetError();
		return nullptr;
	}

	// found a matching function!
	auto &binding = best_function.GetBinding();
	auto selected_function = func.functions.GetFunctionByOffset(binding.GetOverloadIndex());
	auto &bound_function = *selected_function;

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	// If any of the parameters are NULL, the function will just be replaced with a NULL constant.
	// Untyped NULLs never go through type resolution. For typed NULL constants, resolve the function's types only
	// when needed to determine the return type. Preserve result collation without invoking bind callbacks.
	if (bound_function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		const auto &return_type = bound_function.GetReturnType();
		if (HasUntypedNullArgument(regular_args, keyword_args)) {
			return make_uniq<BoundConstantExpression>(Value(NullReturnType(return_type)));
		}
		if (HasNullConstantArgument(context, regular_args, keyword_args)) {
			if (return_type.IsComplete()) {
				BoundScalarFunction null_function(std::move(selected_function));
				for (auto &child : keyword_args) {
					regular_args.push_back(std::move(child.second));
				}
				PropagateReturnCollation(null_function, null_function.GetProperties(), regular_args, string());
				return make_uniq<BoundConstantExpression>(Value(null_function.GetReturnType()));
			}
			auto resolved =
			    ResolveScalarFunctionTypes(std::move(selected_function), binding, regular_args, keyword_args);
			PropagateReturnCollation(resolved.bound_function, resolved.bound_function.GetProperties(), regular_args,
			                         resolved.pushed_collation);
			return make_uniq<BoundConstantExpression>(Value(NullReturnType(resolved.bound_function.GetReturnType())));
		}
	}
	return BindScalarFunction(std::move(selected_function), binding, std::move(regular_args), std::move(keyword_args),
	                          is_operator, binder);
}

static bool RequiresCollationPropagation(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && !type.HasAlias();
}

struct FunctionCollations {
	string collation;
	identifier_map_t<unique_ptr<FunctionCollations>> fields;

	FunctionCollations &GetField(const Identifier &name) {
		auto &field = fields[name];
		if (!field) {
			field = make_uniq<FunctionCollations>();
		}
		return *field;
	}
};

static Identifier CollationFieldName(const LogicalType &type, idx_t index) {
	return StructType::IsUnnamed(type) ? Identifier(to_string(index)) : StructType::GetChildName(type, index);
}

//! Recursively extracts the collation of a (possibly nested) type, e.g. the element collation of a LIST(VARCHAR).
//! target is the resolved argument type and may include fields from other arguments.
static void ExtractCollationFromType(const LogicalType &source_type, const LogicalType &target,
                                     FunctionCollations &collations) {
	// Leave collation of aliased types to their registered callbacks.
	if (source_type.HasAlias() || target.HasAlias()) {
		return;
	}
	switch (source_type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(source_type);
		if (collations.collation.empty()) {
			collations.collation = std::move(collation);
		} else if (!collation.empty() && collations.collation != collation) {
			throw BinderException("Cannot combine types with different collation");
		}
		break;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		auto &source_child = source_type.id() == LogicalTypeId::LIST ? ListType::GetChildType(source_type)
		                                                             : ArrayType::GetChildType(source_type);
		auto &target_child = target.id() == LogicalTypeId::LIST    ? ListType::GetChildType(target)
		                     : target.id() == LogicalTypeId::ARRAY ? ArrayType::GetChildType(target)
		                                                           : source_child;
		ExtractCollationFromType(source_child, target_child, collations);
		break;
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		auto &target_type = StructType::IsStruct(target) && target.HasParameters() ? target : source_type;
		auto &source_children = StructType::GetChildTypes(source_type);
		auto &target_children = StructType::GetChildTypes(target_type);

		// If BOTH source and target_type are named, we can use named indexing, else we use positional.
		if (!StructType::IsUnnamed(source_type) && !StructType::IsUnnamed(target_type)) {
			// Named indexing, so build a map to map source fields to their position
			identifier_map_t<idx_t> src_child_indices;
			for (idx_t i = 0; i < source_children.size(); i++) {
				src_child_indices.emplace(source_children[i].first, i);
			}

			for (idx_t i = 0; i < target_children.size(); i++) {
				// Find index in the source fields, or skip if non existent.
				auto entry = src_child_indices.find(target_children[i].first);
				if (entry == src_child_indices.end()) {
					continue;
				}
				const auto child_index = entry->second;
				auto &field = collations.GetField(CollationFieldName(target_type, i));
				ExtractCollationFromType(source_children[child_index].second, target_children[i].second, field);
			}
		} else {
			// Use positional indexing, as not both source_type and target_type are named.
			for (idx_t i = 0; i < MinValue(target_children.size(), source_children.size()); i++) {
				auto &field = collations.GetField(CollationFieldName(target_type, i));
				ExtractCollationFromType(source_children[i].second, target_children[i].second, field);
			}
		}
		break;
	}
	default:
		break;
	}
}

//! Returns a copy of the type with the collected collations applied to its VARCHAR fields.
static LogicalType ApplyCollationToType(const LogicalType &type, const FunctionCollations &collations) {
	// Rebuilding an aliased type would discard its alias and bypass its collation callbacks
	if (type.HasAlias()) {
		return type;
	}
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return collations.collation.empty() ? type : LogicalType::VARCHAR_COLLATION(collations.collation);
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ApplyCollationToType(ListType::GetChildType(type), collations));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(ApplyCollationToType(ArrayType::GetChildType(type), collations),
		                          ArrayType::GetSize(type));
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		auto children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto entry = collations.fields.find(CollationFieldName(type, i));
			if (entry != collations.fields.end()) {
				children[i].second = ApplyCollationToType(children[i].second, *entry->second);
			}
		}
		return StructType::IsUnnamed(type) ? LogicalType::TUPLE(std::move(children))
		                                   : LogicalType::STRUCT(std::move(children));
	}
	default:
		return type;
	}
}

static string ExtractCollation(const vector<unique_ptr<Expression>> &children) {
	string collation;
	for (auto &arg : children) {
		if (!RequiresCollationPropagation(arg->GetReturnType())) {
			// not a varchar column
			continue;
		}
		auto child_collation = StringType::GetCollation(arg->GetReturnType());
		if (collation.empty()) {
			collation = child_collation;
		} else if (!child_collation.empty() && collation != child_collation) {
			throw BinderException("Cannot combine types with different collation!");
		}
	}
	return collation;
}

//! Pushes the collations of the arguments into the children, returns the collation to propagate to the return type
static string PushCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                             vector<unique_ptr<Expression>> &children, CollationType type) {
	FunctionCollations collations;
	auto &argument_types = bound_function.GetArguments();
	for (idx_t i = 0; i < children.size(); i++) {
		auto &source_type = children[i]->GetReturnType();
		auto &target_type = argument_types[i].IsComplete() ? argument_types[i] : source_type;
		ExtractCollationFromType(source_type, target_type, collations);
	}
	if (collations.collation.empty() && collations.fields.empty()) {
		// no collation to push
		return string();
	}
	// push collations to the children
	for (idx_t i = 0; i < children.size(); i++) {
		auto &arg = children[i];
		auto &arg_type = argument_types[i];
		auto &target_type = arg_type.IsComplete() ? arg_type : arg->GetReturnType();
		// apply the collations to the (possibly nested) varchar leaves of the argument type
		auto collated_type = ApplyCollationToType(target_type, collations);
		arg = BoundCastExpression::AddCastToType(context, std::move(arg), collated_type);
		if (RequiresCollationPropagation(arg->GetReturnType())) {
			// if this is a varchar type - propagate the collation
			arg->SetReturnType(collated_type);
		}
		// now push the actual collation handling
		if (ExpressionBinder::PushCollation(context, arg, collated_type, type)) {
			// Normalization can change field types, so do not cast back to the original argument type.
			arg_type = arg->GetReturnType();
		}
	}
	return collations.collation;
}

//! Pushes collations into the children (if required), returns the collation to propagate to the return type
static string PushArgumentCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                                     const FunctionProperties &props, vector<unique_ptr<Expression>> &children) {
	switch (props.GetCollationHandling()) {
	case FunctionCollationHandling::IGNORE_COLLATIONS:
	case FunctionCollationHandling::PROPAGATE_COLLATIONS:
		return string();
	case FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS:
		return PushCollations(context, bound_function, children, CollationType::COMBINABLE_COLLATIONS);
	case FunctionCollationHandling::PUSH_DISTINCT_COLLATIONS:
		throw InternalException("PUSH_DISTINCT_COLLATIONS is only supported for aggregate functions");
	default:
		throw InternalException("Unrecognized collation handling");
	}
}

//! Propagates the collation of the arguments to the return type of the function (if it returns a varchar)
static void PropagateReturnCollation(BoundSimpleFunction &bound_function, const FunctionProperties &props,
                                     const vector<unique_ptr<Expression>> &children, const string &pushed_collation) {
	if (!RequiresCollationPropagation(bound_function.GetReturnType())) {
		return;
	}
	auto collation = props.GetCollationHandling() == FunctionCollationHandling::PROPAGATE_COLLATIONS
	                     ? ExtractCollation(children)
	                     : pushed_collation;
	if (collation.empty()) {
		return;
	}
	bound_function.SetReturnType(LogicalType::VARCHAR_COLLATION(std::move(collation)));
}

// The names of the named arguments have to stay parallel to the last arguments
static void VerifyArgumentCount(const BoundSimpleFunction &bound_function, idx_t argument_count) {
	const auto resolved_count = bound_function.GetPositionalArgumentCount() + bound_function.GetNamedArguments().size();
	if (resolved_count != argument_count && !bound_function.GetNamedArguments().empty()) {
		throw InternalException("Function %s cannot add or remove arguments in its bind callback when it is "
		                        "called with keyword-only or '**kwargs' arguments",
		                        bound_function.GetName());
	}
}

//! The arguments are cast before the bind callback - verify that it did not change the argument types
static void VerifyBindArgumentTypes(const BoundScalarFunction &bound_function,
                                    const vector<unique_ptr<Expression>> &arguments) {
	auto &argument_types = bound_function.GetArguments();
	if (arguments.size() > argument_types.size()) {
		throw InternalException("Function %s added arguments in its bind callback - argument types must be "
		                        "resolved in the resolve_types callback",
		                        bound_function.GetName());
	}
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &source_type = arguments[i]->GetReturnType();
		if (source_type.id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		auto target_type = argument_types[i];
		PrepareTypeForCast(target_type);
		if (target_type.id() == LogicalTypeId::UNKNOWN || target_type.id() == LogicalTypeId::INVALID) {
			continue;
		}
		if (RequiresCast(source_type, target_type) == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			throw InternalException("Function %s changed the type of argument %llu from %s to %s in its bind "
			                        "callback - argument types must be resolved in the resolve_types callback",
			                        bound_function.GetName(), i + 1, source_type, target_type);
		}
	}
}

//! Incomplete argument types (e.g. a DECIMAL without width and scale) accept any type of that kind - resolve them to
//! the type of the argument
static void ResolveIncompleteArgumentTypes(BoundSimpleFunction &bound_function,
                                           const vector<unique_ptr<Expression>> &arguments) {
	auto &argument_types = bound_function.GetArguments();
	for (idx_t i = 0; i < arguments.size() && i < argument_types.size(); i++) {
		auto &source_type = arguments[i]->GetReturnType();
		if (!IsIncompleteCastTarget(argument_types[i]) || source_type.id() != argument_types[i].id() ||
		    !source_type.IsComplete()) {
			continue;
		}
		argument_types[i] = source_type;
	}
}

static vector<LogicalType> CaptureLogicalArguments(const BoundSimpleFunction &function,
                                                   const vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(function.GetArguments().size() == arguments.size());
	vector<LogicalType> result;
	result.reserve(arguments.size());
	for (idx_t argument_index = 0; argument_index < arguments.size(); argument_index++) {
		auto target_type = function.GetArguments()[argument_index];
		PrepareTypeForCast(target_type);
		result.push_back(target_type.IsComplete() ? target_type : arguments[argument_index]->GetReturnType());
	}
	return result;
}

ResolvedScalarFunctionTypes
FunctionBinder::ResolveScalarFunctionTypes(shared_ptr<const ScalarFunction> function_p, const FunctionBinding &binding,
                                           vector<unique_ptr<Expression>> &arguments,
                                           vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto &function = *function_p;
	// Make a BoundScalarFunction out of the ScalarFunction, so we can store bind info and other properties in it.
	ResolvedScalarFunctionTypes result {BoundScalarFunction(std::move(function_p)), {}};
	auto &bound_function = result.bound_function;

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, arguments, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	bound_function.GetArguments() = std::move(resolved.types);
	bound_function.SetReturnType(binding.return_type);
	arguments = std::move(resolved.arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, arguments));
	ResolveIncompleteArgumentTypes(bound_function, arguments);

	if (bound_function.HasResolveTypesCallback()) {
		ResolveScalarFunctionTypesInput input(context, bound_function, arguments);
		bound_function.GetResolveTypesCallback()(input);
		VerifyArgumentCount(bound_function, arguments.size());
	}
	result.pushed_collation =
	    PushArgumentCollations(context, bound_function, bound_function.GetProperties(), arguments);
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), arguments);
	return result;
}

pair<BoundScalarFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveFunction(shared_ptr<const ScalarFunction> function_p, const FunctionBinding &binding,
                                vector<unique_ptr<Expression>> &arguments,
                                vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto resolved = ResolveScalarFunctionTypes(std::move(function_p), binding, arguments, named_arguments);
	auto &bound_function = resolved.bound_function;

	// The bind callback is called with arguments that are cast to the argument types of the bound function
	unique_ptr<FunctionData> bind_info;
	if (bound_function.HasBindCallback()) {
		BindScalarFunctionInput input(context, bound_function, arguments, binder);
		bind_info = bound_function.GetBindCallback()(input);
		VerifyArgumentCount(bound_function, arguments.size());
		VerifyBindArgumentTypes(bound_function, arguments);
	}

	if (bound_function.HasModifiedDatabasesCallback() && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties);
		bound_function.GetModifiedDatabasesCallback()(context, input);
	}

	PropagateReturnCollation(bound_function, bound_function.GetProperties(), arguments, resolved.pushed_collation);
	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(shared_ptr<const ScalarFunction> function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	return BindScalarFunction(std::move(function), std::move(children), {}, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	return BindScalarFunction(make_shared_ptr<ScalarFunction>(function), std::move(children), {}, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	return BindScalarFunction(make_shared_ptr<ScalarFunction>(function), std::move(children), std::move(keyword_args),
	                          is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(shared_ptr<const ScalarFunction> function,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	auto binding = MatchFunction(function, children, keyword_args);
	return BindScalarFunction(std::move(function), binding, std::move(children), std::move(keyword_args), is_operator,
	                          binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(shared_ptr<const ScalarFunction> function,
                                                          const FunctionBinding &binding,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	auto [bound_function, bind_info] = ResolveFunction(std::move(function), binding, children, keyword_args);

	unique_ptr<Expression> result;

	auto result_func = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(children),
	                                                      std::move(bind_info), is_operator);

	if (binder && result_func->Function().GetStability() == FunctionStability::CONSISTENT_WITHIN_QUERY) {
		// the result depends on the query it is bound in, also when the bind_expression callback folds it
		binder->SetAlwaysRequireRebind();
	}
	if (result_func->Function().HasBindExpressionCallback()) {
		// if a bind_expression callback is registered - call it and emit the resulting expression
		FunctionBindExpressionInput input(context, result_func->FunctionMutable(), result_func->BindInfoMutable().get(),
		                                  result_func->GetChildrenMutable());
		result = result_func->Function().GetBindExpressionCallback()(input);
	}

	if (!result) {
		result = std::move(result_func);
	}

	return result;
}

pair<BoundAggregateFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveFunction(shared_ptr<const AggregateFunction> function_p, const FunctionBinding &binding,
                                vector<unique_ptr<Expression>> &children,
                                vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto &function = *function_p;
	// Make a BoundFunction out of the func
	BoundAggregateFunction bound_function(std::move(function_p));

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, children, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	bound_function.GetArguments() = std::move(resolved.types);
	bound_function.SetReturnType(binding.return_type);
	children = std::move(resolved.arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, children));

	unique_ptr<FunctionData> bind_info;

	if (bound_function.GetCallbacks().HasBindCallback()) {
		BindAggregateFunctionInput input(context, bound_function, children);
		bind_info = bound_function.GetCallbacks().GetBindCallback()(input);

		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}

	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(shared_ptr<const AggregateFunction> function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	return BindAggregateFunction(std::move(function), std::move(children), {}, std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(const AggregateFunction &function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	return BindAggregateFunction(make_shared_ptr<AggregateFunction>(function), std::move(children), {},
	                             std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	return BindAggregateFunction(make_shared_ptr<AggregateFunction>(function), std::move(children),
	                             std::move(keyword_args), std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(shared_ptr<const AggregateFunction> function,
                                      vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	auto binding = MatchFunction(function, children, keyword_args);
	return BindAggregateFunction(std::move(function), std::move(binding), std::move(children), std::move(keyword_args),
	                             std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(shared_ptr<const AggregateFunction> function, FunctionBinding binding,
                                      vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	if (aggr_type == AggregateType::DISTINCT &&
	    function->GetCollationHandling() == FunctionCollationHandling::PUSH_DISTINCT_COLLATIONS) {
		for (auto &child : children) {
			ExpressionBinder::PushCollation(context, child, child->GetReturnType());
		}
		for (auto &argument : keyword_args) {
			ExpressionBinder::PushCollation(context, argument.second, argument.second->GetReturnType());
		}
		// the arguments are replaced by their comparison keys, which may be of other types
		binding = MatchFunction(function, children, keyword_args);
	}
	auto [bound_function, bind_info] = ResolveFunction(std::move(function), binding, children, keyword_args);

	return make_uniq<BoundAggregateExpression>(std::move(bound_function), std::move(children), std::move(filter),
	                                           std::move(bind_info), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunctionCatalogEntry &func,
                                      vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	auto best_function = SelectFunctionFromArguments(func.functions, arguments);
	if (!best_function.IsSuccess()) {
		error = best_function.GetError();
		return nullptr;
	}

	auto &binding = best_function.GetBinding();
	auto bound_function = func.functions.GetFunctionByOffset(binding.GetOverloadIndex());

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	return BindAggregateFunction(std::move(bound_function), binding, std::move(regular_args), std::move(keyword_args),
	                             std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(const AggregateFunctionCatalogEntry &func,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           ErrorData &error,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.emplace_back(Identifier(), std::move(child));
	}
	return BindAggregateFunction(func, std::move(arguments), error, std::move(filter), aggr_type);
}

pair<BoundWindowFunction, unique_ptr<FunctionData>> FunctionBinder::ResolveFunction(
    shared_ptr<const WindowFunction> function_p, const FunctionBinding &binding,
    vector<unique_ptr<Expression>> &children, vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
    optional_ptr<vector<LogicalType>> order_types, optional_ptr<vector<LogicalType>> arg_order_types) {
	auto &function = *function_p;
	BoundWindowFunction bound_function(std::move(function_p));

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, children, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	bound_function.GetArguments() = std::move(resolved.types);
	bound_function.SetReturnType(binding.return_type);
	children = std::move(resolved.arguments);

	auto logical_arguments = CaptureLogicalArguments(bound_function, children);
	auto argument_types = bound_function.GetArguments();

	unique_ptr<FunctionData> bind_info;

	if (bound_function.HasBindCallback()) {
		BindWindowFunctionInput input(context, bound_function, children, order_types, arg_order_types);
		bind_info = bound_function.GetBindCallback()(input);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}

	for (idx_t i = 0; i < argument_types.size() && i < bound_function.GetArguments().size(); i++) {
		if (!argument_types[i].IsComplete() && bound_function.GetArguments()[i].IsComplete()) {
			logical_arguments[i] = bound_function.GetArguments()[i];
		}
	}
	bound_function.SetLogicalArguments(std::move(logical_arguments));
	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(shared_ptr<const WindowFunction> function, vector<unique_ptr<Expression>> children,
                                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	auto binding = MatchFunction(function, children, keyword_args);
	return BindWindowFunction(std::move(function), binding, std::move(children), std::move(keyword_args), order_types,
	                          arg_order_types);
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(shared_ptr<const WindowFunction> function, const FunctionBinding &binding,
                                   vector<unique_ptr<Expression>> children,
                                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	auto [bound_function, bind_info] =
	    ResolveFunction(std::move(function), binding, children, keyword_args, order_types, arg_order_types);
	auto return_type = bound_function.GetReturnType();

	auto window = make_uniq<BoundWindowFunction>(std::move(bound_function));
	auto result = make_uniq<BoundWindowExpression>(return_type, nullptr, std::move(window), std::move(bind_info));
	result->GetChildrenMutable() = std::move(children);

	return result;
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const WindowFunctionCatalogEntry &func,
                                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	// select the best matching overload
	auto best_function = SelectFunctionFromArguments(func.functions, arguments);
	if (!best_function.IsSuccess()) {
		error = best_function.GetError();
		return nullptr;
	}

	auto &binding = best_function.GetBinding();
	auto bound_function = func.functions.GetFunctionByOffset(binding.GetOverloadIndex());

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	return BindWindowFunction(std::move(bound_function), binding, std::move(regular_args), std::move(keyword_args),
	                          order_types, arg_order_types);
}

} // namespace duckdb
