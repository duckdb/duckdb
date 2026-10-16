#include "duckdb/planner/sql_export/sql_export_sensitive_parameters.hpp"

#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

//! Whether both values render as the identical SQL text
static bool RendersIdentically(const Value &value, const Value &declared) {
	try {
		return ConstantExpression::FromValue(value)->ToString() == ConstantExpression::FromValue(declared)->ToString();
	} catch (const NotImplementedException &) {
		return false;
	}
}

bool SQLExportSensitiveParameters::MatchesDeclaredDefault(ClientContext &context, const FunctionParameter &parameter,
                                                          const Value &value) {
	if (!parameter.HasDefaultValue()) {
		return false;
	}
	// SQL equality coerces behind identical outer types (VARIANT payloads) and normalizes representations
	// (INTERVAL fields) - the exemption must prove the rendered text is what rendering the default produces
	auto &declared = *parameter.GetDefaultValue();
	auto cast_default = FunctionBinder::CastToParameterType(context, declared, parameter.GetType());
	return RendersIdentically(value, cast_default) || RendersIdentically(value, declared);
}

optional_ptr<const FunctionParameter>
SQLExportSensitiveParameters::PositionalParameter(const FunctionSignature &signature, idx_t index) {
	idx_t position = 0;
	for (auto &parameter : signature.GetParameters()) {
		if (parameter.GetKind() == FunctionParameterKind::VAR_POSITIONAL) {
			return &parameter;
		}
		if (!parameter.AcceptsPosition()) {
			continue;
		}
		if (position == index) {
			return &parameter;
		}
		position++;
	}
	return nullptr;
}

optional<Identifier> SQLExportSensitiveParameters::FirstSensitiveParameter(const FunctionSignature &signature) {
	for (auto &parameter : signature.GetParameters()) {
		if (parameter.IsSensitive()) {
			return parameter.GetName();
		}
	}
	auto typed_kwargs = signature.GetTypedKwargs();
	if (typed_kwargs) {
		for (auto &option : typed_kwargs->GetOptions()) {
			if (option.sensitive) {
				return option.name;
			}
		}
	}
	return {};
}

optional<Identifier> SQLExportSensitiveParameters::RefusePositional(optional_ptr<ClientContext> context,
                                                                    const FunctionSignature &signature, idx_t index,
                                                                    optional_ptr<const Value> value) {
	auto declared = PositionalParameter(signature, index);
	if (!declared || !declared->IsSensitive()) {
		return {};
	}
	if (context && value && MatchesDeclaredDefault(*context, *declared, *value)) {
		return {};
	}
	return declared->GetName();
}

optional<Identifier> SQLExportSensitiveParameters::RefuseNamed(optional_ptr<ClientContext> context,
                                                               const FunctionSignature &signature,
                                                               const Identifier &name,
                                                               optional_ptr<const Value> value) {
	auto parameter_index = signature.GetParameterIndexByName(name);
	if (parameter_index.IsValid()) {
		auto &declared = signature.GetParameter(parameter_index.GetIndex());
		if (!declared.IsSensitive()) {
			return {};
		}
		if (context && value && MatchesDeclaredDefault(*context, declared, *value)) {
			return {};
		}
		return declared.GetName();
	}
	auto kwargs = signature.GetKwargs();
	if (kwargs && kwargs->IsSensitive()) {
		return name;
	}
	auto typed_kwargs = signature.GetTypedKwargs();
	if (typed_kwargs) {
		auto option = typed_kwargs->Find(name);
		if (option && option->sensitive) {
			return option->name;
		}
	}
	return {};
}

optional<Identifier> SQLExportSensitiveParameters::RefuseBoundArgument(optional_ptr<ClientContext> context,
                                                                       const FunctionSignature &signature,
                                                                       idx_t positional_count,
                                                                       const vector<Identifier> &named_arguments,
                                                                       idx_t index, const Expression &argument) {
	// a default the binder materialized may be wrapped in casts to the parameter type
	reference<const Expression> unwrapped(argument);
	while (BoundCastExpression::IsCast(unwrapped.get())) {
		unwrapped = BoundCastExpression::Child(unwrapped.get().Cast<BoundFunctionExpression>());
	}
	optional_ptr<const Value> value;
	if (unwrapped.get().GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		value = &unwrapped.get().Cast<BoundConstantExpression>().GetValue();
	}
	if (named_arguments.empty() || index < positional_count) {
		return RefusePositional(context, signature, index, value);
	}
	auto named_index = index - positional_count;
	if (named_index >= named_arguments.size()) {
		return {};
	}
	return RefuseNamed(context, signature, named_arguments[named_index], value);
}

} // namespace duckdb
