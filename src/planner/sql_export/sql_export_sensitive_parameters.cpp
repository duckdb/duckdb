#include "duckdb/planner/sql_export/sql_export_sensitive_parameters.hpp"

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

bool SQLExportSensitiveParameters::MatchesDeclaredDefault(ClientContext &context, const FunctionParameter &parameter,
                                                          const Value &value) {
	if (!parameter.HasDefaultValue()) {
		return false;
	}
	auto default_value =
	    FunctionBinder::CastToParameterType(context, *parameter.GetDefaultValue(), parameter.GetType());
	// comparing across types coerces through a common type, which can lose information - only a value of the
	// identical type is evidence that the retained value is the declared default
	if (value.type() != default_value.type()) {
		return false;
	}
	return ValueOperations::NotDistinctFrom(value, default_value);
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

Identifier SQLExportSensitiveParameters::RefusePositional(optional_ptr<ClientContext> context,
                                                          const FunctionSignature &signature, idx_t index,
                                                          optional_ptr<const Value> value) {
	auto declared = PositionalParameter(signature, index);
	if (!declared || !declared->IsSensitive()) {
		return Identifier();
	}
	if (context && value && MatchesDeclaredDefault(*context, *declared, *value)) {
		return Identifier();
	}
	return declared->GetName();
}

Identifier SQLExportSensitiveParameters::RefuseNamed(optional_ptr<ClientContext> context,
                                                     const FunctionSignature &signature, const Identifier &name,
                                                     optional_ptr<const Value> value) {
	auto parameter_index = signature.GetParameterIndexByName(name);
	if (parameter_index.IsValid()) {
		auto &declared = signature.GetParameter(parameter_index.GetIndex());
		if (!declared.IsSensitive()) {
			return Identifier();
		}
		if (context && value && MatchesDeclaredDefault(*context, declared, *value)) {
			return Identifier();
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
	return Identifier();
}

Identifier SQLExportSensitiveParameters::RefuseBoundArgument(optional_ptr<ClientContext> context,
                                                             const FunctionSignature &signature, idx_t positional_count,
                                                             const vector<Identifier> &named_arguments, idx_t index,
                                                             const Expression &argument) {
	optional_ptr<const Value> value;
	if (argument.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		value = &argument.Cast<BoundConstantExpression>().GetValue();
	}
	if (named_arguments.empty() || index < positional_count) {
		return RefusePositional(context, signature, index, value);
	}
	auto named_index = index - positional_count;
	if (named_index >= named_arguments.size()) {
		return Identifier();
	}
	return RefuseNamed(context, signature, named_arguments[named_index], value);
}

} // namespace duckdb
