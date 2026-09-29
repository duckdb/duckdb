#include "duckdb/main/capi_v2/capi_v2_function_internal.hpp"

namespace duckdb::capiv2 {

const Identifier &CV2FunctionBindInfo::GetArgName(idx_t index) const {
	static const Identifier variadic_name;
	const auto positional_count = GetPositionalCount();
	if (index >= positional_count) {
		return GetNamedArgName(index - positional_count);
	}
	if (index < signature.GetPositionalParameterCount()) {
		return signature.GetParameter(index).GetName();
	}
	return variadic_name;
}

idx_t CV2FunctionBindInfo::GetPositionalVariadicCount() const {
	const auto fixed_count = signature.GetPositionalParameterCount();
	const auto positional_count = GetPositionalCount();
	return positional_count > fixed_count ? positional_count - fixed_count : 0;
}

idx_t CV2FunctionBindInfo::GetNamedVariadicCount() const {
	idx_t result = 0;
	for (idx_t i = 0; i < GetArgCount() - GetPositionalCount(); i++) {
		if (!signature.GetParameterIndexByName(GetNamedArgName(i)).IsValid()) {
			result++;
		}
	}
	return result;
}

optional_idx CV2FunctionBindInfo::FindArg(const Identifier &name) const {
	for (idx_t i = signature.GetPositionalOnlyParameterCount(); i < GetArgCount(); i++) {
		const auto &arg_name = GetArgName(i);
		if (!arg_name.empty() && arg_name == name) {
			return optional_idx(i);
		}
	}
	return optional_idx();
}

void CV2FunctionBindInfo::GetArgCounts(idx_t *positional_fixed, idx_t *positional_variadic, idx_t *named_fixed,
                                       idx_t *named_variadic) const {
	const auto positional_count = GetPositionalCount();
	const auto positional_variadic_count = GetPositionalVariadicCount();
	const auto named_variadic_count = GetNamedVariadicCount();
	if (positional_fixed) {
		*positional_fixed = positional_count - positional_variadic_count;
	}
	if (positional_variadic) {
		*positional_variadic = positional_variadic_count;
	}
	if (named_fixed) {
		*named_fixed = GetArgCount() - positional_count - named_variadic_count;
	}
	if (named_variadic) {
		*named_variadic = named_variadic_count;
	}
}

shared_ptr<CV2UserData> CV2FunctionBindInfo::TakeBindData() const {
	if (!out_bind_data.ptr) {
		return nullptr;
	}
	return make_shared_ptr<CV2UserData>(out_bind_data.ptr, out_bind_data.destroy, out_bind_data.equals);
}

CV2ExpressionBindInfo::CV2ExpressionBindInfo(const FunctionSignature &signature, void *user_data,
                                             const BindFunctionInput &input, const vector<Identifier> &named_names)
    : CV2FunctionBindInfo(signature, user_data), input(input), named_names(named_names) {
	D_ASSERT(named_names.size() <= input.GetArguments().size());
}

idx_t CV2ExpressionBindInfo::GetArgCount() const {
	return input.GetArguments().size();
}

idx_t CV2ExpressionBindInfo::GetPositionalCount() const {
	return input.GetArguments().size() - named_names.size();
}

LogicalType CV2ExpressionBindInfo::GetArgType(idx_t index) const {
	return input.GetArguments()[index]->GetReturnType();
}

Value CV2ExpressionBindInfo::GetArgValue(idx_t index) const {
	return input.GetConstant(index);
}

const Identifier &CV2ExpressionBindInfo::GetNamedArgName(idx_t named_index) const {
	return named_names[named_index];
}

CV2ConstantBindInfo::CV2ConstantBindInfo(const FunctionSignature &signature, void *user_data,
                                         const vector<Value> &positional, const named_argument_map_t &named)
    : CV2FunctionBindInfo(signature, user_data), positional(positional) {
	for (auto &entry : named) {
		named_names.push_back(entry.first);
		named_values.push_back(entry.second);
	}
}

idx_t CV2ConstantBindInfo::GetArgCount() const {
	return positional.size() + named_values.size();
}

idx_t CV2ConstantBindInfo::GetPositionalCount() const {
	return positional.size();
}

const Value &CV2ConstantBindInfo::GetArg(idx_t index) const {
	return index < positional.size() ? positional[index] : named_values[index - positional.size()].get();
}

LogicalType CV2ConstantBindInfo::GetArgType(idx_t index) const {
	return GetArg(index).type();
}

Value CV2ConstantBindInfo::GetArgValue(idx_t index) const {
	return GetArg(index);
}

const Identifier &CV2ConstantBindInfo::GetNamedArgName(idx_t named_index) const {
	return named_names[named_index];
}

static void CheckArgIndex(const CV2FunctionBindInfo &info, idx_t index, const char *function_name) {
	if (index >= info.GetArgCount()) {
		throw InvalidInputException("Index out of bounds in %s", function_name);
	}
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_user_data(duckdb_v2_function_bind_info_handle info, void **data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_set_bind_data(duckdb_v2_function_bind_info_handle info, duckdb_v2_opaque *data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_bind_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_arg_count(duckdb_v2_function_bind_info_handle info, idx_t *positional_fixed,
                                                      idx_t *positional_variadic, idx_t *named_fixed,
                                                      idx_t *named_variadic, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		Convert(info)->GetArgCounts(positional_fixed, positional_variadic, named_fixed, named_variadic);
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_arg_type(duckdb_v2_function_bind_info_handle info, idx_t index,
                                                     duckdb_v2_logical_type_handle *type,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &bind_info = *Convert(info);
		CheckArgIndex(bind_info, index, "duckdb_v2_function_bind_get_arg_type");
		*type = Convert(new duckdb::LogicalType(bind_info.GetArgType(index)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_arg_value(duckdb_v2_function_bind_info_handle info, idx_t index,
                                                      duckdb_v2_value_handle *value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &bind_info = *Convert(info);
		CheckArgIndex(bind_info, index, "duckdb_v2_function_bind_get_arg_value");
		*value = Convert(new duckdb::Value(bind_info.GetArgValue(index)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_arg_name(duckdb_v2_function_bind_info_handle info, idx_t index,
                                                     duckdb_v2_identifier_t *name, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() {
		auto &bind_info = *Convert(info);
		CheckArgIndex(bind_info, index, "duckdb_v2_function_bind_get_arg_name");
		*name = Convert(bind_info.GetArgName(index));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_bind_get_arg_index(duckdb_v2_function_bind_info_handle info,
                                                      duckdb_v2_identifier_t name, idx_t *index, bool *found,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(index);
	DUCKDB_CHECK_ARG(found);
	return WithErrorHandler(err, [&]() {
		auto entry = Convert(info)->FindArg(duckdb::Identifier(ConvertIdentifierName(name)));
		*found = entry.IsValid();
		if (entry.IsValid()) {
			*index = entry.GetIndex();
		}
	});
}
