//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/capi_v2_function_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb::capiv2 {

//! The part of a bind callback every function family shares: the arguments of the call site, the user data and the
//! bind data. The arguments are one list: the positional arguments, then the named arguments
class CV2FunctionBindInfo {
public:
	template <class FUNC>
	CV2FunctionBindInfo(const FUNC &function, void *user_data)
	    : in_user_data(user_data), function(function), signature(function.GetDefinition()->GetSignature()) {
	}
	virtual ~CV2FunctionBindInfo() = default;

	virtual LogicalType GetArgType(idx_t index) const = 0;
	virtual Value GetArgValue(idx_t index) const = 0;

	idx_t GetArgCount() const {
		return function.GetPositionalArgumentCount() + function.GetNamedArguments().size();
	}
	//! The parameter name for a declared parameter, the name the caller passed for "**kwargs", empty for "*args"
	const Identifier &GetArgName(idx_t index) const;
	//! Looks up an argument by a name a caller can pass it by, so positional-only parameters are skipped
	optional_idx FindArg(const Identifier &name) const;

public:
	void *in_user_data;
	//! The bind data the callback set, or nullptr
	shared_ptr<CV2UserData> out_bind_data;

	const BoundSimpleFunction &function;
	const FunctionSignature &signature;
};

//! The arguments of a scalar or aggregate call: the bound expressions
class CV2ExpressionBindInfo final : public CV2FunctionBindInfo {
public:
	template <class FUNC>
	CV2ExpressionBindInfo(const FUNC &function, void *user_data, const BindFunctionInput &input)
	    : CV2FunctionBindInfo(function, user_data), input(input) {
		D_ASSERT(GetArgCount() == input.GetArguments().size());
	}

	LogicalType GetArgType(idx_t index) const override;
	Value GetArgValue(idx_t index) const override;

private:
	const BindFunctionInput &input;
};

//! The arguments of a table function call: constants, the positional arguments and the named ones held apart
class CV2ConstantBindInfo final : public CV2FunctionBindInfo {
public:
	CV2ConstantBindInfo(const BoundTableFunction &function, void *user_data, const vector<Value> &positional,
	                    const named_argument_map_t &named);

	LogicalType GetArgType(idx_t index) const override;
	Value GetArgValue(idx_t index) const override;

private:
	const Value &GetArg(idx_t index) const;

	const vector<Value> &positional;
	const named_argument_map_t &named;
};

inline auto Convert(duckdb_v2_function_bind_info_handle info) -> CV2FunctionBindInfo * {
	return reinterpret_cast<CV2FunctionBindInfo *>(info);
}
inline auto Convert(CV2FunctionBindInfo *info) -> duckdb_v2_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_function_bind_info_handle>(info);
}

} // namespace duckdb::capiv2
