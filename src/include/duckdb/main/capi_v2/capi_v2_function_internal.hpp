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

namespace duckdb::capiv2 {

//! The part of a bind callback every function family shares: the arguments of the call site, the user data and the
//! bind data. The arguments are one list: the positional arguments, then the named arguments
class CV2FunctionBindInfo {
public:
	CV2FunctionBindInfo(const FunctionSignature &signature, void *user_data)
	    : in_user_data(user_data), signature(signature) {
	}
	virtual ~CV2FunctionBindInfo() = default;

	virtual idx_t GetArgCount() const = 0;
	//! The number of leading arguments that were matched to the parameters by position
	virtual idx_t GetPositionalCount() const = 0;
	virtual LogicalType GetArgType(idx_t index) const = 0;
	virtual Value GetArgValue(idx_t index) const = 0;

	//! The parameter name for a declared parameter, the name the caller passed for "**kwargs", empty for "*args"
	const Identifier &GetArgName(idx_t index) const;
	//! The arguments "*args" received: those past the parameters a caller can pass by position
	idx_t GetPositionalVariadicCount() const;
	//! The arguments "**kwargs" received: those whose name matches no parameter a caller can pass by name
	idx_t GetNamedVariadicCount() const;
	//! Looks up an argument by a name a caller can pass it by, so positional-only parameters are skipped
	optional_idx FindArg(const Identifier &name) const;
	//! Writes the sizes of the four parts of the argument list, skipping a null out-parameter
	void GetArgCounts(idx_t *positional_fixed, idx_t *positional_variadic, idx_t *named_fixed,
	                  idx_t *named_variadic) const;

	//! The bind data the callback set, or nullptr
	shared_ptr<CV2UserData> TakeBindData() const;

public:
	void *in_user_data;
	duckdb_v2_opaque out_bind_data = {};

protected:
	//! The name of the named argument at the given index, counted from the first named argument
	virtual const Identifier &GetNamedArgName(idx_t named_index) const = 0;

	const FunctionSignature &signature;
};

//! The arguments of a scalar or aggregate call: bound expressions, the positional arguments then the named ones
class CV2ExpressionBindInfo final : public CV2FunctionBindInfo {
public:
	CV2ExpressionBindInfo(const FunctionSignature &signature, void *user_data, const BindFunctionInput &input,
	                      const vector<Identifier> &named_names);

	idx_t GetArgCount() const override;
	idx_t GetPositionalCount() const override;
	LogicalType GetArgType(idx_t index) const override;
	Value GetArgValue(idx_t index) const override;

protected:
	const Identifier &GetNamedArgName(idx_t named_index) const override;

private:
	const BindFunctionInput &input;
	const vector<Identifier> &named_names;
};

//! The arguments of a table function call: constants, the positional arguments and the named ones held apart
class CV2ConstantBindInfo final : public CV2FunctionBindInfo {
public:
	CV2ConstantBindInfo(const FunctionSignature &signature, void *user_data, const vector<Value> &positional,
	                    const named_argument_map_t &named);

	idx_t GetArgCount() const override;
	idx_t GetPositionalCount() const override;
	LogicalType GetArgType(idx_t index) const override;
	Value GetArgValue(idx_t index) const override;

protected:
	const Identifier &GetNamedArgName(idx_t named_index) const override;

private:
	const Value &GetArg(idx_t index) const;

	const vector<Value> &positional;
	vector<const_reference<Identifier>> named_names;
	vector<const_reference<Value>> named_values;
};

inline auto Convert(duckdb_v2_function_bind_info_handle info) -> CV2FunctionBindInfo * {
	return reinterpret_cast<CV2FunctionBindInfo *>(info);
}
inline auto Convert(CV2FunctionBindInfo *info) -> duckdb_v2_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_function_bind_info_handle>(info);
}

} // namespace duckdb::capiv2
