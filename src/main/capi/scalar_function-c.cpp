#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct CScalarFunctionInfo : public ScalarFunctionInfo {
	~CScalarFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	duckdb_scalar_function_bind_t bind = nullptr;
	duckdb_scalar_function_t function = nullptr;
	duckdb_function_info extra_info = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

struct CScalarFunctionBindData : public FunctionData {
	explicit CScalarFunctionBindData(CScalarFunctionInfo &info) : info(info) {
	}

	~CScalarFunctionBindData() override {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<CScalarFunctionBindData>(info);
		if (copy_callback) {
			copy->bind_data = copy_callback(bind_data);
			copy->delete_callback = delete_callback;
			copy->copy_callback = copy_callback;
		}
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CScalarFunctionBindData>();
		return info.extra_info == other.info.extra_info && info.function == other.info.function;
	}

	CScalarFunctionInfo &info;
	void *bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
	duckdb_copy_callback_t copy_callback = nullptr;
};

struct CScalarFunctionInternalBindInfo {
	CScalarFunctionInternalBindInfo(ClientContext &context, ScalarFunction &bound_function,
	                                vector<unique_ptr<Expression>> &arguments, CScalarFunctionBindData &bind_data)
	    : context(context), bound_function(bound_function), arguments(arguments), bind_data(bind_data) {
	}

	ClientContext &context;
	ScalarFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
	CScalarFunctionBindData &bind_data;

	bool success = true;
	string error = "";
};

struct CScalarFunctionInternalFunctionInfo {
	explicit CScalarFunctionInternalFunctionInfo(const CScalarFunctionBindData &bind_data)
	    : bind_data(bind_data), success(true) {};

	const CScalarFunctionBindData &bind_data;

	bool success;
	string error = "";
};

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

duckdb::ScalarFunction &GetCScalarFunction(duckdb_scalar_function function) {
	return *reinterpret_cast<duckdb::ScalarFunction *>(function);
}

duckdb::ScalarFunctionSet &GetCScalarFunctionSet(duckdb_scalar_function_set set) {
	return *reinterpret_cast<duckdb::ScalarFunctionSet *>(set);
}

duckdb::CScalarFunctionInternalBindInfo &GetCScalarFunctionBindInfo(duckdb_bind_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<duckdb::CScalarFunctionInternalBindInfo *>(info);
}

duckdb_bind_info ToCScalarFunctionBindInfo(duckdb::CScalarFunctionInternalBindInfo &info) {
	return reinterpret_cast<duckdb_bind_info>(&info);
}

duckdb::CScalarFunctionInternalFunctionInfo &GetCScalarFunctionInfo(duckdb_function_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<duckdb::CScalarFunctionInternalFunctionInfo *>(info);
}

duckdb_function_info ToCScalarFunctionInfo(duckdb::CScalarFunctionInternalFunctionInfo &info) {
	return reinterpret_cast<duckdb_function_info>(&info);
}

//===--------------------------------------------------------------------===//
// Scalar Function Callbacks
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto &info = bound_function.function_info->Cast<CScalarFunctionInfo>();
	D_ASSERT(info.function);

	auto result = make_uniq<CScalarFunctionBindData>(info);
	if (info.bind) {
		CScalarFunctionInternalBindInfo bind_info(context, bound_function, arguments, *result);
		info.bind(ToCScalarFunctionBindInfo(bind_info));
		if (!bind_info.success) {
			throw BinderException(bind_info.error);
		}
	}

	return std::move(result);
}

void CAPIScalarFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &function = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_info = function.bind_info;
	auto &c_bind_info = bind_info->Cast<CScalarFunctionBindData>();

	auto all_const = input.AllConstant();
	input.Flatten();
	auto c_input = reinterpret_cast<duckdb_data_chunk>(&input);
	auto c_result = reinterpret_cast<duckdb_vector>(&result);

	CScalarFunctionInternalFunctionInfo function_info(c_bind_info);
	auto c_function_info = ToCScalarFunctionInfo(function_info);
	c_bind_info.info.function(c_function_info, c_input, c_result);
	if (!function_info.success) {
		throw InvalidInputException(function_info.error);
	}
	if (all_const && (input.size() == 1 || function.function.GetStability() != FunctionStability::VOLATILE)) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace duckdb

using duckdb::ExpressionWrapper;
using duckdb::GetCScalarFunction;
using duckdb::GetCScalarFunctionBindInfo;
using duckdb::GetCScalarFunctionInfo;
using duckdb::GetCScalarFunctionSet;

duckdb_scalar_function duckdb_create_scalar_function() {
	auto function = new duckdb::ScalarFunction("", {}, duckdb::LogicalType::INVALID, duckdb::CAPIScalarFunction,
	                                           duckdb::CScalarFunctionBind);
	function->function_info = duckdb::make_shared_ptr<duckdb::CScalarFunctionInfo>();
	return reinterpret_cast<duckdb_scalar_function>(function);
}

void duckdb_destroy_scalar_function(duckdb_scalar_function *function) {
	if (function && *function) {
		auto scalar_function = reinterpret_cast<duckdb::ScalarFunction *>(*function);
		delete scalar_function;
		*function = nullptr;
	}
}

void duckdb_scalar_function_set_name(duckdb_scalar_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.name = name;
}

void duckdb_scalar_function_set_varargs(duckdb_scalar_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	scalar_function.varargs = *logical_type;
}

void duckdb_scalar_function_set_special_handling(duckdb_scalar_function function) {
	if (!function) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
}

void duckdb_scalar_function_set_volatile(duckdb_scalar_function function) {
	if (!function) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.SetVolatile();
}

void duckdb_scalar_function_add_parameter(duckdb_scalar_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	scalar_function.arguments.push_back(*logical_type);
}

void duckdb_scalar_function_set_return_type(duckdb_scalar_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	scalar_function.SetReturnType(*logical_type);
}

void *duckdb_scalar_function_get_extra_info(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCScalarFunctionInfo(info);
	return function_info.bind_data.info.extra_info;
}

void *duckdb_scalar_function_bind_get_extra_info(duckdb_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	return bind_info.bind_data.info.extra_info;
}

void *duckdb_scalar_function_get_bind_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCScalarFunctionInfo(info);
	return function_info.bind_data.bind_data;
}

void duckdb_scalar_function_get_client_context(duckdb_bind_info info, duckdb_client_context *out_context) {
	if (!info || !out_context) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	auto wrapper = new duckdb::CClientContextWrapper(bind_info.context);
	*out_context = reinterpret_cast<duckdb_client_context>(wrapper);
}

void duckdb_scalar_function_set_error(duckdb_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &scalar_function = duckdb::GetCScalarFunctionInfo(info);
	scalar_function.error = error;
	scalar_function.success = false;
}

void duckdb_scalar_function_bind_set_error(duckdb_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.error = error;
	bind_info.success = false;
}

idx_t duckdb_scalar_function_bind_get_argument_count(duckdb_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	return bind_info.arguments.size();
}

duckdb_expression duckdb_scalar_function_bind_get_argument(duckdb_bind_info info, idx_t index) {
	if (!info || index >= duckdb_scalar_function_bind_get_argument_count(info)) {
		return nullptr;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	auto wrapper = new ExpressionWrapper();
	wrapper->expr = bind_info.arguments[index]->Copy();
	return reinterpret_cast<duckdb_expression>(wrapper);
}

void duckdb_scalar_function_set_extra_info(duckdb_scalar_function function, void *extra_info,
                                           duckdb_delete_callback_t destroy) {
	if (!function || !extra_info) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto &info = scalar_function.function_info->Cast<duckdb::CScalarFunctionInfo>();
	info.extra_info = reinterpret_cast<duckdb_function_info>(extra_info);
	info.delete_callback = destroy;
}

void duckdb_scalar_function_set_bind(duckdb_scalar_function scalar_function, duckdb_scalar_function_bind_t bind) {
	if (!scalar_function || !bind) {
		return;
	}
	auto &sf = GetCScalarFunction(scalar_function);
	auto &info = sf.function_info->Cast<duckdb::CScalarFunctionInfo>();
	info.bind = bind;
}

void duckdb_scalar_function_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.bind_data.bind_data = bind_data;
	bind_info.bind_data.delete_callback = destroy;
}

void duckdb_scalar_function_set_bind_data_copy(duckdb_bind_info info, duckdb_copy_callback_t copy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.bind_data.copy_callback = copy;
}

void duckdb_scalar_function_set_function(duckdb_scalar_function function, duckdb_scalar_function_t execute_func) {
	if (!function || !execute_func) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto &info = scalar_function.function_info->Cast<duckdb::CScalarFunctionInfo>();
	info.function = execute_func;
}

duckdb_state duckdb_register_scalar_function(duckdb_connection connection, duckdb_scalar_function function) {
	if (!connection || !function) {
		return DuckDBError;
	}
	auto &scalar_function = GetCScalarFunction(function);
	duckdb::ScalarFunctionSet set(scalar_function.name);
	set.AddFunction(scalar_function);
	return duckdb_register_scalar_function_set(connection, reinterpret_cast<duckdb_scalar_function_set>(&set));
}

duckdb_scalar_function_set duckdb_create_scalar_function_set(const char *name) {
	if (!name || !*name) {
		return nullptr;
	}
	auto function = new duckdb::ScalarFunctionSet(name);
	return reinterpret_cast<duckdb_scalar_function_set>(function);
}

void duckdb_destroy_scalar_function_set(duckdb_scalar_function_set *set) {
	if (set && *set) {
		auto scalar_function_set = reinterpret_cast<duckdb::ScalarFunctionSet *>(*set);
		delete scalar_function_set;
		*set = nullptr;
	}
}

duckdb_state duckdb_add_scalar_function_to_set(duckdb_scalar_function_set set, duckdb_scalar_function function) {
	if (!set || !function) {
		return DuckDBError;
	}
	auto &scalar_function_set = GetCScalarFunctionSet(set);
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function_set.AddFunction(scalar_function);
	return DuckDBSuccess;
}

duckdb_state duckdb_register_scalar_function_set(duckdb_connection connection, duckdb_scalar_function_set set) {
	if (!connection || !set) {
		return DuckDBError;
	}
	auto &scalar_function_set = GetCScalarFunctionSet(set);
	for (idx_t idx = 0; idx < scalar_function_set.Size(); idx++) {
		auto &scalar_function = scalar_function_set.GetFunctionReferenceByOffset(idx);
		auto &info = scalar_function.function_info->Cast<duckdb::CScalarFunctionInfo>();

		if (scalar_function.name.empty() || !info.function) {
			return DuckDBError;
		}
		if (duckdb::TypeVisitor::Contains(scalar_function.GetReturnType(), duckdb::LogicalTypeId::INVALID) ||
		    duckdb::TypeVisitor::Contains(scalar_function.GetReturnType(), duckdb::LogicalTypeId::ANY)) {
			return DuckDBError;
		}
		for (const auto &argument : scalar_function.arguments) {
			if (duckdb::TypeVisitor::Contains(argument, duckdb::LogicalTypeId::INVALID)) {
				return DuckDBError;
			}
		}
	}

	try {
		auto con = reinterpret_cast<duckdb::Connection *>(connection);
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = duckdb::Catalog::GetSystemCatalog(*con->context);
			duckdb::CreateScalarFunctionInfo sf_info(scalar_function_set);
			sf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateFunction(*con->context, sf_info);
		});
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}
