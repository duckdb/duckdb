#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"

namespace duckdb {
namespace {

struct CCopyFunctionInfo : public CopyFunctionInfo {
	~CCopyFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	duckdb_copy_function_to_bind_t bind_to = nullptr;
	duckdb_copy_function_to_global_init_t global_init = nullptr;
	duckdb_copy_function_to_sink_t sink = nullptr;
	duckdb_copy_function_to_finalize_t finalize = nullptr;

	void *extra_info = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------

struct CCopyToBindInfo : FunctionData {

	shared_ptr<CopyFunctionInfo> function_info;

	void* bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;

	unique_ptr<FunctionData> Copy() const override {
		throw InternalException("CCopyToBindInfo cannot be copied");
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CCopyToBindInfo>();
		return bind_data == other.bind_data && delete_callback == other.delete_callback;
	}

	~CCopyToBindInfo() override {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}
};

struct CCopyFunctionToInternalBindInfo {
	CCopyFunctionToInternalBindInfo(ClientContext &context, CopyFunctionBindInput &input,
								   const vector<LogicalType> &sql_types, const vector<string> &names,
								   const CCopyFunctionInfo &function_info)
		: context(context), input(input), sql_types(sql_types), names(names), function_info(function_info), success(true) {
	}

	ClientContext &context;
	CopyFunctionBindInput &input;
	const vector<LogicalType> &sql_types;
	const vector<string> &names;
	const CCopyFunctionInfo &function_info;
	bool success;
	string error;

	// Supplied by the user
	void* bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

duckdb_bind_info ToCCopyToBindInfo(CCopyFunctionToInternalBindInfo &info) {
	return reinterpret_cast<duckdb_bind_info>(&info);
}

unique_ptr<FunctionData> CCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
											 const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto &info = input.function_info->Cast<CCopyFunctionInfo>();


	auto result = make_uniq<CCopyToBindInfo>(input.function_info);

	if (info.bind_to) {
		// Call the user-defined bind function
		CCopyFunctionToInternalBindInfo bind_info(context, input, sql_types, names, info);
		info.bind_to(ToCCopyToBindInfo(bind_info));

		// Pass on user bind data to the result
		result->bind_data = bind_info.bind_data;
		result->delete_callback = bind_info.delete_callback;

		if (!bind_info.success) {
			throw BinderException(bind_info.error);
		}
	}
	return std::move(result);
}

struct CCopyToSinkInternalInfo {

};

//----------------------------------------------------------------------------------------------------------------------
// Global Initialize
//----------------------------------------------------------------------------------------------------------------------

struct CCopyToGlobalInitInfo {
	CCopyToGlobalInitInfo(ClientContext &context, FunctionData &bind_data)
		: context(context), bind_data(bind_data) {
	}

	ClientContext &context;
	FunctionData &bind_data;
};

duckdb_init_info ToCCopyToGlobalInitInfo(CCopyToGlobalInitInfo &info) {
	return reinterpret_cast<duckdb_init_info>(&info);
}

unique_ptr<GlobalFunctionData> CGlobalInit(ClientContext &context, FunctionData &bind_data) {
	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	// Global init is optional
	if (function_info.global_init) {
		// Call the user-defined global init function
		CCopyToGlobalInitInfo global_init_info(context, bind_data);
		function_info.global_init(ToCCopyToGlobalInitInfo(global_init_info));
	}

	return nullptr;
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------

duckdb_copy_to_sink_info ToCCopyToSinkInfo(CCopyToSinkInternalInfo &info) {
	return reinterpret_cast<duckdb_copy_to_sink_info>(&info);
}

void CCopyToSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
							   LocalFunctionData &lstate, DataChunk &input) {

	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	// Flatten input (we dont support compressed execution yet!)
	input.Flatten();

	CCopyToSinkInternalInfo copy_to_sink_info;

	// Sink is required!
	function_info.sink(ToCCopyToSinkInfo(copy_to_sink_info), reinterpret_cast<duckdb_data_chunk>(&input));
}


//----------------------------------------------------------------------------------------------------------------------
// Combine
//----------------------------------------------------------------------------------------------------------------------

void CCopyToCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
								  LocalFunctionData &lstate) {
	// Do nothing for now (this isnt exposed to the C-API yet)
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------

struct CCopyToFinalizeInternalInfo {

};

duckdb_copy_to_finalize_info ToCCopyToFinalizeInfo(CCopyToFinalizeInternalInfo &info) {
	return reinterpret_cast<duckdb_copy_to_finalize_info>(&info);
}

void CCopyToFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	// Finalize is optional
	if (function_info.finalize) {

		CCopyToFinalizeInternalInfo copy_to_finalize_info;

		function_info.finalize(ToCCopyToFinalizeInfo(copy_to_finalize_info));
	}
}

} // namespace
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// C-API
//----------------------------------------------------------------------------------------------------------------------
duckdb_copy_function duckdb_create_copy_function() {
	auto function = new duckdb::CopyFunction("");

	function->function_info = duckdb::make_shared_ptr<duckdb::CCopyFunctionInfo>();

	return reinterpret_cast<duckdb_copy_function>(function);
}

void duckdb_copy_function_set_name(duckdb_copy_function copy_function, const char* name) {
	if (!copy_function || !name) {
		return;
	}
	auto& copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);
	copy_function_ref.name = name;
}

void duckdb_destroy_copy_function(duckdb_copy_function* copy_function) {
	if (copy_function && *copy_function) {
		auto function = reinterpret_cast<duckdb::CopyFunction*>(*copy_function);
		delete function;
		*copy_function = nullptr;
	}
}

duckdb_state duckdb_register_copy_function(duckdb_connection connection, duckdb_copy_function copy_function) {
	if (!connection || !copy_function) {
		return DuckDBError;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);

	// Check that the copy function has a valid name
	if (copy_function_ref.name.empty()) {
		return DuckDBError;
	}

	// TODO: check other fields
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	auto is_copy_to = false;
	auto is_copy_from = false;

	if (info.sink) {
		// Set the copy function callbacks
		is_copy_to = true;
		copy_function_ref.copy_to_sink = duckdb::CCopyToSink;
		copy_function_ref.copy_to_initialize_global = duckdb::CGlobalInit;
		copy_function_ref.copy_to_combine = duckdb::CCopyToCombine;
		copy_function_ref.copy_to_finalize = duckdb::CCopyToFinalize;
	}


	if (!is_copy_to && !is_copy_from) {
		// At least one of copy to or copy from must be implemented
		return DuckDBError;
	}


	auto& conn = *reinterpret_cast<duckdb::Connection*>(connection);
	// Register the copy function with the connection
	// conn.context->CopyFunctionSet().AddFunction(...); // Uncomment and implement when CCopyFunction has necessary fields

	return DuckDBSuccess;
}
//----------------------------------------------------------------------------------------------------------------------
// COPY (...) TO (...)
//----------------------------------------------------------------------------------------------------------------------

void duckdb_copy_function_to_add_option(duckdb_copy_function copy_function, const char* name,
									 duckdb_logical_type type, const char* description) {
	if (!copy_function || !name || !type) {
		return;
	}
	auto& copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);

	// Add option to the copy function
	// TODO: This requires CCopyFunction to have a way to store options
}

void duckdb_copy_function_to_set_bind(duckdb_copy_function copy_function,
									   duckdb_copy_function_to_bind_t bind) {
	if (!copy_function || !bind) {
		return;
	}
	// TODO: Implement
	auto& copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);
	auto& info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C bind callback
	info.bind_to = bind;
}

void duckdb_copy_function_to_bind_set_error(duckdb_bind_info info, const char* error) {
	if (!info || !error) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo*>(info);

	// Set the error message
	info_ref.error = error;
	info_ref.success = false;
}

idx_t duckdb_copy_function_to_bind_get_column_count(duckdb_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo*>(info);
	return info_ref.sql_types.size();
}

duckdb_logical_type duckdb_copy_function_to_bind_get_column_type(duckdb_bind_info info, idx_t col_idx) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo*>(info);
	if (col_idx >= info_ref.sql_types.size()) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(info_ref.sql_types[col_idx]));
}

duckdb_value duckdb_copy_function_to_bind_get_option(duckdb_bind_info info, const char* name) {
	if (!info || !name) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo*>(info);
	// Look up the option by name and return its value
	auto &options = info_ref.input.info.options;
	auto it = options.find(name);
	if (it == options.end()) {
		return nullptr;
	}
	auto value_vec = it->second;
	if (value_vec.empty()) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_value>(new duckdb::Value(value_vec[0]));
}

void duckdb_copy_function_to_bind_set_bind_data(duckdb_bind_info info, void* bind_data,
											   duckdb_delete_callback_t destructor) {
	if (!info) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo*>(info);

	// Store the bind data and destructor
	info_ref.bind_data = bind_data;
	info_ref.delete_callback = destructor;
}

void duckdb_copy_function_to_set_global_init(duckdb_copy_function copy_function,
											   duckdb_copy_function_to_global_init_t init) {
	if (!copy_function || !init) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C global init callback
	info.global_init = init;

}

void duckdb_copy_function_to_global_init_set_error(duckdb_init_info info, const char* error) {
	if (!info || !error) {
		return;
	}
}

void* duckdb_copy_function_to_global_init_get_bind_data(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	// TODO: Implement
}

void duckdb_copy_function_to_global_init_set_global_state(duckdb_init_info info, void* global_state,
														   duckdb_delete_callback_t destructor) {
	if (!info) {
		return;
	}
	// TODO: Implement
}

void duckdb_copy_function_to_set_sink(duckdb_copy_function copy_function,
									  duckdb_copy_function_to_sink_t function) {
	if (!copy_function || !function) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C sink callback
	info.sink = function;
}

void duckdb_copy_function_to_sink_set_error(duckdb_copy_to_sink_info info, const char* error) {
	if (!info || !error) {
		return;
	}
}

void* duckdb_copy_function_to_sink_get_bind_data(duckdb_copy_to_sink_info info) {
	if (!info) {
		return nullptr;
	}
	// TODO: Implement
}

void* duckdb_copy_function_to_sink_get_global_state(duckdb_copy_to_sink_info info) {
	if (!info) {
		return nullptr;
	}
	// TODO: Implement
}

void duckdb_copy_function_to_set_finalize(duckdb_copy_function copy_function,
										   duckdb_copy_function_to_finalize_t finalize) {
	if (!copy_function || !finalize) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction*>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C finalize callback
	info.finalize = finalize;
	// TODO: Implement
}

void duckdb_copy_function_to_finalize_set_error(duckdb_copy_to_finalize_info info, const char* error) {
	if (!info || !error) {
		return;
	}
	// TODO: Implement
}

void* duckdb_copy_function_to_finalize_get_bind_data(duckdb_copy_to_finalize_info info) {
	if (!info) {
		return nullptr;
	}
	// TODO: Implement
}

void* duckdb_copy_function_to_finalize_get_global_state(duckdb_copy_to_finalize_info info) {
	if (!info) {
		return nullptr;
	}
	// TODO: Implement
}

//----------------------------------------------------------------------------------------------------------------------
// COPY (...) FROM
//----------------------------------------------------------------------------------------------------------------------

void duckdb_copy_function_from_add_option(duckdb_copy_function copy_function, const char* name,
									   duckdb_logical_type type, const char* description) {
	if (!copy_function || !name || !type) {
		return;
	}
	// TODO: Implement
}

void duckdb_copy_function_from_set_bind(duckdb_copy_function copy_function,
									   duckdb_copy_function_from_bind_t bind) {
	if (!copy_function || !bind) {
		return;
	}
	// TODO: Implement
}

duckdb_value duckdb_copy_function_from_bind_get_option(duckdb_bind_info info, const char* name) {
	if (!info || !name) {
		return nullptr;
	}
	// TODO: Implement
}

void duckdb_copy_function_from_bind_set_error(duckdb_bind_info info, const char* error) {
	if (!info || !error) {
		return;
	}
	// TODO: Implement
}

void duckdb_copy_function_from_bind_set_bind_data(duckdb_bind_info info, void* bind_data,
											   duckdb_delete_callback_t destructor) {
	if (!info) {
		return;
	}
	// TODO: Implement
}

void duckdb_copy_function_from_set_function(duckdb_copy_function copy_function,
											duckdb_table_function table_function) {
	if (!copy_function || !table_function) {
		return;
	}
	// TODO: Implement
}