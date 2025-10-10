#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

//----------------------------------------------------------------------------------------------------------------------
// Common Copy Function Info
//----------------------------------------------------------------------------------------------------------------------

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

	duckdb_copy_function_bind_t bind_to = nullptr;
	duckdb_copy_function_global_init_t global_init = nullptr;
	duckdb_copy_function_sink_t sink = nullptr;
	duckdb_copy_function_finalize_t finalize = nullptr;

	void *extra_info = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

Value MakeValueFromCopyOptions(const case_insensitive_map_t<vector<Value>> &options) {
	child_list_t<duckdb::Value> option_list;
	for (auto &entry : options) {
		// Uppercase the option name, to make it simpler for users
		auto name = StringUtil::Upper(entry.first);
		auto &values = entry.second;

		if (values.empty()) {
			// Null!
			option_list.emplace_back(std::move(name), Value());
			continue;
		}
		if (values.size() == 1) {
			// Single value
			option_list.emplace_back(std::move(name), values[0]);
			continue;
		}

		auto is_same_type = true;
		auto first_type = values[0].type();
		for (auto &val : values) {
			if (val.type() != first_type) {
				// Different types, cannot unify
				is_same_type = false;
				break;
			}
		}

		// Is same type: create a list of that type
		if (is_same_type) {
			option_list.emplace_back(std::move(name), Value::LIST(first_type, values));
			continue;
		}

		// Different types: create an unnamed struct
		child_list_t<Value> children;
		for (auto &val : values) {
			children.emplace_back("", val);
		}
		option_list.emplace_back(std::move(name), Value::STRUCT(children));
	}

	if (option_list.empty()) {
		// No options
		return Value();
	}

	// Return a struct of all options
	return Value::STRUCT(std::move(option_list));
}

} // namespace
} // namespace duckdb

duckdb_copy_function duckdb_create_copy_function() {
	auto function = new duckdb::CopyFunction("");

	function->function_info = duckdb::make_shared_ptr<duckdb::CCopyFunctionInfo>();

	return reinterpret_cast<duckdb_copy_function>(function);
}

void duckdb_copy_function_set_name(duckdb_copy_function copy_function, const char *name) {
	if (!copy_function || !name) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);
	copy_function_ref.name = name;
}

void duckdb_destroy_copy_function(duckdb_copy_function *copy_function) {
	if (copy_function && *copy_function) {
		auto function = reinterpret_cast<duckdb::CopyFunction *>(*copy_function);
		delete function;
		*copy_function = nullptr;
	}
}

void duckdb_copy_function_set_extra_info(duckdb_copy_function function, void *extra_info,
                                         duckdb_delete_callback_t destroy) {
	if (!function) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();
	info.extra_info = extra_info;
	info.delete_callback = destroy;
}

//----------------------------------------------------------------------------------------------------------------------
// Copy To Bind
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {
struct CCopyToBindInfo : FunctionData {

	shared_ptr<CopyFunctionInfo> function_info;
	void *bind_data = nullptr;
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
	    : context(context), input(input), sql_types(sql_types), names(names), function_info(function_info),
	      success(true) {
	}

	ClientContext &context;
	CopyFunctionBindInput &input;
	const vector<LogicalType> &sql_types;
	const vector<string> &names;
	const CCopyFunctionInfo &function_info;
	bool success;
	string error;

	// Supplied by the user
	void *bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

unique_ptr<FunctionData> CCopyToBind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {
	auto &info = input.function_info->Cast<CCopyFunctionInfo>();

	auto result = make_uniq<CCopyToBindInfo>();
	result->function_info = input.function_info;

	if (info.bind_to) {
		// Call the user-defined bind function
		CCopyFunctionToInternalBindInfo bind_info(context, input, sql_types, names, info);
		info.bind_to(reinterpret_cast<duckdb_copy_function_bind_info>(&bind_info));

		// Pass on user bind data to the result
		result->bind_data = bind_info.bind_data;
		result->delete_callback = bind_info.delete_callback;

		if (!bind_info.success) {
			throw BinderException(bind_info.error);
		}
	}
	return std::move(result);
}

} // namespace
} // namespace duckdb

void duckdb_copy_function_set_bind(duckdb_copy_function copy_function, duckdb_copy_function_bind_t bind) {
	if (!copy_function || !bind) {
		return;
	}

	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C bind callback
	info.bind_to = bind;
}

void duckdb_copy_function_bind_set_error(duckdb_copy_function_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);

	// Set the error message
	info_ref.error = error;
	info_ref.success = false;
}

void *duckdb_copy_function_bind_get_extra_info(duckdb_copy_function_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);
	return info_ref.function_info.extra_info;
}

duckdb_client_context duckdb_copy_function_bind_get_client_context(duckdb_copy_function_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);
	auto wrapper = new duckdb::CClientContextWrapper(info_ref.context);
	return reinterpret_cast<duckdb_client_context>(wrapper);
}

idx_t duckdb_copy_function_bind_get_column_count(duckdb_copy_function_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);
	return info_ref.sql_types.size();
}

duckdb_logical_type duckdb_copy_function_bind_get_column_type(duckdb_copy_function_bind_info info, idx_t col_idx) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);
	if (col_idx >= info_ref.sql_types.size()) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(info_ref.sql_types[col_idx]));
}

duckdb_value duckdb_copy_function_bind_get_options(duckdb_copy_function_bind_info info) {
	if (!info) {
		return nullptr;
	}

	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);
	auto &options = info_ref.input.info.options;

	// return as struct of options
	auto options_value = duckdb::MakeValueFromCopyOptions(options);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(options_value));
	;
}

void duckdb_copy_function_bind_set_bind_data(duckdb_copy_function_bind_info info, void *bind_data,
                                             duckdb_delete_callback_t destructor) {
	if (!info) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyFunctionToInternalBindInfo *>(info);

	// Store the bind data and destructor
	info_ref.bind_data = bind_data;
	info_ref.delete_callback = destructor;
}

//----------------------------------------------------------------------------------------------------------------------
// Copy To Global Initialize
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

struct CCopyToGlobalState : GlobalFunctionData {

	void *global_state = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;

	~CCopyToGlobalState() override {
		if (global_state && delete_callback) {
			delete_callback(global_state);
		}
		global_state = nullptr;
		delete_callback = nullptr;
	}
};

struct CCopyToGlobalInitInfo {
	CCopyToGlobalInitInfo(ClientContext &context, FunctionData &bind_data, const string &file_path)
	    : context(context), bind_data(bind_data), file_path(file_path) {
	}

	ClientContext &context;
	FunctionData &bind_data;
	const string &file_path;

	string error;
	bool success = true;

	void *global_state = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

unique_ptr<GlobalFunctionData> CCopyToGlobalInit(ClientContext &context, FunctionData &bind_data,
                                                 const string &file_path) {
	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	auto result = make_uniq<CCopyToGlobalState>();

	if (function_info.global_init) {

		// Call the user-defined global init function
		CCopyToGlobalInitInfo global_init_info(context, bind_data, file_path);
		function_info.global_init(reinterpret_cast<duckdb_copy_function_global_init_info>(&global_init_info));

		// Pass on user global state to the result
		result->global_state = global_init_info.global_state;
		result->delete_callback = global_init_info.delete_callback;

		if (!global_init_info.success) {
			throw InvalidInputException(global_init_info.error);
		}
	}

	return std::move(result);
}

} // namespace
} // namespace duckdb

void duckdb_copy_function_set_global_init(duckdb_copy_function copy_function, duckdb_copy_function_global_init_t init) {
	if (!copy_function || !init) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C global init callback
	info.global_init = init;
}

void duckdb_copy_function_global_init_set_error(duckdb_copy_function_global_init_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);

	// Set the error message
	info_ref.error = error;
	info_ref.success = false;
}

void *duckdb_copy_function_global_init_get_extra_info(duckdb_copy_function_global_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);
	return info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>()
	    .function_info->Cast<duckdb::CCopyFunctionInfo>()
	    .extra_info;
}

duckdb_client_context duckdb_copy_function_global_init_get_client_context(duckdb_copy_function_global_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);
	auto wrapper = new duckdb::CClientContextWrapper(info_ref.context);
	return reinterpret_cast<duckdb_client_context>(wrapper);
}

void *duckdb_copy_function_global_init_get_bind_data(duckdb_copy_function_global_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);
	auto &bind_info = info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>();

	return bind_info.bind_data;
}

void duckdb_copy_function_global_init_set_global_state(duckdb_copy_function_global_init_info info, void *global_state,
                                                       duckdb_delete_callback_t destructor) {
	if (!info) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);
	info_ref.global_state = global_state;
	info_ref.delete_callback = destructor;
}

const char *duckdb_copy_function_global_init_get_file_path(duckdb_copy_function_global_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToGlobalInitInfo *>(info);
	return info_ref.file_path.c_str();
}

//----------------------------------------------------------------------------------------------------------------------
// Copy To Local Initialize
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

unique_ptr<LocalFunctionData> CCopyToLocalInit(ExecutionContext &context, FunctionData &bind_data) {
	// This isnt exposed to the C-API yet, so we just return empty local function data
	return make_uniq<LocalFunctionData>();
}

} // namespace
} // namespace duckdb
//----------------------------------------------------------------------------------------------------------------------
// Copy To Sink
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

struct CCopyToSinkInfo {

	CCopyToSinkInfo(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate)
	    : context(context), bind_data(bind_data), gstate(gstate) {
	}

	ClientContext &context;
	FunctionData &bind_data;
	GlobalFunctionData &gstate;
	string error;
	bool success = true;
};

void CCopyToSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                 LocalFunctionData &lstate, DataChunk &input) {

	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	// Flatten input (we dont support compressed execution yet!)
	// TODO: Dont flatten!
	input.Flatten();

	CCopyToSinkInfo copy_to_sink_info(context.client, bind_data, gstate);

	// Sink is required!
	function_info.sink(reinterpret_cast<duckdb_copy_function_sink_info>(&copy_to_sink_info),
	                   reinterpret_cast<duckdb_data_chunk>(&input));

	if (!copy_to_sink_info.success) {
		throw InvalidInputException(copy_to_sink_info.error);
	}
}

} // namespace
} // namespace duckdb

void duckdb_copy_function_set_sink(duckdb_copy_function copy_function, duckdb_copy_function_sink_t function) {
	if (!copy_function || !function) {
		return;
	}
	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C sink callback
	info.sink = function;
}

void duckdb_copy_function_sink_set_error(duckdb_copy_function_sink_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToSinkInfo *>(info);
	// Set the error message
	info_ref.error = error;
	info_ref.success = false;
}

void *duckdb_copy_function_sink_get_extra_info(duckdb_copy_function_sink_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToSinkInfo *>(info);
	return info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>()
	    .function_info->Cast<duckdb::CCopyFunctionInfo>()
	    .extra_info;
}

duckdb_client_context duckdb_copy_function_sink_get_client_context(duckdb_copy_function_sink_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToSinkInfo *>(info);
	auto wrapper = new duckdb::CClientContextWrapper(info_ref.context);
	return reinterpret_cast<duckdb_client_context>(wrapper);
}

void *duckdb_copy_function_sink_get_bind_data(duckdb_copy_function_sink_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToSinkInfo *>(info);
	auto &bind_info = info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>();

	return bind_info.bind_data;
}

void *duckdb_copy_function_sink_get_global_state(duckdb_copy_function_sink_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToSinkInfo *>(info);
	auto &gstate = info_ref.gstate.Cast<duckdb::CCopyToGlobalState>();

	return gstate.global_state;
}

//----------------------------------------------------------------------------------------------------------------------
// Copy To Combine
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

void CCopyToCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
	// Do nothing for now (this isnt exposed to the C-API yet)
}

} // namespace
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Copy To Finalize
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

struct CCopyToFinalizeInfo {
	CCopyToFinalizeInfo(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate)
	    : context(context), bind_data(bind_data), gstate(gstate) {
	}

	ClientContext &context;
	FunctionData &bind_data;
	GlobalFunctionData &gstate;

	string error;
	bool success = true;
};

void CCopyToFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &bind_info = bind_data.Cast<CCopyToBindInfo>();
	auto &function_info = bind_info.function_info->Cast<CCopyFunctionInfo>();

	// Finalize is optional
	if (function_info.finalize) {
		CCopyToFinalizeInfo copy_to_finalize_info(context, bind_data, gstate);
		function_info.finalize(reinterpret_cast<duckdb_copy_function_finalize_info>(&copy_to_finalize_info));

		if (!copy_to_finalize_info.success) {
			throw InvalidInputException(copy_to_finalize_info.error);
		}
	}
}

} // namespace
} // namespace duckdb

void duckdb_copy_function_set_finalize(duckdb_copy_function copy_function, duckdb_copy_function_finalize_t finalize) {
	if (!copy_function || !finalize) {
		return;
	}

	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);
	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	// Set C finalize callback
	info.finalize = finalize;
}

void duckdb_copy_function_finalize_set_error(duckdb_copy_function_finalize_info info, const char *error) {
	if (!info || !error) {
		return;
	}

	auto &info_ref = *reinterpret_cast<duckdb::CCopyToFinalizeInfo *>(info);
	// Set the error message
	info_ref.error = error;
	info_ref.success = false;
}

void *duckdb_copy_function_finalize_get_extra_info(duckdb_copy_function_finalize_info info) {
	if (!info) {
		return nullptr;
	}

	auto &info_ref = *reinterpret_cast<duckdb::CCopyToFinalizeInfo *>(info);
	return info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>()
	    .function_info->Cast<duckdb::CCopyFunctionInfo>()
	    .extra_info;
}

duckdb_client_context duckdb_copy_function_finalize_get_client_context(duckdb_copy_function_finalize_info info) {
	if (!info) {
		return nullptr;
	}
	auto &info_ref = *reinterpret_cast<duckdb::CCopyToFinalizeInfo *>(info);
	auto wrapper = new duckdb::CClientContextWrapper(info_ref.context);
	return reinterpret_cast<duckdb_client_context>(wrapper);
}

void *duckdb_copy_function_finalize_get_bind_data(duckdb_copy_function_finalize_info info) {
	if (!info) {
		return nullptr;
	}

	auto &info_ref = *reinterpret_cast<duckdb::CCopyToFinalizeInfo *>(info);
	auto &bind_info = info_ref.bind_data.Cast<duckdb::CCopyToBindInfo>();
	return bind_info.bind_data;
}

void *duckdb_copy_function_finalize_get_global_state(duckdb_copy_function_finalize_info info) {
	if (!info) {
		return nullptr;
	}

	auto &info_ref = *reinterpret_cast<duckdb::CCopyToFinalizeInfo *>(info);
	auto &gstate = info_ref.gstate.Cast<duckdb::CCopyToGlobalState>();
	return gstate.global_state;
}

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------

void duckdb_copy_function_set_copy_from_function(duckdb_copy_function copy_function,
                                                 duckdb_table_function table_function) {
	// TODO: Implement copy from
}

duckdb_state duckdb_register_copy_function(duckdb_connection connection, duckdb_copy_function copy_function) {
	if (!connection || !copy_function) {
		return DuckDBError;
	}

	auto &copy_function_ref = *reinterpret_cast<duckdb::CopyFunction *>(copy_function);

	// Check that the copy function has a valid name
	if (copy_function_ref.name.empty()) {
		return DuckDBError;
	}

	auto &info = copy_function_ref.function_info->Cast<duckdb::CCopyFunctionInfo>();

	auto is_copy_to = false;
	auto is_copy_from = false;

	if (info.sink) {
		// Set the copy function callbacks
		is_copy_to = true;
		copy_function_ref.copy_to_bind = duckdb::CCopyToBind;
		copy_function_ref.copy_to_initialize_global = duckdb::CCopyToGlobalInit;
		copy_function_ref.copy_to_initialize_local = duckdb::CCopyToLocalInit;
		copy_function_ref.copy_to_sink = duckdb::CCopyToSink;
		copy_function_ref.copy_to_combine = duckdb::CCopyToCombine;
		copy_function_ref.copy_to_finalize = duckdb::CCopyToFinalize;
	}

	if (!is_copy_to && !is_copy_from) {
		// At least one of copy to or copy from must be implemented
		return DuckDBError;
	}

	auto &conn = *reinterpret_cast<duckdb::Connection *>(connection);
	try {
		conn.context->RunFunctionInTransaction([&]() {
			auto &catalog = duckdb::Catalog::GetSystemCatalog(*conn.context);
			duckdb::CreateCopyFunctionInfo cp_info(copy_function_ref);
			cp_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateCopyFunction(*conn.context, cp_info);
		});
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}
