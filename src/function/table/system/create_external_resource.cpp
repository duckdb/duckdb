#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/external_resource_type_registry.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

//! Look up a key in a MAP(VARCHAR, VARCHAR) Value. Returns false if absent (or the map is NULL).
static bool MapTryGet(const Value &map_value, const string &key, string &out) {
	if (map_value.IsNull()) {
		return false;
	}
	for (auto &entry : MapValue::GetChildren(map_value)) {
		auto &kv = StructValue::GetChildren(entry);
		if (!kv[0].IsNull() && StringValue::Get(kv[0]) == key) {
			out = kv[1].IsNull() ? string() : StringValue::Get(kv[1]);
			return true;
		}
	}
	return false;
}

//! The recipe's handle/result columns are part of the connect contract: they must be MAPs. Validate and
//! normalize to MAP(VARCHAR, VARCHAR), so a recipe that returns something else (e.g. a scalar) gets a clear
//! error instead of crashing when the value is later read or emitted.
static Value RequireResourceMap(const Value &value, const string &function_name, const string &column) {
	if (value.IsNull()) {
		return value;
	}
	if (value.type().id() != LogicalTypeId::MAP) {
		throw InvalidInputException(
		    "create_external_resource: function \"%s\" must return a MAP in its '%s' column, got %s", function_name,
		    column, value.type().ToString());
	}
	return value.DefaultCastAs(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
}

//===--------------------------------------------------------------------===//
// create_external_resource(type_name [, params := MAP])
//
// Resolves the resource type from the registry, invokes its `create` callback, blocks polling `status`
// until the resource is 'ready', enforces the catalog baseline (uri + attached_db_type), and returns the
// endpoint + deleter binding. Because the current connection's context lock is held during execution,
// the callbacks are run on a SEPARATE internal Connection (its own lock + transaction). Callbacks must
// therefore be non-TEMP (visible instance-wide).
//===--------------------------------------------------------------------===//

struct CreateExternalResourceBindData : public TableFunctionData {
	string type_name;
	Value params;
};

struct CreateExternalResourceState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> CreateExternalResourceBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CreateExternalResourceBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("create_external_resource: the type name must not be NULL");
	}
	result->type_name = StringValue::Get(input.inputs[0]);
	result->params = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, vector<Value>(), vector<Value>());
	for (auto &np : input.named_parameters) {
		auto key = StringUtil::Lower(np.first.GetIdentifierName());
		if (key == "params" && !np.second.IsNull()) {
			result->params = np.second;
		}
	}

	auto map_vv = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names.emplace_back("handle");
	return_types.emplace_back(map_vv);
	// Ready-state fields (from the status result), surfaced for direct use by ATTACH.
	names.emplace_back("uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("attached_db_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("result");
	return_types.emplace_back(map_vv);
	// The deleter binding: a (function name, payload) pair. Tearing the resource down is
	// `CALL <deleter_function>(<deleter_payload>)`. deleter_payload is the create handle.
	names.emplace_back("deleter_function");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("deleter_payload");
	return_types.emplace_back(map_vv);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CreateExternalResourceInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<CreateExternalResourceState>();
}

static void CreateExternalResourceFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<CreateExternalResourceState>();
	if (state.done) {
		return;
	}
	auto &bind_data = data_p.bind_data->Cast<CreateExternalResourceBindData>();

	auto type = ExternalResourceTypeRegistry::Get(context).Lookup(bind_data.type_name);
	if (!type) {
		throw InvalidInputException("create_external_resource: unknown resource type \"%s\"", bind_data.type_name);
	}
	if (type->create_function.empty()) {
		throw InvalidInputException("create_external_resource: resource type \"%s\" has no create function",
		                            bind_data.type_name);
	}

	// Separate internal connection: the current connection's context lock is held here.
	Connection con(DatabaseInstance::GetDatabase(context));
	auto sql = "SELECT * FROM " + type->create_function + "(" + bind_data.params.ToSQLString() + ")";
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw IOException("create_external_resource: create function \"%s\" failed: %s", type->create_function,
		                  result->GetError());
	}
	if (result->RowCount() == 0) {
		throw InvalidInputException("create_external_resource: create function \"%s\" returned no rows",
		                            type->create_function);
	}
	// Contract: `create` returns a single 'handle' column (a MAP; opaque to us, passed back to status/destroy).
	auto handle = RequireResourceMap(result->GetValue(0, 0), type->create_function, "handle");

	if (type->status_function.empty()) {
		throw InvalidInputException(
		    "create_external_resource: resource type \"%s\" has no status function (required to await readiness)",
		    bind_data.type_name);
	}

	// Blocking: poll status(handle) until `state` is terminal ('ready' / 'failed'). A synchronous type's
	// status simply returns 'ready' on the first poll.
	Value status_result;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(300);
	while (true) {
		auto status_sql = "SELECT state, result FROM " + type->status_function + "(" + handle.ToSQLString() + ")";
		auto sres = con.Query(status_sql);
		if (sres->HasError()) {
			throw IOException("create_external_resource: status function \"%s\" failed: %s", type->status_function,
			                  sres->GetError());
		}
		if (sres->RowCount() == 0) {
			throw InvalidInputException("create_external_resource: status function \"%s\" returned no rows",
			                            type->status_function);
		}
		auto state_val = sres->GetValue(0, 0);
		auto status_state = state_val.IsNull() ? string() : state_val.ToString();
		if (status_state == "ready") {
			status_result = RequireResourceMap(sres->GetValue(1, 0), type->status_function, "result");
			break;
		}
		if (status_state == "failed") {
			throw IOException("create_external_resource: resource \"%s\" reported state 'failed'", bind_data.type_name);
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			throw IOException("create_external_resource: timed out awaiting readiness for \"%s\" (last state '%s')",
			                  bind_data.type_name, status_state);
		}
		std::this_thread::sleep_for(std::chrono::seconds(5));
	}

	// Baseline enforcement: an catalog resource must expose 'uri' + 'attached_db_type' in its result.
	string uri, attached_db_type;
	bool has_uri = MapTryGet(status_result, "uri", uri);
	bool has_type = MapTryGet(status_result, "attached_db_type", attached_db_type);
	if (type->kind == "catalog") {
		if (!has_uri || uri.empty()) {
			throw InvalidInputException(
			    "create_external_resource: catalog resource \"%s\" is missing 'uri' in its status result",
			    bind_data.type_name);
		}
		if (!has_type || attached_db_type.empty()) {
			throw InvalidInputException("create_external_resource: catalog resource \"%s\" is missing "
			                            "'attached_db_type' in its status result",
			                            bind_data.type_name);
		}
	}

	// Emit: handle, uri, attached_db_type, result, deleter_function, deleter_payload.
	output.data[0].Append(handle);
	output.data[1].Append(has_uri ? Value(uri) : Value(LogicalType::VARCHAR));
	output.data[2].Append(has_type ? Value(attached_db_type) : Value(LogicalType::VARCHAR));
	output.data[3].Append(status_result);
	output.data[4].Append(type->destroy_function.empty() ? Value(LogicalType::VARCHAR) : Value(type->destroy_function));
	output.data[5].Append(handle);
	state.done = true;
}

void CreateExternalResourceFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction fn("create_external_resource", {LogicalType::VARCHAR}, CreateExternalResourceFunction,
	                 CreateExternalResourceBind, CreateExternalResourceInit);
	fn.named_parameters["params"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	set.AddFunction(fn);
}

//===--------------------------------------------------------------------===//
// destroy_external_resource(deleter_function, deleter_payload)
//
// Invokes a deleter binding produced by create_external_resource: CALL <deleter_function>(<payload>).
// Runs on a separate internal Connection (same reasoning as create). Generic: any (string, Value).
//===--------------------------------------------------------------------===//

struct DestroyExternalResourceBindData : public TableFunctionData {
	string deleter_function;
	Value deleter_payload;
};

struct DestroyExternalResourceState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> DestroyExternalResourceBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DestroyExternalResourceBindData>();
	if (input.inputs[0].IsNull() || StringValue::Get(input.inputs[0]).empty()) {
		throw InvalidInputException("destroy_external_resource: the deleter function must not be NULL or empty");
	}
	result->deleter_function = StringValue::Get(input.inputs[0]);
	result->deleter_payload = input.inputs[1];

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DestroyExternalResourceInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	return make_uniq<DestroyExternalResourceState>();
}

static void DestroyExternalResourceFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DestroyExternalResourceState>();
	if (state.done) {
		return;
	}
	auto &bind_data = data_p.bind_data->Cast<DestroyExternalResourceBindData>();

	// CALL <deleter_function>(<deleter_payload>) on a separate internal connection.
	Connection con(DatabaseInstance::GetDatabase(context));
	auto sql = "SELECT * FROM " + bind_data.deleter_function + "(" + bind_data.deleter_payload.ToSQLString() + ")";
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw IOException("destroy_external_resource: deleter function \"%s\" failed: %s", bind_data.deleter_function,
		                  result->GetError());
	}
	output.data[0].Append(Value::BOOLEAN(true));
	state.done = true;
}

void DestroyExternalResourceFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("destroy_external_resource", {LogicalType::VARCHAR, LogicalType::ANY},
	                              DestroyExternalResourceFunction, DestroyExternalResourceBind,
	                              DestroyExternalResourceInit));
}

} // namespace duckdb
