#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/external_resources_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// duckdb_external_resources()
//
// Lists the external resources locally registered in this instance's ExternalResourcesManager
// (managed = true).
//===--------------------------------------------------------------------===//

struct ExternalResourceRow {
	string name;
	string type;
	Value handle;
	string uri;
	string attached_db_type;
	bool managed;
};

struct ExternalResourcesGlobalState : public GlobalTableFunctionState {
	ExternalResourcesGlobalState() : offset(0) {
	}
	vector<ExternalResourceRow> rows;
	idx_t offset;
};

static unique_ptr<FunctionData> ExternalResourcesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("managed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("attached_db_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("handle");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> ExternalResourcesInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<ExternalResourcesGlobalState>();
	// Locally registered resources — always shown (managed = true).
	for (auto &instance : ExternalResourcesManager::Get(context).List()) {
		ExternalResourceRow row;
		row.name = instance.name;
		row.type = instance.type;
		row.handle = instance.handle;
		row.uri = instance.uri;
		row.attached_db_type = instance.attached_db_type;
		row.managed = true;
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

static Value NullableString(const string &s) {
	return s.empty() ? Value(LogicalType::VARCHAR) : Value(s);
}

static void ExternalResourcesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ExternalResourcesGlobalState>();
	auto empty_handle = Value(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &r = state.rows[state.offset++];
		output.data[0].Append(NullableString(r.name));
		output.data[1].Append(Value(r.type));
		output.data[2].Append(Value::BOOLEAN(r.managed));
		output.data[3].Append(NullableString(r.uri));
		output.data[4].Append(NullableString(r.attached_db_type));
		output.data[5].Append(r.handle.IsNull() ? empty_handle : r.handle);
		count++;
	}
}

void DuckDBExternalResourcesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_external_resources", {}, ExternalResourcesFunction, ExternalResourcesBind,
	                              ExternalResourcesInit));
}

//===--------------------------------------------------------------------===//
// register_external_resource(type, name, handle, uri := , attached_db_type := , deleter_function := )
//
// Registry write: record an already-provisioned resource in this instance's ExternalResourcesManager under
// `name`, so it shows up in duckdb_external_resources() and can be torn down later. Pure — it provisions
// nothing; feed it the result of create_external_resource(). The durable identity is (type, handle).
//===--------------------------------------------------------------------===//

struct RegisterExternalResourceBindData : public TableFunctionData {
	ExternalResource resource;
};

struct RegisterExternalResourceState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RegisterExternalResourceBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RegisterExternalResourceBindData>();
	auto &resource = result->resource;
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("register_external_resource: the type and name must not be NULL");
	}
	resource.type = StringValue::Get(input.inputs[0]);
	resource.name = StringValue::Get(input.inputs[1]);
	// The handle is the durable identity and doubles as the deleter payload.
	resource.handle = input.inputs[2];
	resource.deleter_payload = input.inputs[2];
	for (auto &np : input.named_parameters) {
		auto key = StringUtil::Lower(np.first.GetIdentifierName());
		if (np.second.IsNull()) {
			continue;
		}
		if (key == "uri") {
			resource.uri = StringValue::Get(np.second);
		} else if (key == "attached_db_type") {
			resource.attached_db_type = StringValue::Get(np.second);
		} else if (key == "deleter_function") {
			// Qualify against the registering connection's search path: teardown runs the deleter on a separate
			// internal connection. Fall back to the name as given when it does not resolve (yet) - the defining
			// extension may not be loaded at registration time.
			auto deleter = StringValue::Get(np.second);
			auto qualified = QualifyTableCallback(context, deleter);
			resource.deleter_function = qualified.empty() ? deleter : qualified;
		}
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RegisterExternalResourceInit(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	return make_uniq<RegisterExternalResourceState>();
}

static void RegisterExternalResourceFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RegisterExternalResourceState>();
	if (state.done) {
		return;
	}
	auto &bind_data = data_p.bind_data->Cast<RegisterExternalResourceBindData>();
	// Add() validates name/type are non-empty and rejects a duplicate name.
	ExternalResourcesManager::Get(context).Add(bind_data.resource);
	output.data[0].Append(Value::BOOLEAN(true));
	state.done = true;
}

void RegisterExternalResourceFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction fn(
	    "register_external_resource",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	    RegisterExternalResourceFunction, RegisterExternalResourceBind, RegisterExternalResourceInit);
	fn.named_parameters["uri"] = LogicalType::VARCHAR;
	fn.named_parameters["attached_db_type"] = LogicalType::VARCHAR;
	fn.named_parameters["deleter_function"] = LogicalType::VARCHAR;
	set.AddFunction(fn);
}

//===--------------------------------------------------------------------===//
// deregister_external_resource(name)
//
// Registry write: forget a locally registered resource (ExternalResourcesManager::Remove). Does NOT tear
// the resource down — that is destroy_external_resource's job. Throws if `name` is not registered.
//===--------------------------------------------------------------------===//

struct DeregisterExternalResourceBindData : public TableFunctionData {
	string name;
};

struct DeregisterExternalResourceState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> DeregisterExternalResourceBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto result = make_uniq<DeregisterExternalResourceBindData>();
	if (input.inputs[0].IsNull() || StringValue::Get(input.inputs[0]).empty()) {
		throw InvalidInputException("deregister_external_resource: the name must not be NULL or empty");
	}
	result->name = StringValue::Get(input.inputs[0]);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DeregisterExternalResourceInit(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	return make_uniq<DeregisterExternalResourceState>();
}

static void DeregisterExternalResourceFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DeregisterExternalResourceState>();
	if (state.done) {
		return;
	}
	auto &bind_data = data_p.bind_data->Cast<DeregisterExternalResourceBindData>();
	auto removed = ExternalResourcesManager::Get(context).Remove(bind_data.name);
	if (!removed) {
		throw InvalidInputException("deregister_external_resource: \"%s\" is not registered", bind_data.name);
	}
	output.data[0].Append(Value::BOOLEAN(true));
	state.done = true;
}

void DeregisterExternalResourceFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("deregister_external_resource", {LogicalType::VARCHAR},
	                              DeregisterExternalResourceFunction, DeregisterExternalResourceBind,
	                              DeregisterExternalResourceInit));
}

} // namespace duckdb
