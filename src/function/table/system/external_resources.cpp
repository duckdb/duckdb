#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/external_resource_type_registry.hpp"
#include "duckdb/main/external_resources_manager.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// duckdb_external_resources(discover := false)
//
// Lists the external resources this instance knows about. `discover := false` (the default) returns just
// the locally registered resources (managed = true) and stays local. `discover := true` also calls each
// resource type's `list`
// callback to discover resources that exist externally, unioning in the ones not locally registered
// (managed = false) — the reconciliation view that feeds REGISTER.
//===--------------------------------------------------------------------===//

struct ExternalResourceRow {
	string name; // local registration name; empty for a discovered (unmanaged) resource
	string type;
	Value handle;
	string uri;
	string attached_db_type;
	bool managed;
	string reference; // from the type's `list` callback
	string state;     // from the type's `list` callback
};

struct ExternalResourcesBindData : public TableFunctionData {
	bool all = false;
};

struct ExternalResourcesGlobalState : public GlobalTableFunctionState {
	ExternalResourcesGlobalState() : offset(0) {
	}
	vector<ExternalResourceRow> rows;
	idx_t offset;
};

static unique_ptr<FunctionData> ExternalResourcesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ExternalResourcesBindData>();
	for (auto &np : input.named_parameters) {
		if (StringUtil::Lower(np.first.GetIdentifierName()) == "discover" && !np.second.IsNull()) {
			result->all = BooleanValue::Get(np.second);
		}
	}

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("managed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("reference");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("attached_db_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("handle");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	return std::move(result);
}

//! A stable key for a handle, so a discovered resource can be matched against a locally registered one.
static string HandleKey(const Value &handle) {
	return handle.IsNull() ? string() : handle.ToString();
}

//! Call a type's `list` callback and append the resources it discovers that are not already locally
//! managed (keyed by handle). Best-effort: a type whose list fails is skipped, not fatal.
static void DiscoverExternalResources(ClientContext &context, const ExternalResourceType &type,
                                      const set<string> &managed_handles, vector<ExternalResourceRow> &rows) {
	// Separate internal connection (the current connection's context lock is held here), resolving the
	// callback in the type's registration-time search path.
	Connection con(DatabaseInstance::GetDatabase(context));
	if (!type.search_path.empty()) {
		auto set_res = con.Query("SET search_path = " + Value(type.search_path).ToSQLString());
		if (set_res->HasError()) {
			return;
		}
	}
	auto sql =
	    "SELECT * FROM " + QualifiedName::Parse(type.list_function).ToString() + "(MAP {}::MAP(VARCHAR, VARCHAR))";
	auto res = con.Query(sql);
	if (res->HasError()) {
		return;
	}
	// Resolve the columns we consume by name; only `handle` is required.
	idx_t handle_idx = DConstants::INVALID_INDEX, reference_idx = DConstants::INVALID_INDEX,
	      state_idx = DConstants::INVALID_INDEX;
	for (idx_t c = 0; c < res->names.size(); c++) {
		auto col = StringUtil::Lower(res->names[c]);
		if (col == "handle") {
			handle_idx = c;
		} else if (col == "reference") {
			reference_idx = c;
		} else if (col == "state") {
			state_idx = c;
		}
	}
	if (handle_idx == DConstants::INVALID_INDEX) {
		return;
	}
	for (idx_t r = 0; r < res->RowCount(); r++) {
		auto handle = res->GetValue(handle_idx, r);
		if (managed_handles.count(HandleKey(handle)) > 0) {
			continue; // already shown as a locally managed resource
		}
		ExternalResourceRow row;
		row.type = type.name;
		row.handle = std::move(handle);
		row.managed = false;
		if (reference_idx != DConstants::INVALID_INDEX) {
			auto ref = res->GetValue(reference_idx, r);
			row.reference = ref.IsNull() ? string() : ref.ToString();
		}
		if (state_idx != DConstants::INVALID_INDEX) {
			auto st = res->GetValue(state_idx, r);
			row.state = st.IsNull() ? string() : st.ToString();
		}
		rows.push_back(std::move(row));
	}
}

static unique_ptr<GlobalTableFunctionState> ExternalResourcesInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ExternalResourcesBindData>();
	auto result = make_uniq<ExternalResourcesGlobalState>();

	// Locally registered resources — always shown (managed = true).
	set<string> managed_handles;
	for (auto &instance : ExternalResourcesManager::Get(context).List()) {
		ExternalResourceRow row;
		row.name = instance.name;
		row.type = instance.type;
		row.handle = instance.handle;
		row.uri = instance.uri;
		row.attached_db_type = instance.attached_db_type;
		row.managed = true;
		managed_handles.insert(HandleKey(instance.handle));
		result->rows.push_back(std::move(row));
	}

	// Discovery union: for each type with a `list` callback, add the externally-existing resources that
	// are not locally managed.
	if (bind_data.all) {
		for (auto &type : ExternalResourceTypeRegistry::Get(context).List()) {
			if (type.list_function.empty()) {
				continue;
			}
			DiscoverExternalResources(context, type, managed_handles, result->rows);
		}
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
		output.data[3].Append(NullableString(r.state));
		output.data[4].Append(NullableString(r.reference));
		output.data[5].Append(NullableString(r.uri));
		output.data[6].Append(NullableString(r.attached_db_type));
		output.data[7].Append(r.handle.IsNull() ? empty_handle : r.handle);
		count++;
	}
}

void DuckDBExternalResourcesFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction fn("duckdb_external_resources", {}, ExternalResourcesFunction, ExternalResourcesBind,
	                 ExternalResourcesInit);
	fn.named_parameters["discover"] = LogicalType::BOOLEAN;
	set.AddFunction(fn);
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
