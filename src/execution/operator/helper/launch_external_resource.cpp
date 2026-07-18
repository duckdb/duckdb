#include "duckdb/execution/operator/helper/launch_external_resource.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

LaunchedResource ProvisionExternalResource(ClientContext &client, const string &provider,
                                           const unordered_map<string, Value> &params, const string &resource_name,
                                           const Value &adopt_handle) {
	if (provider.empty()) {
		throw BinderException("EXTERNAL RESOURCE: a resource type is required");
	}
	vector<Value> param_keys, param_values;
	for (auto &entry : params) {
		if (entry.second.IsNull()) {
			continue;
		}
		param_keys.emplace_back(entry.first);
		param_values.emplace_back(entry.second.ToString());
	}
	auto params_map =
	    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(param_keys), std::move(param_values));

	// Provision on a separate internal connection (the current connection's context lock is held).
	Connection con(DatabaseInstance::GetDatabase(client));
	// resource_name is forwarded only for observability (it labels the recipe-call log entries).
	auto name_arg = resource_name.empty() ? string() : ", resource_name := " + Value(resource_name).ToSQLString();
	// A handle triggers the adopt path (skip create, resolve the given handle).
	auto handle_arg = adopt_handle.IsNull() ? string() : ", handle := " + adopt_handle.ToSQLString();
	auto sql = "SELECT uri, attached_db_type, result, deleter_function, deleter_payload "
	           "FROM create_external_resource(" +
	           Value(provider).ToSQLString() + ", params := " + params_map.ToSQLString() + name_arg + handle_arg + ")";
	auto res = con.Query(sql);
	if (res->HasError()) {
		throw IOException("EXTERNAL RESOURCE: provisioning '%s' failed: %s", provider, res->GetError());
	}
	if (res->RowCount() == 0) {
		throw IOException("EXTERNAL RESOURCE: create_external_resource returned no rows for '%s'", provider);
	}
	LaunchedResource out;
	// Read the deleter binding first: the resource now exists but nothing owns it yet, so if it turns out
	// to be unusable below, this is the only chance to tear it down rather than strand it.
	auto del_fn = res->GetValue(3, 0);
	out.deleter_function = del_fn.IsNull() ? string() : StringValue::Get(del_fn);
	out.deleter_payload = res->GetValue(4, 0);

	auto uri_val = res->GetValue(0, 0);
	if (uri_val.IsNull()) {
		// A resource without an endpoint is legal (it just cannot be attached), but one provisioned
		// solely in order to attach to it is useless now, so reap it rather than strand it. Only ever
		// reap what we created: an adopted handle names a resource the caller merely borrows, and
		// destroying it would tear down something someone else owns. Best-effort, so a teardown failure
		// is logged rather than masking the error below.
		if (adopt_handle.IsNull()) {
			ResourceDeleter(DatabaseInstance::GetDatabase(client), out.deleter_function, out.deleter_payload, provider,
			                resource_name)
			    .TryDelete();
		}
		throw IOException("EXTERNAL RESOURCE: provisioning '%s' returned a NULL uri", provider);
	}
	out.uri = uri_val.ToString();
	auto type_val = res->GetValue(1, 0);
	out.attached_db_type = type_val.IsNull() ? string() : type_val.ToString();
	out.result = res->GetValue(2, 0);
	return out;
}

void ApplyLaunchedResource(const LaunchedResource &launched, AttachInfo &info) {
	info.path = launched.uri;
	// A NULL/empty db type falls back to the extension prefix extracted from the uri.
	if (!launched.attached_db_type.empty()) {
		info.options["type"] = Value(launched.attached_db_type);
	}
	// The remaining connect options (e.g. token) flow through as attach options.
	if (!launched.result.IsNull()) {
		for (auto &entry : MapValue::GetChildren(launched.result)) {
			auto &kv = StructValue::GetChildren(entry);
			auto key = StringValue::Get(kv[0]);
			if (key == "uri" || key == "attached_db_type") {
				continue;
			}
			info.options[key] = kv[1];
		}
	}
}

} // namespace duckdb
