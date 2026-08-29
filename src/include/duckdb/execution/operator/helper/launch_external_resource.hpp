//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/launch_external_resource.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class ClientContext;
struct AttachInfo;

//! Result of provisioning an external resource (from `ATTACH/CONNECT TO EXTERNAL RESOURCE ...`): everything needed
//! to attach the endpoint and later tear it down.
struct LaunchedResource {
	string uri;
	string attached_db_type;
	Value result; // MAP of connect options (uri/attached_db_type + e.g. token)
	string deleter_function;
	Value deleter_payload;
};

//! Provision an external resource of type `provider` with the given create params via
//! `create_external_resource`, returning the endpoint, connect options and deleter binding. Runs on a
//! separate internal connection (the caller holds its context lock).
LaunchedResource ProvisionExternalResource(ClientContext &client, const string &provider,
                                           const unordered_map<string, Value> &params,
                                           const string &resource_name = string(), const Value &adopt_handle = Value());

//! Rewrite an AttachInfo to point at a provisioned resource: set path=uri, options["type"], and flow the
//! remaining result options (e.g. token) as attach options.
void ApplyLaunchedResource(const LaunchedResource &launched, AttachInfo &info);

} // namespace duckdb
