#include "duckdb/main/extension/linked_extension_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb_static_extension.h"

#include <mutex>

namespace duckdb {

namespace {

struct RegisteredExtension {
	string name;
	duckdb_extension_describe_t describe;
};

struct RegistryState {
	std::mutex lock;
	vector<LinkedExtension> extensions;
	vector<RegisteredExtension> registered;
	vector<string> errors;
};

// function-local so that registrations during static initialization always find it constructed
RegistryState &GetRegistryState() {
	static RegistryState state;
	return state;
}

void SetDescriptorError(duckdb_extension_descriptor *descriptor, const char *message) {
	auto &error = *static_cast<string *>(descriptor->internal);
	error = message ? message : "";
}

//! Returns an empty string on success, otherwise the reason the registration failed.
string Register(duckdb_extension_describe_t describe) {
	StaticExtensionDescription description;
	auto error = LinkedExtensionRegistry::Describe(describe, description);
	if (!error.empty()) {
		return error;
	}
	auto &state = GetRegistryState();
	std::lock_guard<std::mutex> guard(state.lock);
	for (auto &entry : state.registered) {
		if (!StringUtil::CIEquals(entry.name, description.name)) {
			continue;
		}
		if (entry.describe == describe) {
			return string();
		}
		return "extension '" + description.name + "' is registered by two different describe functions";
	}
	state.registered.push_back({description.name, describe});
	state.extensions.push_back({description.name, [describe](DuckDB &db) {
		                            db.LoadStaticExtension(describe);
	                            }});
	return string();
}

} // namespace

string LinkedExtensionRegistry::Describe(duckdb_extension_describe_t describe, StaticExtensionDescription &result) {
	if (!describe) {
		return "no describe function was given";
	}
	string error;
	auto &descriptor = result.descriptor;
	descriptor = duckdb_extension_descriptor();
	descriptor.version = DUCKDB_EXTENSION_DESCRIPTOR_VERSION;
	descriptor.set_error = SetDescriptorError;
	descriptor.internal = &error;

	auto status = describe(&descriptor);
	descriptor.internal = nullptr;
	result.name = descriptor.name ? descriptor.name : "";
	result.version = descriptor.extension_version ? descriptor.extension_version : "";
	const string subject = result.name.empty() ? string("an extension") : "extension '" + result.name + "'";
	if (status != 0) {
		return subject + " refused to register: " + (error.empty() ? string("no reason given") : error);
	}
	if (!error.empty()) {
		return subject + " reported an error but returned success: " + error;
	}
	if (descriptor.version < 1 || descriptor.version > DUCKDB_EXTENSION_DESCRIPTOR_VERSION) {
		return StringUtil::Format("%s filled descriptor layout %d, but layout %d was offered", subject,
		                          descriptor.version, DUCKDB_EXTENSION_DESCRIPTOR_VERSION);
	}
	if (result.name.empty()) {
		return "an extension describe function did not set a name";
	}
	if (!descriptor.entry_cpp && !descriptor.entry_capi_v1 && !descriptor.entry_capi_v2) {
		return subject + " did not set an entry point";
	}
	return string();
}

vector<LinkedExtension> LinkedExtensionRegistry::Get() {
	auto &state = GetRegistryState();
	std::lock_guard<std::mutex> guard(state.lock);
	if (!state.errors.empty()) {
		throw InvalidConfigurationException("Failed to register statically linked extensions: %s",
		                                    StringUtil::Join(state.errors, "; "));
	}
	return state.extensions;
}

} // namespace duckdb

duckdb_state duckdb_register_static_extension(duckdb_extension_describe_t describe) {
	try {
		auto error = duckdb::Register(describe);
		if (error.empty()) {
			return DuckDBSuccess;
		}
		auto &state = duckdb::GetRegistryState();
		std::lock_guard<std::mutex> guard(state.lock);
		state.errors.push_back(std::move(error));
	} catch (...) { // NOLINT: never throw across the C API
	}
	return DuckDBError;
}
