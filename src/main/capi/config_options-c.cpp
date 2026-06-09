#include "duckdb/main/capi/capi_internal.hpp"

namespace duckdb {
namespace {

struct CConfigOption {
	string name;
	LogicalType type;
	Value default_value;
	SetScope default_scope = SetScope::SESSION;
	string description;
};

} // namespace
} // namespace duckdb

duckdb_config_option duckdb_create_config_option() {
	auto coption = new duckdb::CConfigOption();
	return reinterpret_cast<duckdb_config_option>(coption);
}

void duckdb_destroy_config_option(duckdb_config_option *option) {
	if (!option || !*option) {
		return;
	}
	auto coption = *reinterpret_cast<duckdb::CConfigOption **>(option);
	delete coption;

	*option = nullptr;
}

void duckdb_config_option_set_name(duckdb_config_option option, const char *name) {
	if (!option || !name) {
		return;
	}
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);
	coption->name = name;
}

void duckdb_config_option_set_type(duckdb_config_option option, duckdb_logical_type type) {
	if (!option || !type) {
		return;
	}
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);
	coption->type = *reinterpret_cast<duckdb::LogicalType *>(type);
}

void duckdb_config_option_set_default_value(duckdb_config_option option, duckdb_value default_value) {
	if (!option || !default_value) {
		return;
	}
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);
	auto cvalue = reinterpret_cast<duckdb::Value *>(default_value);

	if (coption->type.id() == duckdb::LogicalTypeId::INVALID) {
		coption->type = cvalue->type();
		coption->default_value = *cvalue;
		return;
	}

	if (coption->type != cvalue->type()) {
		coption->default_value = cvalue->DefaultCastAs(coption->type, false);
		return;
	}

	coption->default_value = *cvalue;
}

void duckdb_config_option_set_default_scope(duckdb_config_option option, duckdb_config_option_scope scope) {
	if (!option) {
		return;
	}
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);
	switch (scope) {
	case DUCKDB_CONFIG_OPTION_SCOPE_LOCAL:
		coption->default_scope = duckdb::SetScope::LOCAL;
		break;
	// Set the option for the current session/connection only.
	case DUCKDB_CONFIG_OPTION_SCOPE_SESSION:
		coption->default_scope = duckdb::SetScope::SESSION;
		break;
	// Set the option globally for all sessions/connections.
	case DUCKDB_CONFIG_OPTION_SCOPE_GLOBAL:
		coption->default_scope = duckdb::SetScope::GLOBAL;
		break;
	default:
		return;
	}
}

void duckdb_config_option_set_description(duckdb_config_option option, const char *description) {
	if (!option || !description) {
		return;
	}
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);
	coption->description = description;
}

duckdb_state duckdb_register_config_option(duckdb_connection connection, duckdb_config_option option) {
	if (!connection || !option) {
		return DuckDBError;
	}

	auto conn = reinterpret_cast<duckdb::Connection *>(connection);
	auto coption = reinterpret_cast<duckdb::CConfigOption *>(option);

	if (coption->name.empty() || coption->type.id() == duckdb::LogicalTypeId::INVALID) {
		return DuckDBError;
	}

	// TODO: This is not transactional... but theres no easy way to make it so currently.
	try {
		if (conn->context->db->config.HasExtensionOption(coption->name)) {
			// Option already exists
			return DuckDBError;
		}
		conn->context->db->config.AddExtensionOption(coption->name, coption->description, coption->type,
		                                             coption->default_value, nullptr, coption->default_scope);
	} catch (...) {
		return DuckDBError;
	}

	return DuckDBSuccess;
}

duckdb_value duckdb_client_context_get_config_option(duckdb_client_context context, const char *option_name,
                                                     duckdb_config_option_scope *out_scope) {
	if (!context || !option_name) {
		return nullptr;
	}

	auto wrapper = reinterpret_cast<duckdb::CClientContextWrapper *>(context);
	auto &ctx = wrapper->context;

	duckdb_config_option_scope res_scope = DUCKDB_CONFIG_OPTION_SCOPE_INVALID;
	duckdb::Value *res_value = nullptr;

	duckdb::Value result;
	switch (ctx.TryGetCurrentSetting(option_name, result).GetScope()) {
	case duckdb::SettingScope::LOCAL:
		// This is a bit messy, but "session" is presented as LOCAL on the "settings" side of the API.
		res_value = new duckdb::Value(std::move(result));
		res_scope = DUCKDB_CONFIG_OPTION_SCOPE_SESSION;
		break;
	case duckdb::SettingScope::GLOBAL:
		res_value = new duckdb::Value(std::move(result));
		res_scope = DUCKDB_CONFIG_OPTION_SCOPE_GLOBAL;
		break;
	default:
		res_value = nullptr;
		res_scope = DUCKDB_CONFIG_OPTION_SCOPE_INVALID;
		break;
	}

	if (out_scope) {
		*out_scope = res_scope;
	}
	return reinterpret_cast<duckdb_value>(res_value);
}
