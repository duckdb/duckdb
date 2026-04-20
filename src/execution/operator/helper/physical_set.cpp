#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/setting_info.hpp"

namespace duckdb {

void PhysicalSet::SetGenericVariable(ClientContext &context, idx_t setting_index, SetScope scope, Value target_value) {
	if (scope == SetScope::GLOBAL) {
		auto &config = DBConfig::GetConfig(context);
		config.SetOption(setting_index, std::move(target_value));
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		client_config.user_settings.SetUserSetting(setting_index, std::move(target_value));
	}
}

void PhysicalSet::SetExtensionVariable(ClientContext &context, ExtensionOption &extension_option, const String &name,
                                       SetScope scope, const Value &value) {
	// Resolve AUTOMATIC against the option's default_scope first so downstream
	// checks and callbacks work off the effective scope, not the raw one.
	SetScope effective_scope = scope == SetScope::AUTOMATIC ? extension_option.default_scope : scope;

	// Compatibility: a GLOBAL-only option can't be set locally/session, and a
	// SESSION-only option can't be set locally.
	if ((extension_option.default_scope == SetScope::GLOBAL && effective_scope != SetScope::GLOBAL) ||
	    (extension_option.default_scope == SetScope::SESSION && effective_scope == SetScope::LOCAL)) {
		throw InvalidInputException("parameter \"%s\" cannot be set locally", name);
	}
	// Global writes are DB-wide and shouldn't land mid-transaction.
	if (effective_scope == SetScope::GLOBAL && !context.transaction.IsAutoCommit()) {
		throw InvalidInputException("parameter \"%s\" is global and cannot be set inside a transaction", name);
	}

	Value target_value = value.CastAs(context, extension_option.type);
	if (extension_option.set_function) {
		extension_option.set_function(context, effective_scope, target_value);
	}
	SetGenericVariable(context, extension_option.setting_index.GetIndex(), effective_scope, std::move(target_value));
}

void PhysicalSet::SetVariable(ClientContext &context, const String &name, SetScope scope, const Value &value) {
	auto &config = DBConfig::GetConfig(context);
	config.CheckLock(name);
	// SET LOCAL is only meaningful inside an explicit transaction; PG rejects
	// it outside with an error.
	if (scope == SetScope::LOCAL && context.transaction.IsAutoCommit()) {
		throw InvalidInputException("SET LOCAL can only be used in transaction blocks");
	}
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		ExtensionOption extension_option;
		if (!config.TryGetExtensionOption(name, extension_option)) {
			Catalog::AutoloadExtensionByConfigName(context, name);
			if (!config.TryGetExtensionOption(name, extension_option)) {
				throw InvalidInputException("Extension parameter %s was not found after autoloading", name);
			}
		}
		// Invoke the pre-SET observer after resolution so the handler only sees
		// valid option names and can safely snapshot the current value.
		if (context.setting_change_handler) {
			context.setting_change_handler(context, string(name.data(), name.size()), scope);
		}
		SetExtensionVariable(context, extension_option, name, scope, value);
		return;
	}
	if (context.setting_change_handler) {
		context.setting_change_handler(context, string(name.data(), name.size()), scope);
	}
	SetScope variable_scope = GetSettingScope(*option, scope);
	Value input_val = value.CastAs(context, DBConfig::ParseLogicalType(option->parameter_type));
	if (option->default_value) {
		if (option->set_callback) {
			SettingCallbackInfo info(context, variable_scope);
			option->set_callback(info, input_val);
		}
		SetGenericVariable(context, option->setting_idx.GetIndex(), variable_scope, std::move(input_val));
		return;
	}
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context);
		config.SetOption(&db, *option, input_val);
		break;
	}
	case SetScope::LOCAL:
	case SetScope::SESSION:
		// SET LOCAL is tracked for rollback by the setting_change_handler; at
		// this layer it applies via the same set_local path as SESSION.
		if (!option->set_local) {
			throw CatalogException("option \"%s\" cannot be set locally", name);
		}
		option->set_local(context, input_val);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

SetScope PhysicalSet::GetSettingScope(const ConfigurationOption &option, SetScope variable_scope) {
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option.set_local) {
			return SetScope::SESSION;
		}
		if (option.set_global) {
			return SetScope::GLOBAL;
		}
		// generic setting
		switch (option.scope) {
		case SettingScopeTarget::LOCAL_ONLY:
		case SettingScopeTarget::LOCAL_DEFAULT:
			return SetScope::SESSION;
		case SettingScopeTarget::GLOBAL_ONLY:
		case SettingScopeTarget::GLOBAL_DEFAULT:
			return SetScope::GLOBAL;
		default:
			throw InvalidInputException("Setting \"%s\" does not have a valid scope defined", option.name);
		}
	}
	if (variable_scope == SetScope::SESSION && option.scope == SettingScopeTarget::GLOBAL_ONLY) {
		throw InvalidInputException("Setting \"%s\" cannot be set as a session variable - it can only be set globally",
		                            option.name);
	}
	if (variable_scope == SetScope::GLOBAL && option.scope == SettingScopeTarget::LOCAL_ONLY) {
		throw InvalidInputException(
		    "Setting \"%s\" cannot be set as a global variable - it can only be set per session", option.name);
	}
	return variable_scope;
}

SourceResultType PhysicalSet::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	SetVariable(context.client, name, scope, value);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
