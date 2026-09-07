#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

namespace duckdb {

namespace capiv2 {

auto GetErrorCodeFromExceptionType(ExceptionType type) -> DUCKDB_V2_ERROR {
	switch (type) {
	// Invalid Input
	case ExceptionType::INVALID_INPUT:
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	case ExceptionType::OUT_OF_RANGE:
		return DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE;
	case ExceptionType::OBJECT_SIZE:
		return DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE;
	// IO
	case ExceptionType::IO:
		return DUCKDB_V2_ERROR_IO_GENERAL;
	case ExceptionType::NETWORK:
		return DUCKDB_V2_ERROR_IO_NETWORK;
	case ExceptionType::HTTP:
		return DUCKDB_V2_ERROR_IO_HTTP;
	// Resource
	case ExceptionType::OUT_OF_MEMORY:
		return DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
	case ExceptionType::CONNECTION:
		return DUCKDB_V2_ERROR_RESOURCE_CONNECTION;
	case ExceptionType::DEPENDENCY:
		return DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY;
	case ExceptionType::MISSING_EXTENSION:
		return DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION;
	case ExceptionType::AUTOLOAD:
		return DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD;
	case ExceptionType::RESOURCE_IN_USE:
		return DUCKDB_V2_ERROR_RESOURCE_IN_USE;
	// Type
	case ExceptionType::CONVERSION:
		return DUCKDB_V2_ERROR_TYPE_CONVERSION;
	case ExceptionType::UNKNOWN_TYPE:
		return DUCKDB_V2_ERROR_TYPE_UNKNOWN;
	case ExceptionType::INVALID_TYPE:
		return DUCKDB_V2_ERROR_TYPE_INVALID;
	case ExceptionType::MISMATCH_TYPE:
		return DUCKDB_V2_ERROR_TYPE_MISMATCH;
	case ExceptionType::DECIMAL:
		return DUCKDB_V2_ERROR_TYPE_DECIMAL;
	case ExceptionType::DIVIDE_BY_ZERO:
		return DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO;
	// Query
	case ExceptionType::PARSER:
		return DUCKDB_V2_ERROR_QUERY_PARSER;
	case ExceptionType::SYNTAX:
		return DUCKDB_V2_ERROR_QUERY_SYNTAX;
	case ExceptionType::BINDER:
		return DUCKDB_V2_ERROR_QUERY_BINDER;
	case ExceptionType::PLANNER:
		return DUCKDB_V2_ERROR_QUERY_PLANNER;
	case ExceptionType::OPTIMIZER:
		return DUCKDB_V2_ERROR_QUERY_OPTIMIZER;
	case ExceptionType::EXPRESSION:
		return DUCKDB_V2_ERROR_QUERY_EXPRESSION;
	case ExceptionType::EXECUTOR:
		return DUCKDB_V2_ERROR_QUERY_EXECUTOR;
	case ExceptionType::SCHEDULER:
		return DUCKDB_V2_ERROR_QUERY_SCHEDULER;
	case ExceptionType::NOT_IMPLEMENTED:
		return DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED;
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED;
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED;
	// Database
	case ExceptionType::CATALOG:
		return DUCKDB_V2_ERROR_DATABASE_CATALOG;
	case ExceptionType::TRANSACTION:
		return DUCKDB_V2_ERROR_DATABASE_TRANSACTION;
	case ExceptionType::CONSTRAINT:
		return DUCKDB_V2_ERROR_DATABASE_CONSTRAINT;
	case ExceptionType::INDEX:
		return DUCKDB_V2_ERROR_DATABASE_INDEX;
	case ExceptionType::SEQUENCE:
		return DUCKDB_V2_ERROR_DATABASE_SEQUENCE;
	case ExceptionType::STAT:
		return DUCKDB_V2_ERROR_DATABASE_STATISTICS;
	case ExceptionType::SERIALIZATION:
		return DUCKDB_V2_ERROR_DATABASE_SERIALIZATION;
	// Configuration
	case ExceptionType::SETTINGS:
		return DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS;
	case ExceptionType::INVALID_CONFIGURATION:
		return DUCKDB_V2_ERROR_CONFIGURATION_INVALID;
	case ExceptionType::PERMISSION:
		return DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION;
	// Runtime
	case ExceptionType::INTERNAL:
		return DUCKDB_V2_ERROR_RUNTIME_INTERNAL;
	case ExceptionType::FATAL:
		return DUCKDB_V2_ERROR_RUNTIME_FATAL;
	case ExceptionType::INTERRUPT:
		return DUCKDB_V2_ERROR_RUNTIME_INTERRUPT;
	case ExceptionType::NULL_POINTER:
		return DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER;
	default:
		return DUCKDB_V2_ERROR_API;
	}
}

// Inverse of GetErrorCodeFromExceptionType: maps a V2 error code back to the
// DuckDB ExceptionType that round-trips to it. Returns false for codes with no
// specific exception type (notably the DUCKDB_V2_ERROR_API sentinel and any
// unmapped value), so callers can fall back to a phase-appropriate default.
auto TryGetExceptionTypeFromErrorCode(DUCKDB_V2_ERROR code) -> optional<ExceptionType> {
	switch (code) {
	case DUCKDB_V2_ERROR_INPUT_INVALID:
		return ExceptionType::INVALID_INPUT;
	case DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE:
		return ExceptionType::OUT_OF_RANGE;
	case DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE:
		return ExceptionType::OBJECT_SIZE;
	// IO
	case DUCKDB_V2_ERROR_IO_GENERAL:
		return ExceptionType::IO;

	case DUCKDB_V2_ERROR_IO_NETWORK:
		return ExceptionType::NETWORK;

	case DUCKDB_V2_ERROR_IO_HTTP:
		return ExceptionType::HTTP;

	// Resource
	case DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY:
		return ExceptionType::OUT_OF_MEMORY;

	case DUCKDB_V2_ERROR_RESOURCE_CONNECTION:
		return ExceptionType::CONNECTION;

	case DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY:
		return ExceptionType::DEPENDENCY;

	case DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION:
		return ExceptionType::MISSING_EXTENSION;

	case DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD:
		return ExceptionType::AUTOLOAD;
	case DUCKDB_V2_ERROR_RESOURCE_IN_USE:
		return ExceptionType::RESOURCE_IN_USE;
	// Type
	case DUCKDB_V2_ERROR_TYPE_CONVERSION:
		return ExceptionType::CONVERSION;

	case DUCKDB_V2_ERROR_TYPE_UNKNOWN:
		return ExceptionType::UNKNOWN_TYPE;

	case DUCKDB_V2_ERROR_TYPE_INVALID:
		return ExceptionType::INVALID_TYPE;

	case DUCKDB_V2_ERROR_TYPE_MISMATCH:
		return ExceptionType::MISMATCH_TYPE;

	case DUCKDB_V2_ERROR_TYPE_DECIMAL:
		return ExceptionType::DECIMAL;

	case DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO:
		return ExceptionType::DIVIDE_BY_ZERO;

	// Query
	case DUCKDB_V2_ERROR_QUERY_PARSER:
		return ExceptionType::PARSER;

	case DUCKDB_V2_ERROR_QUERY_SYNTAX:
		return ExceptionType::SYNTAX;

	case DUCKDB_V2_ERROR_QUERY_BINDER:
		return ExceptionType::BINDER;

	case DUCKDB_V2_ERROR_QUERY_PLANNER:
		return ExceptionType::PLANNER;

	case DUCKDB_V2_ERROR_QUERY_OPTIMIZER:
		return ExceptionType::OPTIMIZER;

	case DUCKDB_V2_ERROR_QUERY_EXPRESSION:
		return ExceptionType::EXPRESSION;

	case DUCKDB_V2_ERROR_QUERY_EXECUTOR:
		return ExceptionType::EXECUTOR;

	case DUCKDB_V2_ERROR_QUERY_SCHEDULER:
		return ExceptionType::SCHEDULER;

	case DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED:
		return ExceptionType::NOT_IMPLEMENTED;

	case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED:
		return ExceptionType::PARAMETER_NOT_RESOLVED;

	case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED:
		return ExceptionType::PARAMETER_NOT_ALLOWED;

	// Database
	case DUCKDB_V2_ERROR_DATABASE_CATALOG:
		return ExceptionType::CATALOG;

	case DUCKDB_V2_ERROR_DATABASE_TRANSACTION:
		return ExceptionType::TRANSACTION;

	case DUCKDB_V2_ERROR_DATABASE_CONSTRAINT:
		return ExceptionType::CONSTRAINT;

	case DUCKDB_V2_ERROR_DATABASE_INDEX:
		return ExceptionType::INDEX;

	case DUCKDB_V2_ERROR_DATABASE_SEQUENCE:
		return ExceptionType::SEQUENCE;

	case DUCKDB_V2_ERROR_DATABASE_STATISTICS:
		return ExceptionType::STAT;

	case DUCKDB_V2_ERROR_DATABASE_SERIALIZATION:
		return ExceptionType::SERIALIZATION;

	// Configuration
	case DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS:
		return ExceptionType::SETTINGS;

	case DUCKDB_V2_ERROR_CONFIGURATION_INVALID:
		return ExceptionType::INVALID_CONFIGURATION;

	case DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION:
		return ExceptionType::PERMISSION;

	// Runtime
	case DUCKDB_V2_ERROR_RUNTIME_INTERNAL:
		return ExceptionType::INTERNAL;

	case DUCKDB_V2_ERROR_RUNTIME_FATAL:
		return ExceptionType::FATAL;

	case DUCKDB_V2_ERROR_RUNTIME_INTERRUPT:
		return ExceptionType::INTERRUPT;

	case DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER:
		return ExceptionType::NULL_POINTER;
	default:
		return {};
	}
}

auto RenderCaughtError(DUCKDB_V2_ERROR &code, string &text, string &raw_message) noexcept -> void {
	// Set the fallback code first (non-throwing), then render the detail.
	code = DUCKDB_V2_ERROR_API;
	try {
		// The bare throw re-raises the exception currently being handled so the catch clauses can dispatch on its type.
		try {
			throw;
		} catch (const duckdb::Exception &ex) {
			ErrorData error_data(ex);
			code = GetErrorCodeFromExceptionType(error_data.Type());
			text = error_data.Message();
			raw_message = error_data.RawMessage();
		} catch (const std::bad_alloc &) {
			code = DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
			text = "Out of memory.";
		} catch (const std::exception &ex) {
			text = ex.what() ? ex.what() : "An unknown error occurred.";
		} catch (...) {
			text = "An unknown error occurred.";
		}
	} catch (const std::bad_alloc &) {
		// Rendering the detail exhausted memory: that supersedes the original report.
		code = DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
		text.clear();
		raw_message.clear();
	} catch (...) {
		// Rendering the detail failed: keep the code produced so far with no detail.
		text.clear();
		raw_message.clear();
	}
}

auto NullArgumentError(duckdb_v2_error_info_handle *err, const char *function, const char *argument) noexcept
    -> DUCKDB_V2_ERROR {
	const auto code = DUCKDB_V2_ERROR_INPUT_INVALID;
	if (!err) {
		return code;
	}
	// This runs outside WithErrorHandler, so nothing may escape across the C ABI. Only allocating the slot itself
	// can fail unrecoverably; the return code is authoritative either way.
	if (!*err) {
		try {
			*err = Convert(new CV2ErrorInfo());
		} catch (const std::bad_alloc &) {
			return DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
		} catch (...) { // NOLINT(bugprone-empty-catch)
			return code;
		}
	}
	// Stamp the code and drop any stale detail with non-throwing operations, so the slot is consistent even if
	// rendering the message below fails.
	auto &out = *Convert(*err);
	out.code = code;
	out.message.clear();
	out.raw_message.clear();
	try {
		// Render through ErrorData so message/raw_message match what WithErrorHandler
		// produces for a thrown InvalidInputException.
		ErrorData error_data(ExceptionType::INVALID_INPUT,
		                     StringUtil::Format("The '%s' argument to '%s' cannot be null", argument, function));
		auto message = error_data.Message();
		auto raw_message = error_data.RawMessage();
		out.message = std::move(message);
		out.raw_message = std::move(raw_message);
	} catch (const std::bad_alloc &) {
		// Rendering the detail exhausted memory: that supersedes the null-argument report.
		out.code = DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
		return out.code;
	} catch (...) { // NOLINT(bugprone-empty-catch)
	}
	return code;
}

//----------------------------------------------------------------------------------------------------------------------
// Option Construction
//----------------------------------------------------------------------------------------------------------------------

// Map DuckDB's SettingScopeTarget to the V2 enum.
// Legacy options (declared via DUCKDB_GLOBAL / DUCKDB_LOCAL / DUCKDB_GLOBAL_LOCAL) carry SettingScopeTarget::INVALID;
// we surface that as UNKNOWN so V2 callers can distinguish "unconstrained legacy" from a declared scope.
static DUCKDB_V2_OPTION_TARGET_SCOPE MapScopeTarget(SettingScopeTarget s) {
	switch (s) {
	case SettingScopeTarget::GLOBAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY;
	case SettingScopeTarget::LOCAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY;
	case SettingScopeTarget::GLOBAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT;
	case SettingScopeTarget::LOCAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT;
	default:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	}
}

// Scan setting_aliases[] for entries pointing at the same canonical
// option (matched by name) and append their alias names.
static void PopulateOptionAliases(const unique_ptr<CV2Option> &out, const Identifier &canonical_name) {
	auto alias_count = DBConfig::GetAliasCount();
	for (idx_t i = 0; i < alias_count; i++) {
		auto alias = DBConfig::GetAliasByIndex(i);
		if (!alias) {
			continue;
		}
		auto aliased = DBConfig::GetOptionByIndex(alias->option_index);
		if (aliased && canonical_name == aliased->name) {
			out->aliases.emplace_back(alias->alias);
		}
	}
}

// Read the effective setting for `name` through `client`'s setting cascade.
// For a databases internal connection (no LOCAL overrides) this returns GLOBAL -> static default;
// For a client connection it returns LOCAL -> GLOBAL -> static default.
// Falls back to `fallback_default` if the cascade returned NULL.
static std::string ReadEffectiveSetting(ClientContext &client, const Identifier &name,
                                        const std::string &fallback_default) {
	if (Value result; client.TryGetCurrentSetting(name, result) && !result.IsNull()) {
		return result.ToString();
	}
	return fallback_default;
}

static unique_ptr<CV2Option> PopulateOptionFromCore(const ConfigurationOption &option, ClientContext &client) {
	auto out = make_uniq<CV2Option>();

	out->name = option.name ? option.name : "";
	out->description = option.description ? option.description : "";
	out->target_scope = MapScopeTarget(option.scope);
	out->default_setting = option.default_value ? option.default_value : "";
	out->aliases.clear();
	PopulateOptionAliases(out, out->name);
	out->setting = ReadEffectiveSetting(client, out->name, out->default_setting);

	return out;
}

// Populate `out` from an extension option. Extension options carry no
// SettingScopeTarget (the V2 enum reports UNKNOWN) and no aliases.
static unique_ptr<CV2Option> PopulateOptionFromExtension(const Identifier &name, const ExtensionOption &ext_option,
                                                         ClientContext &client) {
	auto out = make_uniq<CV2Option>();

	out->name = name;
	out->description = ext_option.description;
	out->target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	out->default_setting = ext_option.default_value.IsNull() ? std::string() : ext_option.default_value.ToString();
	out->aliases.clear();
	out->setting = ReadEffectiveSetting(client, name, out->default_setting);

	return out;
}

unique_ptr<CV2Option> CV2Option::FromIndex(ClientContext &context, DBConfig &config, idx_t index) {
	const auto core_count = DBConfig::GetOptionCount();
	if (index < core_count) {
		auto option = DBConfig::GetOptionByIndex(index);
		if (!option) {
			throw InvalidInputException("core option not found at given index");
		}
		return PopulateOptionFromCore(*option, context);
	}
	const idx_t ext_rel = index - core_count;
	auto ext_settings = config.GetExtensionSettings();
	if (ext_rel >= ext_settings.size()) {
		throw InvalidInputException("option index out of range");
	}
	idx_t i = 0;

	for (const auto &[name, option] : ext_settings) {
		if (i == ext_rel) {
			return PopulateOptionFromExtension(name, option, context);
		}
		++i;
	}
	throw InvalidInputException("option index out of range");
}

unique_ptr<CV2Option> CV2Option::FromName(ClientContext &context, DBConfig &config, std::string_view name) {
	Identifier name_id(name);
	if (auto option = DBConfig::GetOptionByName(name_id)) {
		return PopulateOptionFromCore(*option, context);
	}
	if (ExtensionOption ext_option; config.TryGetExtensionOption(name_id, ext_option)) {
		return PopulateOptionFromExtension(name_id, ext_option, context);
	}
	throw InvalidInputException("unknown configuration option: %s", name_id.GetIdentifierName());
}

} // namespace capiv2

} // namespace duckdb
