#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

namespace duckdb {

namespace capiv2 {

auto RenderCaughtError(DUCKDB_V2_ERROR &code, string &text, optional<string> &raw_message) noexcept -> void {
	// Set the code first (non-throwing), then render the detail.
	code = DUCKDB_V2_ERROR_GENERIC;
	try {
		// The bare throw re-raises the exception currently being handled so the catch clauses can dispatch on its type.
		try {
			throw;
		} catch (const duckdb::Exception &ex) {
			ErrorData error_data(ex);
			text = error_data.Message();
			raw_message = error_data.RawMessage();
		} catch (const std::bad_alloc &) {
			text = "Out of memory.";
		} catch (const std::exception &ex) {
			text = ex.what() ? ex.what() : "An unknown error occurred.";
		} catch (...) {
			text = "An unknown error occurred.";
		}
	} catch (...) {
		// Rendering the detail failed: report the code with no detail.
		text.clear();
		raw_message.reset();
	}
}

auto NullArgumentError(duckdb_v2_error_info_handle *err, const char *function, const char *argument) noexcept
    -> DUCKDB_V2_ERROR {
	const auto code = DUCKDB_V2_ERROR_GENERIC;
	if (!err) {
		return code;
	}
	// This runs outside WithErrorHandler, so nothing may escape across the C ABI. Only allocating the slot itself
	// can fail unrecoverably; the return code is authoritative either way.
	if (!*err) {
		try {
			*err = Convert(new CV2ErrorInfo());
		} catch (...) { // NOLINT(bugprone-empty-catch)
			return code;
		}
	}
	// Stamp the code and drop any stale detail with non-throwing operations, so the slot is consistent even if
	// rendering the message below fails.
	auto &out = *Convert(*err);
	out.code = code;
	out.message.clear();
	out.raw_message.reset();
	try {
		// Render through ErrorData so message/raw_message match what WithErrorHandler
		// produces for a thrown InvalidInputException.
		ErrorData error_data(ExceptionType::INVALID_INPUT,
		                     StringUtil::Format("The '%s' argument to '%s' cannot be null", argument, function));
		auto message = error_data.Message();
		auto raw_message = error_data.RawMessage();
		out.message = std::move(message);
		out.raw_message = std::move(raw_message);
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
		if (canonical_name == alias->setting_name) {
			out->aliases.emplace_back(alias->alias);
		}
	}
}

string CV2OptionSource::ReadSetting(const Identifier &name, const string &fallback) const {
	if (staged_settings) {
		auto staged = staged_settings->find(name);
		if (staged != staged_settings->end()) {
			return staged->second;
		}
	}
	Value result;
	auto found = context ? context->TryGetCurrentSetting(name, result) : config.TryGetCurrentSetting(name, result);
	if (found && !result.IsNull()) {
		return result.ToString();
	}
	return fallback;
}

static unique_ptr<CV2Option> PopulateOptionFromCore(const ConfigurationOption &option, const CV2OptionSource &source) {
	auto out = make_uniq<CV2Option>();

	out->name = option.name ? option.name : "";
	out->description = option.description ? option.description : "";
	out->target_scope = MapScopeTarget(option.scope);
	out->default_setting = option.default_value ? option.default_value : "";
	out->aliases.clear();
	PopulateOptionAliases(out, out->name);
	out->setting = source.ReadSetting(out->name, out->default_setting);

	return out;
}

// Populate `out` from an extension option. Extension options carry no
// SettingScopeTarget (the V2 enum reports UNKNOWN) and no aliases.
static unique_ptr<CV2Option> PopulateOptionFromExtension(const Identifier &name, const ExtensionOption &ext_option,
                                                         const CV2OptionSource &source) {
	auto out = make_uniq<CV2Option>();

	out->name = name;
	out->description = ext_option.description;
	out->target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	out->default_setting = ext_option.default_value.IsNull() ? std::string() : ext_option.default_value.ToString();
	out->aliases.clear();
	out->setting = source.ReadSetting(name, out->default_setting);

	return out;
}

idx_t CV2Option::Count(const CV2OptionSource &source) {
	return DBConfig::GetOptionCount() + source.GetConfig().GetExtensionSettings().size();
}

unique_ptr<CV2Option> CV2Option::FromIndex(const CV2OptionSource &source, idx_t index) {
	const auto core_count = DBConfig::GetOptionCount();
	if (index < core_count) {
		auto option = DBConfig::GetOptionByIndex(index);
		if (!option) {
			throw InvalidInputException("core option not found at given index");
		}
		return PopulateOptionFromCore(*option, source);
	}
	const idx_t ext_rel = index - core_count;
	auto ext_settings = source.GetConfig().GetExtensionSettings();
	if (ext_rel >= ext_settings.size()) {
		throw InvalidInputException("option index out of range");
	}
	idx_t i = 0;

	for (const auto &[name, option] : ext_settings) {
		if (i == ext_rel) {
			return PopulateOptionFromExtension(name, option, source);
		}
		++i;
	}
	throw InvalidInputException("option index out of range");
}

unique_ptr<CV2Option> CV2Option::FromName(const CV2OptionSource &source, std::string_view name) {
	Identifier name_id(name);
	if (auto option = DBConfig::GetOptionByName(name_id)) {
		return PopulateOptionFromCore(*option, source);
	}
	if (ExtensionOption ext_option; source.GetConfig().TryGetExtensionOption(name_id, ext_option)) {
		return PopulateOptionFromExtension(name_id, ext_option, source);
	}
	throw InvalidInputException("unknown configuration option: %s", name_id.GetIdentifierName());
}

} // namespace capiv2

} // namespace duckdb
