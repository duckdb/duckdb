#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_option_create(duckdb_v2_identifier_t name, duckdb_v2_str setting,
                                        duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if ((!name.ptr && name.len > 0) || (!setting.ptr && setting.len > 0) || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_create");
		}
		*out_option = nullptr;
		auto wrapper = duckdb::make_uniq<CV2Option>();
		wrapper->name = duckdb::Identifier(Convert(name));
		wrapper->setting = Convert(setting);
		*out_option = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_destroy(duckdb_v2_option_handle *option) {
	return WithErrorHandler(nullptr, [&]() {
		if (!option) {
			return;
		}
		if (*option) {
			delete Convert(*option);
			*option = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_name(duckdb_v2_option_handle option, duckdb_v2_identifier_t *out_name,
                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_name");
		}
		*out_name = Convert(Convert(option)->name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_setting(duckdb_v2_option_handle option, duckdb_v2_str *out_setting,
                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_setting) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_setting");
		}
		*out_setting = Convert(Convert(option)->setting);
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_default_setting(duckdb_v2_option_handle option, duckdb_v2_str *out_default_setting,
                                                     duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_default_setting) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_default_setting");
		}
		*out_default_setting = Convert(Convert(option)->default_setting);
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_description(duckdb_v2_option_handle option, duckdb_v2_str *out_description,
                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_description) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_description");
		}
		*out_description = Convert(Convert(option)->description);
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_target_scope(duckdb_v2_option_handle option,
                                                  DUCKDB_V2_OPTION_TARGET_SCOPE *out_target_scope,
                                                  duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_target_scope) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_target_scope");
		}
		*out_target_scope = Convert(option)->target_scope;
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_alias_count(duckdb_v2_option_handle option, idx_t *out_count,
                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_alias_count");
		}
		*out_count = static_cast<idx_t>(Convert(option)->aliases.size());
	});
}

DUCKDB_V2_ERROR duckdb_v2_option_get_alias(duckdb_v2_option_handle option, idx_t index,
                                           duckdb_v2_identifier_t *out_alias, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!option || !out_alias) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_option_get_alias");
		}
		const auto &wrapper = *Convert(option);
		if (index >= wrapper.aliases.size()) {
			*out_alias = duckdb_v2_str {nullptr, 0};
			throw duckdb::InvalidInputException("alias index out of range in duckdb_v2_option_get_alias");
		}
		*out_alias = Convert(wrapper.aliases[index]);
	});
}
