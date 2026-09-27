#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

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
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_name);
	return WithErrorHandler(err, [&]() { *out_name = Convert(Convert(option)->name); });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_setting(duckdb_v2_option_handle option, duckdb_v2_str *out_setting,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_setting);
	return WithErrorHandler(err, [&]() { *out_setting = Convert(Convert(option)->setting); });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_default_setting(duckdb_v2_option_handle option, duckdb_v2_str *out_default_setting,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_default_setting);
	return WithErrorHandler(err, [&]() { *out_default_setting = Convert(Convert(option)->default_setting); });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_description(duckdb_v2_option_handle option, duckdb_v2_str *out_description,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_description);
	return WithErrorHandler(err, [&]() { *out_description = Convert(Convert(option)->description); });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_target_scope(duckdb_v2_option_handle option,
                                                  DUCKDB_V2_OPTION_TARGET_SCOPE *out_target_scope,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_target_scope);
	return WithErrorHandler(err, [&]() { *out_target_scope = Convert(option)->target_scope; });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_alias_count(duckdb_v2_option_handle option, idx_t *out_count,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_count);
	return WithErrorHandler(err, [&]() { *out_count = static_cast<idx_t>(Convert(option)->aliases.size()); });
}

DUCKDB_V2_ERROR duckdb_v2_option_get_alias(duckdb_v2_option_handle option, idx_t index,
                                           duckdb_v2_identifier_t *out_alias, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(option);
	DUCKDB_CHECK_ARG(out_alias);
	return WithErrorHandler(err, [&]() {
		const auto &wrapper = *Convert(option);
		if (index >= wrapper.aliases.size()) {
			*out_alias = duckdb_v2_str {nullptr, 0};
			throw duckdb::InvalidInputException("alias index out of range in duckdb_v2_option_get_alias");
		}
		*out_alias = Convert(wrapper.aliases[index]);
	});
}

// ---------------------------------------------------------------------------
// Context option getters: the connection's cascade, read from inside DuckDB
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_context_get_option_by_name(duckdb_v2_context_handle ctx, duckdb_v2_identifier_t name,
                                                     duckdb_v2_option_handle *out_option,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		CV2OptionSource source(*Convert(ctx));
		*out_option = Convert(CV2Option::FromName(source, ConvertIdentifierName(name)).release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_context_get_option_count(duckdb_v2_context_handle ctx, idx_t *out_count,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	DUCKDB_CHECK_ARG(out_count);
	return WithErrorHandler(err, [&]() { *out_count = CV2Option::Count(CV2OptionSource(*Convert(ctx))); });
}

DUCKDB_V2_ERROR duckdb_v2_context_get_option_by_index(duckdb_v2_context_handle ctx, idx_t index,
                                                      duckdb_v2_option_handle *out_option,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		CV2OptionSource source(*Convert(ctx));
		*out_option = Convert(CV2Option::FromIndex(source, index).release());
	});
}
