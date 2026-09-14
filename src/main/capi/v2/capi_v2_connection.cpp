#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

namespace duckdb {
namespace capiv2 {
namespace {

// Map V2's user-facing scope choice to DuckDB's SetScope.
SetScope MapSettingScope(DUCKDB_V2_SETTING_SCOPE s) {
	switch (s) {
	case DUCKDB_V2_SETTING_SCOPE_GLOBAL:
		return SetScope::GLOBAL;
	case DUCKDB_V2_SETTING_SCOPE_LOCAL:
		return SetScope::SESSION;
	default:
		return SetScope::AUTOMATIC;
	}
}

} // namespace

} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_connect(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn,
                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(db);
	DUCKDB_CHECK_ARG(out_conn);
	*out_conn = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *db_wrapper = Convert(db);
		auto connection = duckdb::make_uniq<duckdb::Connection>(*db_wrapper->database);
		*out_conn = Convert(connection.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_disconnect(duckdb_v2_connection_handle *conn) {
	return WithErrorHandler(nullptr, [&]() {
		if (!conn) {
			return;
		}
		if (*conn) {
			delete Convert(*conn);
			*conn = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_set(duckdb_v2_connection_handle conn, duckdb_v2_option_handle option,
                                                DUCKDB_V2_SETTING_SCOPE scope, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(option);
	return WithErrorHandler(err, [&]() {
		auto *opt = Convert(option);
		auto &client = *Convert(conn)->context;
		duckdb::PhysicalSet::SetVariable(client, opt->name, MapSettingScope(scope), duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name,
                                                duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &client = *Convert(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = CV2Option::FromName(client, config, Convert(name));
		*out_option = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get_count(duckdb_v2_connection_handle conn, idx_t *out_count,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(out_count);
	return WithErrorHandler(err, [&]() {
		auto &client = *Convert(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get_by_index(duckdb_v2_connection_handle conn, idx_t index,
                                                         duckdb_v2_option_handle *out_option,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &client = *Convert(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = CV2Option::FromIndex(client, config, index);
		*out_option = Convert(wrapper.release());
	});
}

// ---------------------------------------------------------------------------
// Query process management
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_connection_interrupt(duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	return WithErrorHandler(err, [&]() {
		// Record that the cancellation was user-initiated.
		auto &context = *Convert(conn)->context;
		GetBusySlot(context)->cancel_requested.store(true);

		// ClientContext::Interrupt is an atomic store; safe to call from any thread
		context.Interrupt();
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_progress_get(duckdb_v2_connection_handle conn, double *out_percentage,
                                                  uint64_t *out_rows_processed, uint64_t *out_total_rows_to_process,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(out_percentage);
	DUCKDB_CHECK_ARG(out_rows_processed);
	DUCKDB_CHECK_ARG(out_total_rows_to_process);
	return WithErrorHandler(err, [&]() {
		auto progress = Convert(conn)->context->GetQueryProgress();
		*out_percentage = progress.GetPercentage();
		*out_rows_processed = progress.GetRowsProcessed();
		*out_total_rows_to_process = progress.GetTotalRowsToProcess();
	});
}
