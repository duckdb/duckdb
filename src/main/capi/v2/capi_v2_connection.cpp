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

DUCKDB_V2_ERROR duckdb_v2_connection_create(duckdb_v2_instance_handle instance, duckdb_v2_connection_handle *out_conn,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(out_conn);
	*out_conn = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &instance_wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(instance_wrapper.lock);
		auto connection = duckdb::make_uniq<CV2Connection>(instance_wrapper.GetDatabase());
		*out_conn = Convert(connection.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_destroy(duckdb_v2_connection_handle *conn) {
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

DUCKDB_V2_ERROR duckdb_v2_connection_get_context(duckdb_v2_connection_handle conn,
                                                 duckdb_v2_context_handle *out_context,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(out_context);
	*out_context = nullptr;
	return WithErrorHandler(err, [&]() { *out_context = Convert(&Convert(conn)->context_handle); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_set_option(duckdb_v2_connection_handle conn, const duckdb_v2_identifier_t *name,
                                                const duckdb_v2_str *setting, DUCKDB_V2_SETTING_SCOPE scope,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(setting);
	return WithErrorHandler(err, [&]() {
		auto &client = *Convert(conn)->context;
		duckdb::PhysicalSet::SetVariable(client, duckdb::Identifier(ConvertIdentifierName(name)),
		                                 MapSettingScope(scope), duckdb::Value(duckdb::string(Convert(setting))));
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
