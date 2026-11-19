#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_profiler.hpp"

using namespace duckdb;
using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_connection_create(duckdb_v2_instance_handle instance, duckdb_v2_connection_handle *out_conn,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(out_conn);
	*out_conn = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &instance_wrapper = *Convert(instance);
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

// ---------------------------------------------------------------------------
// Query process management
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_connection_interrupt(duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	return WithErrorHandler(err, [&]() {
		// Record that the cancellation was user-initiated.
		auto &context = *Convert(conn)->context;
		GetBusySlot(context)->cancel_requests.fetch_add(1);

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

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_connection_get_current_catalog(duckdb_v2_connection_handle conn, duckdb_v2_text_sink_fn sink,
                                                         void *user_data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(sink);
	return WithErrorHandler(err, [&]() {
		auto &context = *Convert(conn)->context;
		auto entry = ClientData::Get(context).catalog_search_path->GetDefault();
		if (entry.GetCatalog() == INVALID_CATALOG) {
			entry.SetCatalog(DatabaseManager::GetDefaultDatabase(context));
		}
		InvokeTextSink(sink, entry.GetCatalog().GetIdentifierName(), user_data);
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_get_current_schema(duckdb_v2_connection_handle conn, duckdb_v2_text_sink_fn sink,
                                                        void *user_data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(sink);
	return WithErrorHandler(err, [&]() {
		auto &context = *Convert(conn)->context;
		auto entry = ClientData::Get(context).catalog_search_path->GetDefault();
		InvokeTextSink(sink, entry.GetSchema().GetIdentifierName(), user_data);
	});
}

static void SetCurrentSearchEntry(duckdb_v2_connection_handle conn, const CatalogSearchEntry &entry) {
	auto &context = *Convert(conn)->context;
	context.RunFunctionInTransaction(
	    [&]() { ClientData::Get(context).catalog_search_path->Set(entry, CatalogSetPathType::SET_SCHEMA); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_set_current_catalog(duckdb_v2_connection_handle conn, const char *catalog,
                                                         idx_t catalog_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_STR_ARG(catalog, catalog_length);
	return WithErrorHandler(err, [&]() {
		SetCurrentSearchEntry(conn, CatalogSearchEntry(Identifier(std::string(Convert(catalog, catalog_length))),
		                                               Identifier(DEFAULT_SCHEMA)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_set_current_schema(duckdb_v2_connection_handle conn, const char *schema,
                                                        idx_t schema_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_STR_ARG(schema, schema_length);
	return WithErrorHandler(err, [&]() {
		SetCurrentSearchEntry(conn, CatalogSearchEntry(Identifier(INVALID_CATALOG),
		                                               Identifier(std::string(Convert(schema, schema_length)))));
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_get_profiling_information(duckdb_v2_connection_handle conn, const char *format,
                                                               idx_t format_length, duckdb_v2_text_sink_fn sink,
                                                               void *user_data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(sink);
	DUCKDB_CHECK_STR_ARG(format, format_length);
	return WithErrorHandler(err, [&]() {
		std::string format_name(Convert(format, format_length));
		ProfilerPrintFormat print_format =
		    format_name.empty() ? ProfilerPrintFormat::Default() : ProfilerPrintFormat(format_name);
		InvokeTextSink(sink, Convert(conn)->GetProfilingInformation(print_format), user_data);
	});
}
