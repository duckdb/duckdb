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

struct QueryProgressWrapperV2 {
	double percentage = -1;
	uint64_t rows_processed = 0;
	uint64_t total_rows_to_process = 0;
};

} // namespace

auto Convert(duckdb_v2_query_progress_handle progress) -> QueryProgressWrapperV2 * {
	return reinterpret_cast<QueryProgressWrapperV2 *>(progress);
}

auto Convert(QueryProgressWrapperV2 *progress) -> duckdb_v2_query_progress_handle {
	return reinterpret_cast<duckdb_v2_query_progress_handle>(progress);
}

} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_connect(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn,
                                  duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!db || !out_conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connect");
		}

		*out_conn = nullptr;
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
	return WithErrorHandler(err, [&]() {
		if (!conn || !option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_set");
		}
		auto *opt = Convert(option);
		auto &client = *Convert(conn)->context;
		duckdb::PhysicalSet::SetVariable(client, opt->name, MapSettingScope(scope), duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name,
                                                duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn || (!name.ptr && name.len > 0) || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get");
		}
		*out_option = nullptr;
		auto &client = *Convert(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = CV2Option::FromName(client, config, Convert(name));
		*out_option = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get_count(duckdb_v2_connection_handle conn, idx_t *out_count,
                                                      duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_count");
		}
		auto &client = *Convert(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_option_get_by_index(duckdb_v2_connection_handle conn, idx_t index,
                                                         duckdb_v2_option_handle *out_option,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_by_index");
		}
		*out_option = nullptr;
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
	return WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_interrupt");
		}

		// Record that the cancellation was user-initiated.
		auto &context = *Convert(conn)->context;
		GetBusySlot(context)->cancel_requested.store(true);

		// ClientContext::Interrupt is an atomic store; safe to call from any thread
		context.Interrupt();
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_query_progress(duckdb_v2_connection_handle conn,
                                                    duckdb_v2_query_progress_handle *out_progress,
                                                    duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn || !out_progress) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_query_progress");
		}
		*out_progress = nullptr;
		auto progress = Convert(conn)->context->GetQueryProgress();
		auto wrapper = duckdb::make_uniq<QueryProgressWrapperV2>();
		wrapper->percentage = progress.GetPercentage();
		wrapper->rows_processed = progress.GetRowsProcessed();
		wrapper->total_rows_to_process = progress.GetTotalRowsToProcess();
		*out_progress = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_query_progress_get_percentage(duckdb_v2_query_progress_handle progress,
                                                        double *out_percentage, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!progress || !out_percentage) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_percentage");
		}
		*out_percentage = Convert(progress)->percentage;
	});
}

DUCKDB_V2_ERROR duckdb_v2_query_progress_get_rows_processed(duckdb_v2_query_progress_handle progress,
                                                            uint64_t *out_rows_processed,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!progress || !out_rows_processed) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_rows_processed");
		}
		*out_rows_processed = Convert(progress)->rows_processed;
	});
}

DUCKDB_V2_ERROR duckdb_v2_query_progress_get_total_rows_to_process(duckdb_v2_query_progress_handle progress,
                                                                   uint64_t *out_total_rows_to_process,
                                                                   duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!progress || !out_total_rows_to_process) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_total_rows_to_process");
		}
		*out_total_rows_to_process = Convert(progress)->total_rows_to_process;
	});
}

DUCKDB_V2_ERROR duckdb_v2_query_progress_destroy(duckdb_v2_query_progress_handle *progress) {
	return WithErrorHandler(nullptr, [&]() {
		if (!progress) {
			return;
		}
		if (*progress) {
			delete Convert(*progress);
			*progress = nullptr;
		}
	});
}
