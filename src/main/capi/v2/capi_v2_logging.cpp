#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {
namespace capiv2 {

static LogLevel ConvertLogLevel(DUCKDB_V2_LOG_LEVEL level) {
	switch (level) {
	case DUCKDB_V2_LOG_LEVEL_TRACE:
		return LogLevel::LOG_TRACE;
	case DUCKDB_V2_LOG_LEVEL_DEBUG:
		return LogLevel::LOG_DEBUG;
	case DUCKDB_V2_LOG_LEVEL_INFO:
		return LogLevel::LOG_INFO;
	case DUCKDB_V2_LOG_LEVEL_WARNING:
		return LogLevel::LOG_WARNING;
	case DUCKDB_V2_LOG_LEVEL_ERROR:
		return LogLevel::LOG_ERROR;
	case DUCKDB_V2_LOG_LEVEL_FATAL:
		return LogLevel::LOG_FATAL;
	default:
		throw InvalidInputException("unknown log level passed to duckdb_v2_context_log");
	}
}

} // namespace capiv2
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_context_log(duckdb_v2_context_handle ctx, DUCKDB_V2_LOG_LEVEL level, duckdb_v2_str log_type,
                                      duckdb_v2_str message, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	return WithErrorHandler(err, [&]() {
		const auto log_level = ConvertLogLevel(level);
		// ShouldLog/WriteLog take a C string, so the borrowed view has to be materialized.
		const duckdb::string type(Convert(log_type));
		const auto *type_name = type.empty() ? duckdb::DefaultLogType::NAME : type.c_str();

		auto &logger = duckdb::Logger::Get(*Convert(ctx));
		if (logger.ShouldLog(type_name, log_level)) {
			logger.WriteLog(type_name, log_level, duckdb::string(Convert(message)));
		}
	});
}
