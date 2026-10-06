//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct FileHandle;
struct BaseRequest;
struct HTTPResponse;
class PhysicalOperator;
enum class PhysicalOperatorType : uint8_t;
class AttachedDatabase;
class RowGroup;
struct DataTableInfo;
//! Log types provide some structure to the formats that the different log messages can have
//! For now, this holds a type that the VARCHAR value will be auto-cast into.
class LogType {
public:
	//! Construct an unstructured type
	LogType(const string &name_p, const LogLevel &level_p)
	    : name(name_p), level(level_p), is_structured(false), type(LogicalType::VARCHAR) {
	}
	//! Construct a structured type
	LogType(const string &name_p, const LogLevel &level_p, LogicalType structured_type)
	    : name(name_p), level(level_p), is_structured(true), type(std::move(structured_type)) {
		if (!type.IsNested()) {
			throw InternalException("LogType must be nested if the type is explicitly set");
		}
	}

	string name;
	LogLevel level;

	bool is_structured;
	LogicalType type;
};

class DefaultLogType : public LogType {
public:
	static constexpr const char *NAME = "";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	DefaultLogType() : LogType(NAME, LEVEL) {
	}
};

class QueryLogType : public LogType {
public:
	static constexpr const char *NAME = "QueryLog";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	QueryLogType() : LogType(NAME, LEVEL) {};

	static string ConstructLogMessage(const string &str);
};

class FileSystemLogType : public LogType {
public:
	static constexpr const char *NAME = "FileSystem";
	static constexpr LogLevel LEVEL = LogLevel::LOG_TRACE;

	//! Construct the log type
	FileSystemLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes, idx_t pos);
	static string ConstructLogMessage(const FileHandle &handle, const string &op);
};

class HTTPLogType : public LogType {
public:
	static constexpr const char *NAME = "HTTP";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;
	static constexpr auto REDACTED_VALUE = "redacted";

	//! Construct the log types
	HTTPLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(BaseRequest &request, optional_ptr<HTTPResponse> response,
	                                  bool redact_http_logs = true);

	// FIXME: HTTPLogType should be structured probably
	static string ConstructLogMessage(const string &str) {
		return str;
	}

private:
	static const case_insensitive_set_t &RequestHeaderAllowList() {
		//! For extra info, the advise from OpenTelemetry:
		//! https://opentelemetry.io/docs/specs/semconv/registry/attributes/http/#http-request-header
		static const case_insensitive_set_t allow_list = {
		    // HTTP semantics (https://www.rfc-editor.org/rfc/rfc9110.html).
		    "accept",              // Accepted media types and parameters.
		    "accept-charset",      // Accepted character sets.
		    "accept-encoding",     // Accepted content codings.
		    "accept-language",     // Preferred response languages.
		    "connection",          // Connection handling options.
		    "content-encoding",    // Applied content codings.
		    "content-language",    // Languages of the intended audience.
		    "content-length",      // Content size in bytes.
		    "content-type",        // Content media type and parameters.
		    "date",                // Message timestamp.
		    "expect",              // Expected server behavior before sending content.
		    "if-modified-since",   // Modification timestamp used for conditional requests.
		    "if-unmodified-since", // Modification timestamp used for conditional requests.
		    "max-forwards",        // Remaining forwarding limit.
		    "range",               // Requested ranges.
		    "te",                  // Accepted transfer codings and trailer support.
		    "trailer",             // Names of fields sent in trailers.
		    "upgrade",             // Proposed or selected protocols.
		    "user-agent",          // Client product and version information.
		    "via",                 // Intermediary protocols, hosts and software comments.
		    // "authorization", // Contains credentials and signatures.
		    // "content-location", // URI can contain sensitive paths or query parameters.
		    // "from", // Contains the user's email address.
		    // "host", // Identifies the endpoint, including bucket or tenant names.
		    // "if-match", // Contains opaque resource validators.
		    // "if-none-match", // Contains opaque resource validators.
		    // "if-range", // Can contain an opaque resource validator instead of a date.
		    // "proxy-authorization", // Contains proxy credentials.
		    // "referer", // URI can contain sensitive paths or query parameters.

		    // Headers defined outside RFC 9110.
		    "cache-control",     // Caching directives.
		    "transfer-encoding", // Applied transfer codings.
		    // "cookie", // Can contain session credentials and user data.

		    // S3 (https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonRequestHeaders.html).
		    "x-amz-date",                   // Request signing timestamp.
		    "x-amz-request-payer",          // Requester-pays billing mode.
		    "x-amz-server-side-encryption", // Server-side encryption algorithm.
		    // "content-md5", // Fingerprints the request content.
		    // "x-amz-content-sha256", // Can fingerprint the payload, not just contain a fixed signing marker.
		    // "x-amz-copy-source", // Contains a bucket, object path and possibly a version ID.
		    // "x-amz-copy-source-if-match", // Contains opaque resource validators.
		    // "x-amz-copy-source-if-none-match", // Contains opaque resource validators.
		    // "x-amz-copy-source-server-side-encryption-customer-key", // Contains the source encryption key.
		    // "x-amz-copy-source-server-side-encryption-customer-key-md5", // Fingerprints the source encryption key.
		    // "x-amz-expected-bucket-owner", // Identifies an AWS account.
		    // "x-amz-security-token", // Contains session credentials.
		    // "x-amz-s3session-token", // Contains S3 Express session credentials.
		    // "x-amz-server-side-encryption-aws-kms-key-id", // Identifies an encryption key and possibly its account.
		    // "x-amz-server-side-encryption-context", // Contains caller-provided encryption context.
		    // "x-amz-server-side-encryption-customer-key", // Contains the customer encryption key.
		    // "x-amz-server-side-encryption-customer-key-md5", // Fingerprints the customer encryption key.
		    // "x-amz-source-expected-bucket-owner", // Identifies an AWS account.
		    // "x-amz-tagging", // Contains caller-provided object tags.
		    // "x-amz-website-redirect-location", // URI can contain sensitive paths or query parameters.
		};
		return allow_list;
	}

	static const case_insensitive_set_t &ResponseHeaderAllowList() {
		static const case_insensitive_set_t allow_list = {
		    // HTTP semantics (https://www.rfc-editor.org/rfc/rfc9110.html).
		    "accept",           // Accepted media types and parameters.
		    "accept-encoding",  // Accepted content codings.
		    "accept-ranges",    // Supported range units.
		    "allow",            // Supported HTTP methods.
		    "connection",       // Connection handling options.
		    "content-encoding", // Applied content codings.
		    "content-language", // Languages of the intended audience.
		    "content-length",   // Content size in bytes.
		    "content-range",    // Transferred range and total size.
		    "content-type",     // Content media type and parameters.
		    "date",             // Message timestamp.
		    "last-modified",    // Resource modification timestamp.
		    "retry-after",      // Retry delay or timestamp.
		    "server",           // Server product and version information.
		    "trailer",          // Names of fields sent in trailers.
		    "upgrade",          // Proposed or selected protocols.
		    "vary",             // Request field names used for response selection.
		    "via",              // Intermediary protocols, hosts and software comments.
		    // "authentication-info", // Can contain authentication tokens and parameters.
		    // "content-location", // URI can contain sensitive paths or query parameters.
		    // "etag", // Contains an opaque resource validator.
		    // "location", // Redirect URI can contain credentials or signed query parameters.
		    // "proxy-authenticate", // Authentication challenges can contain tokens and realm information.
		    // "proxy-authentication-info", // Can contain authentication tokens and parameters.
		    // "www-authenticate", // Authentication challenges can contain tokens and realm information.

		    // HTTP caching (https://www.rfc-editor.org/rfc/rfc9111.html).
		    "age",           // Time spent in caches.
		    "cache-control", // Caching directives.
		    "expires",       // Cache expiration timestamp.

		    // Other HTTP headers.
		    "transfer-encoding", // Applied transfer codings.
		    // "set-cookie", // Can contain session credentials and user data.

		    // S3 (https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
		    "x-amz-bucket-region",          // Bucket region.
		    "x-amz-server-side-encryption", // Server-side encryption algorithm.
		    // "x-amz-abort-rule-id", // Identifies a lifecycle rule.
		    // "x-amz-copy-source-version-id", // Identifies a source object version.
		    // "x-amz-expiration", // Includes a lifecycle rule ID as well as a date.
		    // "x-amz-id-2", // Request correlation ID, enabling it exposes request identity.
		    // "x-amz-request-id", // Request correlation ID, enabling it exposes request identity.
		    // "x-amz-server-side-encryption-aws-kms-key-id", // Identifies an encryption key and possibly its account.
		    // "x-amz-server-side-encryption-customer-key-md5", // Fingerprints the customer encryption key.
		    // "x-amz-version-id", // Identifies an object version.
		};
		return allow_list;
	}
};

class PhysicalOperatorLogType : public LogType {
public:
	static constexpr const char *NAME = "PhysicalOperator";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	//! Construct the log type
	PhysicalOperatorLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const PhysicalOperator &op, const string &class_p, const string &event,
	                                  const vector<pair<string, string>> &info);
	static string ConstructLogMessage(PhysicalOperatorType operator_type,
	                                  const vector<pair<string, string>> &parameters, const string &class_p,
	                                  const string &event, const vector<pair<string, string>> &info);
};

class MetricsLogType : public LogType {
public:
	static constexpr const char *NAME = "Metrics";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	MetricsLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &metric, const Value &value);
};

class CheckpointLogType : public LogType {
public:
	static constexpr const char *NAME = "Checkpoint";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	//! Construct the log type
	CheckpointLogType();

	static LogicalType GetLogType();

	//! Vacuum
	static string ConstructLogMessage(const AttachedDatabase &db, DataTableInfo &table, idx_t segment_idx,
	                                  idx_t merge_count, idx_t target_count, idx_t merge_rows, idx_t row_start);
	//! Checkpoint
	static string ConstructLogMessage(const AttachedDatabase &db, DataTableInfo &table, idx_t segment_idx,
	                                  RowGroup &row_group, idx_t row_group_start);

private:
	static string CreateLog(const AttachedDatabase &db, DataTableInfo &table, const char *op, vector<Value> map_keys,
	                        vector<Value> map_values);
};

class TransactionLogType : public LogType {
public:
	static constexpr const char *NAME = "Transaction";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	//! Construct the log type
	TransactionLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const AttachedDatabase &db, const char *log_type,
	                                  transaction_t transaction_id = MAX_TRANSACTION_ID);
};

class AdaptiveFilterLogType : public LogType {
public:
	static constexpr const char *NAME = "AdaptiveFilter";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	AdaptiveFilterLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const char *event, const string &file_path, const vector<idx_t> &permutation,
	                                  const vector<pair<string, string>> &info);
};

class ParquetPrefetchLogType : public LogType {
public:
	static constexpr const char *NAME = "ParquetPrefetch";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	ParquetPrefetchLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &file_path, idx_t row_group_id, bool fully_filtered,
	                                  const char *strategy, const vector<vector<string>> &prefetch_groups,
	                                  const vector<string> &minimal_filters, uint64_t accepted_column_gap);
};

class AsyncTaskScheduleLogType : public LogType {
public:
	static constexpr const char *NAME = "AsyncTaskSchedule";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	AsyncTaskScheduleLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &pool, idx_t task_count);
};

class ProgressVerificationLogType : public LogType {
public:
	static constexpr const char *NAME = "ProgressVerification";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	ProgressVerificationLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &invariant, const string &operator_name, const string &pipeline,
	                                  const string &detail);
};

class ExternalResourceLogType : public LogType {
public:
	static constexpr const char *NAME = "ExternalResource";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	ExternalResourceLogType();

	static LogicalType GetLogType();

	//! One recipe callback invocation (create/status/destroy), logged on response. `error` is empty on
	//! success (rendered NULL); `resource_name` is empty for an anonymous resource (rendered NULL).
	static string ConstructLogMessage(const string &resource_type, const string &resource_name, const string &operation,
	                                  const string &error, const Value &extra_info);
};

} // namespace duckdb
