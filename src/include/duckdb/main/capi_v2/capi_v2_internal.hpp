//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/capi_v2_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

// DuckDB C++ internals (also pulls in duckdb.h which defines idx_t, etc.)
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/parse_iterator.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/main/db_instance_cache.hpp"

// DuckDB internals used by the option set/get bridge.
#include "duckdb/main/setting_info.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"

// V2 C API header -- all types use duckdb_v2_ prefix, no collision with V1.
#include "duckdb_v2.h"

// ABI guard: the bridge reinterpret_casts duckdb_v2_bytes <-> duckdb::string_t,
// so the layouts must match. sizeof/alignof tie them together; the offsetof
// checks pin duckdb_v2_bytes's field offsets (string_t's union is private, so
// offsetof can't reach into it).
static_assert(sizeof(duckdb_v2_bytes) == sizeof(duckdb::string_t),
              "duckdb_v2_bytes must match the size of duckdb::string_t");
static_assert(alignof(duckdb_v2_bytes) == alignof(duckdb::string_t),
              "duckdb_v2_bytes must match the alignment of duckdb::string_t");
static_assert(offsetof(duckdb_v2_bytes, value.pointer.length) == 0,
              "duckdb_v2_bytes value.pointer.length must be at offset 0");
static_assert(offsetof(duckdb_v2_bytes, value.pointer.prefix) == 4,
              "duckdb_v2_bytes value.pointer.prefix must be at offset 4");
static_assert(offsetof(duckdb_v2_bytes, value.pointer.ptr) == 8,
              "duckdb_v2_bytes value.pointer.ptr must be at offset 8");
static_assert(offsetof(duckdb_v2_bytes, value.inlined.inlined) == 4,
              "duckdb_v2_bytes value.inlined.inlined must be at offset 4");

namespace duckdb {

namespace capiv2 {

//----------------------------------------------------------------------------------------------------------------------
// Conversion Helpers
//----------------------------------------------------------------------------------------------------------------------
// The one place a caller-supplied byte range enters the engine, so the range
// is validated here rather than at each call site: a null pointer is only
// meaningful when the range is empty, and viewing it with a non-zero length
// would walk address zero. Every entry point converts inside its
// WithErrorHandler scope, so this surfaces as ERROR_INPUT_INVALID.
inline auto Convert(duckdb_v2_str str) -> std::string_view {
	if (!str.ptr) {
		if (str.len > 0) {
			throw InvalidInputException("byte range cannot be null unless it is empty");
		}
		return std::string_view();
	}
	return std::string_view(str.ptr, str.len);
}
inline auto Convert(std::string_view str) -> duckdb_v2_str {
	return duckdb_v2_str {str.data(), str.size()};
}
inline auto Convert(const string &str) -> duckdb_v2_str {
	return duckdb_v2_str {str.data(), str.size()};
}
inline auto Convert(const Identifier &ident) -> duckdb_v2_identifier_t {
	return duckdb_v2_identifier_t {ident.c_str(), ident.size()};
}
inline auto Convert(duckdb_v2_hugeint_t value) -> hugeint_t {
	return hugeint_t(value.upper, value.lower);
}
inline auto Convert(duckdb_v2_uhugeint_t value) -> uhugeint_t {
	return uhugeint_t(value.upper, value.lower);
}
inline auto Convert(hugeint_t value) -> duckdb_v2_hugeint_t {
	return duckdb_v2_hugeint_t {value.lower, value.upper};
}
inline auto Convert(uhugeint_t value) -> duckdb_v2_uhugeint_t {
	return duckdb_v2_uhugeint_t {value.lower, value.upper};
}
inline auto Convert(interval_t value) -> duckdb_v2_interval_t {
	return duckdb_v2_interval_t {value.months, value.days, value.micros};
}
inline auto Convert(duckdb_v2_interval_t value) -> interval_t {
	interval_t out;
	out.months = value.months;
	out.days = value.days;
	out.micros = value.micros;
	return out;
}

//----------------------------------------------------------------------------------------------------------------------
// Handle Types
//----------------------------------------------------------------------------------------------------------------------

class CV2Environment {
public:
	unique_ptr<DBInstanceCache> cache;
	std::atomic<idx_t> open_database_count {0};
};

inline auto Convert(CV2Environment *env) -> duckdb_v2_environment_handle {
	return reinterpret_cast<duckdb_v2_environment_handle>(env);
}

inline auto Convert(duckdb_v2_environment_handle env) -> CV2Environment * {
	return reinterpret_cast<CV2Environment *>(env);
}

class CV2Database {
public:
	CV2Database(CV2Environment &env, shared_ptr<DuckDB> database) : env(env), database(std::move(database)) {
		internal_connection = make_uniq<Connection>(*this->database);
	}

	CV2Environment &env;
	shared_ptr<DuckDB> database;
	unique_ptr<Connection> internal_connection;
};

inline auto Convert(duckdb_v2_database_handle db) -> CV2Database * {
	return reinterpret_cast<CV2Database *>(db);
}

inline auto Convert(CV2Database *db) -> duckdb_v2_database_handle {
	return reinterpret_cast<duckdb_v2_database_handle>(db);
}

using CV2Connection = duckdb::Connection;

inline auto Convert(duckdb_v2_connection_handle conn) -> CV2Connection * {
	return reinterpret_cast<CV2Connection *>(conn);
}

inline auto Convert(CV2Connection *conn) -> duckdb_v2_connection_handle {
	return reinterpret_cast<duckdb_v2_connection_handle>(conn);
}

using CV2SQLStatement = duckdb::SQLStatement;

inline auto Convert(duckdb_v2_sql_statement_handle stmt) -> CV2SQLStatement * {
	return reinterpret_cast<CV2SQLStatement *>(stmt);
}
inline auto Convert(CV2SQLStatement *stmt) -> duckdb_v2_sql_statement_handle {
	return reinterpret_cast<duckdb_v2_sql_statement_handle>(stmt);
}

using CV2Context = duckdb::ClientContext;

inline auto Convert(duckdb_v2_context_handle ctx) -> CV2Context * {
	return reinterpret_cast<CV2Context *>(ctx);
}

inline auto Convert(CV2Context *ctx) -> duckdb_v2_context_handle {
	return reinterpret_cast<duckdb_v2_context_handle>(ctx);
}

class CV2Option {
public:
	Identifier name;
	string setting;
	string default_setting;
	string description;
	DUCKDB_V2_OPTION_TARGET_SCOPE target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	vector<string> aliases;

	static unique_ptr<CV2Option> FromIndex(ClientContext &context, DBConfig &config, idx_t index);
	static unique_ptr<CV2Option> FromName(ClientContext &context, DBConfig &config, std::string_view name);
};

inline auto Convert(duckdb_v2_option_handle opt) -> CV2Option * {
	return reinterpret_cast<CV2Option *>(opt);
}

inline auto Convert(CV2Option *opt) -> duckdb_v2_option_handle {
	return reinterpret_cast<duckdb_v2_option_handle>(opt);
}

using CV2LogicalType = duckdb::LogicalType;

inline auto Convert(duckdb_v2_logical_type_handle opt) -> CV2LogicalType * {
	return reinterpret_cast<CV2LogicalType *>(opt);
}
inline auto Convert(CV2LogicalType *opt) -> duckdb_v2_logical_type_handle {
	return reinterpret_cast<duckdb_v2_logical_type_handle>(opt);
}

using CV2Value = duckdb::Value;

inline auto Convert(duckdb_v2_value_handle val) -> CV2Value * {
	return reinterpret_cast<CV2Value *>(val);
}

inline auto Convert(CV2Value *val) -> duckdb_v2_value_handle {
	return reinterpret_cast<duckdb_v2_value_handle>(val);
}

using CV2DataChunk = duckdb::DataChunk;

inline auto Convert(duckdb_v2_data_chunk_handle chunk) -> CV2DataChunk * {
	return reinterpret_cast<CV2DataChunk *>(chunk);
}

inline auto Convert(CV2DataChunk *chunk) -> duckdb_v2_data_chunk_handle {
	return reinterpret_cast<duckdb_v2_data_chunk_handle>(chunk);
}

using CV2Vector = duckdb::Vector;

inline auto Convert(duckdb_v2_vector_handle vec) -> CV2Vector * {
	return reinterpret_cast<CV2Vector *>(vec);
}

inline auto Convert(CV2Vector *vec) -> duckdb_v2_vector_handle {
	return reinterpret_cast<duckdb_v2_vector_handle>(vec);
}

struct CV2Schema {
	struct Field {
		string name;
		LogicalType type;
	};
	std::deque<Field> fields;
};

inline auto Convert(duckdb_v2_schema_handle schema) -> CV2Schema * {
	return reinterpret_cast<CV2Schema *>(schema);
}
inline auto Convert(CV2Schema *schema) -> duckdb_v2_schema_handle {
	return reinterpret_cast<duckdb_v2_schema_handle>(schema);
}

//----------------------------------------------------------------------------------------------------------------------
// Error Handling
//----------------------------------------------------------------------------------------------------------------------

// Error code <-> exception type conversion
auto GetErrorCodeFromExceptionType(ExceptionType type) -> DUCKDB_V2_ERROR;
auto TryGetExceptionTypeFromErrorCode(DUCKDB_V2_ERROR code) -> optional<ExceptionType>;

// Backing struct for the opaque duckdb_v2_error_info_handle handle. Allocated
// only on failure paths and only when the caller requested detail (i.e.
// passed a non-null err out-parameter).
struct CV2ErrorInfo {
	DUCKDB_V2_ERROR code = DUCKDB_V2_ERROR_NONE;

	// message: full "<Type> Error: <raw>" (ErrorData::Message()). raw_message: the
	// body with that prefix stripped (ErrorData::RawMessage()), in the engine's
	// rendered form (caret block, or JSON under errors_as_json); empty for a
	// directly-set message. Both written on the error path (WithErrorHandler).
	string message;
	string raw_message;

	bool HasError() const {
		return code != DUCKDB_V2_ERROR_NONE;
	}

	[[noreturn]] void ThrowAsException() const {
		// Only throw if there's actually an error!
		D_ASSERT(HasError());

		// Rethrow with the exception class the code maps to, so the error class
		// round-trips (mirrors InvokeWithErrorSlot); InvalidInput is the fallback
		// for codes with no specific type.
		if (const auto type = TryGetExceptionTypeFromErrorCode(code)) {
			throw duckdb::Exception(*type, message);
		}
		throw duckdb::InvalidInputException(message);
	}
};

inline auto Convert(duckdb_v2_error_info_handle info) -> CV2ErrorInfo * {
	return reinterpret_cast<CV2ErrorInfo *>(info);
}

inline auto Convert(CV2ErrorInfo *info) -> duckdb_v2_error_info_handle {
	return reinterpret_cast<duckdb_v2_error_info_handle>(info);
}

//! Invoke a function, and convert any exception into an error code + populate an optional error info handle
template <class T>
DUCKDB_V2_ERROR WithErrorHandler(duckdb_v2_error_info_handle *err, T callback) {
	auto code = static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
	auto text = string();
	auto raw_message = string();

	try {
		// Invoke the callback
		callback();
	} catch (const duckdb::Exception &ex) {
		ErrorData error_data(ex);
		code = GetErrorCodeFromExceptionType(error_data.Type());
		text = error_data.Message();
		raw_message = error_data.RawMessage();
	} catch (const std::exception &ex) {
		code = DUCKDB_V2_ERROR_API;
		text = ex.what() ? ex.what() : "An unknown error occurred.";
	} catch (...) {
		code = DUCKDB_V2_ERROR_API;
		text = "An unknown error occurred.";
	}

	// Success leaves the slot untouched.
	// The return code is authoritative, as allocating on every successful call is unnecessary overhead.
	// A stale info from an earlier failure may therefore survive a later successful call, but that is ok.
	// (because the caller checks the code not the slot, and reads *err only after a failing return)
	if (code == DUCKDB_V2_ERROR_NONE) {
		return code;
	}

	// Failure: report detail through the slot if the caller provided one.
	if (err) {
		if (!*err) {
			// Allocate a new error info handle if not provided
			*err = Convert(new CV2ErrorInfo());
		}
		auto &out = *Convert(*err);
		out.code = code;
		out.message = std::move(text);
		out.raw_message = std::move(raw_message);
	}

	return code;
}

//----------------------------------------------------------------------------------------------------------------------
// Connection Slot
//----------------------------------------------------------------------------------------------------------------------

struct ConnectionBusySlotV2 : public ClientContextState {
	std::atomic<void *> owner {nullptr};
	// True once the consumer called `connection_interrupt` for the active result.
	// This flag is how we distinguish a consumer cancellation (-> CANCELLED status) from an DuckDB-initiated interrupt
	// that shares the INTERRUPT exception type, e.g. a `max_execution_time` timeout (-> error).
	// The DuckDB's internal interrupt_state is not suitable for this, it is set to stop sibling tasks on any error.
	// Reset when a new query claims the slot.
	std::atomic<bool> cancel_requested {false};
};

inline shared_ptr<ConnectionBusySlotV2> GetBusySlot(ClientContext &context) {
	constexpr auto BUSY_SLOT_STATE_KEY = "v2_connection_busy_slot";
	return context.registered_state->GetOrCreate<ConnectionBusySlotV2>(BUSY_SLOT_STATE_KEY);
}

//----------------------------------------------------------------------------------------------------------------------
// Text sinks
//----------------------------------------------------------------------------------------------------------------------
//! Hands the complete text to a caller-supplied sink, in exactly one call, so
//! the sink sees the full length up front. Its only influence on the outcome is
//! the error slot: a code it sets is rethrown as the matching exception type,
//! which WithErrorHandler then maps straight back.
inline void InvokeTextSink(duckdb_v2_text_sink_fn sink, duckdb_v2_str text, void *user_data) {
	// The slot is always live: sinks populate it, they never allocate or destroy it.
	CV2ErrorInfo info;
	auto handle = Convert(&info);
	sink(text, user_data, &handle);
	if (info.HasError()) {
		info.ThrowAsException();
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Caller-supplied output buffers
//----------------------------------------------------------------------------------------------------------------------
//! Reports `required` through out_length, then fills a caller-supplied buffer.
//! A null out_data is a size query: report and return without writing.
//! Shared by the entry points that hand back freshly materialized bytes.

template <class WRITE>
void FillCallerBuffer(uint8_t *out_data, idx_t out_capacity, idx_t *out_length, idx_t required,
                      const char *function_name, WRITE write) {
	*out_length = required;
	if (!out_data) {
		return;
	}
	if (out_capacity < required) {
		throw Exception(ExceptionType::OBJECT_SIZE,
		                StringUtil::Format("%s needs a buffer of %llu bytes, but out_capacity is %llu", function_name,
		                                   static_cast<uint64_t>(required), static_cast<uint64_t>(out_capacity)));
	}
	write(out_data);
}

//! Text flavour of FillCallerBuffer: reports the length without the null
//! terminator, but always writes one, so the buffer is usable as a C string.
//! The capacity must therefore cover the terminator too.
inline void FillCallerText(char *out_text, idx_t out_capacity, idx_t *out_length, const string &text,
                           const char *function_name) {
	*out_length = text.size();
	if (!out_text) {
		return;
	}
	const idx_t required = text.size() + 1;
	if (out_capacity < required) {
		throw Exception(ExceptionType::OBJECT_SIZE,
		                StringUtil::Format("%s needs a buffer of %llu bytes including the null terminator, but "
		                                   "out_capacity is %llu",
		                                   function_name, static_cast<uint64_t>(required),
		                                   static_cast<uint64_t>(out_capacity)));
	}
	memcpy(out_text, text.c_str(), required);
}

//----------------------------------------------------------------------------------------------------------------------
// Misc
//----------------------------------------------------------------------------------------------------------------------

inline void BuildParameterMap(const duckdb_v2_identifier_t *parameter_names,
                              const duckdb_v2_value_handle *parameter_values, idx_t parameter_count,
                              const char *function_name, identifier_map_t<BoundParameterData> &out) {
	for (idx_t i = 0; i < parameter_count; i++) {
		auto name = parameter_names ? parameter_names[i] : duckdb_v2_identifier_t {nullptr, 0};
		// A {NULL, 0} view is a valid (empty) name; only a null pointer with a nonzero length is malformed.
		if (!name.ptr && name.len > 0) {
			throw InvalidInputException("malformed parameter name passed to %s", function_name);
		}
		if (!parameter_values[i]) {
			throw InvalidInputException("null parameter value passed to %s", function_name);
		}
		// Named iff the name view is non-empty; otherwise positional
		auto str = Convert(name);
		Identifier key = (name.ptr && name.len > 0) ? Identifier(str) : Identifier(std::to_string(i + 1));
		out[key] = BoundParameterData(*Convert(parameter_values[i]));
	}
}

} // namespace capiv2

} // namespace duckdb
