// Include the C++ API header (which includes the C API header)
#include "duckdb_cpp.hpp"

#include <cstring>

// The V2 extension header. By default, (client library, or extension statically linked into DuckDB) this library binds
// duckdb_v2_* symbols at link time and only needs the header for the loader-interface types of the extension
// entrypoint. The loadable flavor (DUCKDB_CPP_API_LOADABLE) instead routes every call through the extension vtable
// populated by the entrypoint, so a dynamically loaded extension carries no undefined engine symbols. Users of the
// C++ API never include this header.
#if !defined(DUCKDB_CPP_API_LOADABLE) && !defined(DUCKDB_BUILD_STATIC_EXTENSION)
#define DUCKDB_BUILD_STATIC_EXTENSION
#endif
#include "duckdb_extension_v2.h"

// The vtable global the redirects reference. It is *defined* by the extension's entrypoint, which is what populates it,
// so this archive only declares it. Outside the loadable flavor nothing references it and the declaration is inert.
DUCKDB_EXTENSION_EXTERN

#include <type_traits>

namespace duckdb {
namespace cxx {

//----------------------------------------------------------------------------------------------------------------------
// Internal Implementation Details
//----------------------------------------------------------------------------------------------------------------------

namespace detail {

// Map each C++ wrapper to its underlying C-API handle type. Declared here in the .cpp (not the header).
// This makes handle types stay private to the implementation and consumed by Handle<TYPE>::handle().

template <>
struct HandleTraits<DatabaseOption> {
	using handle = duckdb_v2_option_handle;
};
template <>
struct HandleTraits<Context> {
	using handle = duckdb_v2_context_handle;
};
template <>
struct HandleTraits<Extension> {
	using handle = duckdb_v2_extension_handle;
};
template <>
struct HandleTraits<Connection> {
	using handle = duckdb_v2_connection_handle;
};
template <>
struct HandleTraits<SqlStatement> {
	using handle = duckdb_v2_sql_statement_handle;
};
template <>
struct HandleTraits<StatementIterator> {
	using handle = duckdb_v2_statement_iterator_handle;
};
template <>
struct HandleTraits<Schema> {
	using handle = duckdb_v2_schema_handle;
};
template <>
struct HandleTraits<Database> {
	using handle = duckdb_v2_database_handle;
};
template <>
struct HandleTraits<Environment> {
	using handle = duckdb_v2_environment_handle;
};
template <>
struct HandleTraits<LogicalType> {
	using handle = duckdb_v2_logical_type_handle;
};
template <>
struct HandleTraits<Value> {
	using handle = duckdb_v2_value_handle;
};
template <>
struct HandleTraits<Vector> {
	using handle = duckdb_v2_vector_handle;
};
template <>
struct HandleTraits<Arena> {
	using handle = duckdb_v2_arena_handle;
};
template <>
struct HandleTraits<DataChunk> {
	using handle = duckdb_v2_data_chunk_handle;
};
template <>
struct HandleTraits<QueryResult> {
	using handle = duckdb_v2_result_handle;
};

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Exceptions
//----------------------------------------------------------------------------------------------------------------------

InvalidInputException::InvalidInputException(const std::string &message, std::string raw_message)
    : Exception(DUCKDB_V2_ERROR_INPUT_INVALID, message, std::move(raw_message)) {
}

InterruptException::InterruptException(const std::string &message, std::string raw_message)
    : Exception(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT, message, std::move(raw_message)) {
}

//----------------------------------------------------------------------------------------------------------------------
// Error Handling Helpers
//----------------------------------------------------------------------------------------------------------------------

namespace {
// Perform a DuckDB C-API call, setup an error info object, and throw an exception if it fails.
// This is used to simplify error handling in the C++ wrapper.
template <class F, class... ARGS>
auto CheckedAPICall(F &&func, ARGS &&... args) -> void {
	duckdb_v2_error_info_handle err = nullptr;
	const auto code = func(std::forward<ARGS>(args)..., &err);
	if (code != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_str message_view = {nullptr, 0};
		duckdb_v2_str raw_view = {nullptr, 0};
		if (err) {
			duckdb_v2_error_info_get_text(err, &message_view);
			duckdb_v2_error_info_get_raw_message(err, &raw_view);
		}
		std::string message = message_view.ptr ? std::string(message_view.ptr, message_view.len) : "unknown error";
		std::string raw = raw_view.ptr ? std::string(raw_view.ptr, raw_view.len) : "";
		duckdb_v2_error_info_destroy(&err);
		// Map error codes with a dedicated exception type to that type so callers can catch it directly.
		switch (code) {
		case DUCKDB_V2_ERROR_INPUT_INVALID:
			throw InvalidInputException(message, std::move(raw));
		case DUCKDB_V2_ERROR_RUNTIME_INTERRUPT:
			throw InterruptException(message, std::move(raw));
		default:
			throw Exception(code, std::move(message), std::move(raw));
		}
	}
}

// Borrow a std::string as a length-delimited view for the C API.
auto ToStr(const std::string &s) -> duckdb_v2_str {
	return duckdb_v2_str {s.data(), s.size()};
}
auto ToStr(std::string_view s) -> duckdb_v2_str {
	return duckdb_v2_str {s.data(), s.size()};
}
// Borrow a storage token's bytes. Covers varchar_t through its blob_t base.
auto ToStr(const blob_t &bytes) -> duckdb_v2_str {
	return duckdb_v2_str {bytes.data(), bytes.size()};
}

// The 128-bit mirrors carry the same halves as their C counterparts, converted
// field-wise rather than by reinterpretation so the compiler checks them.
auto ToC(int128_t value) -> duckdb_v2_hugeint_t {
	return duckdb_v2_hugeint_t {value.lower, value.upper};
}
auto ToC(uint128_t value) -> duckdb_v2_uhugeint_t {
	return duckdb_v2_uhugeint_t {value.lower, value.upper};
}
auto FromC(duckdb_v2_hugeint_t value) -> int128_t {
	return int128_t {value.lower, value.upper};
}
auto FromC(duckdb_v2_uhugeint_t value) -> uint128_t {
	return uint128_t {value.lower, value.upper};
}
// View a borrowed C-API string as a std::string_view ({NULL,0} -> empty).
auto FromStr(duckdb_v2_str s) -> std::string_view {
	return s.ptr ? std::string_view(s.ptr, s.len) : std::string_view();
}

// Splits type parameters into the C API's parallel arrays; a TypeParam with an
// empty name crosses as the positional {NULL, 0} view. The arrays borrow from
// `params`, so this must outlive the call it feeds.
class TypeParamArrays {
public:
	explicit TypeParamArrays(const std::vector<TypeParam> &params) {
		name_views.reserve(params.size());
		value_handles.reserve(params.size());
		for (const auto &param : params) {
			name_views.push_back(param.GetName().empty() ? duckdb_v2_identifier_t {nullptr, 0}
			                                             : ToStr(param.GetName()));
			value_handles.push_back(param.GetValue().handle());
		}
	}

	auto names() const -> const duckdb_v2_identifier_t * {
		return name_views.empty() ? nullptr : name_views.data();
	}
	auto values() const -> const duckdb_v2_value_handle * {
		return value_handles.empty() ? nullptr : value_handles.data();
	}

private:
	std::vector<duckdb_v2_identifier_t> name_views;
	std::vector<duckdb_v2_value_handle> value_handles;
};

// Runs the C API's two-call text protocol: a null buffer reports the length,
// then a buffer with room for the terminator receives the text. The library
// never allocates, so there is nothing to free.
template <class F, class... ARGS>
auto RenderText(F &&func, ARGS &&... args) -> std::string {
	idx_t length = 0;
	CheckedAPICall(func, args..., static_cast<char *>(nullptr), static_cast<idx_t>(0), &length);
	std::string out;
	out.resize(length);
	// resize() guarantees length + 1 writable bytes, the last being the terminator.
	CheckedAPICall(func, args..., &out[0], length + 1, &length);
	return out;
}

// Catch any exceptions and propagate them via the error info out-parameter, returning an appropriate error code.
template <class T>
auto WithExceptionGuard(duckdb_v2_error_info_handle *err, T callback) -> DUCKDB_V2_ERROR {
	auto code = static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
	auto text = std::string();

	try {
		// Invoke the callback
		callback();
	} catch (const Exception &ex) {
		code = static_cast<DUCKDB_V2_ERROR>(ex.GetCode());
		text = ex.what();
	} catch (const std::exception &ex) {
		code = DUCKDB_V2_ERROR_API;
		text = ex.what();
	} catch (...) {
		code = DUCKDB_V2_ERROR_API;
		text = "An unknown error occurred.";
	}

	// Pass up to the caller via the out-parameter if they provided one; otherwise swallow.
	if (err && *err) {
		duckdb_v2_error_info_set_code(*err, code);
		duckdb_v2_error_info_set_text(*err, ToStr(text));
	}

	return code;
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Free Functions
//----------------------------------------------------------------------------------------------------------------------

auto LibraryVersion() -> std::string {
	duckdb_v2_str version;
	CheckedAPICall(duckdb_v2_library_version, &version);
	return std::string(version.ptr, version.len);
}

//---------------------------------------------------------------------------
// Environment
//---------------------------------------------------------------------------

Environment::Environment() {
	duckdb_v2_environment_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_create_environment, &_h);
	impl = _h;
}

Environment::~Environment() {
	auto _h = handle();
	duckdb_v2_destroy_environment(&_h);
}

auto Environment::GetOpenDatabaseCount() const -> size_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_environment_database_count, handle(), &count);
	return static_cast<size_t>(count);
}

auto Environment::Open(const std::string &path) -> Database {
	duckdb_v2_database_handle db = nullptr;
	CheckedAPICall(duckdb_v2_open, handle(), ToStr(path), nullptr, static_cast<idx_t>(0), &db);
	return detail::Factory::Make<Database>(db);
}

auto Environment::Open(const std::string &path, const std::vector<DatabaseOption> &options) -> Database {
	std::vector<duckdb_v2_option_handle> handles;
	handles.reserve(options.size());
	for (const auto &option : options) {
		handles.push_back(option.handle());
	}
	duckdb_v2_database_handle db = nullptr;
	CheckedAPICall(duckdb_v2_open, handle(), ToStr(path), handles.empty() ? nullptr : handles.data(),
	               static_cast<idx_t>(handles.size()), &db);
	return detail::Factory::Make<Database>(db);
}

//---------------------------------------------------------------------------
// Database Option
//---------------------------------------------------------------------------

DatabaseOption::DatabaseOption(void *impl) : detail::Handle<DatabaseOption>(impl) {
}

DatabaseOption::DatabaseOption(const std::string &name, const std::string &value) {
	duckdb_v2_option_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_option_create, ToStr(name), ToStr(value), &_h);
	impl = _h;
}

auto DatabaseOption::GetName() const -> std::string_view {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_name, handle(), &name);
	return FromStr(name);
}

auto DatabaseOption::GetValue() const -> std::string_view {
	duckdb_v2_str value = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_setting, handle(), &value);
	return FromStr(value);
}

auto DatabaseOption::GetDefaultValue() const -> std::string_view {
	duckdb_v2_str default_value = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_default_setting, handle(), &default_value);
	return FromStr(default_value);
}

auto DatabaseOption::GetDescription() const -> std::string_view {
	duckdb_v2_str description = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_description, handle(), &description);
	return FromStr(description);
}

auto DatabaseOption::GetAliasCount() const -> size_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_option_get_alias_count, handle(), &count);
	return static_cast<size_t>(count);
}

auto DatabaseOption::GetAliasByIndex(size_t index) const -> std::string_view {
	duckdb_v2_identifier_t alias = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_alias, handle(), static_cast<idx_t>(index), &alias);
	return FromStr(alias);
}

// OptionTargetScope mirrors DUCKDB_V2_OPTION_TARGET_SCOPE numerically; every member is pinned.
static_assert(static_cast<uint8_t>(OptionTargetScope::UNKNOWN) == DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::GLOBAL_ONLY) == DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::LOCAL_ONLY) == DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::GLOBAL_DEFAULT) == DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::LOCAL_DEFAULT) == DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");

auto DatabaseOption::GetTargetScope() const -> OptionTargetScope {
	DUCKDB_V2_OPTION_TARGET_SCOPE scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	CheckedAPICall(duckdb_v2_option_get_target_scope, handle(), &scope);
	return static_cast<OptionTargetScope>(scope);
}

DatabaseOption::~DatabaseOption() {
	auto _h = handle();
	duckdb_v2_option_destroy(&_h);
}

//---------------------------------------------------------------------------
// Database
//---------------------------------------------------------------------------

Database::Database(void *impl) : detail::Handle<Database>(impl) {
}

Database::~Database() {
	auto _h = handle();
	duckdb_v2_close(&_h);
}

auto Database::GetOptionCount() const -> size_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_database_option_get_count, handle(), &count);
	return static_cast<size_t>(count);
}

auto Database::GetOptionByIndex(size_t index) const -> DatabaseOption {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_database_option_get_by_index, handle(), static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

auto Database::GetOption(std::string_view name) const -> DatabaseOption {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_database_option_get, handle(), duckdb_v2_identifier_t {name.data(), name.size()}, &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

auto Database::SetOption(const DatabaseOption &option) -> void {
	CheckedAPICall(duckdb_v2_database_option_set, handle(), option.handle());
}

auto Database::Connect() -> Connection {
	duckdb_v2_connection_handle conn = nullptr;
	CheckedAPICall(duckdb_v2_connect, handle(), &conn);
	return detail::Factory::Make<Connection>(conn, true);
}

//---------------------------------------------------------------------------
// Connection
//---------------------------------------------------------------------------

Connection::Connection(void *impl, bool owned) : detail::Handle<Connection>(impl), owned(owned) {
}

Connection::~Connection() {
	if (owned) {
		auto _h = handle();
		duckdb_v2_disconnect(&_h);
	}
}

auto Connection::GetOptionCount() const -> size_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_connection_option_get_count, handle(), &count);
	return static_cast<size_t>(count);
}

auto Connection::GetOptionByIndex(size_t index) const -> DatabaseOption {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_connection_option_get_by_index, handle(), static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

auto Connection::GetOption(std::string_view name) const -> DatabaseOption {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_connection_option_get, handle(), duckdb_v2_identifier_t {name.data(), name.size()},
	               &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

auto Connection::SetOption(const DatabaseOption &option) -> void {
	SetOption(option, SettingScope::AUTOMATIC);
}

auto Connection::SetOption(const DatabaseOption &option, SettingScope scope) -> void {
	static_assert(static_cast<int>(SettingScope::AUTOMATIC) == DUCKDB_V2_SETTING_SCOPE_AUTOMATIC &&
	                  static_cast<int>(SettingScope::GLOBAL) == DUCKDB_V2_SETTING_SCOPE_GLOBAL &&
	                  static_cast<int>(SettingScope::LOCAL) == DUCKDB_V2_SETTING_SCOPE_LOCAL,
	              "SettingScope must mirror DUCKDB_V2_SETTING_SCOPE");
	CheckedAPICall(duckdb_v2_connection_option_set, handle(), option.handle(),
	               static_cast<DUCKDB_V2_SETTING_SCOPE>(scope));
}

auto Connection::ParseType(std::string_view text) -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_connection_create_type_from_text, handle(), duckdb_v2_str {text.data(), text.size()},
	               &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Connection::CreateType(std::string_view name, const std::vector<TypeParam> &params) -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_connection_create_type_from_name, handle(), ToStr(name), split.names(), split.values(),
	               static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Connection::CreateType(LogicalTypeId id, const std::vector<TypeParam> &params) -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_connection_create_type_from_id, handle(), static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(id),
	               split.names(), split.values(), static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

//----------------------------------------------------------------------------------------------------------------------
// SQL statements
//----------------------------------------------------------------------------------------------------------------------

SqlStatement::SqlStatement(void *impl) : detail::Handle<SqlStatement>(impl) {
}

SqlStatement::~SqlStatement() {
	auto _h = handle();
	duckdb_v2_sql_statement_destroy(&_h);
}

StatementIterator::StatementIterator(void *impl) : detail::Handle<StatementIterator>(impl) {
}

StatementIterator::~StatementIterator() {
	auto _h = handle();
	duckdb_v2_statement_iterator_destroy(&_h);
}

auto StatementIterator::Next() -> SqlStatement {
	duckdb_v2_sql_statement_handle statement = nullptr;
	CheckedAPICall(duckdb_v2_statement_iterator_next, handle(), &statement);
	// An empty handle marks exhaustion.
	return detail::Factory::Make<SqlStatement>(statement);
}

auto Connection::ParseSQL(const char *sql) -> StatementIterator {
	duckdb_v2_statement_iterator_handle iterator = nullptr;
	CheckedAPICall(duckdb_v2_parse_sql, handle(), sql, &iterator);
	return detail::Factory::Make<StatementIterator>(iterator);
}

auto Connection::Execute(const SqlStatement &statement, const Value *parameters, idx_t parameter_count) -> QueryResult {
	// Borrowed, not consumed: pass the handle without releasing it, so the
	// caller's SqlStatement keeps ownership and can be executed again.
	std::vector<duckdb_v2_value_handle> values;
	values.reserve(parameter_count);
	for (idx_t i = 0; i < parameter_count; i++) {
		values.push_back(parameters[i].handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_statement_execute, handle(), statement.handle(), nullptr,
	               parameter_count ? values.data() : nullptr, parameter_count, &result);
	return detail::Factory::Make<QueryResult>(result);
}

auto Connection::Execute(const SqlStatement &statement, const std::vector<NamedParam> &parameters) -> QueryResult {
	// Split into the C API's parallel arrays; an empty name crosses as the positional
	// {NULL, 0} view (mirrors Context::CreateType).
	std::vector<duckdb_v2_identifier_t> names;
	std::vector<duckdb_v2_value_handle> values;
	names.reserve(parameters.size());
	values.reserve(parameters.size());
	for (const auto &param : parameters) {
		names.push_back(param.name.empty() ? duckdb_v2_identifier_t {nullptr, 0} : ToStr(param.name));
		values.push_back(param.value.handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_statement_execute, handle(), statement.handle(), names.empty() ? nullptr : names.data(),
	               values.empty() ? nullptr : values.data(), static_cast<idx_t>(parameters.size()), &result);
	return detail::Factory::Make<QueryResult>(result);
}

auto Connection::Execute(const SqlStatement &statement) -> QueryResult {
	return Execute(statement, nullptr, 0);
}

auto Connection::Execute(const std::string &sql) -> QueryResult {
	auto statements = ParseSQL(sql);
	auto statement = statements.Next();
	if (!statement || statements.Next()) {
		throw InvalidInputException("Execute expects exactly one statement; use ParseSQL for multi-statement input");
	}
	return Execute(statement);
}

auto Connection::Interrupt() -> void {
	CheckedAPICall(duckdb_v2_connection_interrupt, handle());
}

auto Connection::GetQueryProgress() const -> Connection::QueryProgress {
	// Flatten the C snapshot object into the POD struct: capture, read the
	// accessors, destroy.
	duckdb_v2_query_progress_handle snapshot = nullptr;
	CheckedAPICall(duckdb_v2_connection_query_progress, handle(), &snapshot);
	QueryProgress progress {};
	try {
		CheckedAPICall(duckdb_v2_query_progress_get_percentage, snapshot, &progress.percentage);
		CheckedAPICall(duckdb_v2_query_progress_get_rows_processed, snapshot, &progress.rows_processed);
		CheckedAPICall(duckdb_v2_query_progress_get_total_rows_to_process, snapshot, &progress.total_rows_to_process);
	} catch (...) {
		duckdb_v2_query_progress_destroy(&snapshot);
		throw;
	}
	duckdb_v2_query_progress_destroy(&snapshot);
	return progress;
}

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

Context::Context(void *impl) : detail::Handle<Context>(impl) {
}

Context::~Context() {
	// Context lifetime is managed by DuckDB, so we don't destroy the handle here
}

//----------------------------------------------------------------------------------------------------------------------
// Extension
//----------------------------------------------------------------------------------------------------------------------

Extension::Extension(void *impl) : detail::Handle<Extension>(impl) {
}

Extension::~Extension() {
	// The loader is owned by DuckDB for the duration of the load, so we don't destroy the handle here
}

namespace detail {

auto RunExtensionEntry(void (*body)(Extension &, Context &), void *extension, void *context, void *err) -> void {
	auto *error_slot = static_cast<duckdb_v2_error_info_handle *>(err);
	// The guard is the only thing standing between a throwing extension body and unwinding through DuckDB's C frame.
	WithExceptionGuard(error_slot, [&]() {
		auto ext = Factory::Make<Extension>(extension);
		auto ctx = Factory::Make<Context>(context);
		body(ext, ctx);
	});
}

} // namespace detail

auto Context::ParseType(std::string_view text) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_context_create_type_from_text, handle(), duckdb_v2_str {text.data(), text.size()}, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Context::Log(LogLevel level, std::string_view message, std::string_view log_type) const -> void {
	CheckedAPICall(duckdb_v2_context_log, handle(), static_cast<DUCKDB_V2_LOG_LEVEL>(level), ToStr(log_type),
	               ToStr(message));
}

auto Context::CreateType(std::string_view name, const std::vector<TypeParam> &params) const -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_context_create_type_from_name, handle(), ToStr(name), split.names(), split.values(),
	               static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Context::CreateType(LogicalTypeId id, const std::vector<TypeParam> &params) const -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_context_create_type_from_id, handle(), static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(id),
	               split.names(), split.values(), static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------

LogicalType::LogicalType(void *impl) : detail::Handle<LogicalType>(impl) {
}

bool LogicalType::operator==(const LogicalType &other) const {
	if (handle() == other.handle()) {
		return true; // same handle means same logical type
	}
	bool result = false;
	CheckedAPICall(duckdb_v2_logical_type_is_equal, handle(), other.handle(), &result);
	return result;
}

auto LogicalType::GetTypeId() const -> LogicalTypeId {
	auto id = static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(0);
	CheckedAPICall(duckdb_v2_logical_type_get_id, handle(), &id);
	return static_cast<LogicalTypeId>(id);
}

auto LogicalType::GetName() const -> std::string_view {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_logical_type_get_name, handle(), &name);
	return FromStr(name);
}

auto LogicalType::WithAlias(const Context &ctx, std::string_view alias) const -> LogicalType {
	duckdb_v2_logical_type_handle new_type = nullptr;
	CheckedAPICall(duckdb_v2_context_create_type_with_alias, ctx.handle(), handle(), ToStr(alias), &new_type);
	return detail::Factory::Make<LogicalType>(new_type);
}

auto LogicalType::WithAlias(const Connection &conn, std::string_view alias) const -> LogicalType {
	duckdb_v2_logical_type_handle new_type = nullptr;
	CheckedAPICall(duckdb_v2_connection_create_type_with_alias, conn.handle(), handle(), ToStr(alias), &new_type);
	return detail::Factory::Make<LogicalType>(new_type);
}

LogicalType::~LogicalType() {
	auto _h = handle();
	duckdb_v2_logical_type_destroy(&_h);
}

// LogicalTypeId mirrors LOGICAL_TYPE_ID numerically; every member is pinned.
#define DUCKDB_CPP_ASSERT_TYPE_ID(member)                                                                              \
	static_assert(static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(LogicalTypeId::member) == DUCKDB_V2_LOGICAL_TYPE_ID_##member, \
	              "LogicalTypeId::" #member " must mirror DUCKDB_V2_LOGICAL_TYPE_ID_" #member)
DUCKDB_CPP_ASSERT_TYPE_ID(INVALID);
DUCKDB_CPP_ASSERT_TYPE_ID(SQLNULL);
DUCKDB_CPP_ASSERT_TYPE_ID(UNKNOWN);
DUCKDB_CPP_ASSERT_TYPE_ID(ANY);
DUCKDB_CPP_ASSERT_TYPE_ID(TYPE);
DUCKDB_CPP_ASSERT_TYPE_ID(BOOLEAN);
DUCKDB_CPP_ASSERT_TYPE_ID(TINYINT);
DUCKDB_CPP_ASSERT_TYPE_ID(SMALLINT);
DUCKDB_CPP_ASSERT_TYPE_ID(INTEGER);
DUCKDB_CPP_ASSERT_TYPE_ID(BIGINT);
DUCKDB_CPP_ASSERT_TYPE_ID(DATE);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_SEC);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_MS);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(DECIMAL);
DUCKDB_CPP_ASSERT_TYPE_ID(FLOAT);
DUCKDB_CPP_ASSERT_TYPE_ID(DOUBLE);
DUCKDB_CPP_ASSERT_TYPE_ID(VARCHAR);
DUCKDB_CPP_ASSERT_TYPE_ID(BLOB);
DUCKDB_CPP_ASSERT_TYPE_ID(INTERVAL);
DUCKDB_CPP_ASSERT_TYPE_ID(UTINYINT);
DUCKDB_CPP_ASSERT_TYPE_ID(USMALLINT);
DUCKDB_CPP_ASSERT_TYPE_ID(UINTEGER);
DUCKDB_CPP_ASSERT_TYPE_ID(UBIGINT);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_TZ);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_TZ_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME_TZ);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(BIT);
DUCKDB_CPP_ASSERT_TYPE_ID(BIGNUM);
DUCKDB_CPP_ASSERT_TYPE_ID(UHUGEINT);
DUCKDB_CPP_ASSERT_TYPE_ID(HUGEINT);
DUCKDB_CPP_ASSERT_TYPE_ID(UUID);
DUCKDB_CPP_ASSERT_TYPE_ID(GEOMETRY);
DUCKDB_CPP_ASSERT_TYPE_ID(STRUCT);
DUCKDB_CPP_ASSERT_TYPE_ID(LIST);
DUCKDB_CPP_ASSERT_TYPE_ID(MAP);
DUCKDB_CPP_ASSERT_TYPE_ID(ENUM);
DUCKDB_CPP_ASSERT_TYPE_ID(UNION);
DUCKDB_CPP_ASSERT_TYPE_ID(ARRAY);
DUCKDB_CPP_ASSERT_TYPE_ID(VARIANT);
DUCKDB_CPP_ASSERT_TYPE_ID(TUPLE);
#undef DUCKDB_CPP_ASSERT_TYPE_ID

auto LogicalType::ToText() const -> std::string {
	return RenderText(duckdb_v2_logical_type_to_text, handle());
}

auto LogicalType::GetParamCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_logical_type_get_param_count, handle(), &count);
	return count;
}

auto LogicalType::GetParam(idx_t index) const -> TypeParam {
	duckdb_v2_identifier_t name = {nullptr, 0};
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_get_param, handle(), index, &name, &value);
	return TypeParam {std::string(FromStr(name)), detail::Factory::Make<Value>(value)};
}

auto LogicalType::GetDecimalWidth() const -> uint8_t {
	return GetParam(0).GetValue().Get<uint8_t>();
}

auto LogicalType::GetDecimalScale() const -> uint8_t {
	return GetParam(1).GetValue().Get<uint8_t>();
}

auto LogicalType::GetEnumSize() const -> idx_t {
	return GetParamCount();
}

auto LogicalType::GetEnumValue(idx_t index) const -> std::string {
	// Owned string: the backing Value is owned per call, a view would dangle.
	return std::string(GetParam(index).GetValue().Get<varchar_t>());
}

auto LogicalType::GetListChildType() const -> LogicalType {
	return GetParam(0).GetValue().Get<LogicalType>();
}

auto LogicalType::GetArrayChildType() const -> LogicalType {
	return GetParam(0).GetValue().Get<LogicalType>();
}

auto LogicalType::GetArraySize() const -> idx_t {
	return GetParam(1).GetValue().Get<idx_t>();
}

auto LogicalType::GetMapKeyType() const -> LogicalType {
	return GetParam(0).GetValue().Get<LogicalType>();
}

auto LogicalType::GetMapValueType() const -> LogicalType {
	return GetParam(1).GetValue().Get<LogicalType>();
}

auto LogicalType::GetStructChildCount() const -> idx_t {
	return GetParamCount();
}

auto LogicalType::GetStructChildName(idx_t index) const -> std::string {
	return GetParam(index).GetName();
}

auto LogicalType::GetStructChildType(idx_t index) const -> LogicalType {
	return GetParam(index).GetValue().Get<LogicalType>();
}

auto LogicalType::GetUnionMemberCount() const -> idx_t {
	return GetParamCount();
}

auto LogicalType::GetUnionMemberName(idx_t index) const -> std::string {
	return GetParam(index).GetName();
}

auto LogicalType::GetUnionMemberType(idx_t index) const -> LogicalType {
	return GetParam(index).GetValue().Get<LogicalType>();
}

auto LogicalType::GetDecimalInternalTypeId() const -> LogicalTypeId {
	// The committed DECIMAL storage tiers: width <= 4 int16, <= 9 int32,
	// <= 18 int64, <= 38 int128. Pinned against the engine in [capi_v2].
	auto width = GetDecimalWidth();
	if (width <= 4) {
		return LogicalTypeId::SMALLINT;
	}
	if (width <= 9) {
		return LogicalTypeId::INTEGER;
	}
	if (width <= 18) {
		return LogicalTypeId::BIGINT;
	}
	return LogicalTypeId::HUGEINT;
}

auto LogicalType::GetEnumInternalTypeId() const -> LogicalTypeId {
	// The committed ENUM storage tiers: size <= 255 uint8, <= 65535 uint16,
	// else uint32. Pinned against the engine in [capi_v2].
	auto size = GetEnumSize();
	if (size <= 255) {
		return LogicalTypeId::UTINYINT;
	}
	if (size <= 65535) {
		return LogicalTypeId::USMALLINT;
	}
	return LogicalTypeId::UINTEGER;
}

//----------------------------------------------------------------------------------------------------------------------
// Schema
//----------------------------------------------------------------------------------------------------------------------
Schema::Schema(void *impl) : detail::Handle<Schema>(impl) {
}
Schema::~Schema() {
	auto _h = handle();
	duckdb_v2_schema_destroy(&_h);
}
auto Schema::GetFieldCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_schema_get_count, handle(), &count);
	return count;
}
auto Schema::GetFieldName(idx_t index) const -> std::string_view {
	duckdb_v2_identifier_t name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr; // borrowed; unused here
	CheckedAPICall(duckdb_v2_schema_get_field, handle(), index, &name, &type);
	return FromStr(name);
}
auto Schema::GetFieldType(idx_t index) const -> LogicalType {
	duckdb_v2_identifier_t name = {nullptr, 0};
	duckdb_v2_logical_type_handle borrowed = nullptr;
	CheckedAPICall(duckdb_v2_schema_get_field, handle(), index, &name, &borrowed);
	// get_field borrows the type; copy it into an owned handle the wrapper manages.
	duckdb_v2_logical_type_handle owned = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_copy, borrowed, &owned);
	return detail::Factory::Make<LogicalType>(owned);
}

auto Connection::Bind(const SqlStatement &statement) const -> Signature {
	duckdb_v2_schema_handle out_schema = nullptr;
	duckdb_v2_schema_handle out_parameters = nullptr;
	CheckedAPICall(duckdb_v2_statement_bind, handle(), statement.handle(), &out_schema, &out_parameters);
	return Signature {detail::Factory::Make<Schema>(out_schema), detail::Factory::Make<Schema>(out_parameters)};
}

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------

Value::Value(void *impl) : detail::Handle<Value>(impl) {
}

Value::~Value() {
	auto _h = handle();
	duckdb_v2_value_destroy(&_h);
}

auto Value::IsNull() const -> bool {
	bool is_null = false;
	CheckedAPICall(duckdb_v2_value_is_null, handle(), &is_null);
	return is_null;
}

auto Value::GetLogicalType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_logical_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Value::ToText() const -> std::string {
	return RenderText(duckdb_v2_value_to_string, handle());
}

auto Value::Cast(const Context &ctx, const LogicalType &target) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_cast_with_context, ctx.handle(), handle(), target.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::Cast(const Connection &conn, const LogicalType &target) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_cast_with_connection, conn.handle(), handle(), target.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_value_get_child_count, handle(), &count);
	return count;
}

auto Value::GetChild(idx_t index) const -> Value {
	duckdb_v2_value_handle child = nullptr;
	CheckedAPICall(duckdb_v2_value_get_child, handle(), index, &child);
	return detail::Factory::Make<Value>(child);
}

template <>
auto Value::Get() const -> bool {
	bool value = false;
	CheckedAPICall(duckdb_v2_value_get_bool, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> uint8_t {
	uint8_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_utinyint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> uint16_t {
	uint16_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_usmallint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> uint32_t {
	uint32_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_uint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> uint64_t {
	uint64_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_ubigint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> uint128_t {
	duckdb_v2_uhugeint_t value {};
	CheckedAPICall(duckdb_v2_value_get_uhugeint, handle(), &value);
	return FromC(value);
}

template <>
auto Value::Get() const -> int8_t {
	int8_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_tinyint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> int16_t {
	int16_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_smallint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> int32_t {
	int32_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_int, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> int64_t {
	int64_t value = 0;
	CheckedAPICall(duckdb_v2_value_get_bigint, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> int128_t {
	duckdb_v2_hugeint_t value {};
	CheckedAPICall(duckdb_v2_value_get_hugeint, handle(), &value);
	return FromC(value);
}

template <>
auto Value::Get() const -> float {
	float value = 0;
	CheckedAPICall(duckdb_v2_value_get_float, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> double {
	double value = 0;
	CheckedAPICall(duckdb_v2_value_get_double, handle(), &value);
	return value;
}

template <>
auto Value::Get() const -> varchar_t {
	duckdb_v2_str value;
	CheckedAPICall(duckdb_v2_value_get_varchar, handle(), &value);
	return varchar_t(value.ptr, value.len);
}

template <>
auto Value::Get() const -> blob_t {
	duckdb_v2_str value;
	CheckedAPICall(duckdb_v2_value_get_blob, handle(), &value);
	return blob_t(value.ptr, value.len);
}

template <>
auto Value::Get() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

//----------------------------------------------------------------------------------------------------------------------
// Value Constructors
//----------------------------------------------------------------------------------------------------------------------

#define MAKE_VALUE_IMPL(ctx, name, value)                                                                              \
	duckdb_v2_value_handle handle = nullptr;                                                                           \
	CheckedAPICall(name, ctx.handle(), value, &handle);                                                                \
	return detail::Factory::Make<Value>(handle);

// Connection
auto Value::CreateNull(Connection &conn, const LogicalType &type) -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create_null_with_connection, conn.handle(), type.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::Create(Connection &conn, bool value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_bool_with_connection, value)
}

auto Value::Create(Connection &conn, uint8_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_utinyint_with_connection, value)
}

auto Value::Create(Connection &conn, uint16_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_usmallint_with_connection, value)
}

auto Value::Create(Connection &conn, uint32_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_uint_with_connection, value)
}

auto Value::Create(Connection &conn, uint64_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_ubigint_with_connection, value)
}

auto Value::Create(Connection &conn, uint128_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_uhugeint_with_connection, ToC(value))
}

auto Value::Create(Connection &conn, int8_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_tinyint_with_connection, value)
}

auto Value::Create(Connection &conn, int16_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_smallint_with_connection, value)
}

auto Value::Create(Connection &conn, int32_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_int_with_connection, value)
}

auto Value::Create(Connection &conn, int64_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_bigint_with_connection, value)
}

auto Value::Create(Connection &conn, int128_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_hugeint_with_connection, ToC(value))
}

auto Value::Create(Connection &conn, float value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_float_with_connection, value)
}

auto Value::Create(Connection &conn, double value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_double_with_connection, value)
}
auto Value::Create(Connection &conn, blob_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_blob_with_connection, ToStr(value))
}
auto Value::Create(Connection &conn, varchar_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_varchar_with_connection, ToStr(value))
}
auto Value::Create(Connection &conn, const LogicalType &value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_type_with_connection, value.handle())
}

// Context
auto Value::CreateNull(Context &ctx, const LogicalType &type) -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create_null_with_context, ctx.handle(), type.handle(), &value);
	return detail::Factory::Make<Value>(value);
}
auto Value::Create(Context &ctx, bool value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_bool_with_context, value)
}

auto Value::Create(Context &ctx, uint8_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_utinyint_with_context, value)
}

auto Value::Create(Context &ctx, uint16_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_usmallint_with_context, value)
}

auto Value::Create(Context &ctx, uint32_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_uint_with_context, value)
}

auto Value::Create(Context &ctx, uint64_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_ubigint_with_context, value)
}

auto Value::Create(Context &ctx, uint128_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_uhugeint_with_context, ToC(value))
}

auto Value::Create(Context &ctx, int8_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_tinyint_with_context, value)
}

auto Value::Create(Context &ctx, int16_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_smallint_with_context, value)
}

auto Value::Create(Context &ctx, int32_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_int_with_context, value)
}

auto Value::Create(Context &ctx, int64_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_bigint_with_context, value)
}

auto Value::Create(Context &ctx, int128_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_hugeint_with_context, ToC(value))
}

auto Value::Create(Context &ctx, float value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_float_with_context, value)
}

auto Value::Create(Context &ctx, double value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_double_with_context, value)
}

auto Value::Create(Context &ctx, blob_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_blob_with_context, ToStr(value))
}

auto Value::Create(Context &ctx, varchar_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_varchar_with_context, ToStr(value))
}

auto Value::Create(Context &ctx, const LogicalType &value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_type_with_context, value.handle())
}

//----------------------------------------------------------------------------------------------------------------------
// Temporal Values
//----------------------------------------------------------------------------------------------------------------------

// The wrapper types exist to name a SQL type that a plain integer cannot:
// TIMESTAMP, TIMESTAMP_S and TIMESTAMP_NS are all one int64. They carry the
// type's own unit, so nothing is converted here.

template <>
auto Value::Get() const -> date_t {
	int32_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_date, handle(), &payload);
	return date_t {payload};
}

auto Value::Create(Connection &conn, date_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_date_with_connection, value.days)
}

auto Value::Create(Context &ctx, date_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_date_with_context, value.days)
}

template <>
auto Value::Get() const -> dtime_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_time, handle(), &payload);
	return dtime_t {payload};
}

auto Value::Create(Connection &conn, dtime_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_time_with_connection, value.micros)
}

auto Value::Create(Context &ctx, dtime_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_time_with_context, value.micros)
}

template <>
auto Value::Get() const -> dtime_ns_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_time_ns, handle(), &payload);
	return dtime_ns_t {payload};
}

auto Value::Create(Connection &conn, dtime_ns_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_time_ns_with_connection, value.nanos)
}

auto Value::Create(Context &ctx, dtime_ns_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_time_ns_with_context, value.nanos)
}

template <>
auto Value::Get() const -> dtime_tz_t {
	uint64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_time_tz, handle(), &payload);
	return dtime_tz_t(payload);
}

auto Value::Create(Connection &conn, dtime_tz_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_time_tz_with_connection, value.GetBits())
}

auto Value::Create(Context &ctx, dtime_tz_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_time_tz_with_context, value.GetBits())
}

template <>
auto Value::Get() const -> timestamp_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp, handle(), &payload);
	return timestamp_t {payload};
}

auto Value::Create(Connection &conn, timestamp_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_with_connection, value.micros)
}

auto Value::Create(Context &ctx, timestamp_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_with_context, value.micros)
}

template <>
auto Value::Get() const -> timestamp_s_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp_sec, handle(), &payload);
	return timestamp_s_t {payload};
}

auto Value::Create(Connection &conn, timestamp_s_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_sec_with_connection, value.seconds)
}

auto Value::Create(Context &ctx, timestamp_s_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_sec_with_context, value.seconds)
}

template <>
auto Value::Get() const -> timestamp_ms_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp_ms, handle(), &payload);
	return timestamp_ms_t {payload};
}

auto Value::Create(Connection &conn, timestamp_ms_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_ms_with_connection, value.millis)
}

auto Value::Create(Context &ctx, timestamp_ms_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_ms_with_context, value.millis)
}

template <>
auto Value::Get() const -> timestamp_ns_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp_ns, handle(), &payload);
	return timestamp_ns_t {payload};
}

auto Value::Create(Connection &conn, timestamp_ns_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_ns_with_connection, value.nanos)
}

auto Value::Create(Context &ctx, timestamp_ns_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_ns_with_context, value.nanos)
}

template <>
auto Value::Get() const -> timestamp_tz_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp_tz, handle(), &payload);
	return timestamp_tz_t {payload};
}

auto Value::Create(Connection &conn, timestamp_tz_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_tz_with_connection, value.micros)
}

auto Value::Create(Context &ctx, timestamp_tz_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_tz_with_context, value.micros)
}

template <>
auto Value::Get() const -> timestamp_tz_ns_t {
	int64_t payload = 0;
	CheckedAPICall(duckdb_v2_value_get_timestamp_tz_ns, handle(), &payload);
	return timestamp_tz_ns_t {payload};
}

auto Value::Create(Connection &conn, timestamp_tz_ns_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_timestamp_tz_ns_with_connection, value.nanos)
}

auto Value::Create(Context &ctx, timestamp_tz_ns_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_timestamp_tz_ns_with_context, value.nanos)
}

auto ToC(interval_t value) -> duckdb_v2_interval_t {
	return duckdb_v2_interval_t {value.months, value.days, value.micros};
}

template <>
auto Value::Get() const -> interval_t {
	duckdb_v2_interval_t payload {};
	CheckedAPICall(duckdb_v2_value_get_interval, handle(), &payload);
	return interval_t {payload.months, payload.days, payload.micros};
}

auto Value::Create(Connection &conn, interval_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_interval_with_connection, ToC(value))
}

auto Value::Create(Context &ctx, interval_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_interval_with_context, ToC(value))
}

void Value::GetDecimal(int128_t &out, uint8_t width, uint8_t scale) const {
	duckdb_v2_hugeint_t payload {};
	uint8_t actual_width = 0;
	uint8_t actual_scale = 0;
	CheckedAPICall(duckdb_v2_value_get_decimal, handle(), &payload, &actual_width, &actual_scale);
	if (actual_width != width || actual_scale != scale) {
		throw InvalidInputException("Get<width, scale>: value is DECIMAL(" + std::to_string(actual_width) + ", " +
		                            std::to_string(actual_scale) + "), not DECIMAL(" + std::to_string(width) + ", " +
		                            std::to_string(scale) + ")");
	}
	out = FromC(payload);
}

auto Value::CreateDecimal(Connection &conn, int128_t value, uint8_t width, uint8_t scale) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_decimal_with_connection, conn.handle(), ToC(value), width, scale, &out);
	return detail::Factory::Make<Value>(out);
}

auto Value::CreateDecimal(Context &ctx, int128_t value, uint8_t width, uint8_t scale) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_decimal_with_context, ctx.handle(), ToC(value), width, scale, &out);
	return detail::Factory::Make<Value>(out);
}

//----------------------------------------------------------------------------------------------------------------------
// BIT, BIGNUM and UUID
//----------------------------------------------------------------------------------------------------------------------

// Each is a lens over a payload that already has a C representation, so the
// constructors and getters reuse those entry points; what the types add is a
// distinct name for overload resolution and the decode the storage needs.

auto bignum_t::Decode() const -> Decoded {
	const auto *bytes = reinterpret_cast<const uint8_t *>(data());
	Decoded out {};
	idx_t length = 0;
	CheckedAPICall(duckdb_v2_bignum_decode, bytes, static_cast<idx_t>(size()), nullptr, static_cast<idx_t>(0), &length,
	               &out.is_negative);
	out.magnitude.resize(length);
	CheckedAPICall(duckdb_v2_bignum_decode, bytes, static_cast<idx_t>(size()), out.magnitude.data(),
	               static_cast<idx_t>(out.magnitude.size()), &length, &out.is_negative);
	return out;
}

auto bignum_t::Encode(const Decoded &value) -> std::vector<uint8_t> {
	idx_t length = 0;
	CheckedAPICall(duckdb_v2_bignum_encode, value.magnitude.data(), static_cast<idx_t>(value.magnitude.size()),
	               value.is_negative, nullptr, static_cast<idx_t>(0), &length);
	std::vector<uint8_t> storage(length);
	CheckedAPICall(duckdb_v2_bignum_encode, value.magnitude.data(), static_cast<idx_t>(value.magnitude.size()),
	               value.is_negative, storage.data(), static_cast<idx_t>(storage.size()), &length);
	return storage;
}

template <>
auto Value::Get() const -> bit_t {
	duckdb_v2_str payload;
	CheckedAPICall(duckdb_v2_value_get_blob, handle(), &payload);
	return bit_t(payload.ptr, static_cast<uint32_t>(payload.len));
}

template <>
auto Value::Get() const -> bignum_t {
	duckdb_v2_str payload;
	CheckedAPICall(duckdb_v2_value_get_blob, handle(), &payload);
	return bignum_t(payload.ptr, static_cast<uint32_t>(payload.len));
}

template <>
auto Value::Get() const -> uuid_t {
	duckdb_v2_hugeint_t payload {};
	CheckedAPICall(duckdb_v2_value_get_uuid, handle(), &payload);
	return uuid_t(FromC(payload));
}

auto Value::Create(Connection &conn, bit_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_bit_with_connection, ToStr(value))
}

auto Value::Create(Context &ctx, bit_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_bit_with_context, ToStr(value))
}

auto Value::Create(Connection &conn, bignum_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_bignum_with_connection, ToStr(value))
}

auto Value::Create(Context &ctx, bignum_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_bignum_with_context, ToStr(value))
}

auto Value::Create(Connection &conn, uuid_t value) -> Value {
	MAKE_VALUE_IMPL(conn, duckdb_v2_value_create_uuid_with_connection, ToC(value.value))
}

auto Value::Create(Context &ctx, uuid_t value) -> Value {
	MAKE_VALUE_IMPL(ctx, duckdb_v2_value_create_uuid_with_context, ToC(value.value))
}

//----------------------------------------------------------------------------------------------------------------------
// Composite Value Constructors
//----------------------------------------------------------------------------------------------------------------------

// Each is a straight forward to its C entry point: the child types are
// resolved engine-side, so nothing is assembled here beyond flattening the
// borrowed children into handle arrays.
namespace {

auto ChildHandles(const std::vector<Value> &values) -> std::vector<duckdb_v2_value_handle> {
	std::vector<duckdb_v2_value_handle> handles;
	handles.reserve(values.size());
	for (const auto &value : values) {
		handles.push_back(value.handle());
	}
	return handles;
}

// Empty vectors have no data(), and the C API takes NULL for an empty array.
template <class T>
auto DataOrNull(const std::vector<T> &values) -> const T * {
	return values.empty() ? nullptr : values.data();
}

auto FromHandle(duckdb_v2_value_handle value) -> Value {
	return detail::Factory::Make<Value>(value);
}

} // namespace

auto Value::CreateList(Connection &conn, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_list_with_connection, conn.handle(), nullptr, DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

auto Value::CreateList(Context &ctx, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_list_with_context, ctx.handle(), nullptr, DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

auto Value::CreateList(Connection &conn, const LogicalType &child_type) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_list_with_connection, conn.handle(), child_type.handle(), nullptr,
	               static_cast<idx_t>(0), &out);
	return FromHandle(out);
}

auto Value::CreateList(Context &ctx, const LogicalType &child_type) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_list_with_context, ctx.handle(), child_type.handle(), nullptr,
	               static_cast<idx_t>(0), &out);
	return FromHandle(out);
}

auto Value::CreateArray(Connection &conn, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_array_with_connection, conn.handle(), nullptr, DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

auto Value::CreateArray(Context &ctx, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_array_with_context, ctx.handle(), nullptr, DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

namespace {

// STRUCT crosses as parallel name and child arrays.
struct StructArrays {
	explicit StructArrays(const std::vector<std::pair<std::string, Value>> &values) {
		names.reserve(values.size());
		children.reserve(values.size());
		for (const auto &field : values) {
			names.push_back(ToStr(field.first));
			children.push_back(field.second.handle());
		}
	}
	std::vector<duckdb_v2_identifier_t> names;
	std::vector<duckdb_v2_value_handle> children;
};

} // namespace

auto Value::CreateStruct(Connection &conn, NamedValueList values) -> Value {
	StructArrays split(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_struct_with_connection, conn.handle(), DataOrNull(split.names),
	               DataOrNull(split.children), static_cast<idx_t>(values.size()), &out);
	return FromHandle(out);
}

auto Value::CreateStruct(Context &ctx, NamedValueList values) -> Value {
	StructArrays split(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_struct_with_context, ctx.handle(), DataOrNull(split.names),
	               DataOrNull(split.children), static_cast<idx_t>(values.size()), &out);
	return FromHandle(out);
}

auto Value::CreateTuple(Connection &conn, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_tuple_with_connection, conn.handle(), DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

auto Value::CreateTuple(Context &ctx, ValueList values) -> Value {
	auto children = ChildHandles(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_tuple_with_context, ctx.handle(), DataOrNull(children),
	               static_cast<idx_t>(children.size()), &out);
	return FromHandle(out);
}

namespace {

// MAP crosses as parallel key and value arrays.
struct MapArrays {
	explicit MapArrays(const std::vector<std::pair<Value, Value>> &values) {
		keys.reserve(values.size());
		entries.reserve(values.size());
		for (const auto &entry : values) {
			keys.push_back(entry.first.handle());
			entries.push_back(entry.second.handle());
		}
	}
	std::vector<duckdb_v2_value_handle> keys;
	std::vector<duckdb_v2_value_handle> entries;
};

} // namespace

auto Value::CreateMap(Connection &conn, KeyValueList values) -> Value {
	MapArrays split(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_map_with_connection, conn.handle(), nullptr, nullptr, DataOrNull(split.keys),
	               DataOrNull(split.entries), static_cast<idx_t>(values.size()), &out);
	return FromHandle(out);
}

auto Value::CreateMap(Context &ctx, KeyValueList values) -> Value {
	MapArrays split(values);
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_map_with_context, ctx.handle(), nullptr, nullptr, DataOrNull(split.keys),
	               DataOrNull(split.entries), static_cast<idx_t>(values.size()), &out);
	return FromHandle(out);
}

auto Value::CreateMap(Connection &conn, const LogicalType &key_type, const LogicalType &value_type) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_map_with_connection, conn.handle(), key_type.handle(), value_type.handle(),
	               nullptr, nullptr, static_cast<idx_t>(0), &out);
	return FromHandle(out);
}

auto Value::CreateMap(Context &ctx, const LogicalType &key_type, const LogicalType &value_type) -> Value {
	duckdb_v2_value_handle out = nullptr;
	CheckedAPICall(duckdb_v2_value_create_map_with_context, ctx.handle(), key_type.handle(), value_type.handle(),
	               nullptr, nullptr, static_cast<idx_t>(0), &out);
	return FromHandle(out);
}

//----------------------------------------------------------------------------------------------------------------------
// String Heap
//----------------------------------------------------------------------------------------------------------------------
// blob_t mirrors duckdb_v2_bytes; pin it here (both types visible) so any
// layout drift breaks the build rather than the ABI.
static_assert(sizeof(blob_t) == sizeof(duckdb_v2_bytes) && alignof(blob_t) == alignof(duckdb_v2_bytes),
              "blob_t must mirror the C ABI's duckdb_v2_bytes");
static_assert(offsetof(blob_t, value.pointer.length) == offsetof(duckdb_v2_bytes, value.pointer.length) &&
                  offsetof(blob_t, value.pointer.prefix) == offsetof(duckdb_v2_bytes, value.pointer.prefix) &&
                  offsetof(blob_t, value.pointer.ptr) == offsetof(duckdb_v2_bytes, value.pointer.ptr) &&
                  offsetof(blob_t, value.inlined.inlined) == offsetof(duckdb_v2_bytes, value.inlined.inlined),
              "blob_t field offsets must match duckdb_v2_bytes");
static_assert(blob_t::INLINE_LENGTH == DUCKDB_V2_BYTES_INLINE_LENGTH,
              "blob_t::INLINE_LENGTH must match DUCKDB_V2_BYTES_INLINE_LENGTH");

Arena::Arena(void *impl) : detail::Handle<Arena>(impl) {
}

Arena::~Arena() {
	/* String heaps are always borrowed, so we don't destroy the handle here */
}

auto Arena::Allocate(idx_t byte_len) -> uint8_t * {
	uint8_t *ptr = nullptr;
	CheckedAPICall(duckdb_v2_arena_allocate, handle(), byte_len, &ptr);
	return ptr;
}

auto Arena::ThrowStringTooLong(idx_t size) -> void {
	throw Exception(DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE, "Out of Range Error: string length " + std::to_string(size) +
	                                                        " exceeds the maximum a duckdb_v2_bytes can hold");
}

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------
// VectorType mirrors DUCKDB_V2_VECTOR_TYPE numerically; trip here if either
// side is renumbered.
static_assert(static_cast<uint8_t>(VectorType::OTHER) == DUCKDB_V2_VECTOR_TYPE_OTHER,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::FLAT) == DUCKDB_V2_VECTOR_TYPE_FLAT,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::CONSTANT) == DUCKDB_V2_VECTOR_TYPE_CONSTANT,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::DICTIONARY) == DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");

// VectorView mirrors duckdb_v2_vector_view. GetView copies it field-for-field,
// so the compiler checks the pointer types; this pins the one typedef the
// header cannot see.
static_assert(std::is_same<duckdb_v2_sel_t, uint32_t>::value, "VectorView::sel must mirror duckdb_v2_sel_t");

Vector::Vector(void *impl) : detail::Handle<Vector>(impl) {
}

Vector::~Vector() {
	/* Vectors are always borrowed, so we don't destroy the handle here */
}

auto Vector::GetDataMutable() -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_data_mutable, handle(), &data);
	return data;
}

auto Vector::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_vector_get_child_count, handle(), &count);
	return count;
}

auto Vector::GetChild(idx_t index) const -> Vector {
	duckdb_v2_vector_handle child = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_child, handle(), index, &child);
	return detail::Factory::Make<Vector>(child);
}

auto Vector::Flatten() const -> void {
	CheckedAPICall(duckdb_v2_vector_flatten, handle());
}

auto Vector::GetSize() const -> idx_t {
	idx_t size = 0;
	CheckedAPICall(duckdb_v2_vector_get_size, handle(), &size);
	return size;
}

auto Vector::SetSize(idx_t size) -> void {
	CheckedAPICall(duckdb_v2_vector_set_size, handle(), size);
}

auto Vector::GetView() const -> VectorView {
	duckdb_v2_vector_view view {};
	CheckedAPICall(duckdb_v2_vector_get_view, handle(), &view);
	return VectorView {view.data, view.validity, view.sel, view.count};
}

auto Vector::GetVectorType() const -> VectorType {
	DUCKDB_V2_VECTOR_TYPE type = DUCKDB_V2_VECTOR_TYPE_OTHER;
	CheckedAPICall(duckdb_v2_vector_get_vector_type, handle(), &type);
	return static_cast<VectorType>(type);
}

auto Vector::GetValidityMutable() -> ValidityMask {
	uint64_t *words = nullptr;
	CheckedAPICall(duckdb_v2_vector_flat_get_validity_mutable, handle(), &words);
	return ValidityMask {words};
}

auto Vector::SetNull(idx_t row) -> void {
	CheckedAPICall(duckdb_v2_vector_set_null, handle(), row);
}

auto Vector::SetConstantValid(bool valid) -> void {
	CheckedAPICall(duckdb_v2_vector_constant_set_valid, handle(), valid);
}

auto Vector::MakeConstant(const Value &value, idx_t count) -> void {
	CheckedAPICall(duckdb_v2_vector_make_constant, handle(), value.handle(), count);
}

auto Vector::MakeSequence(int64_t start, int64_t increment, idx_t count) -> void {
	CheckedAPICall(duckdb_v2_vector_make_sequence, handle(), start, increment, count);
}

auto Vector::GetValue(idx_t row) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_value, handle(), row, &value);
	return detail::Factory::Make<Value>(value);
}

auto Vector::SetValue(idx_t row, const Value &value) -> void {
	CheckedAPICall(duckdb_v2_vector_set_value, handle(), row, value.handle());
}

// --- end single-cell value bridge ---

auto Vector::CheckWriteRange(idx_t start, idx_t count) const -> void {
	if (count == 0) {
		return;
	}
	// A CONSTANT vector's data array holds a single slot; only index 0 is writable.
	if (GetVectorType() == VectorType::CONSTANT && (start != 0 || count > 1)) {
		throw InvalidInputException("Invalid Input Error: cannot assign a string to a CONSTANT vector at index != 0");
	}
}

auto Vector::AssignString(idx_t index, std::string_view data) -> void {
	CheckWriteRange(index, 1);
	auto heap = GetHeap();
	GetDataMutable<blob_t>()[index] = heap.AddString(data);
}

auto Vector::GetHeap() -> Arena {
	duckdb_v2_arena_handle heap = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_arena, handle(), &heap);
	return detail::Factory::Make<Arena>(heap);
}

auto Vector::SetString(idx_t index, varchar_t value) -> void {
	CheckWriteRange(index, 1);
	GetDataMutable<varchar_t>()[index] = value;
}

//----------------------------------------------------------------------------------------------------------------------
// Data Chunk
//----------------------------------------------------------------------------------------------------------------------
DataChunk::DataChunk(const std::vector<LogicalType> &types) {
	// LogicalType is a Handle (with a vtable), so its storage is not layout-compatible with a raw
	// duckdb_v2_logical_type_handle array. Extract the underlying handles into a contiguous buffer.
	std::vector<duckdb_v2_logical_type_handle> type_pointers;
	type_pointers.reserve(types.size());
	for (const auto &type : types) {
		type_pointers.push_back(type.handle());
	}

	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_create, type_pointers.data(), type_pointers.size(), &chunk);

	impl = chunk;
	owned = true;
}

DataChunk::DataChunk(void *impl, bool owned) : detail::Handle<DataChunk>(impl), owned(owned) {
}

DataChunk::~DataChunk() {
	if (owned) {
		auto _h = handle();
		duckdb_v2_data_chunk_destroy(&_h);
	}
}

auto DataChunk::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_size, handle(), &count);
	return count;
}

auto DataChunk::GetVectorCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector_count, handle(), &count);
	return count;
}

auto DataChunk::GetVector(idx_t index) const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector, handle(), index, &vector);
	return detail::Factory::Make<Vector>(vector);
}

//----------------------------------------------------------------------------------------------------------------------
// Query Result
//----------------------------------------------------------------------------------------------------------------------

QueryResult::QueryResult(void *impl) : detail::Handle<QueryResult>(impl) {
}

QueryResult::~QueryResult() {
	auto _h = handle();
	duckdb_v2_result_destroy(&_h);
}

auto QueryResult::GetSchema() const -> Schema {
	duckdb_v2_schema_handle schema = nullptr;
	CheckedAPICall(duckdb_v2_result_get_schema, handle(), &schema);
	return detail::Factory::Make<Schema>(schema);
}

// ResultType mirrors DUCKDB_V2_RESULT_TYPE numerically; every member is pinned.
static_assert(static_cast<uint8_t>(QueryResult::ResultType::QUERY_RESULT) == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");
static_assert(static_cast<uint8_t>(QueryResult::ResultType::CHANGED_ROWS) == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");
static_assert(static_cast<uint8_t>(QueryResult::ResultType::NOTHING) == DUCKDB_V2_RESULT_TYPE_NOTHING,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");

// StatementType mirrors DUCKDB_V2_STATEMENT_TYPE numerically; every member is pinned.
#define DUCKDB_CPP_ASSERT_STATEMENT_TYPE(member)                                                                       \
	static_assert(static_cast<uint8_t>(QueryResult::StatementType::member) == DUCKDB_V2_STATEMENT_TYPE_##member,       \
	              "StatementType::" #member " must mirror DUCKDB_V2_STATEMENT_TYPE_" #member)
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(INVALID);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(SELECT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(INSERT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(UPDATE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CREATE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DELETE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(PREPARE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXECUTE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ALTER);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(TRANSACTION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(COPY);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ANALYZE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(VARIABLE_SET);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CREATE_FUNC);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXPLAIN);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DROP);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXPORT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(PRAGMA);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(VACUUM);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CALL);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(SET);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(LOAD);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(RELATION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXTENSION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(LOGICAL_PLAN);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ATTACH);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DETACH);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(MULTI);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(COPY_DATABASE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(UPDATE_EXTENSIONS);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(MERGE_INTO);
#undef DUCKDB_CPP_ASSERT_STATEMENT_TYPE

auto QueryResult::GetResultType() const -> ResultType {
	DUCKDB_V2_RESULT_TYPE type = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	CheckedAPICall(duckdb_v2_result_get_result_type, handle(), &type);
	return static_cast<ResultType>(type);
}

auto QueryResult::GetStatementType() const -> StatementType {
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CheckedAPICall(duckdb_v2_result_get_statement_type, handle(), &type);
	return static_cast<StatementType>(type);
}

// StepStatus mirrors DUCKDB_V2_RESULT_STEP_STATUS numerically; trip here if
// either side is renumbered.
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::WAITING) == DUCKDB_V2_RESULT_STEP_STATUS_WAITING,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::CHUNK) == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::FINISHED) == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::CANCELLED) == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");

auto QueryResult::Step() -> StepResult {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	CheckedAPICall(duckdb_v2_result_step, handle(), &chunk, &status);
	return StepResult {static_cast<StepStatus>(status), detail::Factory::Make<DataChunk>(chunk, chunk != nullptr)};
}

auto QueryResult::Wait() -> void {
	CheckedAPICall(duckdb_v2_result_wait, handle());
}

auto QueryResult::FetchChunk() -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_result_fetch_chunk, handle(), &chunk);
	// An empty handle marks end-of-stream.
	return detail::Factory::Make<DataChunk>(chunk, chunk != nullptr);
}

auto QueryResult::Drain() -> idx_t {
	idx_t rows_changed = 0;
	CheckedAPICall(duckdb_v2_result_drain, handle(), &rows_changed);
	return rows_changed;
}

auto QueryResult::RenderBox(idx_t max_rows, idx_t max_width, idx_t max_col_width, const std::string &null_value,
                            idx_t render_mode, idx_t limit) -> std::string {
	auto raw = handle();
	this->release();
	std::string out;

	auto sink = [](duckdb_v2_str text, void *user_data, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&] {
			if (text.ptr && text.len) {
				static_cast<std::string *>(user_data)->append(text.ptr, text.len);
			}
		});
	};

	CheckedAPICall(duckdb_v2_result_render_box, &raw, max_rows, max_width, max_col_width, ToStr(null_value),
	               render_mode, limit, sink, &out);
	return out;
}

} // namespace cxx
} // namespace duckdb
