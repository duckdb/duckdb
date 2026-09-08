// Include the C++ API header (which includes the C API header)
#include "duckdb_cpp.hpp"

#include <atomic>
#include <cctype>
#include <cstring>
#include <memory>

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
struct HandleTraits<PreparedStatement> {
	using handle = duckdb_v2_prepared_statement_handle;
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
struct HandleTraits<Expression> {
	using handle = duckdb_v2_expression_handle;
};
template <>
struct HandleTraits<ColumnDataCollection> {
	using handle = duckdb_v2_column_data_collection_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::AppendState> {
	using handle = duckdb_v2_column_data_collection_append_state_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::SharedScanState> {
	using handle = duckdb_v2_column_data_collection_shared_scan_state_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::WorkerScanState> {
	using handle = duckdb_v2_column_data_collection_worker_scan_state_handle;
};
template <>
struct HandleTraits<QueryResult> {
	using handle = duckdb_v2_result_handle;
};
template <>
struct HandleTraits<ArrowImporter> {
	using handle = duckdb_v2_arrow_importer_handle;
};
template <>
struct HandleTraits<ArrowExporter> {
	using handle = duckdb_v2_arrow_exporter_handle;
};
template <>
struct HandleTraits<FunctionSignature> {
	using handle = duckdb_v2_function_signature_handle;
};
template <>
struct HandleTraits<ScalarFunction> {
	using handle = duckdb_v2_scalar_function_handle;
};
template <>
struct HandleTraits<AggregateFunction> {
	using handle = duckdb_v2_aggregate_function_handle;
};
template <>
struct HandleTraits<TableFunction> {
	using handle = duckdb_v2_table_function_handle;
};
template <>
struct HandleTraits<CopyFunction> {
	using handle = duckdb_v2_copy_function_handle;
};
template <>
struct HandleTraits<CustomType> {
	using handle = duckdb_v2_custom_type_handle;
};
template <>
struct HandleTraits<CastFunction> {
	using handle = duckdb_v2_cast_function_handle;
};
template <>
struct HandleTraits<ReplacementScan> {
	using handle = duckdb_v2_replacement_scan_handle;
};
template <>
struct HandleTraits<QualifiedName> {
	using handle = duckdb_v2_qname_handle;
};
template <>
struct HandleTraits<TableDescription> {
	using handle = duckdb_v2_table_description_handle;
};
template <>
struct HandleTraits<ColumnDescription> {
	using handle = duckdb_v2_column_description_handle;
};
template <>
struct HandleTraits<FileSystem> {
	using handle = duckdb_v2_file_system_handle;
};
template <>
struct HandleTraits<FileHandle> {
	using handle = duckdb_v2_file_handle;
};
template <>
struct HandleTraits<FileOpenOptions> {
	using handle = duckdb_v2_file_open_options_handle;
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

auto RenderQuotedIdentifier(std::string_view name) -> std::string {
	return RenderText(duckdb_v2_identifier_render_quoted, duckdb_v2_identifier_t {name.data(), name.size()});
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

auto Connection::CreateType(std::string_view name) -> LogicalType {
	return CreateType(name, {});
}

auto Connection::CreateType(std::string_view name, const std::vector<TypeParam> &params) -> LogicalType {
	return CreateType(QualifiedName::Create({std::string(name)}), params);
}

auto Connection::GetFileSystem() const -> FileSystem {
	duckdb_v2_file_system_handle fs = nullptr;
	CheckedAPICall(duckdb_v2_file_system_get_from_connection, handle(), &fs);
	return detail::Factory::Make<FileSystem>(fs);
}

auto Connection::CreateType(const QualifiedName &name, const std::vector<TypeParam> &params) -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_connection_create_type_from_name, handle(), name.handle(), split.names(), split.values(),
	               static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Connection::CreateType(LogicalTypeId id) -> LogicalType {
	return CreateType(id, {});
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

//----------------------------------------------------------------------------------------------------------------------
// Prepared statements
//----------------------------------------------------------------------------------------------------------------------

PreparedStatement::PreparedStatement(void *impl) : detail::Handle<PreparedStatement>(impl) {
}

PreparedStatement::~PreparedStatement() {
	auto _h = handle();
	duckdb_v2_prepared_statement_destroy(&_h);
}

auto Connection::Prepare(const SqlStatement &statement, bool require_cacheable) -> PreparedStatement {
	// Borrowed, not consumed: the caller's SqlStatement keeps ownership and can be
	// prepared or executed again.
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	CheckedAPICall(duckdb_v2_prepared_statement_create, handle(), statement.handle(), require_cacheable, &prepared);
	return detail::Factory::Make<PreparedStatement>(prepared);
}

auto PreparedStatement::Execute(const Value *parameters, idx_t parameter_count) -> QueryResult {
	std::vector<duckdb_v2_value_handle> values;
	values.reserve(parameter_count);
	for (idx_t i = 0; i < parameter_count; i++) {
		values.push_back(parameters[i].handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_prepared_statement_execute, handle(), nullptr, parameter_count ? values.data() : nullptr,
	               parameter_count, &result);
	return detail::Factory::Make<QueryResult>(result);
}

auto PreparedStatement::Execute(const std::vector<NamedParam> &parameters) -> QueryResult {
	// Split into the C API's parallel arrays; an empty name crosses as the positional
	// {NULL, 0} view (mirrors Connection::Execute).
	std::vector<duckdb_v2_identifier_t> names;
	std::vector<duckdb_v2_value_handle> values;
	names.reserve(parameters.size());
	values.reserve(parameters.size());
	for (const auto &param : parameters) {
		names.push_back(param.name.empty() ? duckdb_v2_identifier_t {nullptr, 0} : ToStr(param.name));
		values.push_back(param.value.handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_prepared_statement_execute, handle(), names.empty() ? nullptr : names.data(),
	               values.empty() ? nullptr : values.data(), static_cast<idx_t>(parameters.size()), &result);
	return detail::Factory::Make<QueryResult>(result);
}

auto PreparedStatement::Execute() -> QueryResult {
	return Execute(nullptr, 0);
}

auto PreparedStatement::ReusesPlan() const -> bool {
	bool reuses = false;
	CheckedAPICall(duckdb_v2_prepared_statement_reuses_plan, handle(), &reuses);
	return reuses;
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

auto Context::CreateType(std::string_view name) const -> LogicalType {
	return CreateType(name, {});
}

auto Context::CreateType(std::string_view name, const std::vector<TypeParam> &params) const -> LogicalType {
	return CreateType(QualifiedName::Create({std::string(name)}), params);
}

auto Context::GetFileSystem() const -> FileSystem {
	duckdb_v2_file_system_handle fs = nullptr;
	CheckedAPICall(duckdb_v2_file_system_get_from_context, handle(), &fs);
	return detail::Factory::Make<FileSystem>(fs);
}

auto Context::CreateType(const QualifiedName &name, const std::vector<TypeParam> &params) const -> LogicalType {
	TypeParamArrays split(params);
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_context_create_type_from_name, handle(), name.handle(), split.names(), split.values(),
	               static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Context::CreateType(LogicalTypeId id) const -> LogicalType {
	return CreateType(id, {});
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

auto Connection::DescribeTable(const QualifiedName &name) const -> TableDescription {
	duckdb_v2_table_description_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_connection_describe_table, handle(), name.handle(), &_h);
	return detail::Factory::Make<TableDescription>(_h);
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
namespace {
// LogicalType is a Handle (with a vtable), so its storage is not layout-compatible with a raw
// duckdb_v2_logical_type_handle array. Extract the underlying handles into a contiguous buffer.
auto ExtractTypeHandles(const std::vector<LogicalType> &types) -> std::vector<duckdb_v2_logical_type_handle> {
	std::vector<duckdb_v2_logical_type_handle> type_pointers;
	type_pointers.reserve(types.size());
	for (const auto &type : types) {
		type_pointers.push_back(type.handle());
	}
	return type_pointers;
}
} // namespace

DataChunk::DataChunk(const std::vector<LogicalType> &types) {
	const auto type_pointers = ExtractTypeHandles(types);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_create, type_pointers.data(), type_pointers.size(), &chunk);

	impl = chunk;
	owned = true;
}

DataChunk::DataChunk(const Connection &conn, const std::vector<LogicalType> &types) {
	const auto type_pointers = ExtractTypeHandles(types);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_create_with_connection, conn.handle(), type_pointers.data(),
	               type_pointers.size(), &chunk);

	impl = chunk;
	owned = true;
}

DataChunk::DataChunk(const Context &ctx, const std::vector<LogicalType> &types) {
	const auto type_pointers = ExtractTypeHandles(types);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_create_with_context, ctx.handle(), type_pointers.data(), type_pointers.size(),
	               &chunk);

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

auto DataChunk::Copy(const Connection &conn) const -> DataChunk {
	duckdb_v2_data_chunk_handle copy = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_copy_with_connection, conn.handle(), handle(), &copy);
	return detail::Factory::Make<DataChunk>(copy, true);
}

auto DataChunk::Copy(const Context &ctx) const -> DataChunk {
	duckdb_v2_data_chunk_handle copy = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_copy_with_context, ctx.handle(), handle(), &copy);
	return detail::Factory::Make<DataChunk>(copy, true);
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
// Column Data Collection
//----------------------------------------------------------------------------------------------------------------------

ColumnDataCollection::AppendState::AppendState(void *impl) : detail::Handle<AppendState>(impl) {
}

ColumnDataCollection::AppendState::~AppendState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_append_state_destroy(&_h);
}

ColumnDataCollection::SharedScanState::SharedScanState(void *impl) : detail::Handle<SharedScanState>(impl) {
}

ColumnDataCollection::SharedScanState::~SharedScanState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_shared_scan_state_destroy(&_h);
}

ColumnDataCollection::WorkerScanState::WorkerScanState(void *impl) : detail::Handle<WorkerScanState>(impl) {
}

ColumnDataCollection::WorkerScanState::~WorkerScanState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_worker_scan_state_destroy(&_h);
}

ColumnDataCollection::ColumnDataCollection(const Connection &conn, const std::vector<LogicalType> &types) {
	const auto type_pointers = ExtractTypeHandles(types);
	duckdb_v2_column_data_collection_handle collection = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_create_with_connection, conn.handle(), type_pointers.data(),
	               type_pointers.size(), &collection);
	impl = collection;
}

ColumnDataCollection::ColumnDataCollection(const Context &ctx, const std::vector<LogicalType> &types) {
	const auto type_pointers = ExtractTypeHandles(types);
	duckdb_v2_column_data_collection_handle collection = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_create_with_context, ctx.handle(), type_pointers.data(),
	               type_pointers.size(), &collection);
	impl = collection;
}

ColumnDataCollection::ColumnDataCollection(void *impl) : detail::Handle<ColumnDataCollection>(impl) {
}

ColumnDataCollection::~ColumnDataCollection() {
	auto _h = handle();
	duckdb_v2_column_data_collection_destroy(&_h);
}

auto ColumnDataCollection::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_column_data_collection_row_count, handle(), &count);
	return count;
}

auto ColumnDataCollection::Reset() -> void {
	CheckedAPICall(duckdb_v2_column_data_collection_reset, handle());
}

auto ColumnDataCollection::Clear() -> void {
	CheckedAPICall(duckdb_v2_column_data_collection_clear, handle());
}

auto ColumnDataCollection::Combine(ColumnDataCollection &&source) -> void {
	auto source_handle = source.handle();
	CheckedAPICall(duckdb_v2_column_data_collection_combine, handle(), &source_handle);
	// The C API consumed the source; detach the wrapper so its destructor does not double-free. A refused merge threw
	// above and leaves the source intact.
	source.release();
}

auto ColumnDataCollection::CreateAppendState() -> AppendState {
	duckdb_v2_column_data_collection_append_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_append_state_create, handle(), &state);
	return detail::Factory::Make<AppendState>(state);
}

auto ColumnDataCollection::Append(AppendState &state, const DataChunk &chunk) -> void {
	CheckedAPICall(duckdb_v2_column_data_collection_append, handle(), state.handle(), chunk.handle());
}

auto ColumnDataCollection::Append(const DataChunk &chunk) -> void {
	auto state = CreateAppendState();
	Append(state, chunk);
}

auto ColumnDataCollection::CreateSharedScanState() const -> SharedScanState {
	duckdb_v2_column_data_collection_shared_scan_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_shared_scan_state_create, handle(), &state);
	return detail::Factory::Make<SharedScanState>(state);
}

auto ColumnDataCollection::CreateWorkerScanState() const -> WorkerScanState {
	duckdb_v2_column_data_collection_worker_scan_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_worker_scan_state_create, handle(), &state);
	return detail::Factory::Make<WorkerScanState>(state);
}

auto ColumnDataCollection::Scan(SharedScanState &shared, WorkerScanState &worker, DataChunk &chunk) const -> bool {
	bool did_produce = false;
	CheckedAPICall(duckdb_v2_column_data_collection_scan, handle(), shared.handle(), worker.handle(), chunk.handle(),
	               &did_produce);
	return did_produce;
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

auto QueryResult::ToArrowStream(idx_t batch_size) -> ArrowStream {
	// Allocate before detaching: if this throws, the result is still ours and ~QueryResult
	// frees it.
	auto *stream = new ArrowArrayStream {};
	auto raw = handle();
	// The C call takes the result by transfer, consuming it on success and failure alike, so
	// detach now and ~QueryResult will not double-free it.
	this->release();
	try {
		CheckedAPICall(duckdb_v2_result_to_arrow_stream, &raw, batch_size, stream);
	} catch (...) {
		delete stream;
		throw;
	}
	return detail::Factory::Make<ArrowStream>(stream);
}

//----------------------------------------------------------------------------------------------------------------------
// Arrow
//----------------------------------------------------------------------------------------------------------------------

ArrowImporter::ArrowImporter(void *impl) : detail::Handle<ArrowImporter>(impl) {
}

ArrowImporter::ArrowImporter(const Context &context, ArrowSchema &schema, idx_t batch_size)
    : detail::Handle<ArrowImporter>(nullptr) {
	duckdb_v2_arrow_importer_handle importer = nullptr;
	CheckedAPICall(duckdb_v2_arrow_importer_create, context.handle(), &schema, batch_size, &importer);
	impl = importer;
}

ArrowImporter::~ArrowImporter() {
	auto _h = handle();
	duckdb_v2_arrow_importer_destroy(&_h);
}

auto ArrowImporter::GetSchema() const -> Schema {
	duckdb_v2_schema_handle schema = nullptr;
	CheckedAPICall(duckdb_v2_arrow_importer_get_schema, handle(), &schema);
	return detail::Factory::Make<Schema>(schema);
}

auto ArrowImporter::Append(ArrowArray &array, bool consume, bool flush) -> void {
	CheckedAPICall(duckdb_v2_arrow_importer_append, handle(), &array, consume, flush);
}

auto ArrowImporter::Flush() -> void {
	CheckedAPICall(duckdb_v2_arrow_importer_append, handle(), static_cast<ArrowArray *>(nullptr), false, true);
}

auto ArrowImporter::NextChunk() -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_arrow_importer_next_chunk, handle(), &chunk);
	// A null handle, meaning the array is drained, becomes an empty DataChunk.
	return detail::Factory::Make<DataChunk>(chunk, true);
}

ArrowExporter::ArrowExporter(void *impl) : detail::Handle<ArrowExporter>(impl) {
}

ArrowExporter::ArrowExporter(const Context &context, const std::vector<LogicalType> &types,
                             const std::vector<std::string> &names, idx_t batch_size)
    : detail::Handle<ArrowExporter>(nullptr) {
	// LogicalType is a Handle with a vtable, so its storage is not layout-compatible with a raw
	// handle array; extract into contiguous buffers.
	std::vector<duckdb_v2_logical_type_handle> type_handles;
	std::vector<duckdb_v2_str> name_views;
	type_handles.reserve(types.size());
	name_views.reserve(names.size());
	for (const auto &type : types) {
		type_handles.push_back(type.handle());
	}
	for (const auto &name : names) {
		name_views.push_back(ToStr(name));
	}
	if (type_handles.size() != name_views.size()) {
		throw InvalidInputException("ArrowExporter: one name is required per column type");
	}
	duckdb_v2_arrow_exporter_handle exporter = nullptr;
	CheckedAPICall(duckdb_v2_arrow_exporter_create, context.handle(),
	               type_handles.empty() ? nullptr : type_handles.data(),
	               name_views.empty() ? nullptr : name_views.data(), static_cast<idx_t>(type_handles.size()),
	               batch_size, &exporter);
	impl = exporter;
}

ArrowExporter::~ArrowExporter() {
	auto _h = handle();
	duckdb_v2_arrow_exporter_destroy(&_h);
}

auto ArrowExporter::GetSchema(ArrowSchema &out) const -> void {
	CheckedAPICall(duckdb_v2_arrow_exporter_get_schema, handle(), &out);
}

auto ArrowExporter::Append(const DataChunk &chunk, bool flush) -> void {
	// consume is false, so the C call leaves the chunk alone; the copy of the handle only gives it a slot to read.
	auto raw = chunk.handle();
	CheckedAPICall(duckdb_v2_arrow_exporter_append, handle(), &raw, false, flush);
}

auto ArrowExporter::Flush() -> void {
	CheckedAPICall(duckdb_v2_arrow_exporter_append, handle(), static_cast<duckdb_v2_data_chunk_handle *>(nullptr),
	               false, true);
}

auto ArrowExporter::NextArray(ArrowArray &out) -> bool {
	CheckedAPICall(duckdb_v2_arrow_exporter_next_array, handle(), &out);
	return out.release != nullptr;
}

ArrowStream::~ArrowStream() {
	if (stream) {
		if (stream->release) {
			stream->release(stream);
		}
		delete stream;
	}
}

// The Arrow C stream interface reports failure only as an errno-style int with no error code, so both calls below
// surface a generic INVALID_INPUT; the detail comes from get_last_error and is carried in the message.
void ArrowStream::GetSchema(ArrowSchema &out) const {
	if (!stream || !stream->release) {
		throw InvalidInputException("ArrowStream::GetSchema on an empty stream");
	}
	if (stream->get_schema(stream, &out) != 0) {
		const char *msg = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
		throw InvalidInputException(msg ? msg : "Arrow stream get_schema failed");
	}
}

bool ArrowStream::Next(ArrowArray &out) const {
	out.release = nullptr;
	if (!stream || !stream->release) {
		throw InvalidInputException("ArrowStream::Next on an empty stream");
	}
	if (stream->get_next(stream, &out) != 0) {
		const char *msg = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
		throw InvalidInputException(msg ? msg : "Arrow stream get_next failed");
	}
	return out.release != nullptr;
}

//----------------------------------------------------------------------------------------------------------------------
// Function Signature
//----------------------------------------------------------------------------------------------------------------------

FunctionSignature::FunctionSignature(void *impl) : detail::Handle<FunctionSignature>(impl) {
}

FunctionSignature::~FunctionSignature() {
	// The signature is borrowed from its function, so we don't destroy the handle here
}

auto FunctionSignature::AddParameter(const std::string &name, const LogicalType &type) -> FunctionSignature & {
	CheckedAPICall(duckdb_v2_function_signature_add_parameter, handle(), ToStr(name), type.handle(),
	               static_cast<duckdb_v2_value_handle>(nullptr));
	return *this;
}

auto FunctionSignature::AddParameter(const std::string &name, const LogicalType &type, const Value &default_value)
    -> FunctionSignature & {
	CheckedAPICall(duckdb_v2_function_signature_add_parameter, handle(), ToStr(name), type.handle(),
	               default_value.handle());
	return *this;
}

auto FunctionSignature::SetVarArgs(const LogicalType &type) -> FunctionSignature & {
	CheckedAPICall(duckdb_v2_function_signature_set_varargs, handle(), type.handle());
	return *this;
}

auto FunctionSignature::SetReturnType(const LogicalType &type) -> FunctionSignature & {
	CheckedAPICall(duckdb_v2_function_signature_set_return_type, handle(), type.handle());
	return *this;
}

//----------------------------------------------------------------------------------------------------------------------
// Function Properties
//----------------------------------------------------------------------------------------------------------------------

namespace {

// Conversions from the typed C++ property enums to the C API's generic
// (key, value) channel. Every C++ enumerator maps to exactly one C value.

auto ToCValue(FunctionStability value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case FunctionStability::CONSISTENT:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;
	case FunctionStability::VOLATILE:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE;
	case FunctionStability::CONSISTENT_WITHIN_QUERY:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;
}

auto ToCValue(FunctionNullHandling value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case FunctionNullHandling::DEFAULT:
		return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT;
	case FunctionNullHandling::SPECIAL:
		return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT;
}

auto ToCValue(FunctionFallibility value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case FunctionFallibility::INFALLIBLE:
		return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE;
	case FunctionFallibility::FALLIBLE:
		return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE;
}

auto ToCValue(FunctionCollationHandling value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case FunctionCollationHandling::PROPAGATE:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE;
	case FunctionCollationHandling::PUSH_COMBINABLE:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE;
	case FunctionCollationHandling::IGNORE:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE;
}

auto ToCValue(AggregateFunction::OrderDependence value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case AggregateFunction::OrderDependence::DEPENDENT:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;
	case AggregateFunction::OrderDependence::INDEPENDENT:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;
}

auto ToCValue(AggregateFunction::DistinctDependence value) -> DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
	switch (value) {
	case AggregateFunction::DistinctDependence::DEPENDENT:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES;
	case AggregateFunction::DistinctDependence::INDEPENDENT:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES;
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered scalar function: rides the C user_data
// slot so the trampolines can find it; the user's own slot (SetUserData) rides
// inside it. Owned by the registered function, freed at engine teardown.
struct ScalarFunctionInfo {
	ScalarFunction::BindCallback bind_callback = nullptr;
	ScalarFunction::InitCallback init_callback = nullptr;
	ScalarFunction::ExecCallback exec_callback = nullptr;
	detail::UserData user_data;

	ScalarFunctionInfo(ScalarFunction::BindCallback bind_callback, ScalarFunction::InitCallback init_callback,
	                   ScalarFunction::ExecCallback exec_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), init_callback(init_callback), exec_callback(exec_callback),
	      user_data(std::move(user_data)) {
	}

	bool operator==(const ScalarFunctionInfo &other) const {
		return bind_callback == other.bind_callback && init_callback == other.init_callback &&
		       exec_callback == other.exec_callback && user_data.get() == other.user_data.get();
	}
};

// Guard for the inputs' GetUserData: a clear error instead of a null deref.
void *RequireUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call ScalarFunction::SetUserData before Register");
	}
	return ptr;
}

// Guard for the inputs' GetBindData: a clear error instead of a null deref.
void *RequireBindData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no bind data was set; call BindInput::SetBindData in the bind callback");
	}
	return ptr;
}

// Guard for ExecInput::GetInitData: a clear error instead of a null deref.
void *RequireInitData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no init data was set; call InitInput::SetInitData in the init callback");
	}
	return ptr;
}

} // namespace

ScalarFunction::ScalarFunction(void *impl) : detail::Handle<ScalarFunction>(impl) {
}

ScalarFunction::~ScalarFunction() {
	auto _h = handle();
	duckdb_v2_scalar_function_destroy(&_h);
}

auto ScalarFunction::Create(const Connection &conn) -> ScalarFunction {
	duckdb_v2_scalar_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<ScalarFunction>(_h);
}

auto ScalarFunction::Create(const Extension &extension) -> ScalarFunction {
	duckdb_v2_scalar_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<ScalarFunction>(_h);
}

auto ScalarFunction::SetName(const std::string &name) & -> ScalarFunction & {
	auto view = ToStr(name);
	CheckedAPICall(duckdb_v2_scalar_function_set_name, handle(), &view);
	return *this;
}

auto ScalarFunction::GetSignature() -> FunctionSignature {
	duckdb_v2_function_signature_handle sig = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_signature, handle(), &sig);
	return detail::Factory::Make<FunctionSignature>(sig);
}

auto ScalarFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto ScalarFunction::SetBindCallback(BindCallback callback) & -> ScalarFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_scalar_function_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_scalar_function_bind_get_user_data, info, &user_data);
			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<BindInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_set_bind_callback, handle(), trampoline);
	bind_callback = callback;
	return *this;
}

auto ScalarFunction::SetInitCallback(InitCallback callback) & -> ScalarFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_scalar_function_set_init_callback, handle(), nullptr);
		init_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_scalar_function_init_get_user_data, info, &user_data);
			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<InitInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_set_init_callback, handle(), trampoline);
	init_callback = callback;
	return *this;
}

auto ScalarFunction::SetExecCallback(ExecCallback callback) & -> ScalarFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_scalar_function_set_exec_callback, handle(), nullptr);
		exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_scalar_function_exec_get_user_data, info, &user_data);
			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<ExecInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_set_exec_callback, handle(), trampoline);
	exec_callback = callback;
	return *this;
}

auto ScalarFunction::SetStability(FunctionStability value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               ToCValue(value));
	return *this;
}

auto ScalarFunction::SetNullHandling(FunctionNullHandling value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
	               ToCValue(value));
	return *this;
}

auto ScalarFunction::SetFallibility(FunctionFallibility value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               ToCValue(value));
	return *this;
}

auto ScalarFunction::SetCollationHandling(FunctionCollationHandling value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING,
	               ToCValue(value));
	return *this;
}

auto ScalarFunction::Register() -> void {
	// The callback table rides the C user_data slot so the trampolines can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<ScalarFunctionInfo>(
	    new ScalarFunctionInfo(bind_callback, init_callback, exec_callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<ScalarFunctionInfo>,
	                         detail::TypedEquals<ScalarFunctionInfo>};
	CheckedAPICall(duckdb_v2_scalar_function_set_user_data, handle(), &opaque);
	// The function owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_scalar_function_register, handle());
}

void *ScalarFunction::BindInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_bind_get_user_data,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), &user_data);
	const auto &function = *static_cast<const ScalarFunctionInfo *>(user_data);
	return RequireUserData(function.user_data);
}

void ScalarFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                    void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, equals};
	CheckedAPICall(duckdb_v2_scalar_function_bind_set_bind_data,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), &opaque);
}

auto ScalarFunction::BindInput::GetArgCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_scalar_function_bind_get_arg_count,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), &count);
	return count;
}

auto ScalarFunction::BindInput::GetArgType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_bind_get_arg_type,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto ScalarFunction::BindInput::GetConstantArgument(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_bind_get_arg_value,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), index, &value);
	return detail::Factory::Make<Value>(value);
}

auto ScalarFunction::BindInput::TryGetConstantArgument(idx_t index) const -> std::optional<Value> {
	duckdb_v2_value_handle value = nullptr;
	// No error slot: an argument without a constant value is absence here, not a failure to report.
	const auto code = duckdb_v2_scalar_function_bind_get_arg_value(
	    static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), index, &value, nullptr);
	if (code != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto ScalarFunction::BindInput::SetReturnType(const LogicalType &type) -> void {
	CheckedAPICall(duckdb_v2_scalar_function_bind_set_return_type,
	               static_cast<duckdb_v2_scalar_function_bind_info_handle>(args), type.handle());
}

auto ScalarFunction::BindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *ScalarFunction::InitInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_init_get_bind_data,
	               static_cast<duckdb_v2_scalar_function_init_info_handle>(args), &bind_data);
	return RequireBindData(bind_data);
}

void ScalarFunction::InitInput::SetInitDataInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_scalar_function_init_set_init_data,
	               static_cast<duckdb_v2_scalar_function_init_info_handle>(args), &opaque);
}

void *ScalarFunction::InitInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_init_get_user_data,
	               static_cast<duckdb_v2_scalar_function_init_info_handle>(args), &user_data);
	const auto &function = *static_cast<const ScalarFunctionInfo *>(user_data);
	return RequireUserData(function.user_data);
}

auto ScalarFunction::InitInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *ScalarFunction::ExecInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_bind_data,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &bind_data);
	return RequireBindData(bind_data);
}

void *ScalarFunction::ExecInput::GetInitDataInternal() const {
	void *init_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_init_data,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &init_data);
	return RequireInitData(init_data);
}

void *ScalarFunction::ExecInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_user_data,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &user_data);
	const auto &function = *static_cast<const ScalarFunctionInfo *>(user_data);
	return RequireUserData(function.user_data);
}

auto ScalarFunction::ExecInput::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_row_count,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &count);
	return count;
}

auto ScalarFunction::ExecInput::GetArgCount() const -> idx_t {
	uint32_t count = 0;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_arg_count,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &count);
	return count;
}

auto ScalarFunction::ExecInput::GetArg(idx_t index) const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_arg,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), static_cast<uint32_t>(index),
	               &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto ScalarFunction::ExecInput::GetResult() const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_exec_get_result,
	               static_cast<duckdb_v2_scalar_function_exec_info_handle>(args), &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto ScalarFunction::ExecInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

//----------------------------------------------------------------------------------------------------------------------
// Aggregate Function
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered aggregate function: rides the C
// user_data slot so the trampolines can find it; the user's own slot
// (SetUserData) rides inside it. Owned by the registered function, freed at
// engine teardown.
struct AggregateFunctionInfo {
	AggregateFunction::BindCallback bind_callback = nullptr;
	AggregateFunction::SizeCallback size_callback = nullptr;
	AggregateFunction::InitCallback init_callback = nullptr;
	AggregateFunction::UpdateCallback update_callback = nullptr;
	AggregateFunction::CombineCallback combine_callback = nullptr;
	AggregateFunction::FinalizeCallback finalize_callback = nullptr;
	AggregateFunction::DestroyCallback destroy_callback = nullptr;
	detail::UserData user_data;

	AggregateFunctionInfo(AggregateFunction::BindCallback bind_callback, AggregateFunction::SizeCallback size_callback,
	                      AggregateFunction::InitCallback init_callback,
	                      AggregateFunction::UpdateCallback update_callback,
	                      AggregateFunction::CombineCallback combine_callback,
	                      AggregateFunction::FinalizeCallback finalize_callback,
	                      AggregateFunction::DestroyCallback destroy_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), size_callback(size_callback), init_callback(init_callback),
	      update_callback(update_callback), combine_callback(combine_callback), finalize_callback(finalize_callback),
	      destroy_callback(destroy_callback), user_data(std::move(user_data)) {
	}

	bool operator==(const AggregateFunctionInfo &other) const {
		return bind_callback == other.bind_callback && size_callback == other.size_callback &&
		       init_callback == other.init_callback && update_callback == other.update_callback &&
		       combine_callback == other.combine_callback && finalize_callback == other.finalize_callback &&
		       destroy_callback == other.destroy_callback && user_data.get() == other.user_data.get();
	}
};

// Guard for the inputs' GetUserData: a clear error instead of a null deref.
void *RequireAggregateUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call AggregateFunction::SetUserData before Register");
	}
	return ptr;
}

// Guard for the inputs' GetBindData: a clear error instead of a null deref.
void *RequireAggregateBindData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no bind data was set; call BindInput::SetBindData in the bind callback");
	}
	return ptr;
}

} // namespace

AggregateFunction::AggregateFunction(void *impl) : detail::Handle<AggregateFunction>(impl) {
}

AggregateFunction::~AggregateFunction() {
	auto _h = handle();
	duckdb_v2_aggregate_function_destroy(&_h);
}

auto AggregateFunction::Create(const Connection &conn) -> AggregateFunction {
	duckdb_v2_aggregate_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<AggregateFunction>(_h);
}

auto AggregateFunction::Create(const Extension &extension) -> AggregateFunction {
	duckdb_v2_aggregate_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<AggregateFunction>(_h);
}

auto AggregateFunction::SetName(const std::string &name) & -> AggregateFunction & {
	auto view = ToStr(name);
	CheckedAPICall(duckdb_v2_aggregate_function_set_name, handle(), &view);
	return *this;
}

auto AggregateFunction::GetSignature() -> FunctionSignature {
	duckdb_v2_function_signature_handle sig = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_get_signature, handle(), &sig);
	return detail::Factory::Make<FunctionSignature>(sig);
}

auto AggregateFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto AggregateFunction::SetBindCallback(BindCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_bind_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<BindInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_bind_callback, handle(), trampoline);
	bind_callback = callback;
	return *this;
}

auto AggregateFunction::SetSizeCallback(SizeCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_size_callback, handle(), nullptr);
		size_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_size_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<SizeInput>(static_cast<void *>(info));
			function.size_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_size_callback, handle(), trampoline);
	size_callback = callback;
	return *this;
}

auto AggregateFunction::SetInitCallback(InitCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_init_callback, handle(), nullptr);
		init_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_init_info_handle info, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_init_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<InitInput>(static_cast<void *>(info));
			function.init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_init_callback, handle(), trampoline);
	init_callback = callback;
	return *this;
}

auto AggregateFunction::SetUpdateCallback(UpdateCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_update_callback, handle(), nullptr);
		update_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_update_info_handle info,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_update_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<UpdateInput>(static_cast<void *>(info));
			function.update_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_update_callback, handle(), trampoline);
	update_callback = callback;
	return *this;
}

auto AggregateFunction::SetCombineCallback(CombineCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_combine_callback, handle(), nullptr);
		combine_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_combine_info_handle info,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_combine_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<CombineInput>(static_cast<void *>(info));
			function.combine_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_combine_callback, handle(), trampoline);
	combine_callback = callback;
	return *this;
}

auto AggregateFunction::SetFinalizeCallback(FinalizeCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_finalize_callback, handle(), nullptr);
		finalize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_finalize_info_handle info,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<FinalizeInput>(static_cast<void *>(info));
			function.finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_finalize_callback, handle(), trampoline);
	finalize_callback = callback;
	return *this;
}

auto AggregateFunction::SetDestroyCallback(DestroyCallback callback) & -> AggregateFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_aggregate_function_set_destroy_callback, handle(), nullptr);
		destroy_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_destroy_info_handle info,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_aggregate_function_destroy_get_user_data, info, &user_data);
			const auto &function = *static_cast<AggregateFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<DestroyInput>(static_cast<void *>(info));
			function.destroy_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_set_destroy_callback, handle(), trampoline);
	destroy_callback = callback;
	return *this;
}

auto AggregateFunction::SetStability(FunctionStability value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               ToCValue(value));
	return *this;
}

auto AggregateFunction::SetNullHandling(FunctionNullHandling value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
	               ToCValue(value));
	return *this;
}

auto AggregateFunction::SetFallibility(FunctionFallibility value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               ToCValue(value));
	return *this;
}

auto AggregateFunction::SetCollationHandling(FunctionCollationHandling value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING,
	               ToCValue(value));
	return *this;
}

auto AggregateFunction::SetOrderDependence(OrderDependence value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
	               ToCValue(value));
	return *this;
}

auto AggregateFunction::SetDistinctDependence(DistinctDependence value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT, ToCValue(value));
	return *this;
}

auto AggregateFunction::Register() -> void {
	// The callback table rides the C user_data slot so the trampolines can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<AggregateFunctionInfo>(
	    new AggregateFunctionInfo(bind_callback, size_callback, init_callback, update_callback, combine_callback,
	                              finalize_callback, destroy_callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<AggregateFunctionInfo>,
	                         detail::TypedEquals<AggregateFunctionInfo>};
	CheckedAPICall(duckdb_v2_aggregate_function_set_user_data, handle(), &opaque);
	// The function owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_aggregate_function_register, handle());
}

void *AggregateFunction::BindInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_bind_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

void AggregateFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                       void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, equals};
	CheckedAPICall(duckdb_v2_aggregate_function_bind_set_bind_data,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), &opaque);
}

auto AggregateFunction::BindInput::GetArgCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_bind_get_arg_count,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::BindInput::GetArgType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_bind_get_arg_type,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto AggregateFunction::BindInput::GetConstantArgument(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_bind_get_arg_value,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), index, &value);
	return detail::Factory::Make<Value>(value);
}

auto AggregateFunction::BindInput::TryGetConstantArgument(idx_t index) const -> std::optional<Value> {
	duckdb_v2_value_handle value = nullptr;
	// No error slot: an argument without a constant value is absence here, not a failure to report.
	const auto code = duckdb_v2_aggregate_function_bind_get_arg_value(
	    static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), index, &value, nullptr);
	if (code != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto AggregateFunction::BindInput::SetReturnType(const LogicalType &type) -> void {
	CheckedAPICall(duckdb_v2_aggregate_function_bind_set_return_type,
	               static_cast<duckdb_v2_aggregate_function_bind_info_handle>(args), type.handle());
}

auto AggregateFunction::BindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *AggregateFunction::SizeInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_size_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_size_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::SizeInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_size_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_size_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::SizeInput::SetStateSize(idx_t size) -> void {
	CheckedAPICall(duckdb_v2_aggregate_function_size_set_state_size,
	               static_cast<duckdb_v2_aggregate_function_size_info_handle>(args), size);
}

void *AggregateFunction::InitInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_init_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_init_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::InitInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_init_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_init_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::InitInput::GetStateCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_init_get_state_count,
	               static_cast<duckdb_v2_aggregate_function_init_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::InitInput::GetStates() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_init_get_states,
	               static_cast<duckdb_v2_aggregate_function_init_info_handle>(args), &states);
	return states;
}

void *AggregateFunction::UpdateInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::UpdateInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::UpdateInput::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_row_count,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::UpdateInput::GetArgCount() const -> idx_t {
	uint32_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_arg_count,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::UpdateInput::GetArg(idx_t index) const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_arg,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), static_cast<uint32_t>(index),
	               &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto AggregateFunction::UpdateInput::GetStates() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_update_get_states,
	               static_cast<duckdb_v2_aggregate_function_update_info_handle>(args), &states);
	return states;
}

void *AggregateFunction::CombineInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_combine_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_combine_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::CombineInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_combine_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_combine_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::CombineInput::GetStateCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_combine_get_state_count,
	               static_cast<duckdb_v2_aggregate_function_combine_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::CombineInput::GetSources() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_combine_get_sources,
	               static_cast<duckdb_v2_aggregate_function_combine_info_handle>(args), &states);
	return states;
}

auto AggregateFunction::CombineInput::GetTargets() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_combine_get_targets,
	               static_cast<duckdb_v2_aggregate_function_combine_info_handle>(args), &states);
	return states;
}

void *AggregateFunction::FinalizeInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::FinalizeInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::FinalizeInput::GetStateCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_state_count,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::FinalizeInput::GetStates() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_states,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &states);
	return states;
}

auto AggregateFunction::FinalizeInput::GetResult() const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_result,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto AggregateFunction::FinalizeInput::GetResultOffset() const -> idx_t {
	idx_t offset = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_finalize_get_result_offset,
	               static_cast<duckdb_v2_aggregate_function_finalize_info_handle>(args), &offset);
	return offset;
}

void *AggregateFunction::DestroyInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_destroy_get_bind_data,
	               static_cast<duckdb_v2_aggregate_function_destroy_info_handle>(args), &bind_data);
	return RequireAggregateBindData(bind_data);
}

void *AggregateFunction::DestroyInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_destroy_get_user_data,
	               static_cast<duckdb_v2_aggregate_function_destroy_info_handle>(args), &user_data);
	const auto &function = *static_cast<const AggregateFunctionInfo *>(user_data);
	return RequireAggregateUserData(function.user_data);
}

auto AggregateFunction::DestroyInput::GetStateCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_aggregate_function_destroy_get_state_count,
	               static_cast<duckdb_v2_aggregate_function_destroy_info_handle>(args), &count);
	return count;
}

auto AggregateFunction::DestroyInput::GetStates() const -> void ** {
	void **states = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_destroy_get_states,
	               static_cast<duckdb_v2_aggregate_function_destroy_info_handle>(args), &states);
	return states;
}

//----------------------------------------------------------------------------------------------------------------------
// Table Function
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered table function: rides the C user_data
// slot so the trampolines can find it; the user's own slot (SetUserData) rides
// inside it. Owned by the registered function, freed at engine teardown.
struct TableFunctionInfo {
	TableFunction::BindCallback bind_callback = nullptr;
	TableFunction::InitGlobalCallback init_global_callback = nullptr;
	TableFunction::InitLocalCallback init_local_callback = nullptr;
	TableFunction::ExecCallback exec_callback = nullptr;
	TableFunction::ProgressCallback progress_callback = nullptr;
	TableFunction::FilterPushdownCallback filter_pushdown_callback = nullptr;
	detail::UserData user_data;

	TableFunctionInfo(TableFunction::BindCallback bind_callback, TableFunction::InitGlobalCallback init_global_callback,
	                  TableFunction::InitLocalCallback init_local_callback, TableFunction::ExecCallback exec_callback,
	                  TableFunction::ProgressCallback progress_callback,
	                  TableFunction::FilterPushdownCallback filter_pushdown_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), init_global_callback(init_global_callback),
	      init_local_callback(init_local_callback), exec_callback(exec_callback), progress_callback(progress_callback),
	      filter_pushdown_callback(filter_pushdown_callback), user_data(std::move(user_data)) {
	}

	bool operator==(const TableFunctionInfo &other) const {
		return bind_callback == other.bind_callback && init_global_callback == other.init_global_callback &&
		       init_local_callback == other.init_local_callback && exec_callback == other.exec_callback &&
		       progress_callback == other.progress_callback &&
		       filter_pushdown_callback == other.filter_pushdown_callback && user_data.get() == other.user_data.get();
	}
};

// Guard for the inputs' GetUserData: a clear error instead of a null deref.
void *RequireTableUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call TableFunction::SetUserData before Register");
	}
	return ptr;
}

// Guard for the inputs' GetBindData: a clear error instead of a null deref.
void *RequireTableBindData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no bind data was set; call BindInput::SetBindData in the bind callback");
	}
	return ptr;
}

// Guard for the inputs' GetGlobalState: a clear error instead of a null deref.
void *RequireGlobalState(void *ptr) {
	if (!ptr) {
		throw InvalidInputException(
		    "no global state was set; call InitGlobalInput::SetGlobalState in the global init callback");
	}
	return ptr;
}

// Guard for ExecInput::GetLocalState: a clear error instead of a null deref.
void *RequireLocalState(void *ptr) {
	if (!ptr) {
		throw InvalidInputException(
		    "no local state was set; call InitLocalInput::SetLocalState in the local init callback");
	}
	return ptr;
}

} // namespace

TableFunction::TableFunction(void *impl) : detail::Handle<TableFunction>(impl) {
}

TableFunction::~TableFunction() {
	auto _h = handle();
	duckdb_v2_table_function_destroy(&_h);
}

auto TableFunction::Create(const Connection &conn) -> TableFunction {
	duckdb_v2_table_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_table_function_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<TableFunction>(_h);
}

auto TableFunction::Create(const Extension &extension) -> TableFunction {
	duckdb_v2_table_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_table_function_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<TableFunction>(_h);
}

auto TableFunction::SetName(const std::string &name) & -> TableFunction & {
	auto view = ToStr(name);
	CheckedAPICall(duckdb_v2_table_function_set_name, handle(), &view);
	return *this;
}

auto TableFunction::GetSignature() -> FunctionSignature {
	duckdb_v2_function_signature_handle sig = nullptr;
	CheckedAPICall(duckdb_v2_table_function_get_signature, handle(), &sig);
	return detail::Factory::Make<FunctionSignature>(sig);
}

auto TableFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto TableFunction::SetBindCallback(BindCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_bind_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<BindInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_bind_callback, handle(), trampoline);
	bind_callback = callback;
	return *this;
}

auto TableFunction::SetInitGlobalCallback(InitGlobalCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_init_global_callback, handle(), nullptr);
		init_global_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_init_global_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<InitGlobalInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.init_global_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_init_global_callback, handle(), trampoline);
	init_global_callback = callback;
	return *this;
}

auto TableFunction::SetInitLocalCallback(InitLocalCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_init_local_callback, handle(), nullptr);
		init_local_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_init_local_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<InitLocalInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.init_local_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_init_local_callback, handle(), trampoline);
	init_local_callback = callback;
	return *this;
}

auto TableFunction::SetExecCallback(ExecCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_exec_callback, handle(), nullptr);
		exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_exec_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<ExecInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_exec_callback, handle(), trampoline);
	exec_callback = callback;
	return *this;
}

auto TableFunction::SetProgressCallback(ProgressCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_progress_callback, handle(), nullptr);
		progress_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_progress_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_progress_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<ProgressInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.progress_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_progress_callback, handle(), trampoline);
	progress_callback = callback;
	return *this;
}

auto TableFunction::SetFilterPushdownCallback(FilterPushdownCallback callback) & -> TableFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_table_function_set_filter_pushdown_callback, handle(), nullptr);
		filter_pushdown_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_filter_pushdown_info_handle info,
	                            duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_user_data, info, &user_data);
			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<FilterPushdownInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.filter_pushdown_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_set_filter_pushdown_callback, handle(), trampoline);
	filter_pushdown_callback = callback;
	return *this;
}

auto TableFunction::SetProjectionPushdown(bool enable) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_set_projection_pushdown, handle(), enable);
	return *this;
}

auto TableFunction::Register() -> void {
	// The callback table rides the C user_data slot so the trampolines can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<TableFunctionInfo>(
	    new TableFunctionInfo(bind_callback, init_global_callback, init_local_callback, exec_callback,
	                          progress_callback, filter_pushdown_callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<TableFunctionInfo>,
	                         detail::TypedEquals<TableFunctionInfo>};
	CheckedAPICall(duckdb_v2_table_function_set_user_data, handle(), &opaque);
	// The function owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_table_function_register, handle());
}

auto TableFunction::BindInput::AddResultColumn(const std::string &name, const LogicalType &type) -> void {
	CheckedAPICall(duckdb_v2_table_function_bind_add_result_column,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args),
	               duckdb_v2_identifier_t {name.data(), name.size()}, type.handle());
}

void TableFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                   void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, equals};
	CheckedAPICall(duckdb_v2_table_function_bind_set_bind_data,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), &opaque);
}

void *TableFunction::BindInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_user_data,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::BindInput::GetArgCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_bind_get_arg_count,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), &count);
	return count;
}

auto TableFunction::BindInput::GetArgType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_arg_type,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto TableFunction::BindInput::GetArgument(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_arg_value,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), index, &value);
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::SetCardinality(idx_t cardinality, bool is_exact) -> void {
	CheckedAPICall(duckdb_v2_table_function_bind_set_cardinality,
	               static_cast<duckdb_v2_table_function_bind_info_handle>(args), cardinality, is_exact);
}

auto TableFunction::BindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void TableFunction::InitGlobalInput::SetGlobalStateInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_table_function_init_global_set_global_state,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), &opaque);
}

void *TableFunction::InitGlobalInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_global_get_bind_data,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), &bind_data);
	return RequireTableBindData(bind_data);
}

void *TableFunction::InitGlobalInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_global_get_user_data,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::InitGlobalInput::SetMaxThreads(idx_t max_threads) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_global_set_max_threads,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), max_threads);
}

auto TableFunction::InitGlobalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

auto TableFunction::InitGlobalInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_init_global_get_column_count,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), &count);
	return count;
}

auto TableFunction::InitGlobalInput::GetColumnIndex(idx_t index) const -> idx_t {
	idx_t column_index = 0;
	CheckedAPICall(duckdb_v2_table_function_init_global_get_column_index,
	               static_cast<duckdb_v2_table_function_init_global_info_handle>(args), index, &column_index);
	return column_index;
}

void TableFunction::InitLocalInput::SetLocalStateInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_table_function_init_local_set_local_state,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), &opaque);
}

void *TableFunction::InitLocalInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_local_get_bind_data,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), &bind_data);
	return RequireTableBindData(bind_data);
}

void *TableFunction::InitLocalInput::GetGlobalStateInternal() const {
	void *global_state = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_local_get_global_state,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), &global_state);
	return RequireGlobalState(global_state);
}

void *TableFunction::InitLocalInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_local_get_user_data,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::InitLocalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

auto TableFunction::InitLocalInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_init_local_get_column_count,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), &count);
	return count;
}

auto TableFunction::InitLocalInput::GetColumnIndex(idx_t index) const -> idx_t {
	idx_t column_index = 0;
	CheckedAPICall(duckdb_v2_table_function_init_local_get_column_index,
	               static_cast<duckdb_v2_table_function_init_local_info_handle>(args), index, &column_index);
	return column_index;
}

void *TableFunction::ExecInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_bind_data,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &bind_data);
	return RequireTableBindData(bind_data);
}

void *TableFunction::ExecInput::GetGlobalStateInternal() const {
	void *global_state = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_global_state,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &global_state);
	return RequireGlobalState(global_state);
}

void *TableFunction::ExecInput::GetLocalStateInternal() const {
	void *local_state = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_local_state,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &local_state);
	return RequireLocalState(local_state);
}

void *TableFunction::ExecInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_user_data,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::ExecInput::GetOutputChunk() const -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_output_chunk,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &chunk);
	// Borrowed: the engine owns the chunk and reuses it across invocations.
	return detail::Factory::Make<DataChunk>(chunk, false);
}

auto TableFunction::ExecInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

auto TableFunction::ExecInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_exec_get_column_count,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), &count);
	return count;
}

auto TableFunction::ExecInput::GetColumnIndex(idx_t index) const -> idx_t {
	idx_t column_index = 0;
	CheckedAPICall(duckdb_v2_table_function_exec_get_column_index,
	               static_cast<duckdb_v2_table_function_exec_info_handle>(args), index, &column_index);
	return column_index;
}

void *TableFunction::ProgressInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_progress_get_bind_data,
	               static_cast<duckdb_v2_table_function_progress_info_handle>(args), &bind_data);
	return RequireTableBindData(bind_data);
}

void *TableFunction::ProgressInput::GetGlobalStateInternal() const {
	void *global_state = nullptr;
	CheckedAPICall(duckdb_v2_table_function_progress_get_global_state,
	               static_cast<duckdb_v2_table_function_progress_info_handle>(args), &global_state);
	return RequireGlobalState(global_state);
}

void *TableFunction::ProgressInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_progress_get_user_data,
	               static_cast<duckdb_v2_table_function_progress_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::ProgressInput::SetProgress(double progress) -> void {
	CheckedAPICall(duckdb_v2_table_function_progress_set_progress,
	               static_cast<duckdb_v2_table_function_progress_info_handle>(args), progress);
}

auto TableFunction::ProgressInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *TableFunction::FilterPushdownInput::GetBindDataInternal() const {
	void *bind_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_bind_data,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), &bind_data);
	return RequireTableBindData(bind_data);
}

void *TableFunction::FilterPushdownInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_user_data,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), &user_data);
	const auto &function = *static_cast<const TableFunctionInfo *>(user_data);
	return RequireTableUserData(function.user_data);
}

auto TableFunction::FilterPushdownInput::GetFilterCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_filter_count,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), &count);
	return count;
}

auto TableFunction::FilterPushdownInput::GetFilter(idx_t index) const -> Expression {
	duckdb_v2_expression_handle filter = nullptr;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_filter,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), index, &filter);
	return detail::Factory::Make<Expression>(static_cast<void *>(filter));
}

auto TableFunction::FilterPushdownInput::Accept(idx_t index) -> void {
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_accept,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), index);
}

auto TableFunction::FilterPushdownInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_column_count,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), &count);
	return count;
}

auto TableFunction::FilterPushdownInput::GetColumnIndex(idx_t index) const -> idx_t {
	idx_t column_index = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_pushdown_get_column_index,
	               static_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(args), index, &column_index);
	return column_index;
}

auto TableFunction::FilterPushdownInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

//----------------------------------------------------------------------------------------------------------------------
// Bound Expression
//----------------------------------------------------------------------------------------------------------------------

Expression::Expression(void *impl) : detail::Handle<Expression>(impl) {
}

auto Expression::GetType() const -> ExpressionType {
	auto type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
	CheckedAPICall(duckdb_v2_expression_get_type, handle(), &type);
	return static_cast<ExpressionType>(type);
}

auto Expression::GetReturnType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_expression_get_return_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Expression::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_expression_get_child_count, handle(), &count);
	return count;
}

auto Expression::GetChild(idx_t index) const -> Expression {
	duckdb_v2_expression_handle child = nullptr;
	CheckedAPICall(duckdb_v2_expression_get_child, handle(), index, &child);
	return detail::Factory::Make<Expression>(static_cast<void *>(child));
}

auto Expression::GetConstantValue() const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_expression_constant_get_value, handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Expression::GetColumnIndex() const -> idx_t {
	idx_t index = 0;
	CheckedAPICall(duckdb_v2_expression_column_ref_get_index, handle(), &index);
	return index;
}

auto Expression::GetFunctionName() const -> std::string {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_expression_function_get_name, handle(), &name);
	return std::string(FromStr(name));
}

auto Expression::GetFunctionQualifiedName() const -> QualifiedName {
	duckdb_v2_qname_handle name = nullptr;
	CheckedAPICall(duckdb_v2_expression_function_get_qname, handle(), &name);
	return detail::Factory::Make<QualifiedName>(name);
}

auto Expression::GetCastMode() const -> CastMode {
	auto mode = DUCKDB_V2_CAST_MODE_NORMAL;
	CheckedAPICall(duckdb_v2_expression_cast_get_mode, handle(), &mode);
	return static_cast<CastMode>(mode);
}

//----------------------------------------------------------------------------------------------------------------------
// Custom Type
//----------------------------------------------------------------------------------------------------------------------

CustomType::CustomType(void *impl) : detail::Handle<CustomType>(impl) {
}

CustomType::~CustomType() {
	auto _h = handle();
	duckdb_v2_custom_type_destroy(&_h);
}

auto CustomType::Create(const Connection &conn) -> CustomType {
	duckdb_v2_custom_type_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_custom_type_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<CustomType>(_h);
}

auto CustomType::Create(const Extension &extension) -> CustomType {
	duckdb_v2_custom_type_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_custom_type_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<CustomType>(_h);
}

auto CustomType::SetName(const std::string &name) & -> CustomType & {
	CheckedAPICall(duckdb_v2_custom_type_set_name, handle(), ToStr(name));
	return *this;
}

auto CustomType::SetBaseType(const LogicalType &type) & -> CustomType & {
	CheckedAPICall(duckdb_v2_custom_type_set_base_type, handle(), type.handle());
	return *this;
}

auto CustomType::Register() -> void {
	CheckedAPICall(duckdb_v2_custom_type_register, handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Copy Function
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered copy function: rides the C user_data
// slot so the trampolines can find it; the user's own slot (SetUserData) rides
// inside it. Owned by the registered function, freed at engine teardown.
struct CopyFunctionInfo {
	CopyFunction::CopyToBindCallback copy_to_bind_callback = nullptr;
	CopyFunction::CopyToBatchSizeCallback copy_to_batch_size_callback = nullptr;
	CopyFunction::CopyToInitCallback copy_to_init_callback = nullptr;
	CopyFunction::CopyToBatchCallback copy_to_batch_callback = nullptr;
	CopyFunction::CopyToFlushCallback copy_to_flush_callback = nullptr;
	CopyFunction::CopyToFinalizeCallback copy_to_finalize_callback = nullptr;
	CopyFunction::CopyFromBindCallback copy_from_bind_callback = nullptr;
	CopyFunction::CopyFromInitGlobalCallback copy_from_init_global_callback = nullptr;
	CopyFunction::CopyFromInitLocalCallback copy_from_init_local_callback = nullptr;
	CopyFunction::CopyFromExecCallback copy_from_exec_callback = nullptr;
	CopyFunction::CopyFromProgressCallback copy_from_progress_callback = nullptr;
	detail::UserData user_data;

	CopyFunctionInfo(CopyFunction::CopyToBindCallback copy_to_bind_callback,
	                 CopyFunction::CopyToBatchSizeCallback copy_to_batch_size_callback,
	                 CopyFunction::CopyToInitCallback copy_to_init_callback,
	                 CopyFunction::CopyToBatchCallback copy_to_batch_callback,
	                 CopyFunction::CopyToFlushCallback copy_to_flush_callback,
	                 CopyFunction::CopyToFinalizeCallback copy_to_finalize_callback,
	                 CopyFunction::CopyFromBindCallback copy_from_bind_callback,
	                 CopyFunction::CopyFromInitGlobalCallback copy_from_init_global_callback,
	                 CopyFunction::CopyFromInitLocalCallback copy_from_init_local_callback,
	                 CopyFunction::CopyFromExecCallback copy_from_exec_callback,
	                 CopyFunction::CopyFromProgressCallback copy_from_progress_callback, detail::UserData user_data)
	    : copy_to_bind_callback(copy_to_bind_callback), copy_to_batch_size_callback(copy_to_batch_size_callback),
	      copy_to_init_callback(copy_to_init_callback), copy_to_batch_callback(copy_to_batch_callback),
	      copy_to_flush_callback(copy_to_flush_callback), copy_to_finalize_callback(copy_to_finalize_callback),
	      copy_from_bind_callback(copy_from_bind_callback),
	      copy_from_init_global_callback(copy_from_init_global_callback),
	      copy_from_init_local_callback(copy_from_init_local_callback),
	      copy_from_exec_callback(copy_from_exec_callback), copy_from_progress_callback(copy_from_progress_callback),
	      user_data(std::move(user_data)) {
	}

	bool operator==(const CopyFunctionInfo &other) const {
		return copy_to_bind_callback == other.copy_to_bind_callback &&
		       copy_to_batch_size_callback == other.copy_to_batch_size_callback &&
		       copy_to_init_callback == other.copy_to_init_callback &&
		       copy_to_batch_callback == other.copy_to_batch_callback &&
		       copy_to_flush_callback == other.copy_to_flush_callback &&
		       copy_to_finalize_callback == other.copy_to_finalize_callback &&
		       copy_from_bind_callback == other.copy_from_bind_callback &&
		       copy_from_init_global_callback == other.copy_from_init_global_callback &&
		       copy_from_init_local_callback == other.copy_from_init_local_callback &&
		       copy_from_exec_callback == other.copy_from_exec_callback &&
		       copy_from_progress_callback == other.copy_from_progress_callback &&
		       user_data.get() == other.user_data.get();
	}
};

// Guard for the inputs' GetUserData: a clear error instead of a null deref.
void *RequireCopyUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call CopyFunction::SetUserData before Register");
	}
	return ptr;
}

// Guard for the inputs' GetBindData: a clear error instead of a null deref.
void *RequireCopyBindData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no bind data was set; call the bind input's SetBindData in the bind callback");
	}
	return ptr;
}

// Guard for the inputs' GetInitData: a clear error instead of a null deref.
void *RequireCopyInitData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no init data was set; call CopyToInitInput::SetInitData in the init callback");
	}
	return ptr;
}

// Guard for the inputs' GetBatchData: a clear error instead of a null deref.
void *RequireCopyBatchData(void *ptr) {
	if (!ptr) {
		throw InvalidInputException("no batch data was set; call CopyToBatchInput::SetBatchData in the batch callback");
	}
	return ptr;
}

// Guard for the inputs' GetGlobalState: a clear error instead of a null deref.
void *RequireCopyGlobalState(void *ptr) {
	if (!ptr) {
		throw InvalidInputException(
		    "no global state was set; call CopyFromInitGlobalInput::SetGlobalState in the global init callback");
	}
	return ptr;
}

// Guard for the inputs' GetLocalState: a clear error instead of a null deref.
void *RequireCopyLocalState(void *ptr) {
	if (!ptr) {
		throw InvalidInputException(
		    "no local state was set; call CopyFromInitLocalInput::SetLocalState in the local init callback");
	}
	return ptr;
}

} // namespace

CopyFunction::CopyFunction(void *impl) : detail::Handle<CopyFunction>(impl) {
}

CopyFunction::~CopyFunction() {
	auto _h = handle();
	duckdb_v2_copy_function_destroy(&_h);
}

auto CopyFunction::Create(const Connection &conn) -> CopyFunction {
	duckdb_v2_copy_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_copy_function_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<CopyFunction>(_h);
}

auto CopyFunction::Create(const Extension &extension) -> CopyFunction {
	duckdb_v2_copy_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_copy_function_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<CopyFunction>(_h);
}

auto CopyFunction::SetName(const std::string &name) & -> CopyFunction & {
	auto view = ToStr(name);
	CheckedAPICall(duckdb_v2_copy_function_set_name, handle(), &view);
	return *this;
}

auto CopyFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto CopyFunction::SetCopyToBindCallback(CopyToBindCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_bind_callback, handle(), nullptr);
		copy_to_bind_callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_copy_to_bind_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_bind_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToBindInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_bind_callback, handle(), trampoline);
	copy_to_bind_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyToBatchSizeCallback(CopyToBatchSizeCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_batch_size_callback, handle(), nullptr);
		copy_to_batch_size_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_to_batch_size_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_batch_size_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToBatchSizeInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_batch_size_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_batch_size_callback, handle(), trampoline);
	copy_to_batch_size_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyToInitCallback(CopyToInitCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_init_callback, handle(), nullptr);
		copy_to_init_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_to_init_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_init_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToInitInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_init_callback, handle(), trampoline);
	copy_to_init_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyToBatchCallback(CopyToBatchCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_batch_callback, handle(), nullptr);
		copy_to_batch_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_to_batch_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_batch_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToBatchInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_batch_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_batch_callback, handle(), trampoline);
	copy_to_batch_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyToFlushCallback(CopyToFlushCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_flush_callback, handle(), nullptr);
		copy_to_flush_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_to_flush_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_flush_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToFlushInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_flush_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_flush_callback, handle(), trampoline);
	copy_to_flush_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyToFinalizeCallback(CopyToFinalizeCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_to_set_finalize_callback, handle(), nullptr);
		copy_to_finalize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_to_finalize_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_to_finalize_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyToFinalizeInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_to_finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_to_set_finalize_callback, handle(), trampoline);
	copy_to_finalize_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyFromBindCallback(CopyFromBindCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_from_set_bind_callback, handle(), nullptr);
		copy_from_bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_from_bind_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_from_bind_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyFromBindInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_from_bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_from_set_bind_callback, handle(), trampoline);
	copy_from_bind_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyFromInitGlobalCallback(CopyFromInitGlobalCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_from_set_init_global_callback, handle(), nullptr);
		copy_from_init_global_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_from_init_global_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_from_init_global_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyFromInitGlobalInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_from_init_global_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_from_set_init_global_callback, handle(), trampoline);
	copy_from_init_global_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyFromInitLocalCallback(CopyFromInitLocalCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_from_set_init_local_callback, handle(), nullptr);
		copy_from_init_local_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_from_init_local_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_from_init_local_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyFromInitLocalInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_from_init_local_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_from_set_init_local_callback, handle(), trampoline);
	copy_from_init_local_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyFromExecCallback(CopyFromExecCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_from_set_exec_callback, handle(), nullptr);
		copy_from_exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_from_exec_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_from_exec_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyFromExecInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_from_exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_from_set_exec_callback, handle(), trampoline);
	copy_from_exec_callback = callback;
	return *this;
}

auto CopyFunction::SetCopyFromProgressCallback(CopyFromProgressCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_from_set_progress_callback, handle(), nullptr);
		copy_from_progress_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_from_progress_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_copy_from_progress_get_user_data, info, &user_data);
			const auto &function = *static_cast<CopyFunctionInfo *>(user_data);

			auto input =
			    detail::Factory::Make<CopyFromProgressInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.copy_from_progress_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_from_set_progress_callback, handle(), trampoline);
	copy_from_progress_callback = callback;
	return *this;
}

auto CopyFunction::Register() -> void {
	// The callback table rides the C user_data slot so the trampolines can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<CopyFunctionInfo>(new CopyFunctionInfo(
	    copy_to_bind_callback, copy_to_batch_size_callback, copy_to_init_callback, copy_to_batch_callback,
	    copy_to_flush_callback, copy_to_finalize_callback, copy_from_bind_callback, copy_from_init_global_callback,
	    copy_from_init_local_callback, copy_from_exec_callback, copy_from_progress_callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<CopyFunctionInfo>, detail::TypedEquals<CopyFunctionInfo>};
	CheckedAPICall(duckdb_v2_copy_function_set_user_data, handle(), &opaque);
	// The function owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_copy_function_register, handle());
}

void *CopyFunction::CopyToBindInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_bind_get_user_data, static_cast<duckdb_v2_copy_to_bind_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

void CopyFunction::CopyToBindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                        void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, equals};
	CheckedAPICall(duckdb_v2_copy_to_bind_set_bind_data, static_cast<duckdb_v2_copy_to_bind_info_handle>(args),
	               &opaque);
}

auto CopyFunction::CopyToBindInput::GetFilePath() const -> std::string {
	duckdb_v2_str path = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_to_bind_get_file_path, static_cast<duckdb_v2_copy_to_bind_info_handle>(args), &path);
	return std::string(FromStr(path));
}

auto CopyFunction::CopyToBindInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_copy_to_bind_get_column_count, static_cast<duckdb_v2_copy_to_bind_info_handle>(args),
	               &count);
	return count;
}

auto CopyFunction::CopyToBindInput::GetColumnName(idx_t index) const -> std::string {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_to_bind_get_column_name, static_cast<duckdb_v2_copy_to_bind_info_handle>(args), index,
	               &name);
	return std::string(FromStr(name));
}

auto CopyFunction::CopyToBindInput::GetColumnType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_bind_get_column_type, static_cast<duckdb_v2_copy_to_bind_info_handle>(args), index,
	               &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto CopyFunction::CopyToBindInput::GetOptionCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_copy_to_bind_get_option_count, static_cast<duckdb_v2_copy_to_bind_info_handle>(args),
	               &count);
	return count;
}

auto CopyFunction::CopyToBindInput::GetOptionName(idx_t index) const -> std::string {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_to_bind_get_option_name, static_cast<duckdb_v2_copy_to_bind_info_handle>(args), index,
	               &name);
	return std::string(FromStr(name));
}

auto CopyFunction::CopyToBindInput::GetOptionValue(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_bind_get_option_value, static_cast<duckdb_v2_copy_to_bind_info_handle>(args),
	               index, &value);
	return detail::Factory::Make<Value>(value);
}

auto CopyFunction::CopyToBindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyToBatchSizeInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_size_get_bind_data,
	               static_cast<duckdb_v2_copy_to_batch_size_info_handle>(args), &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyToBatchSizeInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_size_get_user_data,
	               static_cast<duckdb_v2_copy_to_batch_size_info_handle>(args), &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyToBatchSizeInput::SetTarget(idx_t rows) -> void {
	CheckedAPICall(duckdb_v2_copy_to_batch_size_set_target, static_cast<duckdb_v2_copy_to_batch_size_info_handle>(args),
	               rows);
}

auto CopyFunction::CopyToBatchSizeInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void CopyFunction::CopyToInitInput::SetInitDataInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_copy_to_init_set_init_data, static_cast<duckdb_v2_copy_to_init_info_handle>(args),
	               &opaque);
}

void *CopyFunction::CopyToInitInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_init_get_bind_data, static_cast<duckdb_v2_copy_to_init_info_handle>(args), &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyToInitInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_init_get_user_data, static_cast<duckdb_v2_copy_to_init_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyToInitInput::GetFilePath() const -> std::string {
	duckdb_v2_str path = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_to_init_get_file_path, static_cast<duckdb_v2_copy_to_init_info_handle>(args), &path);
	return std::string(FromStr(path));
}

auto CopyFunction::CopyToInitInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void CopyFunction::CopyToBatchInput::SetBatchDataInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_copy_to_batch_set_batch_data, static_cast<duckdb_v2_copy_to_batch_info_handle>(args),
	               &opaque);
}

void *CopyFunction::CopyToBatchInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_get_bind_data, static_cast<duckdb_v2_copy_to_batch_info_handle>(args),
	               &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyToBatchInput::GetInitDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_get_init_data, static_cast<duckdb_v2_copy_to_batch_info_handle>(args),
	               &data);
	return RequireCopyInitData(data);
}

void *CopyFunction::CopyToBatchInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_get_user_data, static_cast<duckdb_v2_copy_to_batch_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyToBatchInput::TakeBatch() -> ColumnDataCollection {
	duckdb_v2_column_data_collection_handle collection = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_batch_take_input, static_cast<duckdb_v2_copy_to_batch_info_handle>(args),
	               &collection);
	return detail::Factory::Make<ColumnDataCollection>(collection);
}

auto CopyFunction::CopyToBatchInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyToFlushInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_flush_get_bind_data, static_cast<duckdb_v2_copy_to_flush_info_handle>(args),
	               &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyToFlushInput::GetInitDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_flush_get_init_data, static_cast<duckdb_v2_copy_to_flush_info_handle>(args),
	               &data);
	return RequireCopyInitData(data);
}

void *CopyFunction::CopyToFlushInput::GetBatchDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_flush_get_batch_data, static_cast<duckdb_v2_copy_to_flush_info_handle>(args),
	               &data);
	return RequireCopyBatchData(data);
}

void *CopyFunction::CopyToFlushInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_flush_get_user_data, static_cast<duckdb_v2_copy_to_flush_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyToFlushInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyToFinalizeInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_finalize_get_bind_data, static_cast<duckdb_v2_copy_to_finalize_info_handle>(args),
	               &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyToFinalizeInput::GetInitDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_finalize_get_init_data, static_cast<duckdb_v2_copy_to_finalize_info_handle>(args),
	               &data);
	return RequireCopyInitData(data);
}

void *CopyFunction::CopyToFinalizeInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_to_finalize_get_user_data, static_cast<duckdb_v2_copy_to_finalize_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyToFinalizeInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyFromBindInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_bind_get_user_data, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

void CopyFunction::CopyFromBindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                          void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, equals};
	CheckedAPICall(duckdb_v2_copy_from_bind_set_bind_data, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               &opaque);
}

auto CopyFunction::CopyFromBindInput::GetFilePath() const -> std::string {
	duckdb_v2_str path = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_from_bind_get_file_path, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               &path);
	return std::string(FromStr(path));
}

auto CopyFunction::CopyFromBindInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_copy_from_bind_get_column_count, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               &count);
	return count;
}

auto CopyFunction::CopyFromBindInput::GetColumnName(idx_t index) const -> std::string {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_from_bind_get_column_name, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               index, &name);
	return std::string(FromStr(name));
}

auto CopyFunction::CopyFromBindInput::GetColumnType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_bind_get_column_type, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto CopyFunction::CopyFromBindInput::GetOptionCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_copy_from_bind_get_option_count, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               &count);
	return count;
}

auto CopyFunction::CopyFromBindInput::GetOptionName(idx_t index) const -> std::string {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_copy_from_bind_get_option_name, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               index, &name);
	return std::string(FromStr(name));
}

auto CopyFunction::CopyFromBindInput::GetOptionValue(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_bind_get_option_value, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               index, &value);
	return detail::Factory::Make<Value>(value);
}

auto CopyFunction::CopyFromBindInput::SetCardinality(idx_t cardinality, bool is_exact) -> void {
	CheckedAPICall(duckdb_v2_copy_from_bind_set_cardinality, static_cast<duckdb_v2_copy_from_bind_info_handle>(args),
	               cardinality, is_exact);
}

auto CopyFunction::CopyFromBindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void CopyFunction::CopyFromInitGlobalInput::SetGlobalStateInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_copy_from_init_global_set_global_state,
	               static_cast<duckdb_v2_copy_from_init_global_info_handle>(args), &opaque);
}

void *CopyFunction::CopyFromInitGlobalInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_init_global_get_bind_data,
	               static_cast<duckdb_v2_copy_from_init_global_info_handle>(args), &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyFromInitGlobalInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_init_global_get_user_data,
	               static_cast<duckdb_v2_copy_from_init_global_info_handle>(args), &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyFromInitGlobalInput::SetMaxThreads(idx_t max_threads) -> void {
	CheckedAPICall(duckdb_v2_copy_from_init_global_set_max_threads,
	               static_cast<duckdb_v2_copy_from_init_global_info_handle>(args), max_threads);
}

auto CopyFunction::CopyFromInitGlobalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void CopyFunction::CopyFromInitLocalInput::SetLocalStateInternal(void *data, void (*destructor)(void *)) {
	duckdb_v2_opaque opaque {data, destructor, nullptr};
	CheckedAPICall(duckdb_v2_copy_from_init_local_set_local_state,
	               static_cast<duckdb_v2_copy_from_init_local_info_handle>(args), &opaque);
}

void *CopyFunction::CopyFromInitLocalInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_init_local_get_bind_data,
	               static_cast<duckdb_v2_copy_from_init_local_info_handle>(args), &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyFromInitLocalInput::GetGlobalStateInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_init_local_get_global_state,
	               static_cast<duckdb_v2_copy_from_init_local_info_handle>(args), &data);
	return RequireCopyGlobalState(data);
}

void *CopyFunction::CopyFromInitLocalInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_init_local_get_user_data,
	               static_cast<duckdb_v2_copy_from_init_local_info_handle>(args), &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyFromInitLocalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyFromExecInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_exec_get_bind_data, static_cast<duckdb_v2_copy_from_exec_info_handle>(args),
	               &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyFromExecInput::GetGlobalStateInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_exec_get_global_state, static_cast<duckdb_v2_copy_from_exec_info_handle>(args),
	               &data);
	return RequireCopyGlobalState(data);
}

void *CopyFunction::CopyFromExecInput::GetLocalStateInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_exec_get_local_state, static_cast<duckdb_v2_copy_from_exec_info_handle>(args),
	               &data);
	return RequireCopyLocalState(data);
}

void *CopyFunction::CopyFromExecInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_exec_get_user_data, static_cast<duckdb_v2_copy_from_exec_info_handle>(args),
	               &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyFromExecInput::GetOutputChunk() const -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_exec_get_output_chunk, static_cast<duckdb_v2_copy_from_exec_info_handle>(args),
	               &chunk);
	// Borrowed: the engine owns the chunk and reuses it across invocations.
	return detail::Factory::Make<DataChunk>(chunk, false);
}

auto CopyFunction::CopyFromExecInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

void *CopyFunction::CopyFromProgressInput::GetBindDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_progress_get_bind_data,
	               static_cast<duckdb_v2_copy_from_progress_info_handle>(args), &data);
	return RequireCopyBindData(data);
}

void *CopyFunction::CopyFromProgressInput::GetGlobalStateInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_progress_get_global_state,
	               static_cast<duckdb_v2_copy_from_progress_info_handle>(args), &data);
	return RequireCopyGlobalState(data);
}

void *CopyFunction::CopyFromProgressInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_copy_from_progress_get_user_data,
	               static_cast<duckdb_v2_copy_from_progress_info_handle>(args), &user_data);
	const auto &function = *static_cast<const CopyFunctionInfo *>(user_data);
	return RequireCopyUserData(function.user_data);
}

auto CopyFunction::CopyFromProgressInput::SetProgress(double progress) -> void {
	CheckedAPICall(duckdb_v2_copy_from_progress_set_progress,
	               static_cast<duckdb_v2_copy_from_progress_info_handle>(args), progress);
}

auto CopyFunction::CopyFromProgressInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

//----------------------------------------------------------------------------------------------------------------------
// Cast Function
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered cast: rides the C user_data slot so
// the trampoline can find it; the user's own slot (SetUserData) rides inside
// it. Owned by the registered cast, freed at engine teardown.
struct CastFunctionInfo {
	CastFunction::ExecCallback exec_callback = nullptr;
	detail::UserData user_data;

	CastFunctionInfo(CastFunction::ExecCallback exec_callback, detail::UserData user_data)
	    : exec_callback(exec_callback), user_data(std::move(user_data)) {
	}

	bool operator==(const CastFunctionInfo &other) const {
		return exec_callback == other.exec_callback && user_data.get() == other.user_data.get();
	}
};

// Guard for ExecInput::GetUserData: a clear error instead of a null deref.
void *RequireCastUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call CastFunction::SetUserData before Register");
	}
	return ptr;
}

} // namespace

CastFunction::CastFunction(void *impl) : detail::Handle<CastFunction>(impl) {
}

CastFunction::~CastFunction() {
	auto _h = handle();
	duckdb_v2_cast_function_destroy(&_h);
}

auto CastFunction::Create(const Connection &conn) -> CastFunction {
	duckdb_v2_cast_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<CastFunction>(_h);
}

auto CastFunction::Create(const Extension &extension) -> CastFunction {
	duckdb_v2_cast_function_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<CastFunction>(_h);
}

auto CastFunction::SetSourceType(const LogicalType &type) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_set_source_type, handle(), type.handle());
	return *this;
}

auto CastFunction::SetTargetType(const LogicalType &type) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_set_target_type, handle(), type.handle());
	return *this;
}

auto CastFunction::SetImplicitCastCost(int64_t cost) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_set_implicit_cast_cost, handle(), cost);
	return *this;
}

auto CastFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto CastFunction::SetExecCallback(ExecCallback callback) & -> CastFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_cast_function_set_exec_callback, handle(), nullptr);
		exec_callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_cast_function_exec_get_user_data, info, &user_data);
			const auto &function = *static_cast<CastFunctionInfo *>(user_data);

			auto input = detail::Factory::Make<ExecInput>(static_cast<void *>(info), static_cast<void *>(context));
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_cast_function_set_exec_callback, handle(), trampoline);
	exec_callback = callback;
	return *this;
}

auto CastFunction::Register() -> void {
	// The callback table rides the C user_data slot so the trampoline can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<CastFunctionInfo>(new CastFunctionInfo(exec_callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<CastFunctionInfo>, detail::TypedEquals<CastFunctionInfo>};
	CheckedAPICall(duckdb_v2_cast_function_set_user_data, handle(), &opaque);
	// The cast owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_cast_function_register, handle());
}

void *CastFunction::ExecInput::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_exec_get_user_data,
	               static_cast<duckdb_v2_cast_function_exec_info_handle>(args), &user_data);
	const auto &function = *static_cast<const CastFunctionInfo *>(user_data);
	return RequireCastUserData(function.user_data);
}

auto CastFunction::ExecInput::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_cast_function_exec_get_row_count,
	               static_cast<duckdb_v2_cast_function_exec_info_handle>(args), &count);
	return count;
}

auto CastFunction::ExecInput::GetInput() const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_exec_get_input, static_cast<duckdb_v2_cast_function_exec_info_handle>(args),
	               &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto CastFunction::ExecInput::GetOutput() const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_exec_get_output, static_cast<duckdb_v2_cast_function_exec_info_handle>(args),
	               &vector);
	return detail::Factory::Make<Vector>(vector);
}

auto CastFunction::ExecInput::GetMode() const -> CastMode {
	auto mode = DUCKDB_V2_CAST_MODE_NORMAL;
	CheckedAPICall(duckdb_v2_cast_function_exec_get_mode, static_cast<duckdb_v2_cast_function_exec_info_handle>(args),
	               &mode);
	return static_cast<CastMode>(mode);
}

auto CastFunction::ExecInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

//----------------------------------------------------------------------------------------------------------------------
// File System
//----------------------------------------------------------------------------------------------------------------------

FileSystem::FileSystem(void *impl) : detail::Handle<FileSystem>(impl) {
}

FileSystem::~FileSystem() {
	// Borrowed: the context or connection owns the file system, so there is nothing to release here.
}

auto FileSystem::CreateOpenOptions() const -> FileOpenOptions {
	return FileOpenOptions::Create(*this);
}

auto FileSystem::OpenFile(const std::string &path, std::initializer_list<FileFlags> flags) const -> FileHandle {
	auto options = CreateOpenOptions();
	for (auto flag : flags) {
		options.SetFlag(flag);
	}
	return OpenFile(path, options);
}

auto FileSystem::OpenFile(const std::string &path, const FileOpenOptions &options) const -> FileHandle {
	duckdb_v2_file_handle result = nullptr;
	CheckedAPICall(duckdb_v2_file_system_open, handle(), ToStr(path), options.handle(), &result);
	return detail::Factory::Make<FileHandle>(result);
}

FileOpenOptions::FileOpenOptions(void *impl) : detail::Handle<FileOpenOptions>(impl) {
}

FileOpenOptions::~FileOpenOptions() {
	auto _h = handle();
	duckdb_v2_file_open_options_destroy(&_h);
}

auto FileOpenOptions::Create(const FileSystem &fs) -> FileOpenOptions {
	duckdb_v2_file_open_options_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_file_open_options_create, fs.handle(), &_h);
	return detail::Factory::Make<FileOpenOptions>(_h);
}

auto FileOpenOptions::SetFlag(FileFlags flag) & -> FileOpenOptions & {
	CheckedAPICall(duckdb_v2_file_open_options_set_flag, handle(), static_cast<DUCKDB_V2_FILE_FLAG>(flag));
	return *this;
}

auto FileOpenOptions::SetValue(std::string_view name, const Value &value) & -> FileOpenOptions & {
	CheckedAPICall(duckdb_v2_file_open_options_set_value, handle(), ToStr(name), value.handle());
	return *this;
}

FileHandle::FileHandle(void *impl) : detail::Handle<FileHandle>(impl) {
}

FileHandle::~FileHandle() {
	auto _h = handle();
	duckdb_v2_file_destroy(&_h);
}

void FileHandle::Sync() {
	CheckedAPICall(duckdb_v2_file_sync, handle());
}

void FileHandle::Close() {
	CheckedAPICall(duckdb_v2_file_close, handle());
}

void FileHandle::Seek(idx_t position) {
	CheckedAPICall(duckdb_v2_file_seek, handle(), position);
}

auto FileHandle::Tell() const -> idx_t {
	idx_t position = 0;
	CheckedAPICall(duckdb_v2_file_tell, handle(), &position);
	return position;
}

auto FileHandle::Size() const -> idx_t {
	idx_t size = 0;
	CheckedAPICall(duckdb_v2_file_size, handle(), &size);
	return size;
}

auto FileHandle::Read(void *buffer, idx_t size) -> idx_t {
	idx_t bytes_read = 0;
	CheckedAPICall(duckdb_v2_file_read, handle(), buffer, size, &bytes_read);
	return bytes_read;
}

auto FileHandle::Write(const void *buffer, idx_t size) -> idx_t {
	idx_t bytes_written = 0;
	CheckedAPICall(duckdb_v2_file_write, handle(), buffer, size, &bytes_written);
	return bytes_written;
}

void FileHandle::ReadAt(void *buffer, idx_t size, idx_t location) {
	CheckedAPICall(duckdb_v2_file_read_at, handle(), buffer, size, location);
}

void FileHandle::WriteAt(const void *buffer, idx_t size, idx_t location) {
	CheckedAPICall(duckdb_v2_file_write_at, handle(), buffer, size, location);
}

//----------------------------------------------------------------------------------------------------------------------
// Qualified Name
//----------------------------------------------------------------------------------------------------------------------

QualifiedName::QualifiedName(void *impl) : detail::Handle<QualifiedName>(impl) {
}

QualifiedName::~QualifiedName() {
	auto _h = handle();
	duckdb_v2_qname_destroy(&_h);
}

auto QualifiedName::Parse(std::string_view text) -> QualifiedName {
	duckdb_v2_qname_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_qname_parse, ToStr(text), &_h);
	return detail::Factory::Make<QualifiedName>(_h);
}

auto QualifiedName::Create(const std::vector<std::string> &parts) -> QualifiedName {
	std::vector<duckdb_v2_identifier_t> views;
	views.reserve(parts.size());
	for (auto &part : parts) {
		views.push_back(ToStr(part));
	}
	duckdb_v2_qname_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_qname_create, views.empty() ? nullptr : views.data(), views.size(), &_h);
	return detail::Factory::Make<QualifiedName>(_h);
}

auto QualifiedName::GetPartCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_qname_get_part_count, handle(), &count);
	return count;
}

auto QualifiedName::GetPart(idx_t index) const -> std::string_view {
	duckdb_v2_identifier_t part = {nullptr, 0};
	CheckedAPICall(duckdb_v2_qname_get_part, handle(), index, &part);
	return FromStr(part);
}

auto QualifiedName::GetName() const -> std::string_view {
	return GetPart(GetPartCount() - 1);
}

auto QualifiedName::Render() const -> std::string {
	idx_t length = 0;
	CheckedAPICall(duckdb_v2_qname_render, handle(), nullptr, 0, &length);
	std::string out(length, '\0');
	CheckedAPICall(duckdb_v2_qname_render, handle(), &out[0], length + 1, &length);
	return out;
}

auto QualifiedName::Equals(const QualifiedName &other) const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_qname_equals, handle(), other.handle(), &result);
	return result;
}

auto QualifiedName::Hash() const -> uint64_t {
	uint64_t hash = 0;
	CheckedAPICall(duckdb_v2_qname_hash, handle(), &hash);
	return hash;
}

//----------------------------------------------------------------------------------------------------------------------
// Table Description
//----------------------------------------------------------------------------------------------------------------------

TableDescription::TableDescription(void *impl) : detail::Handle<TableDescription>(impl) {
}

TableDescription::~TableDescription() {
	auto _h = handle();
	duckdb_v2_table_description_destroy(&_h);
}

auto TableDescription::GetQualifiedName() const -> QualifiedName {
	duckdb_v2_qname_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_table_description_get_qname, handle(), &_h);
	return detail::Factory::Make<QualifiedName>(_h);
}

auto TableDescription::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_description_get_column_count, handle(), &count);
	return count;
}

auto TableDescription::GetColumn(idx_t index) const -> ColumnDescription {
	duckdb_v2_column_description_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_table_description_get_column, handle(), index, &_h);
	return detail::Factory::Make<ColumnDescription>(_h);
}

auto TableDescription::IsReadOnly() const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_table_description_is_readonly, handle(), &result);
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Replacement Scan
//----------------------------------------------------------------------------------------------------------------------

namespace {

// The callback table for one registered scan: rides the C user_data slot so
// the trampoline can find it; the user's own slot (SetUserData) rides inside
// it. Owned by the registered scan, freed when its scope ends.
struct ReplacementScanInfo {
	ReplacementScan::Callback callback = nullptr;
	detail::UserData user_data;

	ReplacementScanInfo(ReplacementScan::Callback callback, detail::UserData user_data)
	    : callback(callback), user_data(std::move(user_data)) {
	}

	bool operator==(const ReplacementScanInfo &other) const {
		return callback == other.callback && user_data.get() == other.user_data.get();
	}
};

// Guard for Input::GetUserData: a clear error instead of a null deref.
void *RequireReplacementUserData(const detail::UserData &user_data) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw InvalidInputException("no user data was set; call ReplacementScan::SetUserData before Register");
	}
	return ptr;
}

} // namespace

ReplacementScan::ReplacementScan(void *impl) : detail::Handle<ReplacementScan>(impl) {
}

ReplacementScan::~ReplacementScan() {
	auto _h = handle();
	duckdb_v2_replacement_scan_destroy(&_h);
}

auto ReplacementScan::Create(const Connection &conn) -> ReplacementScan {
	duckdb_v2_replacement_scan_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_replacement_scan_create_with_connection, conn.handle(), &_h);
	return detail::Factory::Make<ReplacementScan>(_h);
}

auto ReplacementScan::Create(const Database &db) -> ReplacementScan {
	duckdb_v2_replacement_scan_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_replacement_scan_create_with_database, db.handle(), &_h);
	return detail::Factory::Make<ReplacementScan>(_h);
}

auto ReplacementScan::Create(const Extension &extension) -> ReplacementScan {
	duckdb_v2_replacement_scan_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_replacement_scan_create_with_extension, extension.handle(), &_h);
	return detail::Factory::Make<ReplacementScan>(_h);
}

auto ReplacementScan::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto ReplacementScan::SetCallback(Callback callback_p) & -> ReplacementScan & {
	if (!callback_p) {
		CheckedAPICall(duckdb_v2_replacement_scan_set_callback, handle(), nullptr);
		callback = nullptr;
		return *this;
	}

	// The C-side callback is one shared trampoline; the user's callback is looked
	// up through the info table riding the user_data slot (set by Register).
	static auto trampoline = [](duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_replacement_scan_get_user_data, info, &user_data);
			const auto &scan = *static_cast<ReplacementScanInfo *>(user_data);

			auto input = detail::Factory::Make<Input>(static_cast<void *>(info), static_cast<void *>(context));
			scan.callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_replacement_scan_set_callback, handle(), trampoline);
	callback = callback_p;
	return *this;
}

auto ReplacementScan::Register() -> void {
	// The callback table rides the C user_data slot so the trampoline can find
	// it; the user's own data (SetUserData, moved out here) rides inside it.
	auto info = std::unique_ptr<ReplacementScanInfo>(new ReplacementScanInfo(callback, std::move(user_data)));
	duckdb_v2_opaque opaque {info.get(), detail::TypedDelete<ReplacementScanInfo>,
	                         detail::TypedEquals<ReplacementScanInfo>};
	CheckedAPICall(duckdb_v2_replacement_scan_set_user_data, handle(), &opaque);
	// The scan owns the table now.
	info.release();

	CheckedAPICall(duckdb_v2_replacement_scan_register, handle());
}

void *ReplacementScan::Input::GetUserDataInternal() const {
	void *user_data = nullptr;
	CheckedAPICall(duckdb_v2_replacement_scan_get_user_data, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               &user_data);
	const auto &scan = *static_cast<const ReplacementScanInfo *>(user_data);
	return RequireReplacementUserData(scan.user_data);
}

auto ReplacementScan::Input::GetName() const -> QualifiedName {
	duckdb_v2_qname_handle name = nullptr;
	CheckedAPICall(duckdb_v2_replacement_scan_get_name, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               &name);
	return detail::Factory::Make<QualifiedName>(name);
}

auto ReplacementScan::Input::SetFunctionName(const QualifiedName &name) -> void {
	CheckedAPICall(duckdb_v2_replacement_scan_set_function_name,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(args), name.handle());
}

auto ReplacementScan::Input::SetFunctionName(std::string_view name) -> void {
	SetFunctionName(QualifiedName::Create({std::string(name)}));
}

auto ReplacementScan::Input::AddArgument(const Value &value) -> void {
	CheckedAPICall(duckdb_v2_replacement_scan_add_argument, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               value.handle());
}

auto ReplacementScan::Input::AddNamedArgument(std::string_view name, const Value &value) -> void {
	CheckedAPICall(duckdb_v2_replacement_scan_add_named_argument,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(args), ToStr(name), value.handle());
}

auto ReplacementScan::Input::SetCollection(const ColumnDataCollection &collection,
                                           const std::vector<std::string> &column_names) -> void {
	std::vector<duckdb_v2_identifier_t> names;
	names.reserve(column_names.size());
	for (auto &name : column_names) {
		names.push_back(ToStr(name));
	}
	CheckedAPICall(duckdb_v2_replacement_scan_set_collection, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               collection.handle(), names.empty() ? nullptr : names.data(), names.size());
}

auto ReplacementScan::Input::SetSubquery(std::string_view sql) -> void {
	CheckedAPICall(duckdb_v2_replacement_scan_set_subquery, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               ToStr(sql));
}

auto ReplacementScan::Input::SetAlias(std::string_view alias) -> void {
	CheckedAPICall(duckdb_v2_replacement_scan_set_alias, static_cast<duckdb_v2_replacement_scan_info_handle>(args),
	               ToStr(alias));
}

auto ReplacementScan::Input::GetContext() const -> Context {
	return detail::Factory::Make<Context>(context);
}

//----------------------------------------------------------------------------------------------------------------------
// Appender
//----------------------------------------------------------------------------------------------------------------------

namespace {

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (size_t i = 0; i < a.size(); i++) {
		auto lhs = static_cast<unsigned char>(a[i]);
		auto rhs = static_cast<unsigned char>(b[i]);
		if (std::tolower(lhs) != std::tolower(rhs)) {
			return false;
		}
	}
	return true;
}

// Parses exactly one statement out of `sql`.
auto ParseSingleStatement(Connection &conn, const std::string &sql) -> SqlStatement {
	auto statements = conn.ParseSQL(sql);
	auto first = statements.Next();
	if (!first) {
		throw InvalidInputException("the appender's query contains no statement");
	}
	if (statements.Next()) {
		throw InvalidInputException("the appender's query must contain exactly one statement");
	}
	return first;
}

// Names the buffers the table constructor generates, so two appenders on one connection never collide.
std::atomic<uint64_t> appender_buffer_counter {0};

} // namespace

Appender::Appender(Connection &conn, std::string_view query, std::vector<LogicalType> column_types,
                   std::string_view buffer_name, const std::vector<std::string> &column_names) {
	Initialize(conn, std::string(query), std::move(column_types), std::string(buffer_name), column_names);
}

void Appender::Initialize(Connection &conn, const std::string &query, std::vector<LogicalType> column_types,
                          const std::string &buffer_name, const std::vector<std::string> &column_names) {
	connection = &conn;
	types = std::move(column_types);
	if (types.empty()) {
		throw InvalidInputException("an appender needs at least one column type");
	}

	buffer = std::make_shared<Buffer>();
	buffer->name = buffer_name;
	buffer->column_names = column_names;
	buffer->collection = std::make_unique<ColumnDataCollection>(conn, types);

	// The scan makes the buffer visible to the statement under its name. It is connection-scoped, so it is invisible
	// to every other connection, and it holds the buffer by shared_ptr because it outlives this object.
	auto scan = ReplacementScan::Create(conn);
	scan.SetCallback([](ReplacementScan::Input &input) {
		auto &shared = input.GetUserData<std::shared_ptr<Buffer>>();
		if (!shared->collection) {
			return; // the appender is gone: decline
		}
		// Only an unqualified reference can be the buffer; anything catalog- or schema-qualified is a real object.
		auto name = input.GetName();
		if (name.GetPartCount() != 1 || !EqualsIgnoreCase(name.GetName(), shared->name)) {
			return; // not ours: decline
		}
		input.SetCollection(*shared->collection, shared->column_names);
	});
	scan.SetUserData<std::shared_ptr<Buffer>>(buffer);
	scan.Register();

	// Parsed once and re-executed per flush.
	statement = std::make_unique<SqlStatement>(ParseSingleStatement(conn, query));
	append_state = std::make_unique<ColumnDataCollection::AppendState>(buffer->collection->CreateAppendState());
}

namespace {

// The table constructor's two derived pieces: the buffer's columns, and the INSERT that drains it.
struct AppenderTablePlan {
	std::vector<LogicalType> types;
	std::vector<std::string> column_names;
	std::string query;
	std::string buffer_name;
};

AppenderTablePlan PlanTableAppender(Connection &conn, std::string_view table) {
	AppenderTablePlan plan;
	plan.buffer_name = "__appender_buffer_" + std::to_string(appender_buffer_counter++);

	// Bind rather than execute: it resolves the table and reports its columns without reading a row.
	auto probe = ParseSingleStatement(conn, "SELECT * FROM " + std::string(table));
	auto signature = conn.Bind(probe);
	auto &schema = signature.output;
	if (schema.GetFieldCount() == 0) {
		throw InvalidInputException("table " + std::string(table) + " has no columns to append to");
	}

	std::string columns;
	for (idx_t i = 0; i < schema.GetFieldCount(); i++) {
		if (i > 0) {
			columns += ", ";
		}
		columns += RenderQuotedIdentifier(schema.GetFieldName(i));
		plan.column_names.emplace_back(schema.GetFieldName(i));
		plan.types.push_back(schema.GetFieldType(i));
	}
	plan.query = "INSERT INTO " + std::string(table) + " (" + columns + ") SELECT * FROM " + plan.buffer_name;
	return plan;
}

} // namespace

Appender::Appender(Connection &conn, std::string_view table) {
	auto plan = PlanTableAppender(conn, table);
	Initialize(conn, plan.query, std::move(plan.types), plan.buffer_name, plan.column_names);
}

Appender::~Appender() {
	// Frees the buffer without flushing. The replacement scan outlives this object and stays registered on the
	// connection, so the shared block survives; nulling the collection makes the scan decline from here on.
	if (buffer) {
		append_state.reset();
		buffer->collection.reset();
	}
}

void Appender::ResetBuffer() {
	// Drop the append state first: it references the segments the clear releases.
	append_state.reset();
	buffer->collection->Clear();
	try {
		append_state = std::make_unique<ColumnDataCollection::AppendState>(buffer->collection->CreateAppendState());
	} catch (...) {
		broken = true;
		throw;
	}
}

void Appender::AppendChunk(DataChunk &chunk) {
	if (broken) {
		throw InvalidInputException("the appender is broken after a failed buffer operation; Clear or destroy it");
	}
	try {
		// The append validates the chunk's columns against the buffer and refuses a mismatch before copying.
		buffer->collection->Append(*append_state, chunk);
	} catch (const Exception &ex) {
		// A validation refusal leaves the buffer untouched; anything else may have copied part of the chunk.
		if (ex.GetCode() != DUCKDB_V2_ERROR_INPUT_INVALID) {
			broken = true;
		}
		throw;
	} catch (...) {
		broken = true;
		throw;
	}
}

void Appender::Flush() {
	if (broken) {
		throw InvalidInputException("the appender is broken after a failed buffer operation; Clear or destroy it");
	}
	if (buffer->collection->GetRowCount() == 0) {
		return;
	}
	try {
		connection->Execute(*statement).Drain();
	} catch (const Exception &ex) {
		// A busy connection or an interrupted run keeps the rows so the flush can be retried; any other failure drops
		// them, so a retry does not re-run the same failing statement over the same rows.
		if (ex.GetCode() != DUCKDB_V2_ERROR_RESOURCE_IN_USE && ex.GetCode() != DUCKDB_V2_ERROR_RUNTIME_INTERRUPT) {
			ResetBuffer();
		}
		throw;
	} catch (...) {
		ResetBuffer();
		throw;
	}
	ResetBuffer();
}

void Appender::Clear() {
	ResetBuffer();
	broken = false;
}

//----------------------------------------------------------------------------------------------------------------------
// Column Description
//----------------------------------------------------------------------------------------------------------------------

ColumnDescription::ColumnDescription(void *impl) : detail::Handle<ColumnDescription>(impl) {
}

ColumnDescription::~ColumnDescription() {
	auto _h = handle();
	duckdb_v2_column_description_destroy(&_h);
}

auto ColumnDescription::GetName() const -> std::string_view {
	duckdb_v2_identifier_t name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_column_description_get_name, handle(), &name);
	return FromStr(name);
}

auto ColumnDescription::GetType() const -> LogicalType {
	duckdb_v2_logical_type_handle borrowed = nullptr;
	CheckedAPICall(duckdb_v2_column_description_get_type, handle(), &borrowed);
	// get_type borrows the type; copy it into an owned handle the wrapper manages.
	duckdb_v2_logical_type_handle owned = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_copy, borrowed, &owned);
	return detail::Factory::Make<LogicalType>(owned);
}

auto ColumnDescription::HasDefault() const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_column_description_has_default, handle(), &result);
	return result;
}

auto ColumnDescription::HasGenerated() const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_column_description_has_generated, handle(), &result);
	return result;
}

} // namespace cxx
} // namespace duckdb
