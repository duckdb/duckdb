#pragma once

//----------------------------------------------------------------------------------------------------------------------
// 8888888b.                    888      8888888b.  888888b.               d8888 8888888b. 8888888
// 888  "Y88b                   888      888  "Y88b 888  "88b             d88888 888   Y88b  888
// 888    888                   888      888    888 888  .88P            d88P888 888    888  888
// 888    888 888  888  .d8888b 888  888 888    888 8888888K.           d88P 888 888   d88P  888
// 888    888 888  888 d88P"    888 .88P 888    888 888  "Y88b         d88P  888 8888888P"   888
// 888    888 888  888 888      888888K  888    888 888    888        d88P   888 888         888
// 888  .d88P Y88b 888 Y88b.    888 "88b 888  .d88P 888   d88P       d8888888888 888         888
// 8888888P"   "Y88888  "Y8888P 888  888 8888888P"  8888888P"       d88P     888 888       8888888
//----------------------------------------------------------------------------------------------------------------------

/// @file duckdb_cpp.hpp
/// The DuckDB C++ API.
///
/// Most classes here are handles: move-only, never copied and never reference-counted. A handle is either owning,
/// releasing its resource when destroyed, or borrowed, in which case its documentation says what it borrows from and
/// how long it stays valid. Whether a handle currently holds a resource is reported by its `explicit operator bool`.
/// The remaining types (`Exception`, `Signature`, the primitive value types, and the like) are ordinary copyable
/// values.
///
/// Errors are reported by throwing; no function returns an error code. Everything thrown derives from `Exception`.
/// Failures that are part of a function's contract are documented with `\@throws`; any other failure surfaces as a
/// plain `Exception`.
///
/// The usual path through the API: an `Environment` opens a `Database`, a `Database` hands out `Connection`s, and a
/// `Connection` parses and executes SQL into a streaming `QueryResult` that yields `DataChunk`s of `Vector`s.

#include <utility>
#include <string>
#include <string_view>
#include <type_traits>
#include <functional>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <limits>

// Arrow C Data Interface structs. Forward declared only: the definitions come from `duckdb_v2.h` or from the
// consumer's own Arrow headers, under the standard ARROW_C_DATA_INTERFACE / ARROW_C_STREAM_INTERFACE guards.
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

namespace duckdb {
namespace cxx {

//----------------------------------------------------------------------------------------------------------------------
// Common Typedefs and forward declarations
//----------------------------------------------------------------------------------------------------------------------

/// Row indices, offsets, counts and sizes throughout the API.
typedef uint64_t idx_t;

class Exception;
class DatabaseOption;
class Environment;
class Database;
class Connection;
class SqlStatement;
class StatementIterator;
class Schema;
class Signature;
class LogicalType;
class Value;
class Vector;
class Arena;
class DataChunk;
class ColumnDataCollection;
class QueryResult;
class PreparedStatement;
class ArrowStream;
class ArrowImporter;
class ArrowExporter;

struct TypeParam;
struct NamedParam;

enum class LogicalTypeId : uint32_t;
enum class CastMode : uint8_t;

template <class CTX>
class TypeBuilder;
class CustomType;
class CastFunction;
class ReplacementScan;
class QualifiedName;
class TableDescription;
class ColumnDescription;
class FileSystem;
class FileHandle;
class FileOpenOptions;

//----------------------------------------------------------------------------------------------------------------------
// Internal Implementation Details
//----------------------------------------------------------------------------------------------------------------------

/// @internal
/// Everything in `duckdb::cxx::detail` is an implementation detail: not part of the public API, and subject to change
/// without notice.
namespace detail {

/// @internal
/// Maps a C++ wrapper type to its underlying C-API handle type. The primary template is intentionally left undefined:
/// each wrapper specializes it in the .cpp, so C handle types never have to appear in this header.
template <class T>
struct HandleTraits;

/// @internal
/// Base class of every wrapper in this API: move-only custody of a single opaque handle.
template <class TYPE>
class Handle {
public:
	friend TYPE;

	Handle(const Handle &) = delete;
	Handle &operator=(const Handle &) = delete;

	Handle(Handle &&other) noexcept : impl(other.impl) {
		other.impl = nullptr;
	}

	Handle &operator=(Handle &&other) noexcept {
		std::swap(impl, other.impl);
		return *this;
	}

	virtual ~Handle() noexcept = default;

	/// @internal The underlying C-API handle, typed indirectly through HandleTraits<TYPE>.
	template <class TR = HandleTraits<TYPE>>
	auto handle() const -> typename TR::handle {
		return static_cast<typename TR::handle>(impl);
	}

	/// True while this wrapper holds a handle; false once it has been moved from.
	explicit operator bool() const noexcept {
		return impl != nullptr;
	}

private:
	Handle() : impl(nullptr) {
	}
	explicit Handle(void *impl) : impl(impl) {
	}

	/// @internal Detaches the handle and leaves this wrapper empty, for calls where the C API takes ownership.
	auto release() noexcept -> void * {
		auto *detached = impl;
		impl = nullptr;
		return detached;
	}
	void *impl;
};

/// @internal
/// Grants the .cpp access to the wrappers' private constructors, without making them public. `Handle::release` is not
/// reachable from here -- only the wrapper type itself befriends its `Handle` base, so calls where the C API takes
/// ownership release from inside a member of the consuming wrapper.
struct Factory {
	template <class T, class... ARGS>
	static auto Make(ARGS &&... args) -> T {
		return T(std::forward<ARGS>(args)...);
	}
};

template <class T>
struct always_false : std::false_type {};

/// @internal
/// Deletes `ptr` as a `T *`: the destructor shape the C API's opaque data slots take.
template <class T>
void TypedDelete(void *ptr) {
	delete static_cast<T *>(ptr);
}

/// @internal
/// Compares two `T *` by `operator==`: the equality shape the C API's opaque data slots take.
template <class T>
bool TypedEquals(void *ptr_a, void *ptr_b) {
	return *static_cast<T *>(ptr_a) == *static_cast<T *>(ptr_b);
}

/// @internal
/// Whether `const T` supports `operator==`.
template <class T, class = void>
struct is_equality_comparable : std::false_type {};
template <class T>
struct is_equality_comparable<T, std::void_t<decltype(std::declval<const T &>() == std::declval<const T &>())>>
    : std::true_type {};

/// @internal
/// The equals callback for a `T`-typed opaque slot: `TypedEquals` when `T` is equality-comparable, null otherwise,
/// which makes the engine fall back to comparing the slots by identity.
/// @internal
/// Names `const Vector &` once per element of a type pack: lets a function declare exactly one vector parameter per
/// argument type. Routed through a class template so the pack expansion is a non-deduced context on every compiler:
/// MSVC misaligns a plain alias-template pack against the call arguments instead of fixing its length from the
/// explicitly given type list.
template <class T>
struct VectorPerArgImpl {
	using type = const Vector &;
};
template <class T>
using VectorPerArg = typename VectorPerArgImpl<T>::type;

template <class T>
constexpr auto SelectEquals() -> bool (*)(void *, void *) {
	if constexpr (is_equality_comparable<T>::value) {
		return TypedEquals<T>;
	} else {
		return nullptr;
	}
}

/// @internal
/// Move-only custody of a user-provided pointer plus the destructor that frees it.
class UserData {
public:
	UserData() : data(nullptr), destructor(nullptr) {
	}
	UserData(void *data, void (*destructor)(void *)) : data(data), destructor(destructor) {
	}

	UserData(const UserData &) = delete;
	UserData &operator=(const UserData &) = delete;

	UserData(UserData &&other) noexcept : data(other.data), destructor(other.destructor) {
		other.data = nullptr;
		other.destructor = nullptr;
	}

	UserData &operator=(UserData &&other) noexcept {
		if (this != &other) {
			if (data && destructor) {
				destructor(data);
			}
			data = other.data;
			destructor = other.destructor;
			other.data = nullptr;
			other.destructor = nullptr;
		}
		return *this;
	}

	~UserData() {
		if (data && destructor) {
			destructor(data);
		}
	}

	auto get() const -> void * {
		return data;
	}

private:
	void *data;
	void (*destructor)(void *);
};

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Exceptions
//----------------------------------------------------------------------------------------------------------------------
// Every failure in this API is reported by throwing. Catch `Exception` to handle any of them; the subclasses single
// out the cases worth reacting to on their own.
// TODO: add more exception types!

/// Base class of all errors thrown by DuckDB. For errors raised by the database engine, `what()` carries the fully
/// formatted message, prefix included, e.g. "Parser Error: ...".
class Exception : public std::runtime_error {
public:
	/// @param code The numeric error code.
	/// @param message The full message, as later returned by `what()`.
	/// @param raw_message The message body without the error-type prefix, when available.
	Exception(const int code, const std::string &message, std::string raw_message = {})
	    : std::runtime_error(message), code(code), raw_message(std::move(raw_message)) {
	}

	/// The numeric error code identifying the kind of error. Prefer catching a typed subclass where one exists.
	auto GetCode() const -> int {
		return code;
	}

	/// The message body alone, without the "Catalog Error:" / "Parser Error:" / ... prefix that `what()` carries.
	/// Empty when no raw message is available.
	auto GetRawMessage() const -> const std::string & {
		return raw_message;
	}

private:
	int code;
	std::string raw_message;
};

/// Invalid input: a malformed SQL string, an argument of the wrong kind, a misused handle, and the like.
class InvalidInputException : public Exception {
public:
	explicit InvalidInputException(const std::string &message, std::string raw_message = {});
};

/// A running query was canceled, e.g. by `Connection::Interrupt`.
class InterruptException : public Exception {
public:
	explicit InterruptException(const std::string &message, std::string raw_message = {});
};

//----------------------------------------------------------------------------------------------------------------------
// Database Option
//----------------------------------------------------------------------------------------------------------------------
// Configuration settings, as name/value pairs.
// Construct a `DatabaseOption` to write a setting, or read one back from an existing `Database` or `Connection` to
// inspect its current value, default value, description or aliases.
// Settings that can only be chosen up front must be passed to `Environment::Open`.

/// At which scope a setting may be written.
enum class OptionTargetScope : uint8_t {
	/// Unknown: the setting declares no target scope, or the option was constructed here and has not been resolved
	/// against a database yet.
	UNKNOWN = 0,
	/// Writable only at GLOBAL (database) scope.
	GLOBAL_ONLY = 1,
	/// Writable only at LOCAL (session) scope.
	LOCAL_ONLY = 2,
	/// Writable at either scope, GLOBAL when unspecified.
	GLOBAL_DEFAULT = 3,
	/// Writable at either scope, LOCAL when unspecified.
	LOCAL_DEFAULT = 4,
};

/// Which scope a connection-level write applies to.
enum class SettingScope : uint8_t {
	/// Resolve from the setting's own target scope, exactly like SQL `SET name = value`.
	AUTOMATIC = 0,
	/// Apply to the whole database.
	GLOBAL = 1,
	/// Apply to this session only.
	LOCAL = 2,
};

/// A single configuration setting
/// This holds the value of the setting plus the metadata DuckDB declares for it.
/// The string accessors return views borrowed from this option, valid until it is destroyed.
class DatabaseOption final : public detail::Handle<DatabaseOption> {
	friend detail::Factory;

public:
	/// An option setting `name` to `value`, to hand to `Environment::Open` or to a `SetOption`. The value is parsed
	/// when the option is applied, so an unknown name or an ill-typed value throws there rather than here.
	/// @param name The setting to write, either its canonical name or one of its aliases.
	/// @param value The new value, in the same textual form SQL's `SET` accepts.
	DatabaseOption(const std::string &name, const std::string &value);

	DatabaseOption(DatabaseOption &&) noexcept = default;
	DatabaseOption &operator=(DatabaseOption &&) noexcept = default;

	/// The setting's name.
	auto GetName() const -> std::string_view;

	/// The value this option carries, as text.
	auto GetValue() const -> std::string_view;

	/// The value the setting falls back to when it is not set. Empty until the option has been read back from a
	/// database or connection.
	auto GetDefaultValue() const -> std::string_view;

	/// A human-readable description of the setting. Empty until the option has been read back from a database or
	/// connection.
	auto GetDescription() const -> std::string_view;

	/// At which scope this setting may be written. UNKNOWN until the option has been read back from a database or
	/// connection.
	auto GetTargetScope() const -> OptionTargetScope;

	/// How many alternative names resolve to this setting. 0 until the option has been read back from a database or
	/// connection.
	auto GetAliasCount() const -> size_t;

	/// One of the setting's aliases.
	/// @param index Alias index in [0, GetAliasCount()).
	auto GetAliasByIndex(size_t index) const -> std::string_view;

	~DatabaseOption() override;

private:
	explicit DatabaseOption(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------
// The client context of an operation in flight, handed to code that DuckDB calls back into.
// It is more or less equivalent to a `Connection`, but viewed from the "inside" of the system.

/// The severity of a log message, compared against the database's configured threshold.
enum class LogLevel : uint32_t {
	LOG_TRACE = 10,
	LOG_DEBUG = 20,
	LOG_INFO = 30,
	LOG_WARNING = 40,
	LOG_ERROR = 50,
	LOG_FATAL = 60,
};

/// A borrowed handle to the client context of a running operation.
/// Only valid for the duration of the callback it was handed to, it is generally not safe to store.
class Context final : public detail::Handle<Context> {
	friend detail::Factory;

public:
	~Context() override;

	/// Parses a SQL type expression into an owned type: primitives, parameterized kinds, and extension types alike.
	/// @param text A type as SQL spells it, e.g. "DECIMAL(18, 3)" or "STRUCT(a INTEGER, b VARCHAR)".
	/// @return The parsed type.
	/// @throws Exception When the text does not name a type.
	auto ParseType(std::string_view text) const -> LogicalType;

	/// Builds a type from a type name plus parameters. Like a structured version of `ParseType`
	/// @param name The type's unqualified name, e.g. "LIST" or "DECIMAL".
	/// @param params The type's parameters, in the order SQL takes them. A `TypeParam` with an empty name is considered
	/// a positional parameter.
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params) const -> LogicalType;
	/// Parameterless overload of the above.
	auto CreateType(std::string_view name) const -> LogicalType;

	/// `CreateType` for a name that may be catalog- or schema-qualified. An unqualified name is resolved along the
	/// search path and then in the system catalog; a qualified one is resolved exactly as written.
	auto CreateType(const QualifiedName &name, const std::vector<TypeParam> &params = {}) const -> LogicalType;

	/// The file system this context reads and writes through. Borrowed, and valid only while the context is.
	auto GetFileSystem() const -> FileSystem;

	/// The id-keyed twin of `CreateType`: the id resolves to its canonical name and binds like it.
	/// @param id The type's id. Without parameters, only ids that name a complete type on their own are accepted;
	/// parameterized kinds such as LIST or DECIMAL require parameters.
	/// @param params The type's parameters, as in the name-keyed overload.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params) const -> LogicalType;
	/// Parameterless overload of the above.
	auto CreateType(LogicalTypeId id) const -> LogicalType;

	/// Starts composing a type step by step.
	/// @return A `TypeBuilder` over this context, for composing a nested type without assembling the parameter vector
	/// by hand.
	auto CreateType() -> TypeBuilder<Context>;

	/// Creates a `Value` in this context; see `Value::Create` for the accepted C++ types.
	/// @param value The C++ value to convert.
	template <class T>
	auto CreateValue(T &&value) -> Value;

	/// Writes a message to DuckDB's log, readable through `SELECT * FROM duckdb_logs`.
	/// Whether the entry is recorded is up to the database's log configuration; a message it filters out is dropped
	/// without error.
	/// @param level The severity of the message.
	/// @param message The message body.
	/// @param log_type The log type to record under, matched case-sensitively. Empty selects the default type.
	auto Log(LogLevel level, std::string_view message, std::string_view log_type = {}) const -> void;

private:
	explicit Context(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// SQL statements
//----------------------------------------------------------------------------------------------------------------------
// A SQL string is parsed once into statements, which can then be bound or executed repeatedly. Parsing is purely
// syntactic: it touches no catalog and opens no transaction, so unknown tables and type errors surface at bind or
// execution time instead.

/// An owned, parsed SQL statement, produced by `StatementIterator::Next` and executed by `Connection::Execute`.
/// Executing borrows the statement, so the same one can be executed any number of times.
class SqlStatement final : public detail::Handle<SqlStatement> {
	friend detail::Factory;

public:
	SqlStatement(SqlStatement &&) noexcept = default;
	SqlStatement &operator=(SqlStatement &&) noexcept = default;

	~SqlStatement() override;

private:
	explicit SqlStatement(void *impl);
};

/// An owned iterator over the statements in a SQL string, produced by `Connection::ParseSQL`.
/// Statements it has already yielded are independent of it and stay valid after the iterator is destroyed.
class StatementIterator final : public detail::Handle<StatementIterator> {
	friend detail::Factory;

public:
	StatementIterator(StatementIterator &&) noexcept = default;
	StatementIterator &operator=(StatementIterator &&) noexcept = default;

	~StatementIterator() override;

	/// The next statement, or an empty handle once the string is exhausted; calling it again then keeps returning
	/// empty.
	/// @throws Exception When the next statement does not parse; the iterator is exhausted afterwards.
	auto Next() -> SqlStatement;

private:
	explicit StatementIterator(void *impl);
};

/// A statement bound and planned once, executable repeatedly. Produced by `Connection::Prepare`.
/// Where `Connection::Execute` re-binds on every call, this may run the plan it built at prepare time; ask
/// `ReusesPlan` which one you got. Execution returns the same `QueryResult`, with identical behaviour.
/// It keeps its connection's session alive, so it stays usable even after the `Connection` is gone.
class PreparedStatement final : public detail::Handle<PreparedStatement> {
	friend detail::Factory;

public:
	PreparedStatement(PreparedStatement &&) noexcept = default;
	PreparedStatement &operator=(PreparedStatement &&) noexcept = default;

	~PreparedStatement() override;

	/// Executes with positional parameters ($1 = parameters[0]), returning a lazy streaming result.
	/// Non-consuming: execute the same statement again, with the same values or different ones.
	/// @param parameters Values for the statement's parameters, bound positionally.
	/// @param parameter_count How many values `parameters` points at.
	/// @return A streaming result. Execution is deferred until the result is read; binding errors throw here.
	/// @throws Exception While an earlier result on the connection is still live. A failed execution leaves the
	/// prepared statement usable.
	auto Execute(const Value *parameters, idx_t parameter_count) -> QueryResult;

	/// Executes a statement that takes no parameters.
	auto Execute() -> QueryResult;

	/// `std::vector` overload of the positional-parameter `Execute`.
	auto Execute(const std::vector<Value> &parameters) -> QueryResult;

	/// Executes with named parameters.
	/// @param parameters One binding per parameter; each binds to $name, or positionally when its name is empty.
	auto Execute(const std::vector<NamedParam> &parameters) -> QueryResult;

	/// Whether executions reuse the plan built at prepare time, rather than re-binding each time and being no
	/// faster than `Connection::Execute`. A static property of the built plan; see the C API's
	/// `duckdb_v2_prepared_statement_reuses_plan` for what qualifies.
	auto ReusesPlan() const -> bool;

private:
	explicit PreparedStatement(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------------------------------------------------
// A session on a `Database`: the handle that parses, binds and executes SQL, creates types and values outside of a
// callback, and carries session-scoped settings. A connection is not thread-safe -- give each thread its own, via
// `Database::Connect` -- with the deliberate exception of `Interrupt` and `GetQueryProgress`, which exist to be called
// while another thread runs a query.

/// A connection to a database.
/// It must not outlive the `Database` it was opened on, and only one result may be live on it at a time.
class Connection final : public detail::Handle<Connection> {
	friend detail::Factory;

public:
	Connection(Connection &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
	}

	Connection &operator=(Connection &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
		return *this;
	}

	/// A snapshot of the running query's progress. Progress is only tracked while the `enable_progress_bar` setting is
	/// on; a percentage of -1 with both row counts at 0 means no information is available.
	struct QueryProgress {
		/// Completion in [0, 100], or -1 when unknown.
		double percentage;
		/// Rows processed so far.
		uint64_t rows_processed;
		/// Rows the query expects to process in total.
		uint64_t total_rows_to_process;
	};

	~Connection() override;

	/// How many settings this connection exposes.
	auto GetOptionCount() const -> size_t;

	/// One setting with its current value on this connection.
	/// @param index Setting index in [0, GetOptionCount()).
	auto GetOptionByIndex(size_t index) const -> DatabaseOption;

	/// One setting with its current value on this connection.
	/// @param name The setting's name or one of its aliases.
	/// @return The setting.
	/// @throws InvalidInputException When no setting goes by that name.
	auto GetOption(std::string_view name) const -> DatabaseOption;

	/// Writes a setting at the scope it declares for itself, like SQL `SET name = value`.
	/// @param option The name/value pair to apply.
	auto SetOption(const DatabaseOption &option) -> void;

	/// Writes a setting at an explicit scope.
	/// @param option The name/value pair to apply.
	/// @param scope GLOBAL to write it database-wide, LOCAL for this session only.
	/// @throws Exception When the setting does not allow the requested scope.
	auto SetOption(const DatabaseOption &option, SettingScope scope) -> void;

	/// Parses a SQL string into an iterator over its statements, without binding or executing any of them.
	/// Parsing happens statement by statement as the iterator advances, so a syntax error surfaces from
	/// `StatementIterator::Next` rather than from this call.
	/// @param sql One or more semicolon-separated SQL statements.
	auto ParseSQL(const char *sql) -> StatementIterator;

	/// `std::string` overload of `ParseSQL`.
	auto ParseSQL(const std::string &sql) -> StatementIterator {
		return ParseSQL(sql.c_str());
	}

	/// Executes a statement, borrowing it rather than consuming it, so the same statement can be executed again.
	/// @param statement The statement to execute.
	/// @param parameters Values for the statement's parameters, bound positionally ($1 = parameters[0]).
	/// @param parameter_count How many values `parameters` points at.
	/// @return A streaming result. Execution is deferred until the result is read; binding errors throw here.
	/// @throws Exception While an earlier result on this connection is still live.
	auto Execute(const SqlStatement &statement, const Value *parameters, idx_t parameter_count) -> QueryResult;

	/// Executes a statement that takes no parameters.
	auto Execute(const SqlStatement &statement) -> QueryResult;

	/// `std::vector` overload of the positional-parameter `Execute`.
	auto Execute(const SqlStatement &statement, const std::vector<Value> &parameters) -> QueryResult;

	/// Executes a statement with named parameters.
	/// @param statement The statement to execute.
	/// @param parameters One binding per parameter; each binds to $name, or positionally when its name is empty. A
	/// statement cannot mix named and positional parameters.
	auto Execute(const SqlStatement &statement, const std::vector<NamedParam> &parameters) -> QueryResult;

	/// Parses and executes a single SQL statement in one call.
	/// @param sql Exactly one SQL statement. Use `ParseSQL` for multi-statement input.
	/// @throws InvalidInputException When `sql` does not hold exactly one statement.
	auto Execute(const std::string &sql) -> QueryResult;

	/// Binds a statement without executing it, borrowing it rather than consuming it.
	/// @param statement The statement to bind.
	/// @return Its signature: the columns it will produce and the parameters it expects.
	auto Bind(const SqlStatement &statement) const -> Signature;

	/// Resolves a table name and snapshots its description: where it resolved, its columns, and per-column facts.
	/// @param name A possibly partial table name, resolved through the search path exactly as SQL resolves it.
	/// @throws Exception When the name resolves to nothing, to a view, or is ambiguous between a schema and an attached
	/// database.
	auto DescribeTable(const QualifiedName &name) const -> TableDescription;

	/// Prepares a statement into a handle that can be executed repeatedly, borrowing it rather than consuming it.
	/// @param statement The statement to prepare.
	/// @param require_cacheable When true, fail rather than return a statement whose plan is re-bound on every
	/// execution, so a caller who wants the handle only for the speedup finds out here.
	/// @throws Exception On a bind or catalog error, while a result on this connection is still live, or when
	/// `require_cacheable` is set and the plan would not be reused.
	auto Prepare(const SqlStatement &statement, bool require_cacheable = false) -> PreparedStatement;

	/// `Context::ParseType` outside a callback.
	/// @param text A type as SQL spells it, e.g. "DECIMAL(18, 3)" or "STRUCT(a INTEGER, b VARCHAR)".
	auto ParseType(std::string_view text) -> LogicalType;

	/// `Context::CreateType` outside a callback.
	/// @param name The type's unqualified name, e.g. "LIST" or "DECIMAL".
	/// @param params The type's parameters, in the order SQL takes them. A `TypeParam` with an empty name is
	/// positional.
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params) -> LogicalType;
	/// Parameterless overload of the above.
	auto CreateType(std::string_view name) -> LogicalType;

	/// `CreateType` for a name that may be catalog- or schema-qualified. An unqualified name is resolved along the
	/// search path and then in the system catalog; a qualified one is resolved exactly as written.
	auto CreateType(const QualifiedName &name, const std::vector<TypeParam> &params = {}) -> LogicalType;

	/// The file system this connection reads and writes through. Borrowed, and valid only while the connection is.
	auto GetFileSystem() const -> FileSystem;

	/// The id-keyed twin of `CreateType`: the id resolves to its canonical name and binds like it.
	/// @param id The type's id. Without parameters, only ids that name a complete type on their own are accepted;
	/// parameterized kinds such as LIST or DECIMAL require parameters.
	/// @param params The type's parameters, as in the name-keyed overload.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params) -> LogicalType;
	/// Parameterless overload of the above.
	auto CreateType(LogicalTypeId id) -> LogicalType;

	/// Starts composing a type step by step.
	/// @return A `TypeBuilder` over this connection, for composing a nested type without assembling the parameter
	/// vector by hand.
	auto CreateType() -> TypeBuilder<Connection>;

	/// Creates a `Value` on this connection; see `Value::Create` for the accepted C++ types.
	/// @param value The C++ value to convert.
	template <class T>
	auto CreateValue(T &&value) -> Value;

	/// Asks the running query to stop. `QueryResult::Step` then reports CANCELLED, and `FetchChunk` / `Drain` throw
	/// `InterruptException`. Callable from any thread, and a no-op when no query is running.
	auto Interrupt() -> void;

	/// Reads how far the running query has come. Callable from any thread.
	auto GetQueryProgress() const -> QueryProgress;

	/// Wraps a connection handle owned by someone else.
	/// @param opaque The borrowed handle; must be a connection handle of the DuckDB C API.
	/// @return A non-owning `Connection`: it must not outlive the handle, and it will not disconnect on destruction.
	static auto FromOpaque(void *opaque) -> Connection {
		return Connection(opaque, false);
	}

private:
	explicit Connection(void *impl, bool owned);
	bool owned = false; // TODO: This should be fixed C++ side
};

//----------------------------------------------------------------------------------------------------------------------
// Database
//----------------------------------------------------------------------------------------------------------------------
// An open database: the catalog, the storage behind it, and the settings shared by every session on it. Databases are
// opened through an `Environment` and worked with through the `Connection`s they hand out.

/// An open database. It must outlive every `Connection` opened on it.
class Database final : public detail::Handle<Database> {
	friend detail::Factory;

public:
	~Database() override;
	Database(Database &&) noexcept = default;
	Database &operator=(Database &&) noexcept = default;

	/// How many settings this database exposes.
	auto GetOptionCount() const -> size_t;

	/// One setting with its current global value.
	/// @param index Setting index in [0, GetOptionCount()).
	auto GetOptionByIndex(size_t index) const -> DatabaseOption;

	/// One setting with its current global value.
	/// @param name The setting's name or one of its aliases; an alias resolves to the canonical setting.
	/// @return The setting.
	/// @throws InvalidInputException When no setting goes by that name.
	auto GetOption(std::string_view name) const -> DatabaseOption;

	/// Writes a setting globally, for this database and every session on it.
	/// @param option The name/value pair to apply.
	auto SetOption(const DatabaseOption &option) -> void;

	/// Opens a new session on this database.
	/// @return An owning `Connection`, which disconnects when destroyed. Open one per thread.
	auto Connect() -> Connection;

private:
	explicit Database(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Extension
//----------------------------------------------------------------------------------------------------------------------
// Loading an extension is how catalog entries (functions, types, casts) and database-level hooks get installed under
// the extension's identity, so DuckDB can attribute them to the extension that provided them. Outside a load, the same
// objects are registered on a `Connection` or a `Database` instead.

/// The extension being loaded, handed to its load entry point.
/// Borrowed for the duration of the load: never store or outlive one.
class Extension final : public detail::Handle<Extension> {
	friend detail::Factory;

public:
	~Extension() override;

	// TODO: (You can't do anything with this yet, but in the future will be able to register functions, types etc.)

private:
	explicit Extension(void *impl);
};

namespace detail {
/// Backs `DUCKDB_CPP_EXTENSION_ENTRYPOINT` in duckdb_cpp_extension.hpp: wraps the raw loader handles and runs the
/// extension's body, reporting a thrown `Exception` through the error slot so it fails the load. The handles are
/// passed untyped to keep the C loader types out of this header.
auto RunExtensionEntry(void (*body)(Extension &, Context &), void *extension, void *context, void *err) -> void;
} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Environment
//----------------------------------------------------------------------------------------------------------------------
// The entry point to the API: an `Environment` opens databases and tracks the ones it has opened. Create one, keep it
// for as long as any database is open, and open databases through it.

/// The environment databases are opened in. It must outlive every `Database` opened through it; destroying it while
/// databases are still open leaks them.
class Environment final : public detail::Handle<Environment> {
	friend detail::Factory;

public:
	Environment();
	~Environment() override;
	Environment(Environment &&) noexcept = default;
	Environment &operator=(Environment &&) noexcept = default;

	/// How many databases are currently open in this environment.
	auto GetOpenDatabaseCount() const -> size_t;

	/// Opens a database with default settings.
	/// @param path The database file, or ":memory:" / the empty string for an in-memory database.
	auto Open(const std::string &path) -> Database;

	/// Opens a database, configuring it up front. Settings such as access_mode and the storage options can only be
	/// chosen here, before the database exists.
	/// @param path The database file, or ":memory:" / the empty string for an in-memory database.
	/// @param options The settings to open with. Borrowed for the call only; the caller keeps them.
	auto Open(const std::string &path, const std::vector<DatabaseOption> &options) -> Database;
};

/// The version of the DuckDB library this program is linked against, e.g. "v1.5.0", with a suffix such as
/// "v1.5.0-dev123" on development builds.
auto LibraryVersion() -> std::string;

/// Renders a name as a SQL identifier, quoting and escaping only when required: the name itself when it is a legal
/// bare identifier, or double-quoted with interior quotes doubled when it is a keyword or contains characters that
/// require quoting. Use it for every name embedded in SQL text rather than quoting by hand.
/// @param name The name to render.
auto RenderQuotedIdentifier(std::string_view name) -> std::string;

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------
// The SQL type of a column, value or parameter. Types are built through a `Context` or `Connection`: by parsing SQL
// text, by name and parameters, or with a `TypeBuilder`. Once built, a `LogicalType` is an owned, self-contained value
// that can be inspected without any scope.

/// The "id" of a built-in SQL type.
/// Parameterized kinds such as LIST and DECIMAL are represented by their id plus parameters, the id alone does not name
/// a complete type.
enum class LogicalTypeId : uint32_t {
	INVALID = 0,
	SQLNULL = 1,
	UNKNOWN = 2,
	ANY = 3,
	TYPE = 6,
	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
	TIMESTAMP = 19,
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
	DOUBLE = 23,
	VARCHAR = 25,
	BLOB = 26,
	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	UINTEGER = 30,
	UBIGINT = 31,
	TIMESTAMP_TZ = 32,
	TIMESTAMP_TZ_NS = 33,
	TIME_TZ = 34,
	TIME_NS = 35,
	BIT = 36,
	BIGNUM = 39,
	UHUGEINT = 49,
	HUGEINT = 50,
	UUID = 54,
	GEOMETRY = 60,
	STRUCT = 100,
	LIST = 101,
	MAP = 102,
	ENUM = 104,
	UNION = 107,
	ARRAY = 108,
	VARIANT = 109,
	TUPLE = 110,
};

/// An owned SQL type: a kind plus its parameters, e.g. DECIMAL(18, 3) or STRUCT(a INTEGER, b VARCHAR), and in the case
/// of extension-defined types, its "alias"
class LogicalType final : public detail::Handle<LogicalType> {
	friend detail::Factory;

public:
	LogicalType(LogicalType &&) noexcept = default;
	LogicalType &operator=(LogicalType &&) noexcept = default;

	~LogicalType() override;

	/// A copy of this type carrying `alias` as its name: the same representation under a distinct identity.
	/// The alias is by default not registered anywhere; parsing or creating a type with this name does not resolve to
	/// this type, unless it is explicitly registered in the catalog separately.
	/// @param ctx The context to create the copy in.
	/// @param alias The name the copy carries. Must not be empty.
	auto WithAlias(const Context &ctx, std::string_view alias) const -> LogicalType;

	/// `WithAlias` outside a callback.
	auto WithAlias(const Connection &conn, std::string_view alias) const -> LogicalType;

	/// The kind of this type, e.g. `LogicalTypeId::DECIMAL` for any DECIMAL regardless of its parameters.
	auto GetTypeId() const -> LogicalTypeId;

	/// The type's name: its alias when it has one, otherwise the fixed name of its kind. Never empty.
	/// @return A view borrowed from this type, valid until it is destroyed.
	auto GetName() const -> std::string_view;

	/// Whether both types are the same type, parameters and alias included.
	bool operator==(const LogicalType &other) const;

	bool operator!=(const LogicalType &other) const {
		return !(*this == other);
	}

	/// Renders the type as SQL text. An aliased type is rendered as its alias.
	auto ToText() const -> std::string;

	/// How many parameters the type carries:
	/// - DECIMAL 2 (width, scale)
	/// - LIST 1 (element type)
	/// - ARRAY 2 (element type, size)
	/// - MAP 2 (key, value types)
	/// - STRUCT and TUPLE one per field
	/// - UNION one per member
	/// - ENUM one per dictionary entry;
	/// - VARCHAR 1 when a collation is set;
	/// - GEOMETRY 1 when a coordinate system is set;
	/// anything else 0.
	auto GetParamCount() const -> idx_t;

	/// Get one of the type's parameters. The parameter's name is empty when it is positional, otherwise it is the name
	/// of the field or member it describes.
	/// @param index Parameter index in [0, GetParamCount()).
	/// @return An owned name and an owned value. Child types arrive as TYPE values.
	auto GetParam(idx_t index) const -> TypeParam;

	// Per-kind shorthands over `GetParam` / `GetParamCount`. They do not verify the type's kind: on a type of another
	// kind they misread that type's parameters or throw, so check the kind first. Names and dictionary entries come
	// back as owned strings.

	/// The total number of digits a DECIMAL holds.
	auto GetDecimalWidth() const -> uint8_t;

	/// The number of digits a DECIMAL keeps after the point.
	auto GetDecimalScale() const -> uint8_t;

	/// How many entries an ENUM's dictionary holds.
	auto GetEnumSize() const -> idx_t;

	/// One ENUM dictionary entry.
	/// @param index Entry index in [0, GetEnumSize()).
	auto GetEnumValue(idx_t index) const -> std::string;

	/// The element type of a LIST.
	auto GetListChildType() const -> LogicalType;

	/// The element type of an ARRAY.
	auto GetArrayChildType() const -> LogicalType;

	/// How many elements every row of an ARRAY holds.
	auto GetArraySize() const -> idx_t;

	/// The key type of a MAP.
	auto GetMapKeyType() const -> LogicalType;

	/// The value type of a MAP.
	auto GetMapValueType() const -> LogicalType;

	/// How many fields a STRUCT has.
	auto GetStructChildCount() const -> idx_t;

	/// The name of one STRUCT field, empty for the fields of an unnamed STRUCT.
	/// @param index Field index in [0, GetStructChildCount()).
	auto GetStructChildName(idx_t index) const -> std::string;

	/// The type of one STRUCT field.
	/// @param index Field index in [0, GetStructChildCount()).
	auto GetStructChildType(idx_t index) const -> LogicalType;

	/// How many members a UNION has.
	auto GetUnionMemberCount() const -> idx_t;

	/// The tag of one UNION member.
	/// @param index Member index in [0, GetUnionMemberCount()).
	auto GetUnionMemberName(idx_t index) const -> std::string;

	/// The type of one UNION member.
	/// @param index Member index in [0, GetUnionMemberCount()).
	auto GetUnionMemberType(idx_t index) const -> LogicalType;

	/// The integer type a DECIMAL's digits are stored in, which is what its vector elements are; determined by the
	/// width.
	auto GetDecimalInternalTypeId() const -> LogicalTypeId;

	/// The unsigned integer type an ENUM's dictionary indices are stored in, which is what its vector elements are;
	/// determined by the dictionary size.
	auto GetEnumInternalTypeId() const -> LogicalTypeId;

private:
	explicit LogicalType(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Schema
//----------------------------------------------------------------------------------------------------------------------
// The shape of a row: which fields it has, in order, and of what type. Used both for what a statement produces and
// for what it expects.

/// An ordered list of (name, type) fields. Names may repeat, and a schema may have no fields at all.
class Schema final : public detail::Handle<Schema> {
	friend detail::Factory;

public:
	Schema(Schema &&) noexcept = default;
	Schema &operator=(Schema &&) noexcept = default;

	~Schema() override;

	/// How many fields the schema has.
	auto GetFieldCount() const -> idx_t;

	/// The name of one field.
	/// @param index Field index in [0, GetFieldCount()).
	/// @return A view borrowed from this schema, valid until it is destroyed.
	auto GetFieldName(idx_t index) const -> std::string_view;

	/// An owned copy of one field's type.
	/// @param index Field index in [0, GetFieldCount()).
	auto GetFieldType(idx_t index) const -> LogicalType;

private:
	explicit Schema(void *impl);
};

/// What `Connection::Bind` learns about a statement without running it.
class Signature {
public:
	/// The columns the statement will produce.
	Schema output;
	/// The statement's parameters, ordered by binding key. A field's name is the key: "1", "2", ... for positional
	/// parameters, the name for named ones.
	Schema parameters;
};

//----------------------------------------------------------------------------------------------------------------------
// Primitive data types
//----------------------------------------------------------------------------------------------------------------------
// One C++ type per SQL type, laid out exactly as it appears as an element of a vector. These are what
// `VectorView::Data<T>` reads arrays of, and what `Value::Get<T>` and `Value::Create` are keyed on, so the type picked
// here also selects the SQL type read or written.
//
// A few of them store their value in an encoded form. Those carry `Decode` / `Encode` or named accessors, and the raw
// field is not the value you want.
//
// DATE and the TIMESTAMP types reserve their most extreme values as +/- infinity sentinels.

/// HUGEINT: a 128-bit signed integer, in two halves.
struct int128_t {
	/// The low 64 bits.
	uint64_t lower;
	/// The high 64 bits, carrying the sign.
	int64_t upper;
};

/// UHUGEINT: a 128-bit unsigned integer, in two halves.
struct uint128_t {
	/// The low 64 bits.
	uint64_t lower;
	/// The high 64 bits.
	uint64_t upper;
};

/// DATE: days since 1970-01-01.
struct date_t {
	int32_t days = 0;
};

/// TIME: microseconds since midnight.
struct dtime_t {
	int64_t micros = 0;
};

/// TIME_NS: nanoseconds since midnight.
struct dtime_ns_t {
	int64_t nanos = 0;
};

/// TIME_TZ: a time of day together with its UTC offset, packed into one integer -- time in the high 40 bits, the
/// offset reverse-ordered in the low 24. Read it through the accessors; a plain shift gives neither.
struct dtime_tz_t {
	static constexpr int OFFSET_BITS = 24;
	static constexpr uint64_t OFFSET_MASK = (uint64_t(1) << OFFSET_BITS) - 1;
	static constexpr int32_t MAX_OFFSET = 16 * 60 * 60 - 1; // +/-15:59:59

	dtime_tz_t() = default;

	/// Wraps the packed form, as it appears as a vector element.
	explicit dtime_tz_t(uint64_t bits) : bits(bits) {
	}

	/// Packs a time and an offset.
	/// @param micros Microseconds since midnight.
	/// @param offset_seconds The UTC offset in seconds, positive east, in [-MAX_OFFSET, MAX_OFFSET].
	dtime_tz_t(int64_t micros, int32_t offset_seconds)
	    : bits((static_cast<uint64_t>(micros) << OFFSET_BITS) |
	           (static_cast<uint64_t>(MAX_OFFSET - offset_seconds) & OFFSET_MASK)) {
	}

	/// Microseconds since midnight.
	auto GetMicros() const -> int64_t {
		return static_cast<int64_t>(bits >> OFFSET_BITS);
	}

	/// The UTC offset in seconds, positive east.
	auto GetOffset() const -> int32_t {
		return MAX_OFFSET - static_cast<int32_t>(bits & OFFSET_MASK);
	}

	/// The packed form, as it appears as a vector element.
	auto GetBits() const -> uint64_t {
		return bits;
	}

private:
	uint64_t bits = 0;
};

/// TIMESTAMP: microseconds since 1970-01-01 00:00:00.
struct timestamp_t {
	int64_t micros = 0;
};

/// TIMESTAMP_SEC: seconds since 1970-01-01 00:00:00.
struct timestamp_s_t {
	int64_t seconds = 0;
};

/// TIMESTAMP_MS: milliseconds since 1970-01-01 00:00:00.
struct timestamp_ms_t {
	int64_t millis = 0;
};

/// TIMESTAMP_NS: nanoseconds since 1970-01-01 00:00:00.
struct timestamp_ns_t {
	int64_t nanos = 0;
};

/// TIMESTAMP_TZ: microseconds since the epoch, in UTC. The display time zone is a session setting, not part of the
/// value.
struct timestamp_tz_t {
	int64_t micros = 0;
};

/// TIMESTAMP_TZ_NS: nanoseconds since the epoch, in UTC.
struct timestamp_tz_ns_t {
	int64_t nanos = 0;
};

/// INTERVAL: a span of time, kept in three separate units because months and days are not fixed lengths of time.
struct interval_t {
	int32_t months;
	int32_t days;
	int64_t micros;
};

/// The element of a LIST vector: where a row's elements are in the child vector, rather than the elements themselves.
struct list_entry_t {
	/// Index of the row's first element in the child vector.
	uint64_t offset = 0;
	/// How many elements the row has.
	uint64_t length = 0;
};

/// DECIMAL(WIDTH, SCALE): an exact number, kept as an integer of WIDTH digits with the point SCALE digits from the
/// right. The width picks the storage integer, so DECIMAL(9, 2) and DECIMAL(18, 2) are different C++ types and their
/// payloads are not interchangeable.
template <int8_t WIDTH, uint8_t SCALE>
struct decimal_t {
	static_assert(WIDTH >= 1 && WIDTH <= 38, "DECIMAL type width must be between 1 and 38");
	static_assert(SCALE <= WIDTH, "DECIMAL scale must be less than or equal to width");
	using storage_type = std::conditional_t<
	    (WIDTH <= 4), int16_t,
	    std::conditional_t<(WIDTH <= 9), int32_t, std::conditional_t<(WIDTH <= 18), int64_t, int128_t>>>;

	storage_type value;
};

/// BLOB: a string of bytes, in the form it takes as a vector element. Up to INLINE_LENGTH bytes live in the element
/// itself; longer ones live elsewhere and the element only points at them.
///
/// A blob never owns its bytes. Constructing one from a `std::string_view` borrows that memory rather than copying it,
/// so a long blob is only valid while whatever holds the bytes is: use `Arena::AddString` / `Vector::AssignString` to
/// put bytes somewhere that lives as long as the vector.
struct blob_t {
	/// The longest byte string that fits in a vector element without being stored elsewhere.
	static constexpr uint32_t INLINE_LENGTH = 12;
	/// How many leading bytes a non-inlined blob keeps alongside the pointer, for comparisons.
	static constexpr uint32_t PREFIX_LENGTH = 4;

	union {
		struct {
			uint32_t length;
			char prefix[PREFIX_LENGTH];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[INLINE_LENGTH];
		} inlined;
	} value;

	/// An empty blob.
	blob_t() {
		std::memset(&value, 0, sizeof(value));
	}

	/// Borrows the bytes of `str`, which must outlive the blob unless they fit inline.
	// NOLINTNEXTLINE
	blob_t(std::string_view str) : blob_t(str.data(), static_cast<uint32_t>(str.size())) {
	}

	/// Borrows `size` bytes at `data`, which must outlive the blob unless they fit inline.
	blob_t(const char *data, uint32_t size) {
		if (!data || size == 0) {
			std::memset(&value, 0, sizeof(value));
			value.inlined.length = size;
			return;
		}
		if (size <= INLINE_LENGTH) {
			std::memset(&value, 0, sizeof(value));
			value.inlined.length = size;
			std::memcpy(value.inlined.inlined, data, size);
		} else {
			std::memset(&value, 0, sizeof(value));
			value.pointer.length = size;
			value.pointer.ptr = const_cast<char *>(data); // NOLINT
			std::memcpy(value.pointer.prefix, data, PREFIX_LENGTH);
		}
	}

	explicit operator std::string_view() const {
		return std::string_view(data(), size());
	}

	explicit operator std::string() const {
		return std::string(data(), size());
	}

	/// Whether the bytes live in the value itself rather than behind the pointer.
	auto is_inlined() const -> bool {
		return value.inlined.length <= INLINE_LENGTH;
	}

	/// The number of bytes.
	auto size() const -> uint32_t {
		return value.inlined.length;
	}

	/// The bytes, wherever they live. Not null-terminated.
	auto data() const -> const char * {
		return is_inlined() ? value.inlined.inlined : value.pointer.ptr;
	}

	/// Mutable access to the bytes. Editing a non-inlined blob's bytes leaves its cached prefix stale, so prefer
	/// building a new value over patching one in place.
	auto data_mut() -> char * {
		return is_inlined() ? value.inlined.inlined : value.pointer.ptr;
	}

	/// The bytes as a view, valid as long as the blob and whatever holds its bytes are.
	auto view() const -> std::string_view {
		return std::string_view(data(), size());
	}
};

/// VARCHAR: like `blob_t`, but naming a string of UTF-8 text rather than of arbitrary bytes.
struct varchar_t : blob_t {
	using blob_t::blob_t;
};

/// BIT: a string of bits, stored as a padding-count byte followed by the data bytes. The count says how many leading
/// bits of the first data byte are not part of the bit string, and those padding bits are set to 1. The raw bytes are
/// not the bits: read them through the accessors, and note that the constructors inherited from `blob_t` take these
/// encoded bytes, not a bit string.
struct bit_t : blob_t {
	using blob_t::blob_t;

	/// How many leading bits of the first data byte are padding.
	auto GetPaddingBits() const -> uint8_t {
		return size() > 0 ? static_cast<uint8_t>(data()[0]) : 0;
	}

	/// The data bytes, past the padding-count byte.
	auto GetBits() const -> const char * {
		return size() > 0 ? data() + 1 : data();
	}

	/// How many data bytes there are.
	auto GetBitsSize() const -> uint32_t {
		return size() > 0 ? size() - 1 : 0;
	}

	/// How many bits the string actually holds, padding excluded.
	auto GetBitCount() const -> uint64_t {
		return GetBitsSize() == 0 ? 0 : static_cast<uint64_t>(GetBitsSize()) * 8 - GetPaddingBits();
	}
};

/// BIGNUM: an arbitrary-precision integer. Its bytes are encoded so that comparing them matches numeric order, which
/// among other things bit-inverts negative values, so they are not the magnitude. `Decode` and `Encode` translate; the
/// constructors inherited from `blob_t` take the encoded bytes.
struct bignum_t : blob_t {
	using blob_t::blob_t;

	/// A BIGNUM as a number: the integer is (-1)^is_negative * unsigned_big_endian(magnitude). Owns its bytes.
	struct Decoded {
		/// The magnitude, big-endian, without leading zeroes; zero is a single 0x00.
		std::vector<uint8_t> magnitude;
		/// Whether the integer is negative.
		bool is_negative;
	};

	/// The magnitude and sign this value stands for.
	auto Decode() const -> Decoded;

	/// The inverse: the bytes to build a BIGNUM value from.
	/// @param value The magnitude and sign, with the magnitude canonical: at least one byte, no leading zeroes, and
	/// zero written as a single 0x00 with `is_negative` false.
	static auto Encode(const Decoded &value) -> std::vector<uint8_t>;
};

/// UUID: a 128-bit identifier, stored as an integer with its high bit flipped so that comparing the integers matches
/// UUID order. The canonical bytes only come out through `Decode`. Distinct from `int128_t`, which names HUGEINT.
struct uuid_t {
	int128_t value {};

	uuid_t() = default;

	/// Wraps the stored form, as it appears as a vector element.
	explicit uuid_t(int128_t storage) : value(storage) {
	}

	/// A UUID as its canonical 16 big-endian bytes.
	struct Decoded {
		uint8_t bytes[16];
	};

	/// The canonical 16 big-endian bytes.
	auto Decode() const -> Decoded {
		const uint64_t upper = static_cast<uint64_t>(value.upper) ^ (static_cast<uint64_t>(1) << 63);
		Decoded out {};
		for (int i = 0; i < 8; i++) {
			out.bytes[i] = static_cast<uint8_t>((upper >> (56 - 8 * i)) & 0xFF);
			out.bytes[8 + i] = static_cast<uint8_t>((value.lower >> (56 - 8 * i)) & 0xFF);
		}
		return out;
	}

	/// The inverse: the stored form for a set of canonical bytes.
	/// @param bytes The UUID's 16 big-endian bytes.
	static auto Encode(const Decoded &bytes) -> uuid_t {
		uint64_t upper = 0;
		uint64_t lower = 0;
		for (int i = 0; i < 8; i++) {
			upper = (upper << 8) | bytes.bytes[i];
			lower = (lower << 8) | bytes.bytes[8 + i];
		}
		return uuid_t(int128_t {lower, static_cast<int64_t>(upper ^ (static_cast<uint64_t>(1) << 63))});
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------
// A single SQL value, type included: what a statement parameter binds to, what a type parameter carries, and the way
// to read or write one cell of a vector without caring how it is represented. Values are owned and self-contained;
// creating one is scoped to a `Context` or `Connection`.
//
// For bulk data, go through `Vector` instead: a value costs an allocation, so it is the wrong tool per row.

/// An owned SQL value.
class Value final : public detail::Handle<Value> {
	friend detail::Factory;

public:
	~Value() override;

	Value(Value &&) noexcept = default;
	Value &operator=(Value &&) noexcept = default;

	/// Whether this is SQL NULL. A NULL value still has a type.
	auto IsNull() const -> bool;

	/// An owned copy of the value's type.
	auto GetLogicalType() const -> LogicalType;

	/// The value rendered the way DuckDB prints it, e.g. in query output.
	auto ToText() const -> std::string;

	/// Casts the value to another type, following the same rules as a SQL cast.
	/// @param ctx The context to cast in.
	/// @param target The type to cast to.
	/// @return The converted value. Throws when the cast is not allowed or the value does not fit.
	auto Cast(const Context &ctx, const LogicalType &target) const -> Value;

	/// `Cast` outside a callback.
	auto Cast(const Connection &conn, const LogicalType &target) const -> Value;

	/// Reads the value as `T`, where `T` is one of the primitive types above, or `LogicalType` for a TYPE value.
	/// Numeric, temporal and boolean values of another type are converted, following cast rules; the remaining `T`s
	/// require a value of the matching type. Types with no specialization here do not compile.
	/// @return The value. For `varchar_t` / `blob_t` / `bit_t` / `bignum_t` the bytes are borrowed from this value and
	/// stay valid only as long as it does.
	/// @throws Exception When the value is NULL, or when it cannot be read as `T`.
	template <class T>
	auto Get() const -> T = delete;

	/// Reads the value as a DECIMAL, e.g. `value.Get<18, 3>()`, spelled with the width and scale instead of a type.
	/// @return The value.
	/// @throws InvalidInputException Unless the value is a DECIMAL of exactly this width and scale; payloads are not
	/// interchangeable between widths.
	template <int8_t WIDTH, uint8_t SCALE>
	auto Get() const -> decimal_t<WIDTH, SCALE> {
		int128_t value {};
		GetDecimal(value, WIDTH, SCALE);
		if constexpr (std::is_same_v<typename decimal_t<WIDTH, SCALE>::storage_type, int128_t>) {
			return decimal_t<WIDTH, SCALE> {value};
		} else {
			return decimal_t<WIDTH, SCALE> {static_cast<typename decimal_t<WIDTH, SCALE>::storage_type>(value.lower)};
		}
	}

	/// A NULL of the given type.
	/// @param conn The connection to create the value on.
	/// @param type The type the NULL carries.
	static auto CreateNull(Connection &conn, const LogicalType &type) -> Value;

	/// `CreateNull` inside a callback.
	static auto CreateNull(Context &ctx, const LogicalType &type) -> Value;

	/// Creates a value from a C++ value. The overload picked decides the SQL type, so `dtime_t` yields TIME and
	/// `dtime_ns_t` yields TIME_NS; a `LogicalType` yields a TYPE value. Types with no overload here do not compile --
	/// cast or build them through the composite constructors instead. Byte strings are copied in, so the value does not
	/// borrow from the `varchar_t` / `blob_t` handed to it.
	/// @param conn The connection to create the value on.
	/// @param value The C++ value to convert.
	static auto Create(Connection &conn, bool value) -> Value;
	static auto Create(Connection &conn, uint8_t value) -> Value;
	static auto Create(Connection &conn, uint16_t value) -> Value;
	static auto Create(Connection &conn, uint32_t value) -> Value;
	static auto Create(Connection &conn, uint64_t value) -> Value;
	static auto Create(Connection &conn, uint128_t value) -> Value;
	static auto Create(Connection &conn, int8_t value) -> Value;
	static auto Create(Connection &conn, int16_t value) -> Value;
	static auto Create(Connection &conn, int32_t value) -> Value;
	static auto Create(Connection &conn, int64_t value) -> Value;
	static auto Create(Connection &conn, int128_t value) -> Value;
	static auto Create(Connection &conn, float value) -> Value;
	static auto Create(Connection &conn, double value) -> Value;
	static auto Create(Connection &conn, varchar_t value) -> Value;
	static auto Create(Connection &conn, blob_t value) -> Value;
	static auto Create(Connection &conn, const LogicalType &type) -> Value;
	static auto Create(Connection &conn, date_t value) -> Value;
	static auto Create(Connection &conn, dtime_t value) -> Value;
	static auto Create(Connection &conn, dtime_ns_t value) -> Value;
	static auto Create(Connection &conn, dtime_tz_t value) -> Value;
	static auto Create(Connection &conn, timestamp_t value) -> Value;
	static auto Create(Connection &conn, timestamp_s_t value) -> Value;
	static auto Create(Connection &conn, timestamp_ms_t value) -> Value;
	static auto Create(Connection &conn, timestamp_ns_t value) -> Value;
	static auto Create(Connection &conn, timestamp_tz_t value) -> Value;
	static auto Create(Connection &conn, timestamp_tz_ns_t value) -> Value;
	static auto Create(Connection &conn, interval_t value) -> Value;

	template <int8_t WIDTH, uint8_t SCALE>
	static auto Create(Connection &conn, decimal_t<WIDTH, SCALE> value) -> Value {
		return CreateDecimal(conn, WidenDecimal(value.value), WIDTH, SCALE);
	}

	static auto Create(Connection &conn, bit_t value) -> Value;
	static auto Create(Connection &conn, bignum_t value) -> Value;
	static auto Create(Connection &conn, uuid_t value) -> Value;
	template <class T>
	static auto Create(Connection &conn, T value) -> Value = delete;

	/// `Create` inside a callback.
	/// @param ctx The context to create the value in.
	/// @param value The C++ value to convert.
	static auto Create(Context &ctx, bool value) -> Value;
	static auto Create(Context &ctx, uint8_t value) -> Value;
	static auto Create(Context &ctx, uint16_t value) -> Value;
	static auto Create(Context &ctx, uint32_t value) -> Value;
	static auto Create(Context &ctx, uint64_t value) -> Value;
	static auto Create(Context &ctx, uint128_t value) -> Value;
	static auto Create(Context &ctx, int8_t value) -> Value;
	static auto Create(Context &ctx, int16_t value) -> Value;
	static auto Create(Context &ctx, int32_t value) -> Value;
	static auto Create(Context &ctx, int64_t value) -> Value;
	static auto Create(Context &ctx, int128_t value) -> Value;
	static auto Create(Context &ctx, float value) -> Value;
	static auto Create(Context &ctx, double value) -> Value;
	static auto Create(Context &ctx, varchar_t value) -> Value;
	static auto Create(Context &ctx, blob_t value) -> Value;
	static auto Create(Context &ctx, const LogicalType &type) -> Value;
	static auto Create(Context &ctx, date_t value) -> Value;
	static auto Create(Context &ctx, dtime_t value) -> Value;
	static auto Create(Context &ctx, dtime_ns_t value) -> Value;
	static auto Create(Context &ctx, dtime_tz_t value) -> Value;
	static auto Create(Context &ctx, timestamp_t value) -> Value;
	static auto Create(Context &ctx, timestamp_s_t value) -> Value;
	static auto Create(Context &ctx, timestamp_ms_t value) -> Value;
	static auto Create(Context &ctx, timestamp_ns_t value) -> Value;
	static auto Create(Context &ctx, timestamp_tz_t value) -> Value;
	static auto Create(Context &ctx, timestamp_tz_ns_t value) -> Value;
	static auto Create(Context &ctx, interval_t value) -> Value;

	template <int8_t WIDTH, uint8_t SCALE>
	static auto Create(Context &ctx, decimal_t<WIDTH, SCALE> value) -> Value {
		return CreateDecimal(ctx, WidenDecimal(value.value), WIDTH, SCALE);
	}

	static auto Create(Context &ctx, bit_t value) -> Value;
	static auto Create(Context &ctx, bignum_t value) -> Value;
	static auto Create(Context &ctx, uuid_t value) -> Value;
	template <class T>
	static auto Create(Context &ctx, T value) -> Value = delete;

	// Composite construction. Each constructor infers the composite's type from the children it is given, which is why
	// only the built-in composites are reachable this way: build an aliased or extension-registered composite by
	// casting one of these to it.
	//
	// Children are borrowed for the duration of the call and copied into the result, so the caller keeps its inputs
	// and nothing in the result points back at them. Each comes in both scope forms, like `Create` and `CreateNull`.
	using ValueList = const std::vector<Value> &;
	using NamedValueList = const std::vector<std::pair<std::string, Value>> &;
	using KeyValueList = const std::vector<std::pair<Value, Value>> &;

	/// A LIST of the given elements.
	/// @param conn The connection to create the value on.
	/// @param values The elements. The element type is the common type of all of them and each is cast to it, so
	/// mixing INTEGER and VARCHAR yields VARCHAR elements. Must not be empty: with no element there is no type to
	/// infer, so use the child-type overload for an empty LIST.
	/// @throws Exception When the elements have no common type.
	static auto CreateList(Connection &conn, ValueList values) -> Value;

	/// `CreateList` inside a callback.
	static auto CreateList(Context &ctx, ValueList values) -> Value;

	/// An empty LIST.
	/// @param conn The connection to create the value on.
	/// @param child_type The element type, not the LIST type.
	static auto CreateList(Connection &conn, const LogicalType &child_type) -> Value;

	/// `CreateList` inside a callback.
	static auto CreateList(Context &ctx, const LogicalType &child_type) -> Value;

	/// An ARRAY of the given elements, its size being how many there are.
	/// @param conn The connection to create the value on.
	/// @param values The elements, typed as in `CreateList`. Must not be empty: the smallest ARRAY holds one element.
	static auto CreateArray(Connection &conn, ValueList values) -> Value;

	/// `CreateArray` inside a callback.
	static auto CreateArray(Context &ctx, ValueList values) -> Value;

	/// A TUPLE, i.e. a struct whose fields have no names.
	/// @param conn The connection to create the value on.
	/// @param values The fields, in order. May be empty: the empty tuple is a type of its own.
	static auto CreateTuple(Connection &conn, ValueList values = {}) -> Value;

	/// `CreateTuple` inside a callback.
	static auto CreateTuple(Context &ctx, ValueList values = {}) -> Value;

	/// A STRUCT of the given fields.
	/// @param conn The connection to create the value on.
	/// @param values The (name, value) fields, in order. Names should be unique and either all set or all empty; this
	/// is not validated. May be empty: the empty struct is a type of its own.
	static auto CreateStruct(Connection &conn, NamedValueList values = {}) -> Value;

	/// `CreateStruct` inside a callback.
	static auto CreateStruct(Context &ctx, NamedValueList values = {}) -> Value;

	/// A MAP of the given entries.
	/// @param conn The connection to create the value on.
	/// @param values The (key, value) entries. The key and value types are the common types over all entries, and each
	/// entry is cast to them, as in `CreateList`. Keys must be unique and not NULL. Must not be empty: with no entry
	/// there are no types to infer, so use the key/value-type overload for an empty MAP.
	static auto CreateMap(Connection &conn, KeyValueList values) -> Value;

	/// `CreateMap` inside a callback.
	static auto CreateMap(Context &ctx, KeyValueList values) -> Value;

	/// An empty MAP.
	/// @param conn The connection to create the value on.
	/// @param key_type The key type, not the MAP type.
	/// @param value_type The value type, not the MAP type.
	static auto CreateMap(Connection &conn, const LogicalType &key_type, const LogicalType &value_type) -> Value;

	/// `CreateMap` inside a callback.
	static auto CreateMap(Context &ctx, const LogicalType &key_type, const LogicalType &value_type) -> Value;

	/// How many children a composite value has: elements for LIST and ARRAY, fields for STRUCT and TUPLE, two per
	/// entry for MAP, 2 for UNION, and 0 for anything else. A NULL value has no children.
	auto GetChildCount() const -> idx_t;

	/// An owned copy of one child. A MAP's children alternate key and value, so entry `i` is children 2*i and 2*i+1; a
	/// UNION's children are its tag, then its active member.
	/// @param index Child index in [0, GetChildCount()).
	auto GetChild(idx_t index) const -> Value;

	/// Shorthand for `GetChild`.
	auto operator[](idx_t index) const -> Value {
		return GetChild(index);
	}

private:
	/// @internal Reads the backing integer, throwing unless the value's own width and scale are the ones asked for.
	void GetDecimal(int128_t &out, uint8_t width, uint8_t scale) const;

	/// @internal The runtime forwarder behind the templated DECIMAL constructors, so those can be defined in the header
	/// without naming a C type.
	static auto CreateDecimal(Connection &conn, int128_t value, uint8_t width, uint8_t scale) -> Value;
	static auto CreateDecimal(Context &ctx, int128_t value, uint8_t width, uint8_t scale) -> Value;

	/// @internal Sign-extends a DECIMAL's backing integer to the widest storage tier, so one entry point can carry
	/// every tier.
	static auto WidenDecimal(int64_t value) -> int128_t {
		return int128_t {static_cast<uint64_t>(value), value < 0 ? -1 : 0};
	}
	static auto WidenDecimal(int128_t value) -> int128_t {
		return value;
	}

	explicit Value(void *impl);
};

template <>
auto Value::Get() const -> bool;
template <>
auto Value::Get() const -> uint8_t;
template <>
auto Value::Get() const -> uint16_t;
template <>
auto Value::Get() const -> uint32_t;
template <>
auto Value::Get() const -> uint64_t;
template <>
auto Value::Get() const -> uint128_t;
template <>
auto Value::Get() const -> int8_t;
template <>
auto Value::Get() const -> int16_t;
template <>
auto Value::Get() const -> int32_t;
template <>
auto Value::Get() const -> int64_t;
template <>
auto Value::Get() const -> int128_t;
template <>
auto Value::Get() const -> float;
template <>
auto Value::Get() const -> double;
template <>
auto Value::Get() const -> varchar_t;
template <>
auto Value::Get() const -> blob_t;
template <>
auto Value::Get() const -> date_t;
template <>
auto Value::Get() const -> dtime_t;
template <>
auto Value::Get() const -> dtime_ns_t;
template <>
auto Value::Get() const -> dtime_tz_t;
template <>
auto Value::Get() const -> timestamp_t;
template <>
auto Value::Get() const -> timestamp_s_t;
template <>
auto Value::Get() const -> timestamp_ms_t;
template <>
auto Value::Get() const -> timestamp_ns_t;
template <>
auto Value::Get() const -> timestamp_tz_t;
template <>
auto Value::Get() const -> timestamp_tz_ns_t;
template <>
auto Value::Get() const -> interval_t;
template <>
auto Value::Get() const -> bit_t;
template <>
auto Value::Get() const -> bignum_t;
template <>
auto Value::Get() const -> uuid_t;
template <>
auto Value::Get() const -> LogicalType;

template <class T>
auto Connection::CreateValue(T &&value) -> Value {
	return Value::Create(*this, std::forward<T>(value));
}

template <class T>
auto Context::CreateValue(T &&value) -> Value {
	return Value::Create(*this, std::forward<T>(value));
}

/// One parameter of a type: a value, plus a name when the parameter is a named one. A parameter with an empty name is
/// positional.
struct TypeParam {
	/// A named parameter.
	/// @param name The parameter's name.
	/// @param value The parameter's value; moved in.
	TypeParam(std::string_view name, Value value) : name(name), value(std::move(value)) {
	}

	/// A positional parameter.
	/// @param value The parameter's value; moved in.
	explicit TypeParam(Value value) : name(""), value(std::move(value)) {
	}

	/// The parameter's name, empty when it is positional.
	auto GetName() const -> const std::string & {
		return name;
	}

	/// The parameter's value.
	auto GetValue() const -> const Value & {
		return value;
	}

	/// The parameter's value, mutably.
	auto GetValue() -> Value & {
		return value;
	}

private:
	std::string name;
	Value value;
};

/// Builds a type a piece at a time, without assembling a parameter vector by hand. Start one from
/// `Context::CreateType()` or `Connection::CreateType()`, chain the setters, and call `Build`.
/// Nested types are added by passing a callback that fills in a builder of its own.
template <class CTX>
class TypeBuilder {
public:
	explicit TypeBuilder(CTX &context) : ctx(context) {
	}

	/// Identifies the type to build by its id. Takes precedence over `SetName`.
	auto SetTypeId(LogicalTypeId type_id_p) -> TypeBuilder & {
		type_id = type_id_p;
		return *this;
	}

	/// Identifies the type to build by name, which is how extension-registered types are reached.
	auto SetName(std::string_view name_p) -> TypeBuilder & {
		name = name_p;
		return *this;
	}

	/// Appends a positional type parameter.
	auto AddParam(const LogicalType &type) -> TypeBuilder & {
		params.emplace_back("", ctx.CreateValue(type));
		return *this;
	}

	/// Appends a named type parameter, e.g. a STRUCT field.
	auto AddParam(std::string_view name_p, const LogicalType &type) -> TypeBuilder & {
		params.emplace_back(name_p, ctx.CreateValue(type));
		return *this;
	}

	/// Appends a named value parameter, e.g. a VARCHAR's collation.
	template <class T, class = std::enable_if_t<!std::is_invocable_v<T &, TypeBuilder &>>>
	auto AddParam(std::string_view name_p, const T &value) -> TypeBuilder & {
		params.emplace_back(name_p, ctx.CreateValue(value));
		return *this;
	}

	/// Appends a positional value parameter, e.g. a DECIMAL's width.
	template <class T, class = std::enable_if_t<!std::is_invocable_v<T &, TypeBuilder &>>>
	auto AddParam(const T &value) -> TypeBuilder & {
		params.emplace_back("", ctx.CreateValue(value));
		return *this;
	}

	/// Appends a named parameter holding a nested type.
	/// @param name_p The parameter's name.
	/// @param builder Called with a fresh builder; whatever it composes becomes the parameter.
	template <class F, class = std::enable_if_t<std::is_invocable_v<F &, TypeBuilder &>>>
	auto AddParam(std::string_view name_p, F &&builder) -> TypeBuilder & {
		TypeBuilder nested_builder(ctx);
		builder(nested_builder);
		params.emplace_back(name_p, ctx.CreateValue(nested_builder.Build()));
		return *this;
	}

	/// Appends a positional parameter holding a nested type.
	/// @param builder Called with a fresh builder; whatever it composes becomes the parameter.
	template <class F, class = std::enable_if_t<std::is_invocable_v<F &, TypeBuilder &>>>
	auto AddParam(F &&builder) -> TypeBuilder & {
		TypeBuilder nested_builder(ctx);
		builder(nested_builder);
		params.emplace_back("", ctx.CreateValue(nested_builder.Build()));
		return *this;
	}

	/// Builds the type, by id when one was set and by name otherwise. The builder keeps its name, id and parameters, so
	/// a later `Build` repeats the same type.
	auto Build() -> LogicalType {
		if constexpr (std::is_same_v<CTX, Connection>) {
			if (type_id != LogicalTypeId::INVALID) {
				return ctx.CreateType(type_id, params);
			} else {
				return ctx.CreateType(name, params);
			}
		} else if constexpr (std::is_same_v<CTX, Context>) {
			if (type_id != LogicalTypeId::INVALID) {
				return ctx.CreateType(type_id, params);
			} else {
				return ctx.CreateType(name, params);
			}
		} else {
			static_assert(detail::always_false<CTX>::value, "TypeBuilder can only be used with Connection or Context");
			return ctx.CreateType(name, params);
		}
	}

private:
	LogicalTypeId type_id = LogicalTypeId::INVALID;
	std::string name;
	std::vector<TypeParam> params;
	CTX &ctx;
};

/// One parameter binding for the named-parameter `Connection::Execute` overload.
struct NamedParam {
	/// The parameter to bind, matching $name case-insensitively. When empty, the binding is positional instead: the
	/// first `NamedParam` binds $1, the second $2, and so on. A statement cannot mix named and positional parameters.
	std::string name;
	/// The value to bind.
	Value value;
};

//----------------------------------------------------------------------------------------------------------------------
// Arena
//----------------------------------------------------------------------------------------------------------------------
// Vectors of variable-length types like strings, blobs, bits, bignums and geometries contain an auxiliary arena
// allocator called the "String Heap", which lives for as long as the vector itself lives. This is used to store the
// actual bytes e.g. a `blob_t` references in the non-inlined case. Writing a string or a blob to a vector is
// therefore usually done in two steps: allocate the bytes in the arena (heap), then place the resulting blob_t in the
// vector, referencing the bytes. `Vector::AssignString` does both at once and is what most fills want. Go through the
// heap directly when the bytes and their placement are decided separately, e.g. to write one copy of a value that
// several rows point at.

/// A borrowed handle to a vector's string heap, valid until that vector is destroyed or reshaped, e.g. by a `Flatten`.
class Arena final : public detail::Handle<Arena> {
	friend detail::Factory;

public:
	Arena(Arena &&) noexcept = default;
	Arena &operator=(Arena &&) noexcept = default;

	~Arena() override;

	/// Reserves uninitialized bytes in the heap, to build a value in place rather than copy one in.
	/// @param byte_len How many bytes to reserve.
	/// @return The reserved memory, valid as long as the heap is. Write it, wrap it in a `varchar_t` / `blob_t`, and
	/// place that with `Vector::SetString`.
	auto Allocate(idx_t byte_len) -> uint8_t *;

	/// Copies a string into the heap.
	/// @param data The bytes to copy. Anything up to `varchar_t::INLINE_LENGTH` is kept in the token itself and never
	/// reaches the heap.
	/// @return A token to place with `Vector::SetString`, valid as long as the heap is.
	/// @throws Exception When the data exceeds the 4 GiB an element can describe.
	auto AddString(std::string_view data) -> varchar_t {
		// TODO: UTF8-validate
		if (data.size() > std::numeric_limits<uint32_t>::max()) {
			ThrowStringTooLong(data.size());
		}
		const auto size = static_cast<uint32_t>(data.size());
		if (size <= varchar_t::INLINE_LENGTH) {
			return varchar_t(data.data(), size);
		}
		auto *bytes = Allocate(size);
		std::memcpy(bytes, data.data(), size);
		return varchar_t(reinterpret_cast<char *>(bytes), size);
	}

	/// `AddString` for arbitrary bytes rather than text.
	auto AddBlob(std::string_view data) -> blob_t {
		if (data.size() > std::numeric_limits<uint32_t>::max()) {
			ThrowStringTooLong(data.size());
		}
		const auto size = static_cast<uint32_t>(data.size());
		if (size <= blob_t::INLINE_LENGTH) {
			return blob_t(data.data(), size);
		}
		auto *bytes = Allocate(size);
		std::memcpy(bytes, data.data(), size);
		return blob_t(reinterpret_cast<char *>(bytes), size);
	}

private:
	explicit Arena(void *impl);

	/// @internal Throws when a value exceeds the maximum byte string length.
	[[noreturn]] static void ThrowStringTooLong(idx_t size);
};

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------
// A column of values: one array of a primitive type, a validity mask saying which rows are NULL, and child vectors
// for the nested types. This is the preferred way to write/read data in and out of DuckDB. One call to
// `Vector::GetView` or `Vector::GetDataMutable` and the per-row work is plain array indexing -- as opposed to `Value`,
// which handles one cell at a time, and cost a lot in e.g. allocation overhead.
//
// The same column can be laid out in more than one way: a vector whose rows all share a value is stored once, and one
// that selects from another is stored as a selection over it. Reading through `VectorView` handles all of those.
// When writing you will generally produce a FLAT vector, but you can also call `Flatten` to force any vector into the
// basic contiguous data layout.

/// How a vector is laid out.
enum class VectorType : uint8_t {
	/// A layout with no direct read support here, such as a compressed or shredded one. `Flatten` first.
	OTHER = 0,
	/// One element per row.
	FLAT = 1,
	/// A single element standing for every row.
	CONSTANT = 2,
	/// A selection over another vector's elements.
	DICTIONARY = 3,
};

/// A read view over a vector: its data, validity, selection and row count, fetched in a single call so that everything
/// below is plain inline arithmetic.
///
/// The pointers are borrowed from the vector and stay valid until the chunk that owns it is destroyed -- or until the
/// vector is reshaped, e.g. by `Flatten` or `SetSize`, so re-take the view after any such call.
struct VectorView {
	/// The buffer holding the elements, to be read as an array of the primitive type matching the vector's type.
	const void *data;
	/// The validity mask, or `nullptr` when every row is valid.
	const uint64_t *validity;
	/// The selection: which element each row reads, or nullptr when row `i` reads element `i`.
	const uint32_t *sel;
	/// How many rows the view covers.
	idx_t count;

	/// The buffer, typed.
	template <class T>
	auto Data() const -> const T * {
		return static_cast<const T *>(data);
	}

	/// The element row `i` reads. A CONSTANT vector sends every row to element 0.
	auto SelAt(idx_t i) const -> idx_t {
		return sel ? static_cast<idx_t>(sel[i]) : i;
	}

	/// Whether an element is a value rather than NULL.
	/// @param row An element index, i.e. one already put through `SelAt`.
	auto RowIsValid(idx_t row) const -> bool {
		return !validity || (validity[row >> 6] & (static_cast<uint64_t>(1) << (row & 63))) != 0;
	}

	/// Whether a row holds a value rather than NULL. Validity follows the selection, so this is the one to use with a
	/// loop counter.
	/// @param i A row index in [0, count).
	auto IsValid(idx_t i) const -> bool {
		return RowIsValid(SelAt(i));
	}

	/// Whether every row in [0, count) holds a value rather than NULL.
	auto AllValid() const -> bool {
		if (!validity) {
			return true;
		}
		if (sel) {
			for (idx_t i = 0; i < count; i++) {
				if (!IsValid(i)) {
					return false;
				}
			}
			return true;
		}
		// Compare whole words, masking out the bits past `count` in the last one
		const auto whole_words = count / 64;
		for (idx_t i = 0; i < whole_words; i++) {
			if (validity[i] != ~static_cast<uint64_t>(0)) {
				return false;
			}
		}
		const auto tail_bits = count % 64;
		if (tail_bits != 0) {
			const auto tail_mask = (static_cast<uint64_t>(1) << tail_bits) - 1;
			if ((validity[whole_words] & tail_mask) != tail_mask) {
				return false;
			}
		}
		return true;
	}
};

/// A writer over a FLAT vector's validity mask, from `Vector::GetValidityMutable`. Word W bit N covers row W*64+N, and
/// a set bit means the row holds a value rather than NULL.
///
/// Writes here touch one vector's mask and nothing else. Since a NULL STRUCT or ARRAY row requires its descendant
/// elements to be NULL as well, use `Vector::SetNull` (which will set the validity of child vectors recursively) for
/// those rather than clearing the parent's bit. When filling nested masks by hand, use `SetAllInvalid` to clear the
/// child masks up front and then set the individual bits as values are written.
struct ValidityMask {
	/// The raw mask words, for writes this struct does not cover.
	uint64_t *words;

	/// Marks a row as holding a value.
	auto SetValid(idx_t row) -> void {
		words[row >> 6] |= uint64_t(1) << (row & 63);
	}

	/// Marks a row as NULL.
	auto SetInvalid(idx_t row) -> void {
		words[row >> 6] &= ~(uint64_t(1) << (row & 63));
	}

	/// Whether a row holds a value rather than NULL.
	auto RowIsValid(idx_t row) const -> bool {
		return (words[row >> 6] & (uint64_t(1) << (row & 63))) != 0;
	}

	/// Marks rows [0, count) NULL. Writes whole words, so bits past `count` in the last word are cleared too, which
	/// only matters if the vector grows afterwards.
	auto SetAllInvalid(idx_t count) -> void {
		for (idx_t i = 0; i < (count + 63) / 64; i++) {
			words[i] = 0;
		}
	}

	/// Marks rows [0, count) as holding values: the counterpart of `SetAllInvalid`, to reset a mask before refilling
	/// rather than setting each bit as it is written. Writes whole words, so bits past `count` in the last word are
	/// set too, which only matters if the vector grows afterwards.
	auto SetAllValid(idx_t count) -> void {
		for (idx_t i = 0; i < (count + 63) / 64; i++) {
			words[i] = ~uint64_t(0);
		}
	}
};

/// A borrowed handle to one column of a chunk, or to one child of another vector. Valid for as long as the chunk or
/// parent vector it belongs to is valid.
class Vector final : public detail::Handle<Vector> {
	friend detail::Factory;

public:
	Vector(Vector &&) noexcept = default;
	Vector &operator=(Vector &&) noexcept = default;

	~Vector() override;

	/// The buffer for writing, typed. The vector must be FLAT or CONSTANT, and `T` must match its type.
	template <class T>
	auto GetDataMutable() -> T * {
		return static_cast<T *>(GetDataMutable());
	}

	/// The buffer for writing, untyped.
	/// @throws InvalidInputException Unless the vector is FLAT or CONSTANT.
	auto GetDataMutable() -> void *;

	/// How many child vectors this one has: 1 for LIST and ARRAY, one per field for STRUCT and TUPLE, 2 for MAP (the
	/// keys and the values), one per member plus the leading tag for UNION, and 0 for anything else.
	auto GetChildCount() const -> idx_t;

	/// One child vector, e.g. a LIST's elements or a STRUCT's field. A LIST's child is sized to its capacity, not to
	/// the parent's row count.
	/// @param index Child index in [0, GetChildCount()).
	/// @return A borrowed handle, valid until this vector is destroyed or reshaped.
	auto GetChild(idx_t index) const -> Vector;

	/// Rewrites the vector as a FLAT one, materializing one element per row. Pointers and views taken from it
	/// beforehand do not survive this.
	auto Flatten() const -> void;

	/// How many rows the vector holds.
	auto GetSize() const -> idx_t;

	/// Sets how many rows the vector holds, reallocating if needed, which invalidates views and pointers taken
	/// earlier. A chunk derives its row count from its vectors, so this is also how a hand-filled chunk gets its
	/// cardinality: size every column alike.
	auto SetSize(idx_t size) -> void;

	/// Reads the vector in a single call, so that the per-row work afterwards is inline.
	/// @return A view borrowed from the vector. Taking the view of a DICTIONARY vector may flatten the vector it
	/// selects from, which invalidates views taken from that one earlier.
	/// @throws InvalidInputException On `VectorType::OTHER`; `Flatten` first.
	auto GetView() const -> VectorView;

	/// How the vector is laid out.
	auto GetVectorType() const -> VectorType;

	/// The validity mask for writing, allocating it if the vector does not have one yet.
	/// @throws InvalidInputException Unless the vector is FLAT.
	auto GetValidityMutable() -> ValidityMask;

	/// Sets a row NULL, including the descendant elements of a STRUCT or ARRAY row. LIST (and MAP) children are left
	/// alone, since a NULL list has no elements to speak of. Use this rather than clearing mask bits for any nested
	/// type.
	/// @param row A row index within the vector's size.
	/// @throws InvalidInputException On a non-FLAT vector or an out-of-range row.
	auto SetNull(idx_t row) -> void;

	/// Sets the single validity bit of a CONSTANT vector. Setting it valid writes no value: element 0 keeps whatever
	/// was last written to it.
	/// @param valid Whether the vector holds a value rather than NULL.
	/// @throws InvalidInputException Unless the vector is CONSTANT.
	auto SetConstantValid(bool valid) -> void;

	/// Rewrites the vector as a CONSTANT one: a single value standing for every row.
	/// @param value The value every row takes.
	/// @param count How many rows the vector then holds.
	/// @throws InvalidInputException When the value's type does not match the vector's.
	auto MakeConstant(const Value &value, idx_t count) -> void;

	/// Rewrites the vector as the arithmetic sequence start, start + increment, ... The result reads as
	/// `VectorType::OTHER`, so `Flatten` before reading it through `GetView`.
	/// @param start The first row's value.
	/// @param increment The step between rows.
	/// @param count How many rows the vector then holds.
	auto MakeSequence(int64_t start, int64_t increment, idx_t count) -> void;

	/// Reads one cell as a `Value`, whatever the layout and whatever the type -- including the types without a view
	/// layout, such as VARIANT -- and without flattening. Costs an owned value per call, so read bulk data through
	/// `GetView` instead.
	/// @param row A row index within the vector's size.
	auto GetValue(idx_t row) const -> Value;

	/// Writes one cell from a `Value`, whatever the type. The value is copied in and cast to the vector's type. Costs
	/// a value write per call, so fill bulk data through `GetDataMutable` instead.
	/// @param row A row index within the vector's size.
	/// @param value The value to write.
	/// @throws InvalidInputException On a non-FLAT vector or an out-of-range row; flatten first.
	auto SetValue(idx_t row, const Value &value) -> void;

	/// Borrows this vector's string heap, to write bytes whose placement is decided separately -- deduplicating,
	/// scattering, reordering. For a straightforward fill prefer `AssignString`.
	/// @return The heap. The vector must be of a string-backed type such as VARCHAR, BLOB, BIT or BIGNUM.
	auto GetHeap() -> Arena;

	/// Copies a string into the vector's heap and writes the resulting element in one step. Looks the heap up per call,
	/// so flattening in between is safe.
	/// @param index The element to write: any index within the size of a FLAT vector, only 0 for a CONSTANT one.
	/// @param data The bytes to copy. The vector must be of a string-backed type such as VARCHAR, BLOB, BIT or BIGNUM.
	auto AssignString(idx_t index, std::string_view data) -> void;

	/// Writes an element that was written into the heap beforehand.
	/// @param index The element to write: any index within the size of a FLAT vector, only 0 for a CONSTANT one.
	/// @param value A token from this vector's own heap. A non-inlined token from another vector dangles.
	auto SetString(idx_t index, varchar_t value) -> void;

private:
	explicit Vector(void *impl);

	/// @internal Throws `InvalidInputException` if [start, start + count) is not writable: a CONSTANT vector has a
	/// single element, so only index 0 may be written.
	auto CheckWriteRange(idx_t start, idx_t count) const -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// DataChunk
//----------------------------------------------------------------------------------------------------------------------
// A batch of rows: one `Vector` per column, all of the same logical length.
// Chunks are the main "unit of execution" in DuckDB, and usually contain a couple of thousand rows at a time.

/// A batch of rows, column by column. A chunk owns its vectors, so it must outlive any `Vector`, `VectorView` or
/// pointer taken from it.
class DataChunk final : public detail::Handle<DataChunk> {
	friend detail::Factory;

public:
	/// An empty chunk with a column per type, ready to be filled: write the columns' data and give every column its
	/// row count with `Vector::SetSize`.
	/// @param types One type per column. Types containing ANY are rejected.
	explicit DataChunk(const std::vector<LogicalType> &types);

	/// Like `DataChunk(types)`, but the chunk's memory is allocated through the connection's database rather than the
	/// default allocator, so it is accounted to that database.
	/// @param conn The connection whose database supplies the chunk's memory.
	/// @param types One type per column. Types containing ANY are rejected.
	DataChunk(const Connection &conn, const std::vector<LogicalType> &types);

	/// The `Context` flavor of the connection-scoped constructor, inside a callback.
	DataChunk(const Context &ctx, const std::vector<LogicalType> &types);

	DataChunk(DataChunk &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
	}
	DataChunk &operator=(DataChunk &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
		return *this;
	}

	~DataChunk() override;

	/// How many columns the chunk has.
	auto GetVectorCount() const -> idx_t;

	/// How many rows the chunk holds.
	auto GetRowCount() const -> idx_t;

	/// Access one column, e.g. to read or write its data.
	/// @param index Column index in [0, GetVectorCount()).
	/// @return A borrowed handle, valid for as long as this chunk is.
	auto GetVector(idx_t index) const -> Vector;

	/// A deep copy of this chunk, its memory allocated through the connection's database. The copy is flattened and
	/// owns all its data, so it stays valid after this chunk -- or whatever backs it, such as a
	/// `ColumnDataCollection` scan -- is gone.
	/// @param conn The connection whose database supplies the copy's memory.
	auto Copy(const Connection &conn) const -> DataChunk;

	/// The `Context` flavor of `Copy`, inside a callback.
	auto Copy(const Context &ctx) const -> DataChunk;

private:
	explicit DataChunk(void *impl, bool owned);
	bool owned = false; // TODO: This should be fixed C++ side
};

//----------------------------------------------------------------------------------------------------------------------
// Column Data Collection
//----------------------------------------------------------------------------------------------------------------------
// A buffer-managed set of rows: append chunks in, scan them back out in order. DuckDB owns the memory and spills the
// rows to disk when they outgrow it, so a collection can hold far more than memory allows. A collection must not be
// scanned while it is being appended to, and must not be appended to concurrently: for parallel appends, fill one
// collection per thread and `Combine` them at the end.

/// An owned collection of rows, all sharing one set of column types fixed at construction.
/// It must not outlive the `Connection` or `Context` it was created from.
class ColumnDataCollection final : public detail::Handle<ColumnDataCollection> {
	friend detail::Factory;

public:
	/// Opaque state for appending, from `CreateAppendState`. Only meaningful with the collection that created it, and
	/// invalidated by `Reset`.
	class AppendState final : public detail::Handle<AppendState> {
		friend detail::Factory;

	public:
		AppendState(AppendState &&) noexcept = default;
		AppendState &operator=(AppendState &&) noexcept = default;
		~AppendState() override;

	private:
		explicit AppendState(void *impl);
	};

	/// Opaque state shared by every thread of one scan, from `CreateSharedScanState`: it coordinates which rows each
	/// worker reads and tracks the scan's overall progress. Only meaningful with the collection that created it, and
	/// invalidated by `Reset`.
	class SharedScanState final : public detail::Handle<SharedScanState> {
		friend detail::Factory;

	public:
		SharedScanState(SharedScanState &&) noexcept = default;
		SharedScanState &operator=(SharedScanState &&) noexcept = default;
		~SharedScanState() override;

	private:
		explicit SharedScanState(void *impl);
	};

	/// Opaque per-thread state of one scan, from `CreateWorkerScanState`. It also keeps the buffers backing the chunk
	/// its thread most recently scanned alive: scans are zero-copy, so a scanned chunk's data is only valid until this
	/// state's next `Scan` or its destruction.
	class WorkerScanState final : public detail::Handle<WorkerScanState> {
		friend detail::Factory;

	public:
		WorkerScanState(WorkerScanState &&) noexcept = default;
		WorkerScanState &operator=(WorkerScanState &&) noexcept = default;
		~WorkerScanState() override;

	private:
		explicit WorkerScanState(void *impl);
	};

	/// An empty collection, its memory managed by the connection's database.
	/// @param conn The connection whose database supplies the collection's memory.
	/// @param types One type per column, at least one; every chunk appended must match them exactly. Types containing
	/// ANY are rejected.
	ColumnDataCollection(const Connection &conn, const std::vector<LogicalType> &types);

	/// The `Context` flavor, inside a callback.
	ColumnDataCollection(const Context &ctx, const std::vector<LogicalType> &types);

	ColumnDataCollection(ColumnDataCollection &&) noexcept = default;
	ColumnDataCollection &operator=(ColumnDataCollection &&) noexcept = default;

	~ColumnDataCollection() override;

	/// How many rows the collection holds.
	auto GetRowCount() const -> idx_t;

	/// Drops all rows and releases their memory, keeping the column types; the collection is immediately appendable
	/// again. Outstanding append and scan states are invalidated: create new ones. Prefer `Clear` when the collection
	/// is about to be refilled.
	auto Reset() -> void;

	/// Drops all rows but keeps their memory, so the next appends write into buffers that are already allocated;
	/// prefer it over `Reset` when the collection is refilled repeatedly. The column types are unchanged and the
	/// collection is immediately appendable again. Outstanding append and scan states are invalidated: create new
	/// ones.
	auto Clear() -> void;

	/// Moves another collection's rows to the end of this one, consuming it. The source must have the same column
	/// types, and both collections must come from the same database -- the rows keep their original buffers rather
	/// than being copied.
	/// @param source The collection to consume. Left untouched when the merge is refused.
	/// @throws InvalidInputException When the column types differ, or when `source` is this collection.
	auto Combine(ColumnDataCollection &&source) -> void;

	/// Starts appending: the returned state carries the append's progress between `Append` calls.
	auto CreateAppendState() -> AppendState;

	/// Copies a chunk's rows to the end of the collection.
	/// @param state The append state to append through.
	/// @param chunk The rows to append. The chunk's column types must equal the collection's exactly, and the chunk is
	/// only borrowed: it can be reused, refilled and appended again.
	/// @throws InvalidInputException When the chunk's columns do not match the collection's.
	auto Append(AppendState &state, const DataChunk &chunk) -> void;

	/// One-shot `Append`, creating and discarding an append state internally. Prefer keeping a state across calls when
	/// appending more than once.
	auto Append(const DataChunk &chunk) -> void;

	/// Starts a scan over the collection's rows. One shared state per scan; each participating thread additionally
	/// gets its own `CreateWorkerScanState`.
	auto CreateSharedScanState() const -> SharedScanState;

	/// Per-thread state for a scan started with `CreateSharedScanState`.
	auto CreateWorkerScanState() const -> WorkerScanState;

	/// Reads the next rows of the scan into a chunk. Threads sharing one `SharedScanState` each receive disjoint rows,
	/// so together they scan the collection exactly once.
	///
	/// The scan is zero-copy where possible: the chunk's vectors may reference the collection's buffers, kept alive by
	/// the worker state, so the chunk's data is only valid until that state's next `Scan` or its destruction.
	/// Make a copy with `DataChunk::Copy` to keep the scanned data longer.
	/// @param shared The scan's shared state.
	/// @param worker This thread's worker state.
	/// @param chunk The chunk to read into; its column types must equal the collection's exactly. Reset to empty once
	/// the scan is exhausted.
	/// @return Whether rows were produced; false once the scan is exhausted.
	/// @throws InvalidInputException When the chunk's columns do not match the collection's.
	auto Scan(SharedScanState &shared, WorkerScanState &worker, DataChunk &chunk) const -> bool;

private:
	explicit ColumnDataCollection(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Appender
//----------------------------------------------------------------------------------------------------------------------

/// Buffers rows on the client side and writes them to the database in bulk.
/// An appender pairs a `ColumnDataCollection` with a statement that reads it: `AppendChunk` fills the buffer without
/// touching the database, and `Flush` runs the statement over everything buffered so far and empties the buffer.
/// Many small inserts thereby become one bulk insert.
///
/// The appender is built on the public API alone: a connection-scoped `ReplacementScan` exposes the buffer to the
/// statement under a name. The same can be done by hand when a different shape is needed.
///
/// An appender is bound to the connection it was created on and can only be flushed there. It is not thread-safe:
/// append from one thread at a time, or give each thread its own appender. It must not outlive its `Connection`.
///
/// Destroying an appender does not flush: rows buffered but not flushed are dropped. Since replacement scans cannot
/// be unregistered, every appender leaves an inert scan registered on its connection; create appenders once and
/// reuse them rather than creating one per batch.
class Appender {
public:
	/// Creates an appender that inserts into `table`.
	/// The buffer takes the table's columns, in order, with their types, resolved once at construction: the appender
	/// keeps targeting that table even if the search path changes later.
	/// @param conn The connection to buffer against and flush on.
	/// @param table The table to append to, optionally qualified.
	/// @throws Exception When the table cannot be resolved.
	/// @note A table with generated columns needs the query constructor instead: this one lists every column, and
	/// the engine refuses an INSERT that names a generated one.
	Appender(Connection &conn, std::string_view table);

	/// Creates an appender that flushes by running `query`, which reads the buffer as a table named `buffer_name`.
	/// Use this when a plain INSERT of every column is not what is needed: a subset of columns, an UPDATE or MERGE
	/// driven by the buffer, or an INSERT with an ON CONFLICT clause.
	/// @param conn The connection to buffer against and flush on.
	/// @param query Exactly one statement, referring to the buffer by `buffer_name`.
	/// @param column_types One type per buffer column, at least one.
	/// @param buffer_name The name `query` reads the buffer under. Must be unique among the appenders on this
	/// connection: the first one registered claims the name.
	/// @param column_names Names for the buffer's columns; empty names them col1..colN.
	/// @throws InvalidInputException When `column_types` is empty, or `query` is not exactly one statement.
	Appender(Connection &conn, std::string_view query, std::vector<LogicalType> column_types,
	         std::string_view buffer_name, const std::vector<std::string> &column_names = {});

	Appender(const Appender &) = delete;
	Appender &operator=(const Appender &) = delete;
	Appender(Appender &&) noexcept = default;
	Appender &operator=(Appender &&) noexcept = default;

	/// Drops whatever is still buffered. Does not flush.
	~Appender();

	/// The buffer's column types, in order. Build a chunk over these to fill and append, e.g.
	/// `DataChunk chunk(appender.ColumnTypes())`. Valid for the appender's lifetime.
	auto ColumnTypes() const -> const std::vector<LogicalType> & {
		return types;
	}

	/// Buffers a whole chunk. Its column types must equal `ColumnTypes()` exactly; a mismatch is refused before
	/// anything is copied.
	/// @throws InvalidInputException When the chunk's columns do not match, or a previous buffer operation failed.
	void AppendChunk(DataChunk &chunk);

	/// Runs the statement over everything buffered and empties the buffer, keeping its memory for the next batch.
	/// Does nothing when the buffer is empty.
	/// @throws Exception When the statement fails. The rows are kept when the connection was busy or the run was
	/// interrupted, so the flush can be retried; any other failure drops them.
	void Flush();

	/// Empties the buffer without running the statement, and recovers from a failed buffer operation.
	void Clear();

private:
	// Shared with the replacement scan that exposes the buffer. The scan outlives the appender, since scans cannot be
	// unregistered, so it holds this by shared_ptr; the appender's destructor nulls the collection to make it decline.
	struct Buffer {
		std::string name;
		std::vector<std::string> column_names;
		std::unique_ptr<ColumnDataCollection> collection;
	};

	// The shared body of both constructors.
	void Initialize(Connection &conn, const std::string &query, std::vector<LogicalType> column_types,
	                const std::string &buffer_name, const std::vector<std::string> &column_names);
	void ResetBuffer();

	// The connection the buffer's replacement scan is registered on, and the only one a flush can run on.
	Connection *connection = nullptr;
	std::vector<LogicalType> types;
	std::shared_ptr<Buffer> buffer;
	// The statement, parsed once and re-executed per flush, and the append state, recreated after every reset.
	std::unique_ptr<SqlStatement> statement;
	std::unique_ptr<ColumnDataCollection::AppendState> append_state;
	// Set when a buffer operation failed mid-flight, leaving the buffered rows indeterminate: appends and flushes
	// refuse until Clear recovers.
	bool broken = false;
};

//----------------------------------------------------------------------------------------------------------------------
// Arrow
//----------------------------------------------------------------------------------------------------------------------
// Interop with the Arrow C Data Interface, in both directions. `ArrowExporter` fills caller-allocated Arrow structs,
// which the caller then releases; `ArrowImporter` hands an Arrow array's buffers to `DataChunk`s zero-copy. Both
// resolve their schema once, at construction, and both gather rows up to a batch size across inputs, so the last
// input is marked with `flush`.

/// Converts Arrow arrays into `DataChunk`s against one resolved `ArrowSchema`. Construct it once, for example at a
/// table function's bind time, and reuse it for every array of that shape. Give it an array with `Append`, then call
/// `NextChunk` until it returns an empty handle. Pass `flush` on the last array, or call `Flush`.
/// The importer borrows the context it was created with and must not outlive it. One array is in flight at a time,
/// and an importer must not be used from two threads at once.
class ArrowImporter final : public detail::Handle<ArrowImporter> {
	friend detail::Factory;

public:
	/// Resolves `schema` against `context`, extension types included.
	/// @param context A context with an active transaction: resolving reads the catalog.
	/// @param schema The schema to resolve. Read, not consumed; the caller keeps ownership.
	/// @param batch_size Maximum rows per chunk. A long array is split across several chunks, and rows left over
	/// that do not fill a batch are held back and joined with the next array unless flushed. 0 means no maximum:
	/// one chunk per array.
	ArrowImporter(const Context &context, ArrowSchema &schema, idx_t batch_size = 0);

	ArrowImporter(ArrowImporter &&) noexcept = default;
	ArrowImporter &operator=(ArrowImporter &&) noexcept = default;

	~ArrowImporter() override;

	/// The resolved DuckDB schema: the name and logical type of every column, which is how a table function declares
	/// its result columns from an Arrow schema.
	auto GetSchema() const -> Schema;

	/// Gives the importer one array to convert. Take the chunks with `NextChunk`.
	/// @param array The array to convert.
	/// @param consume True to hand it over: its `release` is set to NULL, and the chunks reference its buffers
	/// zero-copy and keep them alive. False to keep it, in which case it must stay valid until the drain finishes and
	/// every chunk is a copy. A chunk that joins rows held back from the previous array is a copy either way.
	/// @param flush True to mark the end of the input, releasing rows that do not fill a batch as a final short chunk.
	/// @throws InvalidInputException When the previous array is not drained, or the array's shape does not match the
	/// resolved schema.
	auto Append(ArrowArray &array, bool consume = true, bool flush = false) -> void;

	/// Releases the held rows as a short chunk without supplying another array. Same as `Append` with no array and
	/// `flush` set.
	auto Flush() -> void;

	/// The next chunk of the appended array, or an empty handle once the array is drained. Rows that do not fill a
	/// batch are held back for the next array unless flushed, so an empty handle does not mean every row has come
	/// out. Runs under the context the importer was created with, which must still be alive.
	auto NextChunk() -> DataChunk;

private:
	explicit ArrowImporter(void *impl);
};

/// Converts `DataChunk`s into Arrow arrays for one fixed column list. The session's Arrow settings are captured at
/// construction, so the schema it reports and the arrays it produces always agree. Give it a chunk with `Append`,
/// then call `NextArray` until it returns false. Pass `flush` on the last chunk, or call `Flush`.
/// An exporter must not be used from two threads at once.
class ArrowExporter final : public detail::Handle<ArrowExporter> {
	friend detail::Factory;

public:
	/// @param context A context with an active transaction, whose Arrow settings are captured.
	/// @param types The column types.
	/// @param names The column names, one per type.
	/// @param batch_size Maximum rows per array. A long chunk is split across several arrays, and rows left over
	/// that do not fill a batch are held back and joined with the next chunk unless flushed. 0 means no maximum:
	/// one array per chunk.
	ArrowExporter(const Context &context, const std::vector<LogicalType> &types, const std::vector<std::string> &names,
	              idx_t batch_size = 0);

	ArrowExporter(ArrowExporter &&) noexcept = default;
	ArrowExporter &operator=(ArrowExporter &&) noexcept = default;

	~ArrowExporter() override;

	/// The Arrow schema of the arrays this exporter produces, written into the caller-allocated `out`, which the
	/// caller then owns and releases with `out.release(&out)`.
	auto GetSchema(ArrowSchema &out) const -> void;

	/// Converts one chunk in full, making the arrays it completed available from `NextArray`. The chunk is borrowed
	/// and read before this returns, since the conversion copies. Rows that do not complete a batch are held back
	/// and finished by the next chunk.
	/// @param flush True to mark the end of the input, releasing the held rows as a final short array.
	/// @throws InvalidInputException When the chunk's types do not match the exporter's.
	auto Append(const DataChunk &chunk, bool flush = false) -> void;

	/// Releases the held rows as a short array without supplying another chunk. Same as `Append` with `flush` set.
	auto Flush() -> void;

	/// Takes the next completed array into the caller-allocated `out`, which the caller then owns and releases.
	/// @return False when none is ready, leaving `out` released. Rows held back towards an unfinished batch are not
	/// an array yet; they come out after a further `Append` or a `Flush`.
	auto NextArray(ArrowArray &out) -> bool;

private:
	explicit ArrowExporter(void *impl);
};

/// An owning handle to an Arrow C Data Interface stream, produced by `QueryResult::ToArrowStream`. Destroying it
/// releases the stream, which closes the query and frees the connection for its next one. Arrays handed out by
/// `Next` are owned by the caller and released independently of this.
///
/// Unlike the other wrappers this does not derive from `detail::Handle`: it owns a raw `ArrowArrayStream` rather than
/// an opaque DuckDB handle, so the handle machinery does not apply.
class ArrowStream final {
	friend detail::Factory;

public:
	ArrowStream(ArrowStream &&other) noexcept : stream(other.stream) {
		other.stream = nullptr;
	}
	ArrowStream &operator=(ArrowStream &&other) noexcept {
		std::swap(stream, other.stream);
		return *this;
	}
	ArrowStream(const ArrowStream &) = delete;
	ArrowStream &operator=(const ArrowStream &) = delete;

	~ArrowStream();

	/// True while this holds a live stream, false once it has been moved from or detached.
	explicit operator bool() const noexcept {
		return stream != nullptr;
	}

	/// Borrows the underlying stream, which this still owns. Hand its address to an Arrow consumer that does not
	/// take ownership.
	auto get() const noexcept -> ArrowArrayStream * {
		return stream;
	}

	/// Detaches the underlying stream, handing the caller ownership and the duty to release it. Leaves this empty.
	auto Detach() noexcept -> ArrowArrayStream * {
		auto detached = stream;
		stream = nullptr;
		return detached;
	}

	/// Reads the stream's schema into `out`, which the caller then owns and releases.
	/// @throws InvalidInputException On failure, or when this stream is empty.
	void GetSchema(ArrowSchema &out) const;

	/// Fetches the next array into `out`, which the caller then owns and releases.
	/// @return False at end of stream, where `out` is left released.
	/// @throws InvalidInputException On failure, or when this stream is empty.
	bool Next(ArrowArray &out) const;

private:
	explicit ArrowStream(ArrowArrayStream *stream) : stream(stream) {
	}
	ArrowArrayStream *stream = nullptr;
};

//----------------------------------------------------------------------------------------------------------------------
// Result
//----------------------------------------------------------------------------------------------------------------------
// A `QueryResult` is a lazily executed stream of chunks, produced by a query.
// Execution is deferred until the result is asked for its first chunk, and only one result may be live on a connection
// at a time, so read a result to the end (or destroy it) before executing again.
//
// There are two ways to consume a result, synchronously or asynchronously
// - `FetchChunk` blocks until the next chunk is ready, and is what most callers probably want.
// - `Step` performs a bounded amount of work and reports what came of it, which allows an async runtime to drive a
// query without necessarily occupying a thread indefinitely.

/// A streaming query result.
class QueryResult final : public detail::Handle<QueryResult> {
	friend detail::Factory;

public:
	/// The status of one `Step`.
	enum class StepStatus : uint8_t {
		/// No chunk was produced by this step; call `Wait`, or come back later.
		WAITING = 0,
		/// A chunk was produced.
		CHUNK = 1,
		/// The result is exhausted. Sticky.
		FINISHED = 2,
		/// The query was canceled. Sticky.
		CANCELLED = 3,
	};

	/// The outcome of one `Step`.
	struct StepResult {
		/// What the step accomplished.
		StepStatus status;
		/// The chunk produced, empty unless `status` is CHUNK.
		DataChunk chunk;
	};

	/// The shape of a result.
	enum class ResultType : uint8_t {
		/// Produces rows: SELECT, EXPLAIN, RETURNING, and other row-producing statements.
		QUERY_RESULT = 0,
		/// Carries a count of affected rows: INSERT / UPDATE / DELETE and the like, without RETURNING.
		CHANGED_ROWS = 1,
		/// Produces no rows: most DDL and utility statements.
		NOTHING = 2,
	};

	/// The kind of SQL statement a result came from.
	enum class StatementType : uint8_t {
		INVALID = 0,
		SELECT = 1,
		INSERT = 2,
		UPDATE = 3,
		CREATE = 4,
		DELETE = 5,
		PREPARE = 6,
		EXECUTE = 7,
		ALTER = 8,
		TRANSACTION = 9,
		COPY = 10,
		ANALYZE = 11,
		VARIABLE_SET = 12,
		CREATE_FUNC = 13,
		EXPLAIN = 14,
		DROP = 15,
		EXPORT = 16,
		PRAGMA = 17,
		VACUUM = 18,
		CALL = 19,
		SET = 20,
		LOAD = 21,
		RELATION = 22,
		EXTENSION = 23,
		LOGICAL_PLAN = 24,
		ATTACH = 25,
		DETACH = 26,
		MULTI = 27,
		COPY_DATABASE = 28,
		UPDATE_EXTENSIONS = 29,
		MERGE_INTO = 30,
	};

	QueryResult(QueryResult &&) noexcept = default;
	QueryResult &operator=(QueryResult &&) noexcept = default;

	~QueryResult() override;

	/// The result's columns, their names and types, as one owned `Schema`.
	/// @throws InvalidInputException When the schema is not available yet; step the result first.
	auto GetSchema() const -> Schema;

	/// The shape of the result, so a caller can decide between consuming rows and draining without inspecting the SQL.
	/// @throws InvalidInputException When the shape is not available yet; step the result first.
	auto GetResultType() const -> ResultType;

	/// The kind of SQL statement this result came from.
	/// @throws InvalidInputException When the kind is not available yet; step the result first.
	auto GetStatementType() const -> StatementType;

	/// Does a bounded amount of work and returns without blocking.
	/// @return What the step accomplished, and the chunk if it produced one.
	/// @throws Exception On an execution error; the error is sticky, and later `Step`s rethrow it.
	auto Step() -> StepResult;

	/// Blocks until `Step` may be able to make progress, returning immediately once the result is finished or
	/// canceled. May not block at all.
	/// @throws Exception On an execution error.
	auto Wait() -> void;

	/// The next chunk, blocking until it is ready.
	/// @return The chunk, or an empty one at the end of the stream; calling it again then keeps returning empty.
	/// @throws InterruptException When the query was canceled.
	auto FetchChunk() -> DataChunk;

	/// Runs the result to the end, applying its side effects and discarding any rows.
	/// @return The number of rows affected for a CHANGED_ROWS result, 0 otherwise.
	/// @throws InterruptException When the query was canceled.
	auto Drain() -> idx_t;

	/// Renders the result as the boxed table the CLI prints, consuming it. Whatever has not been read yet is
	/// materialized in memory first.
	/// @param max_rows How many rows to print before eliding the middle, 0 for the default.
	/// @param max_width How wide the table may be, 0 for the default.
	/// @param max_col_width How wide a single column may be, 0 for the default.
	/// @param null_value What to print for a NULL cell; empty prints "NULL".
	/// @param render_mode 0 to lay the result out in rows, 1 in columns; anything else throws.
	/// @param limit The LIMIT the query itself applied, 0 for none. When the result fills it exactly, the footer reads
	/// "? rows", since there may have been more.
	auto RenderBox(idx_t max_rows = 0, idx_t max_width = 0, idx_t max_col_width = 0, const std::string &null_value = "",
	               idx_t render_mode = 0, idx_t limit = 0) -> std::string;

	/// Exports the result as a lazy `ArrowStream`, consuming it. Nothing is executed here: the stream converts as its
	/// consumer pulls. A result that has already yielded chunks produces a stream over what remains.
	/// @param batch_size Target rows per Arrow array, 0 for the default of 131072.
	/// @return The stream, which owns the query from now on and frees the connection when released.
	auto ToArrowStream(idx_t batch_size = 0) -> ArrowStream;

private:
	explicit QueryResult(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Function Signature
//----------------------------------------------------------------------------------------------------------------------

/// A function's declared parameters, variadic tail, and return type.
/// Borrowed from the `ScalarFunction` or `AggregateFunction` it was read from via `GetSignature`.
/// Valid for as long as the owning function is.
/// Setters mutate the function's signature in place.
class FunctionSignature final : public detail::Handle<FunctionSignature> {
	friend detail::Factory;

public:
	FunctionSignature(FunctionSignature &&) noexcept = default;
	FunctionSignature &operator=(FunctionSignature &&) noexcept = default;

	~FunctionSignature() override;

	/// Appends a parameter without a default value. `LogicalTypeId::ANY` is accepted and leaves the argument un-cast.
	/// @param name The parameter's name.
	/// @param type The parameter's type.
	auto AddParameter(const std::string &name, const LogicalType &type) -> FunctionSignature &;

	/// Appends a parameter with a default value: the caller may omit it, the function still receives the default.
	/// @param name The parameter's name.
	/// @param type The parameter's type.
	/// @param default_value The value the parameter takes when the caller omits it.
	auto AddParameter(const std::string &name, const LogicalType &type, const Value &default_value)
	    -> FunctionSignature &;

	/// Sets the variadic tail type, allowing any number of extra arguments after the fixed parameters. Pass
	/// `LogicalTypeId::ANY` to leave the tail un-cast. Overwrites any prior variadic tail.
	/// @param type The type every extra argument is cast to.
	auto SetVarArgs(const LogicalType &type) -> FunctionSignature &;

	/// Sets the return type. Overwrites any prior return type.
	/// @param type The return type.
	auto SetReturnType(const LogicalType &type) -> FunctionSignature &;

private:
	explicit FunctionSignature(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Function Properties
//----------------------------------------------------------------------------------------------------------------------
// Properties shared by every function category, set with the function classes' property setters (`SetStability`,
// `SetFallibility`, ...) to configure metadata that influences planning and execution. Category-specific properties
// (e.g. aggregate order/DISTINCT dependence) are declared as nested enums on the relevant function class.

/// How stable/deterministic a function's result is, used by the optimizer.
enum class FunctionStability : uint8_t {
	/// Always returns the same result for the same input.
	CONSISTENT = 0,
	/// The result may differ per row (e.g. random()).
	VOLATILE = 1,
	/// Stable within a single query/transaction but may change across queries (e.g. now()).
	CONSISTENT_WITHIN_QUERY = 2,
};

/// Whether a function handles NULL inputs itself.
enum class FunctionNullHandling : uint8_t {
	/// If any argument is NULL the result is NULL and the function is not invoked for that row.
	DEFAULT = 0,
	/// The function is invoked even when arguments are NULL and decides the result itself.
	SPECIAL = 1,
};

/// Whether a function can raise a runtime error.
enum class FunctionFallibility : uint8_t {
	/// The function never raises a runtime error. Declaring this promises the callbacks never throw; an exception
	/// thrown anyway becomes an internal error.
	INFALLIBLE = 0,
	/// The function may raise a runtime error for some inputs (default).
	FALLIBLE = 1,
};

/// How a function interacts with collations on its arguments.
enum class FunctionCollationHandling : uint8_t {
	/// Combines collations from its inputs and propagates them to its result (default).
	PROPAGATE = 0,
	/// Combinable collations are executed on the input arguments before the function runs.
	PUSH_COMBINABLE = 1,
	/// Collations are ignored by the function.
	IGNORE = 2,
};

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined scalar function, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, describe it (name, signature,
/// callbacks), then call `Register`. The function object may be destroyed after registration; the registered function
/// lives on in the catalog.
///
/// The callbacks receive their state through the input objects: `SetUserData` plants data readable from every
/// callback, the bind callback may plant bind data for init and exec, and the init callback may plant init data for
/// exec. A callback reports failure by throwing; the exception surfaces as the query's error.
class ScalarFunction final : public detail::Handle<ScalarFunction> {
	friend detail::Factory;

public:
	class BindInput;
	class InitInput;
	class ExecInput;

	/// Called once per query while the function call is bound. Optional; required when the return type is ANY.
	using BindCallback = void (*)(BindInput &input);
	/// Called once per execution thread before the first `ExecCallback` on it. Optional.
	using InitCallback = void (*)(InitInput &input);
	/// Called for every batch of rows; must fill the result vector. Required.
	using ExecCallback = void (*)(ExecInput &input);

	ScalarFunction(ScalarFunction &&) noexcept = default;
	ScalarFunction &operator=(ScalarFunction &&) noexcept = default;

	~ScalarFunction() override;

	/// Creates a function that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> ScalarFunction;
	/// Creates a function that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> ScalarFunction;

	/// Sets the function's name, as SQL will call it.
	auto SetName(const std::string &name) & -> ScalarFunction &;

	/// The function's signature, borrowed for in-place mutation. Registration requires a return type that is either
	/// a fully defined concrete type, or ANY combined with a bind callback that resolves it.
	auto GetSignature() -> FunctionSignature;

	/// Calls `configure` with the function's signature, borrowed for in-place mutation. Registration requires a return
	/// type that is either a fully defined concrete type, or ANY combined with a bind callback that resolves it.
	template <class F>
	auto WithSignature(F &&configure) & -> ScalarFunction & {
		auto sig = GetSignature();
		configure(sig);
		return *this;
	}

	/// Constructs user data of type `T`, carried by the registered function and freed at engine teardown; read it from
	/// any callback via the inputs' `GetUserData<T>`. Consumed by `Register`: set it again before re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> ScalarFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> ScalarFunction &;
	auto SetInitCallback(InitCallback callback) & -> ScalarFunction &;
	auto SetExecCallback(ExecCallback callback) & -> ScalarFunction &;

	/// How stable the function's result is across rows and queries. Defaults to `CONSISTENT`.
	auto SetStability(FunctionStability value) & -> ScalarFunction &;
	/// Whether the function handles NULL inputs itself. Defaults to `DEFAULT` (NULL in, NULL out).
	auto SetNullHandling(FunctionNullHandling value) & -> ScalarFunction &;
	/// Whether the function can raise a runtime error. Defaults to `FALLIBLE`; declaring `INFALLIBLE` promises the
	/// callbacks never throw.
	auto SetFallibility(FunctionFallibility value) & -> ScalarFunction &;
	/// How the function interacts with collations on its arguments. Defaults to `PROPAGATE`.
	auto SetCollationHandling(FunctionCollationHandling value) & -> ScalarFunction &;

	/// Registers the function in the catalog it was created against. The function object remains valid and may be
	/// adjusted and registered again; user data set via `SetUserData` is consumed by the first `Register`.
	/// @throws InvalidInputException When the name, exec callback, or a usable return type is missing.
	auto Register() -> void;

private:
	explicit ScalarFunction(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	BindCallback bind_callback = nullptr;
	InitCallback init_callback = nullptr;
	ExecCallback exec_callback = nullptr;
	detail::UserData user_data;

public:
	/// What the bind callback works with. Borrowed, valid only for the callback duration.
	class BindInput {
		friend detail::Factory;

	public:
		/// Constructs bind data of type `T`, owned by the bound function call and readable from the init and exec
		/// callbacks via `GetBindData<T>`. The engine compares bind data when it compares expressions: by
		/// `operator==` when `T` has one, by identity otherwise.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::SelectEquals<T>(), detail::TypedDelete<T>);
		}

		/// The user data set via `ScalarFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many arguments this call passes: one per argument of the call, variadic tail arguments included.
		/// Valid indices for `GetArgType` and `GetConstantArgument` are [0, GetArgCount()).
		auto GetArgCount() const -> idx_t;

		/// One argument's resolved type, as the binder settled it. An ANY parameter reports the type the caller
		/// actually passed.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetArgType(idx_t index) const -> LogicalType;

		/// The constant value of one argument, folded at bind time. Use it for arguments the function needs to know
		/// before execution, e.g. a format string or a target type.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		/// @throws Exception When the argument is not a constant expression, e.g. a column reference.
		auto GetConstantArgument(idx_t index) const -> Value;

		/// `GetConstantArgument` without the failure: nullopt instead of an exception when the argument carries no
		/// constant value, i.e. it is not a constant expression, its value is not yet known (an unresolved
		/// prepared-statement parameter), or the index is out of range. Use it when a non-constant argument should
		/// fall back to the runtime value instead of failing the query.
		/// @param index Argument index in [0, GetArgCount()).
		auto TryGetConstantArgument(idx_t index) const -> std::optional<Value>;

		/// Resolves the declared return type; required, and only permitted, when the signature declared it as ANY.
		/// @param type The concrete return type of this bound call.
		auto SetReturnType(const LogicalType &type) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	/// What the init callback works with. Borrowed, valid only for the callback duration.
	class InitInput {
		friend detail::Factory;

	public:
		/// Constructs init data of type `T`, owned by this execution thread's function state and readable from the
		/// exec callback via `GetInitData<T>`.
		template <class T, class... ARGS>
		void SetInitData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetInitDataInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `ScalarFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The context the function is initialized in. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		InitInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetInitDataInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the exec callback works with. Borrowed, valid only for the callback duration.
	class ExecInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The init data set via `InitInput::SetInitData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		/// The user data set via `ScalarFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many rows this execution must produce: the exec callback writes exactly this many rows to the result
		/// vector. May be less than a full vector; with all-constant arguments the function runs for a single row and
		/// the engine expands the result.
		auto GetRowCount() const -> idx_t;

		/// How many argument vectors this execution carries: one per argument of the call, variadic tail arguments
		/// included. Valid indices for `GetArg` are [0, GetArgCount()).
		auto GetArgCount() const -> idx_t;

		/// One argument's vector.
		/// @param index Argument index in [0, GetArgCount()).
		auto GetArg(idx_t index) const -> Vector;

		/// The result vector to fill.
		auto GetResult() const -> Vector;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		ExecInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Aggregate Function
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined aggregate function, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, describe it (name, signature,
/// callbacks), then call `Register`. The function object may be destroyed after registration; the registered function
/// lives on in the catalog.
///
/// The aggregate keeps one state per group. The size callback reports how large a single state is; the init callback
/// constructs a batch of freshly allocated states; the update callback folds a batch of input rows into their rows'
/// states; the combine callback merges partial states (e.g. across threads); the finalize callback turns a batch of
/// states into result rows; and the optional destroy callback releases resources a state owns.
///
/// The callbacks receive their state through the input objects: `SetUserData` plants data readable from every
/// callback, and the bind callback may plant bind data readable from every later callback. A callback reports failure
/// by throwing; the exception surfaces as the query's error -- except in the destroy callback, whose errors are
/// dropped, as it runs on a path that must not fail.
class AggregateFunction final : public detail::Handle<AggregateFunction> {
	friend detail::Factory;

public:
	/// Whether the aggregate's result depends on the order in which rows are aggregated.
	enum class OrderDependence : uint8_t {
		/// The result depends on input order (default).
		DEPENDENT = 0,
		/// The result does not depend on input order.
		INDEPENDENT = 1,
	};

	/// Whether the aggregate's result is affected by a DISTINCT modifier.
	enum class DistinctDependence : uint8_t {
		/// The result is affected by DISTINCT (default).
		DEPENDENT = 0,
		/// The result is not affected by DISTINCT.
		INDEPENDENT = 1,
	};

	class BindInput;
	class SizeInput;
	class InitInput;
	class UpdateInput;
	class CombineInput;
	class FinalizeInput;
	class DestroyInput;

	/// Called once per query while the function call is bound. Optional; required when the return type is ANY.
	using BindCallback = void (*)(BindInput &input);
	/// Called to size a single aggregate state; must call `SetStateSize`. Required.
	using SizeCallback = void (*)(SizeInput &input);
	/// Called to initialize a batch of freshly allocated states in place. Required.
	using InitCallback = void (*)(InitInput &input);
	/// Called to fold a batch of input rows into their rows' states. Required.
	using UpdateCallback = void (*)(UpdateInput &input);
	/// Called to merge a batch of source states into their target states. Required.
	using CombineCallback = void (*)(CombineInput &input);
	/// Called to turn a batch of states into result rows. Required.
	using FinalizeCallback = void (*)(FinalizeInput &input);
	/// Called to release resources a batch of states owns. Optional; must not fail.
	using DestroyCallback = void (*)(DestroyInput &input);

	AggregateFunction(AggregateFunction &&) noexcept = default;
	AggregateFunction &operator=(AggregateFunction &&) noexcept = default;

	~AggregateFunction() override;

	/// Creates a function that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> AggregateFunction;
	/// Creates a function that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> AggregateFunction;

	/// Sets the function's name, as SQL will call it.
	auto SetName(const std::string &name) & -> AggregateFunction &;

	/// The function's signature, borrowed for in-place mutation. Registration requires a return type that is either
	/// a fully defined concrete type, or ANY combined with a bind callback that resolves it.
	auto GetSignature() -> FunctionSignature;

	/// Calls `configure` with the function's signature, borrowed for in-place mutation. Registration requires a return
	/// type that is either a fully defined concrete type, or ANY combined with a bind callback that resolves it.
	template <class F>
	auto WithSignature(F &&configure) & -> AggregateFunction & {
		auto sig = GetSignature();
		configure(sig);
		return *this;
	}

	/// Constructs user data of type `T`, carried by the registered function and freed at engine teardown; read it from
	/// any callback via the inputs' `GetUserData<T>`. Consumed by `Register`: set it again before re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> AggregateFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> AggregateFunction &;
	auto SetSizeCallback(SizeCallback callback) & -> AggregateFunction &;
	auto SetInitCallback(InitCallback callback) & -> AggregateFunction &;
	auto SetUpdateCallback(UpdateCallback callback) & -> AggregateFunction &;
	auto SetCombineCallback(CombineCallback callback) & -> AggregateFunction &;
	auto SetFinalizeCallback(FinalizeCallback callback) & -> AggregateFunction &;
	auto SetDestroyCallback(DestroyCallback callback) & -> AggregateFunction &;

	/// How stable the function's result is across rows and queries. Defaults to `CONSISTENT`.
	auto SetStability(FunctionStability value) & -> AggregateFunction &;
	/// Whether the function handles NULL inputs itself. Defaults to `DEFAULT` (NULL in, NULL out).
	auto SetNullHandling(FunctionNullHandling value) & -> AggregateFunction &;
	/// Whether the function can raise a runtime error. Defaults to `FALLIBLE`; declaring `INFALLIBLE` promises the
	/// callbacks never throw.
	auto SetFallibility(FunctionFallibility value) & -> AggregateFunction &;
	/// How the function interacts with collations on its arguments. Defaults to `PROPAGATE`.
	auto SetCollationHandling(FunctionCollationHandling value) & -> AggregateFunction &;
	/// Whether the result depends on the order in which rows are aggregated. Defaults to `DEPENDENT`.
	auto SetOrderDependence(OrderDependence value) & -> AggregateFunction &;
	/// Whether the result is affected by a DISTINCT modifier. Defaults to `DEPENDENT`.
	auto SetDistinctDependence(DistinctDependence value) & -> AggregateFunction &;

	/// Registers the function in the catalog it was created against. The function object remains valid and may be
	/// adjusted and registered again; user data set via `SetUserData` is consumed by the first `Register`.
	/// @throws InvalidInputException When the name, a required callback, or a usable return type is missing.
	auto Register() -> void;

private:
	explicit AggregateFunction(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	BindCallback bind_callback = nullptr;
	SizeCallback size_callback = nullptr;
	InitCallback init_callback = nullptr;
	UpdateCallback update_callback = nullptr;
	CombineCallback combine_callback = nullptr;
	FinalizeCallback finalize_callback = nullptr;
	DestroyCallback destroy_callback = nullptr;
	detail::UserData user_data;

public:
	/// What the bind callback works with. Borrowed, valid only for the callback duration.
	class BindInput {
		friend detail::Factory;

	public:
		/// Constructs bind data of type `T`, owned by the bound function call and readable from every later callback
		/// via `GetBindData<T>`. The engine compares bind data when it compares expressions: by `operator==` when `T`
		/// has one, by identity otherwise.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::SelectEquals<T>(), detail::TypedDelete<T>);
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many arguments this call passes: one per argument of the call, variadic tail arguments included.
		/// Valid indices for `GetArgType` and `GetConstantArgument` are [0, GetArgCount()).
		auto GetArgCount() const -> idx_t;

		/// One argument's resolved type, as the binder settled it. An ANY parameter reports the type the caller
		/// actually passed.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetArgType(idx_t index) const -> LogicalType;

		/// The constant value of one argument, folded at bind time. Use it for arguments the function needs to know
		/// before execution, e.g. a format string or a target type.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		/// @throws Exception When the argument is not a constant expression, e.g. a column reference.
		auto GetConstantArgument(idx_t index) const -> Value;

		/// `GetConstantArgument` without the failure: nullopt instead of an exception when the argument carries no
		/// constant value, i.e. it is not a constant expression, its value is not yet known (an unresolved
		/// prepared-statement parameter), or the index is out of range. Use it when a non-constant argument should
		/// fall back to the runtime value instead of failing the query.
		/// @param index Argument index in [0, GetArgCount()).
		auto TryGetConstantArgument(idx_t index) const -> std::optional<Value>;

		/// Resolves the declared return type; required, and only permitted, when the signature declared it as ANY.
		/// @param type The concrete return type of this bound call.
		auto SetReturnType(const LogicalType &type) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	/// What the size callback works with. Borrowed, valid only for the callback duration.
	class SizeInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Reports the size of a single aggregate state, in bytes; the callback must call this.
		/// @param size The state size, in bytes.
		auto SetStateSize(idx_t size) -> void;

	private:
		explicit SizeInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the init callback works with. Borrowed, valid only for the callback duration.
	class InitInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many states this invocation must initialize: the length of `GetStates`.
		auto GetStateCount() const -> idx_t;

		/// The states to initialize, one pointer per state. Each points to uninitialized memory of the size the size
		/// callback reported; the callback must initialize all of them in place.
		auto GetStates() const -> void **;

	private:
		explicit InitInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the update callback works with. Borrowed, valid only for the callback duration.
	class UpdateInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many input rows this invocation carries: the length of the argument vectors and of `GetStates`.
		auto GetRowCount() const -> idx_t;

		/// How many argument vectors this invocation carries: one per argument of the call, variadic tail arguments
		/// included. Valid indices for `GetArg` are [0, GetArgCount()).
		auto GetArgCount() const -> idx_t;

		/// One argument's vector.
		/// @param index Argument index in [0, GetArgCount()).
		auto GetArg(idx_t index) const -> Vector;

		/// The states to update, one pointer per input row: row i of every argument vector must be aggregated into
		/// state i. Different rows may point to the same state.
		auto GetStates() const -> void **;

	private:
		explicit UpdateInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the combine callback works with. Borrowed, valid only for the callback duration.
	class CombineInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many source/target state pairs this invocation must combine: the length of `GetSources` and
		/// `GetTargets`.
		auto GetStateCount() const -> idx_t;

		/// The source states, one pointer per pair. Source i must be combined into target i; the source must not be
		/// modified.
		auto GetSources() const -> void **;

		/// The target states, one pointer per pair. Source i must be combined into target i.
		auto GetTargets() const -> void **;

	private:
		explicit CombineInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the finalize callback works with. Borrowed, valid only for the callback duration.
	class FinalizeInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many states this invocation must finalize: the length of `GetStates` and the number of rows to write.
		auto GetStateCount() const -> idx_t;

		/// The states to finalize, one pointer per state. State i must be finalized into result row
		/// `GetResultOffset() + i`.
		auto GetStates() const -> void **;

		/// The result vector to fill, starting at `GetResultOffset`.
		auto GetResult() const -> Vector;

		/// The offset in the result vector at which to start writing: state i must be finalized into result row
		/// `GetResultOffset() + i`.
		auto GetResultOffset() const -> idx_t;

	private:
		explicit FinalizeInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the destroy callback works with. Borrowed, valid only for the callback duration.
	class DestroyInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `AggregateFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many states this invocation must destroy: the length of `GetStates`.
		auto GetStateCount() const -> idx_t;

		/// The states to destroy, one pointer per state. The callback must release any resources the states own.
		auto GetStates() const -> void **;

	private:
		explicit DestroyInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Bound Expression
//----------------------------------------------------------------------------------------------------------------------

/// The type of a bound expression node. Mirrors the C API's `DUCKDB_V2_EXPRESSION_TYPE`: the node types a filter
/// predicate can contain, every other node type reporting `INVALID`.
///
/// Comparisons, `BETWEEN` and casts are regular scalar function calls once bound: they have the function's children
/// and name, and only their type tells them apart from any other function. A cast has one child, the value being
/// cast, and its target type is the node's return type.
enum class ExpressionType : uint8_t {
	/// A node type this API does not model. Its children can still be walked.
	INVALID = 0,
	/// A cast. One child; `Expression::GetCastMode` tells a `TRY_CAST` apart.
	OPERATOR_CAST = 12,
	/// Logical `NOT`. One child.
	OPERATOR_NOT = 13,
	/// `IS NULL`. One child.
	OPERATOR_IS_NULL = 14,
	/// `IS NOT NULL`. One child.
	OPERATOR_IS_NOT_NULL = 15,
	/// `=`. Two children.
	COMPARE_EQUAL = 25,
	/// `<>`. Two children.
	COMPARE_NOTEQUAL = 26,
	/// `<`. Two children.
	COMPARE_LESSTHAN = 27,
	/// `>`. Two children.
	COMPARE_GREATERTHAN = 28,
	/// `<=`. Two children.
	COMPARE_LESSTHANOREQUALTO = 29,
	/// `>=`. Two children.
	COMPARE_GREATERTHANOREQUALTO = 30,
	/// `IN`. The first child is the value tested, the remaining children are the candidates.
	COMPARE_IN = 35,
	/// `NOT IN`. The first child is the value tested, the remaining children are the candidates.
	COMPARE_NOT_IN = 36,
	/// `IS DISTINCT FROM`. Two children.
	COMPARE_DISTINCT_FROM = 37,
	/// `BETWEEN`. Three children: the value tested, the lower bound and the upper bound.
	COMPARE_BETWEEN = 38,
	/// `IS NOT DISTINCT FROM`. Two children.
	COMPARE_NOT_DISTINCT_FROM = 40,
	/// Logical `AND`. Two or more children.
	CONJUNCTION_AND = 50,
	/// Logical `OR`. Two or more children.
	CONJUNCTION_OR = 51,
	/// A constant. No children; read it with `Expression::GetConstantValue`.
	VALUE_CONSTANT = 75,
	/// A prepared statement parameter whose value is not known yet. No children.
	VALUE_PARAMETER = 76,
	/// A call to a scalar function other than the ones listed above. The children are its arguments; read the name
	/// with `Expression::GetFunctionName`.
	BOUND_FUNCTION = 141,
	/// A `CASE` expression. The children are each `WHEN` condition followed by its `THEN` result, then the `ELSE`
	/// result.
	CASE_EXPR = 150,
	/// `COALESCE`. One or more children.
	OPERATOR_COALESCE = 152,
	/// A reference to a column. No children; read it with `Expression::GetColumnIndex`.
	BOUND_COLUMN_REF = 228,
};

/// A node of a bound expression tree: an expression the engine has resolved every column and function of, handed
/// to callbacks that may want to look inside a predicate (see `TableFunction::FilterPushdownInput`). Borrowed and
/// read-only: valid only for the duration of the callback that handed it out, and the children obtained via
/// `GetChild` share their parent's lifetime.
class Expression final : public detail::Handle<Expression> {
	friend detail::Factory;

public:
	Expression(Expression &&) noexcept = default;
	Expression &operator=(Expression &&) noexcept = default;
	~Expression() override = default;

	/// The node's type, which decides what its children mean and which of the accessors below apply.
	auto GetType() const -> ExpressionType;
	/// The logical type the node evaluates to. For a cast, the target type.
	auto GetReturnType() const -> LogicalType;
	/// How many child nodes the node has. Works for every type, including `ExpressionType::INVALID`.
	auto GetChildCount() const -> idx_t;
	/// A child node, ordered as `ExpressionType` describes.
	/// @throws InvalidInputException When the index is out of bounds.
	auto GetChild(idx_t index) const -> Expression;
	/// The value of a `VALUE_CONSTANT` node.
	/// @throws InvalidInputException When the node is not a constant.
	auto GetConstantValue() const -> Value;
	/// The column a `BOUND_COLUMN_REF` node points at. The index counts the columns of the operator the predicate is
	/// evaluated against; resolve it through whatever handed out the expression, e.g.
	/// `TableFunction::FilterPushdownInput::GetColumnIndex`.
	/// @throws InvalidInputException When the node is not a column reference.
	auto GetColumnIndex() const -> idx_t;
	/// The name of the scalar function a function node calls: `BOUND_FUNCTION`, the comparisons, `COMPARE_BETWEEN`
	/// and `OPERATOR_CAST`. A comparison's name is its operator, e.g. `<`; `BETWEEN` and casts carry internal names,
	/// so dispatch on the type for those.
	/// @throws InvalidInputException When the node is not a function call.
	auto GetFunctionName() const -> std::string;
	/// The qualified name of the scalar function a function node calls: the name of `GetFunctionName`, qualified with
	/// the catalog and schema the function was resolved in where known.
	/// @throws InvalidInputException When the node is not a function call.
	auto GetFunctionQualifiedName() const -> QualifiedName;
	/// Whether an `OPERATOR_CAST` node is a regular `CAST` or a `TRY_CAST`.
	/// @throws InvalidInputException When the node is not a cast.
	auto GetCastMode() const -> CastMode;

private:
	explicit Expression(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Table Function
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined table function, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, describe it (name, signature,
/// callbacks), then call `Register`. The function object may be destroyed after registration; the registered function
/// lives on in the catalog.
///
/// A table function produces a table rather than a value: the bind callback declares the columns it returns, and the
/// exec callback is then invoked repeatedly to fill batches of rows until it produces an empty one. Between them, the
/// optional init callbacks set up the state the scan runs on: one global state shared by every thread, and one local
/// state per thread.
///
/// The callbacks receive their state through the input objects: `SetUserData` plants data readable from every
/// callback, and the bind callback may plant bind data readable from every later callback. A callback reports failure
/// by throwing; the exception surfaces as the query's error.
class TableFunction final : public detail::Handle<TableFunction> {
	friend detail::Factory;

public:
	class BindInput;
	class InitGlobalInput;
	class InitLocalInput;
	class ExecInput;
	class ProgressInput;
	class FilterPushdownInput;

	/// Called once per query while the function call is bound; declares the columns the function returns. Required.
	using BindCallback = void (*)(BindInput &input);
	/// Called once per scan, before the first `ExecCallback`. Optional.
	using InitGlobalCallback = void (*)(InitGlobalInput &input);
	/// Called once per scanning thread, before the first `ExecCallback` on it. Optional.
	using InitLocalCallback = void (*)(InitLocalInput &input);
	/// Called repeatedly to produce the next batch of rows; an empty batch ends the scan. Required.
	using ExecCallback = void (*)(ExecInput &input);
	/// Called on demand during execution to report how far the scan has advanced. Optional.
	using ProgressCallback = void (*)(ProgressInput &input);
	/// Called while the query is optimized, possibly more than once, with the predicates the query applies to the
	/// function's rows; accepts the ones the function will apply itself. Optional.
	using FilterPushdownCallback = void (*)(FilterPushdownInput &input);

	TableFunction(TableFunction &&) noexcept = default;
	TableFunction &operator=(TableFunction &&) noexcept = default;

	~TableFunction() override;

	/// Creates a function that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> TableFunction;
	/// Creates a function that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> TableFunction;

	/// Sets the function's name, as SQL will call it.
	auto SetName(const std::string &name) & -> TableFunction &;

	/// The function's signature, borrowed for in-place mutation. A parameter without a default value becomes a
	/// required positional argument, one with a default becomes a named argument the caller may omit. Registration
	/// rejects a return type: a table function declares the columns it returns from its bind callback.
	auto GetSignature() -> FunctionSignature;

	/// Calls `configure` with the function's signature, borrowed for in-place mutation. See `GetSignature`.
	template <class F>
	auto WithSignature(F &&configure) & -> TableFunction & {
		auto sig = GetSignature();
		configure(sig);
		return *this;
	}

	/// Constructs user data of type `T`, carried by the registered function and freed at engine teardown; read it from
	/// any callback via the inputs' `GetUserData<T>`. Consumed by `Register`: set it again before re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> TableFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> TableFunction &;
	auto SetInitGlobalCallback(InitGlobalCallback callback) & -> TableFunction &;
	auto SetInitLocalCallback(InitLocalCallback callback) & -> TableFunction &;
	auto SetExecCallback(ExecCallback callback) & -> TableFunction &;
	auto SetProgressCallback(ProgressCallback callback) & -> TableFunction &;
	auto SetFilterPushdownCallback(FilterPushdownCallback callback) & -> TableFunction &;

	/// Declares whether the function supports projection pushdown. Defaults to false. With it, the engine asks for
	/// only the columns a query uses: the exec callback's output chunk holds one vector per requested column, and
	/// the init and exec inputs' `GetColumnIndex` says which declared column each vector stands for. Without it the
	/// output chunk always holds every declared column, and the engine drops the unused ones itself.
	auto SetProjectionPushdown(bool enable) & -> TableFunction &;

	/// Registers the function in the catalog it was created against. The function object remains valid and may be
	/// adjusted and registered again; user data set via `SetUserData` is consumed by the first `Register`.
	/// @throws InvalidInputException When the name, bind callback or exec callback is missing, or the signature
	/// declares a return type.
	auto Register() -> void;

private:
	explicit TableFunction(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	BindCallback bind_callback = nullptr;
	InitGlobalCallback init_global_callback = nullptr;
	InitLocalCallback init_local_callback = nullptr;
	ExecCallback exec_callback = nullptr;
	ProgressCallback progress_callback = nullptr;
	FilterPushdownCallback filter_pushdown_callback = nullptr;
	detail::UserData user_data;

public:
	/// What the bind callback works with. Borrowed, valid only for the callback duration.
	class BindInput {
		friend detail::Factory;

	public:
		/// Declares one of the columns the function returns. Call it once per column, in order: the exec callback's
		/// output chunk carries one vector per declared column, in the same order. At least one column is required.
		/// @param name The column's name.
		/// @param type The column's type. Must be a fully defined concrete type; ANY is rejected.
		auto AddResultColumn(const std::string &name, const LogicalType &type) -> void;

		/// Constructs bind data of type `T`, owned by the bound function call and readable from every later callback
		/// via `GetBindData<T>`. The engine compares bind data when it compares expressions: by `operator==` when `T`
		/// has one, by identity otherwise.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::SelectEquals<T>(), detail::TypedDelete<T>);
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many arguments this call passes. The arguments are the signature's parameters in order -- a parameter
		/// the call site omitted still appears, carrying its declared default -- followed by any variadic tail
		/// arguments. Valid indices for `GetArgType` and `GetArgument` are [0, GetArgCount()).
		auto GetArgCount() const -> idx_t;

		/// One argument's type.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetArgType(idx_t index) const -> LogicalType;

		/// One argument's value. A table function's arguments are always constants, so this only fails on a bad index.
		/// @param index Argument index in [0, GetArgCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetArgument(idx_t index) const -> Value;

		/// Hints how many rows the scan will produce, for the optimizer. Producing a different number of rows is not
		/// an error.
		/// @param cardinality The estimated row count.
		/// @param is_exact Whether the estimate is exact, which also makes it an upper bound.
		auto SetCardinality(idx_t cardinality, bool is_exact) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	/// What the global init callback works with. Borrowed, valid only for the callback duration.
	class InitGlobalInput {
		friend detail::Factory;

	public:
		/// Constructs global state of type `T`, shared by every thread scanning the function and readable from the
		/// local init, exec and progress callbacks. Since every thread sees the same object, the function must
		/// synchronize its own access to it.
		template <class T, class... ARGS>
		void SetGlobalState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetGlobalStateInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Caps how many threads may scan the function in parallel. Defaults to 1, a single-threaded scan. The engine
		/// creates at most this many local states, and may use fewer.
		/// @param max_threads The maximum thread count. Must be at least 1.
		auto SetMaxThreads(idx_t max_threads) -> void;

		/// How many columns the scan produces: with projection pushdown the columns the query uses, which is the number
		/// of vectors in the exec callback's output chunk; without it, the columns declared in bind.
		auto GetColumnCount() const -> idx_t;
		/// Which declared column (in `BindInput::AddResultColumn` order) the scan's column at `index` stands for. The
		/// identity without projection pushdown.
		/// @throws InvalidInputException When the index is out of bounds.
		auto GetColumnIndex(idx_t index) const -> idx_t;

		/// The scan's context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		InitGlobalInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetGlobalStateInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the local init callback works with. Borrowed, valid only for the callback duration.
	class InitLocalInput {
		friend detail::Factory;

	public:
		/// Constructs local state of type `T`, owned by this scanning thread and readable from the exec callback via
		/// `ExecInput::GetLocalState<T>`. No other thread observes it, so it needs no synchronization.
		template <class T, class... ARGS>
		void SetLocalState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetLocalStateInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `InitGlobalInput::SetGlobalState`, typically to claim this thread's share of the
		/// work from it. Shared with every other scanning thread; access must be synchronized.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many columns the scan produces; see `InitGlobalInput::GetColumnCount`.
		auto GetColumnCount() const -> idx_t;
		/// Which declared column the scan's column at `index` stands for; see `InitGlobalInput::GetColumnIndex`.
		/// @throws InvalidInputException When the index is out of bounds.
		auto GetColumnIndex(idx_t index) const -> idx_t;

		/// The scan's context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		InitLocalInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetLocalStateInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the exec callback works with. Borrowed, valid only for the callback duration.
	class ExecInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `InitGlobalInput::SetGlobalState`. Shared with every other scanning thread;
		/// access must be synchronized.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The local state set via `InitLocalInput::SetLocalState`, private to this thread.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetLocalState() const -> T & {
			return *static_cast<T *>(GetLocalStateInternal());
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The chunk to write the next batch of rows into, with one vector per column declared in bind. It starts out
		/// empty on every invocation: write the rows, then give the batch its row count with `Vector::SetSize` on the
		/// chunk's first vector, which the engine propagates to the others. Leaving it empty ends the scan.
		/// @return A borrowed chunk, valid only for the callback duration.
		auto GetOutputChunk() const -> DataChunk;

		/// How many vectors the output chunk holds; see `InitGlobalInput::GetColumnCount`.
		auto GetColumnCount() const -> idx_t;
		/// Which declared column the output chunk's vector at `index` must be filled with; see
		/// `InitGlobalInput::GetColumnIndex`.
		/// @throws InvalidInputException When the index is out of bounds.
		auto GetColumnIndex(idx_t index) const -> idx_t;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		ExecInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetLocalStateInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the progress callback works with. Borrowed, valid only for the callback duration.
	class ProgressInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `InitGlobalInput::SetGlobalState`. This callback runs while the scan is running,
		/// so the state must be read in a thread-safe way.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Reports how far the scan has advanced, as a fraction between 0.0 and 1.0; values outside that range are
		/// clamped. A callback that returns without calling this reports no progress.
		/// @param progress The fraction of the scan that is complete.
		auto SetProgress(double progress) -> void;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		ProgressInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the filter pushdown callback works with. Borrowed, valid only for the callback duration.
	///
	/// The callback runs while the query is optimized, after bind and before any init callback, with the predicates
	/// the query applies to the function's rows. Accepting one with `Accept` is a promise: the engine stops
	/// applying it, so the scan must produce only rows that satisfy it. Predicates left unaccepted are applied by
	/// the engine as usual, so a callback that recognizes nothing can simply return. The optimizer may run the
	/// callback more than once for the same query, each time with the predicates not yet accepted, and never with
	/// none.
	class FilterPushdownInput {
		friend detail::Factory;

	public:
		/// The bind data set via `BindInput::SetBindData`, mutable: the same object the init and exec callbacks later
		/// receive, so an accepted predicate can be recorded in it for the scan to apply.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> T & {
			return *static_cast<T *>(GetBindDataInternal());
		}

		/// The user data set via `TableFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many predicates are offered. They are combined with `AND`.
		auto GetFilterCount() const -> idx_t;
		/// A predicate: a bound expression evaluating to `BOOLEAN`. Resolve the column references it contains via
		/// `GetColumnIndex`.
		/// @return A borrowed expression, valid only for the callback duration.
		/// @throws InvalidInputException When the index is out of bounds.
		auto GetFilter(idx_t index) const -> Expression;
		/// Accepts the predicate at `index`: the function will apply it itself.
		/// @throws InvalidInputException When the index is out of bounds.
		auto Accept(idx_t index) -> void;
		/// How many columns the predicates can refer to: the columns the query reads from the function, which is what
		/// `Expression::GetColumnIndex` indexes.
		auto GetColumnCount() const -> idx_t;
		/// Resolves the index a column reference node reports to the declared column (in `BindInput::AddResultColumn`
		/// order) it refers to.
		/// @throws InvalidInputException When the index is out of bounds.
		auto GetColumnIndex(idx_t index) const -> idx_t;

		/// The query's context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		FilterPushdownInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Custom Type
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined type, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, give it a name and a base type, then
/// call `Register`. The type object may be destroyed after registration; the registered type lives on in the catalog.
///
/// A custom type borrows its base type's internal representation, so the execution engine needs no special handling
/// for it, while staying logically distinct from the base type so it can carry its own casts. Values of it are
/// produced and read with the base type's vector accessors; to hand one back out under the custom name, alias the
/// base type with `LogicalType::WithAlias`.
class CustomType final : public detail::Handle<CustomType> {
	friend detail::Factory;

public:
	CustomType(CustomType &&) noexcept = default;
	CustomType &operator=(CustomType &&) noexcept = default;

	~CustomType() override;

	/// Creates a type that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> CustomType;
	/// Creates a type that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> CustomType;

	/// Sets the type's name, as SQL will refer to it, and as every logical type instance of it carries as its alias.
	auto SetName(const std::string &name) & -> CustomType &;

	/// Sets the type whose representation this type borrows. Must be a fully defined concrete type.
	auto SetBaseType(const LogicalType &type) & -> CustomType &;

	/// Registers the type in the catalog it was created against. The type object remains valid and may be adjusted
	/// and registered again.
	/// @throws InvalidInputException When the name or the base type is missing, or the base type is not concrete.
	auto Register() -> void;

private:
	explicit CustomType(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Copy Function
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined file format for `COPY`, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, describe it (name, callbacks), then
/// call `Register`. The function object may be destroyed after registration; the registered function lives on in the
/// catalog and is reached from SQL with `COPY ... TO 'path' (FORMAT name)` and `COPY table FROM 'path' (FORMAT name)`.
///
/// The two directions are configured separately and a function may implement either or both. The `COPY ... TO` side
/// gathers the rows being written into batches and drives its callbacks in this order: bind (once, during planning),
/// batch size (once, during planning, unless the statement sets `BATCH_SIZE`), init (once per output file), batch
/// (once per batch, possibly on several threads at once), flush (once per prepared batch, never concurrently for the
/// same file) and finalize (once per output file). The `COPY ... FROM` side reads like a table function whose columns
/// are fixed by the target table: bind (once, during planning), init global (once per statement), init local (once
/// per thread), exec (repeatedly, until it produces an empty batch), plus an optional progress hook.
///
/// The callbacks receive their state through the input objects: `SetUserData` plants data readable from every
/// callback of either side, and each side's bind callback may plant bind data readable from that side's later
/// callbacks. A callback reports failure by throwing; the exception surfaces as the query's error.
class CopyFunction final : public detail::Handle<CopyFunction> {
	friend detail::Factory;

public:
	class CopyToBindInput;
	class CopyToBatchSizeInput;
	class CopyToInitInput;
	class CopyToBatchInput;
	class CopyToFlushInput;
	class CopyToFinalizeInput;
	class CopyFromBindInput;
	class CopyFromInitGlobalInput;
	class CopyFromInitLocalInput;
	class CopyFromExecInput;
	class CopyFromProgressInput;

	/// Called once per `COPY ... TO` statement while it is bound. Optional.
	using CopyToBindCallback = void (*)(CopyToBindInput &input);
	/// Called after bind, for a `COPY ... TO` statement that sets no `BATCH_SIZE` itself, to report how many rows a
	/// batch should carry; must call `SetTarget`. Optional: without it, a batch is cut for every chunk of rows sunk,
	/// i.e. a vector at a time.
	using CopyToBatchSizeCallback = void (*)(CopyToBatchSizeInput &input);
	/// Called once per output file before its first batch is prepared. Optional.
	using CopyToInitCallback = void (*)(CopyToInitInput &input);
	/// Called to prepare a batch of rows for writing; may run on several threads at once. Required for the side.
	using CopyToBatchCallback = void (*)(CopyToBatchInput &input);
	/// Called to write a prepared batch to the output; never concurrently for the same file. Required for the side.
	using CopyToFlushCallback = void (*)(CopyToFlushInput &input);
	/// Called once per output file after its last batch has been flushed. Optional.
	using CopyToFinalizeCallback = void (*)(CopyToFinalizeInput &input);

	/// Called once per `COPY ... FROM` statement while it is bound. Required for the side.
	using CopyFromBindCallback = void (*)(CopyFromBindInput &input);
	/// Called once per statement at the start of execution. Optional.
	using CopyFromInitGlobalCallback = void (*)(CopyFromInitGlobalInput &input);
	/// Called once per thread reading the file. Optional.
	using CopyFromInitLocalCallback = void (*)(CopyFromInitLocalInput &input);
	/// Called to fill the next batch of rows; leaving the chunk empty ends the read. Required for the side.
	using CopyFromExecCallback = void (*)(CopyFromExecInput &input);
	/// Called on demand during execution to report progress. Optional.
	using CopyFromProgressCallback = void (*)(CopyFromProgressInput &input);

	CopyFunction(CopyFunction &&) noexcept = default;
	CopyFunction &operator=(CopyFunction &&) noexcept = default;

	~CopyFunction() override;

	/// Creates a function that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> CopyFunction;
	/// Creates a function that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> CopyFunction;

	/// Sets the function's name: the format SQL selects it with, as in `COPY ... TO 'path' (FORMAT name)`.
	auto SetName(const std::string &name) & -> CopyFunction &;

	/// Constructs user data of type `T`, carried by the registered function and freed at engine teardown; read it from
	/// any callback via the inputs' `GetUserData<T>`. Consumed by `Register`: set it again before re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> CopyFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetCopyToBindCallback(CopyToBindCallback callback) & -> CopyFunction &;
	auto SetCopyToBatchSizeCallback(CopyToBatchSizeCallback callback) & -> CopyFunction &;
	auto SetCopyToInitCallback(CopyToInitCallback callback) & -> CopyFunction &;
	auto SetCopyToBatchCallback(CopyToBatchCallback callback) & -> CopyFunction &;
	auto SetCopyToFlushCallback(CopyToFlushCallback callback) & -> CopyFunction &;
	auto SetCopyToFinalizeCallback(CopyToFinalizeCallback callback) & -> CopyFunction &;

	auto SetCopyFromBindCallback(CopyFromBindCallback callback) & -> CopyFunction &;
	auto SetCopyFromInitGlobalCallback(CopyFromInitGlobalCallback callback) & -> CopyFunction &;
	auto SetCopyFromInitLocalCallback(CopyFromInitLocalCallback callback) & -> CopyFunction &;
	auto SetCopyFromExecCallback(CopyFromExecCallback callback) & -> CopyFunction &;
	auto SetCopyFromProgressCallback(CopyFromProgressCallback callback) & -> CopyFunction &;

	/// Registers the function in the catalog it was created against. The function object remains valid and may be
	/// adjusted and registered again; user data set via `SetUserData` is consumed by the first `Register`.
	/// @throws InvalidInputException When the name is missing, neither side is configured, a configured
	/// `COPY ... TO` side lacks its batch or flush callback, or a configured `COPY ... FROM` side lacks its bind or
	/// exec callback.
	auto Register() -> void;

private:
	explicit CopyFunction(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	CopyToBindCallback copy_to_bind_callback = nullptr;
	CopyToBatchSizeCallback copy_to_batch_size_callback = nullptr;
	CopyToInitCallback copy_to_init_callback = nullptr;
	CopyToBatchCallback copy_to_batch_callback = nullptr;
	CopyToFlushCallback copy_to_flush_callback = nullptr;
	CopyToFinalizeCallback copy_to_finalize_callback = nullptr;
	CopyFromBindCallback copy_from_bind_callback = nullptr;
	CopyFromInitGlobalCallback copy_from_init_global_callback = nullptr;
	CopyFromInitLocalCallback copy_from_init_local_callback = nullptr;
	CopyFromExecCallback copy_from_exec_callback = nullptr;
	CopyFromProgressCallback copy_from_progress_callback = nullptr;
	detail::UserData user_data;

public:
	/// What the `COPY ... TO` bind callback works with. Borrowed, valid only for the callback duration.
	class CopyToBindInput {
		friend detail::Factory;

	public:
		/// Constructs bind data of type `T`, owned by the bound statement and readable from every later `COPY ... TO`
		/// callback via `GetBindData<T>`. The engine compares bind data when it compares statements: by `operator==`
		/// when `T` has one, by identity otherwise.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::SelectEquals<T>(), detail::TypedDelete<T>);
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The path the statement writes to, as written. The path of each file actually written is only known to the
		/// init callback.
		auto GetFilePath() const -> std::string;

		/// How many columns are being written: the columns of every batch's rows. Valid indices for `GetColumnName`
		/// and `GetColumnType` are [0, GetColumnCount()).
		auto GetColumnCount() const -> idx_t;

		/// One column's name.
		/// @param index Column index in [0, GetColumnCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetColumnName(idx_t index) const -> std::string;

		/// One column's type.
		/// @param index Column index in [0, GetColumnCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetColumnType(idx_t index) const -> LogicalType;

		/// How many options the statement passed to the function, ordered by name. The engine's own options (e.g.
		/// `USE_TMP_FILE`) are not included. Valid indices for the option accessors are [0, GetOptionCount()).
		auto GetOptionCount() const -> idx_t;

		/// One option's name, a SQL identifier matched case-insensitively.
		/// @param index Option index in [0, GetOptionCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetOptionName(idx_t index) const -> std::string;

		/// One option's value: the value itself for `DELIM ','`, the BOOLEAN true for a bare option such as `HEADER`,
		/// and a tuple (an unnamed STRUCT with one field per element, in order) for a parenthesized list.
		/// @param index Option index in [0, GetOptionCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetOptionValue(idx_t index) const -> Value;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToBindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... TO` batch size callback works with. Borrowed, valid only for the callback duration.
	class CopyToBatchSizeInput {
		friend detail::Factory;

	public:
		/// The bind data set via `CopyToBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Reports how many rows a batch should carry, as the target the engine cuts batches at; the callback must
		/// call this. A batch may still be smaller (the last one of a file, or when `BATCH_SIZE_BYTES` cuts it first).
		/// @param rows The number of rows a batch should carry; must be greater than 0.
		auto SetTarget(idx_t rows) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToBatchSizeInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... TO` init callback works with. Borrowed, valid only for the callback duration.
	class CopyToInitInput {
		friend detail::Factory;

	public:
		/// Constructs init data of type `T`, owned by the file being written and readable from the batch, flush and
		/// finalize callbacks of that file via `GetInitData<T>`. Batches may be prepared on several threads at once, so
		/// the batch callback must synchronize its own access to it.
		template <class T, class... ARGS>
		void SetInitData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetInitDataInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `CopyToBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The path of the file being written, after the engine's own rewrites (e.g. a temporary name while the file
		/// is being written, or a per-partition path).
		auto GetFilePath() const -> std::string;

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToInitInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetInitDataInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... TO` batch callback works with. Borrowed, valid only for the callback duration.
	class CopyToBatchInput {
		friend detail::Factory;

	public:
		/// Constructs batch data of type `T`: the prepared form of the batch, handed to the flush callback via
		/// `CopyToFlushInput::GetBatchData<T>` and freed once the batch has been flushed.
		template <class T, class... ARGS>
		void SetBatchData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBatchDataInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `CopyToBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The init data set via `CopyToInitInput::SetInitData` for the file this batch belongs to.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Takes ownership of the rows of the batch: one column per column reported by
		/// `CopyToBindInput::GetColumnCount`, in the same order. The batch can only be taken once; a batch that is
		/// never taken is released when the callback returns. It may be kept beyond the callback, e.g. moved into the
		/// batch data via `SetBatchData`.
		/// @throws InvalidInputException When the batch was already taken.
		auto TakeBatch() -> ColumnDataCollection;

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToBatchInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBatchDataInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... TO` flush callback works with. Borrowed, valid only for the callback duration.
	class CopyToFlushInput {
		friend detail::Factory;

	public:
		/// The bind data set via `CopyToBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The init data set via `CopyToInitInput::SetInitData` for the file this batch belongs to.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		/// The batch data set via `CopyToBatchInput::SetBatchData` for the batch being flushed.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBatchData() const -> T & {
			return *static_cast<T *>(GetBatchDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToFlushInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetBatchDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... TO` finalize callback works with. Borrowed, valid only for the callback duration.
	class CopyToFinalizeInput {
		friend detail::Factory;

	public:
		/// The bind data set via `CopyToBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The init data set via `CopyToInitInput::SetInitData` for the file being finalized.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyToFinalizeInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... FROM` bind callback works with. Borrowed, valid only for the callback duration.
	class CopyFromBindInput {
		friend detail::Factory;

	public:
		/// Constructs bind data of type `T`, owned by the bound statement and readable from every later
		/// `COPY ... FROM` callback via `GetBindData<T>`. The engine compares bind data when it compares statements:
		/// by `operator==` when `T` has one, by identity otherwise.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::SelectEquals<T>(), detail::TypedDelete<T>);
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The path the statement reads from, as written. The engine expands no globs and checks nothing; both are
		/// left to the function.
		auto GetFilePath() const -> std::string;

		/// How many columns the target table expects: the columns every batch the exec callback produces must carry,
		/// in this order. Valid indices for `GetColumnName` and `GetColumnType` are [0, GetColumnCount()).
		auto GetColumnCount() const -> idx_t;

		/// One column's name.
		/// @param index Column index in [0, GetColumnCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetColumnName(idx_t index) const -> std::string;

		/// One column's type.
		/// @param index Column index in [0, GetColumnCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetColumnType(idx_t index) const -> LogicalType;

		/// How many options the statement passed to the function, ordered by name; every option other than `FORMAT`.
		/// Valid indices for the option accessors are [0, GetOptionCount()).
		auto GetOptionCount() const -> idx_t;

		/// One option's name, a SQL identifier matched case-insensitively.
		/// @param index Option index in [0, GetOptionCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetOptionName(idx_t index) const -> std::string;

		/// One option's value: the value itself for `DELIM ','`, the BOOLEAN true for a bare option such as `HEADER`,
		/// and a tuple (an unnamed STRUCT with one field per element, in order) for a parenthesized list.
		/// @param index Option index in [0, GetOptionCount()).
		/// @throws InvalidInputException When the index is out of range.
		auto GetOptionValue(idx_t index) const -> Value;

		/// Hints how many rows the read will produce, for the optimizer. Producing a different number of rows is not
		/// an error.
		/// @param cardinality The estimated row count.
		/// @param is_exact Whether the estimate is exact, which also makes it an upper bound.
		auto SetCardinality(idx_t cardinality, bool is_exact) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyFromBindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... FROM` global init callback works with. Borrowed, valid only for the callback duration.
	class CopyFromInitGlobalInput {
		friend detail::Factory;

	public:
		/// Constructs global state of type `T`, shared by every thread reading the file and readable from the local
		/// init, exec and progress callbacks. Since every thread sees the same object, the function must synchronize
		/// its own access to it.
		template <class T, class... ARGS>
		void SetGlobalState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetGlobalStateInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `CopyFromBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Caps how many threads may read the file in parallel. Defaults to 1, a single-threaded read. The engine
		/// creates at most this many local states, and may use fewer.
		/// @param max_threads The maximum thread count. Must be at least 1.
		auto SetMaxThreads(idx_t max_threads) -> void;

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyFromInitGlobalInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetGlobalStateInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... FROM` local init callback works with. Borrowed, valid only for the callback duration.
	class CopyFromInitLocalInput {
		friend detail::Factory;

	public:
		/// Constructs local state of type `T`, owned by this reading thread and readable from the exec callback via
		/// `CopyFromExecInput::GetLocalState<T>`. No other thread observes it, so it needs no synchronization.
		template <class T, class... ARGS>
		void SetLocalState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetLocalStateInternal(ptr, detail::TypedDelete<T>);
		}

		/// The bind data set via `CopyFromBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `CopyFromInitGlobalInput::SetGlobalState`, typically to claim this thread's share
		/// of the work from it. Shared with every other reading thread; access must be synchronized.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The query context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyFromInitLocalInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetLocalStateInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... FROM` exec callback works with. Borrowed, valid only for the callback duration.
	class CopyFromExecInput {
		friend detail::Factory;

	public:
		/// The bind data set via `CopyFromBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `CopyFromInitGlobalInput::SetGlobalState`. Shared with every other reading thread;
		/// access must be synchronized.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The local state set via `CopyFromInitLocalInput::SetLocalState`, private to this thread.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetLocalState() const -> T & {
			return *static_cast<T *>(GetLocalStateInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The chunk to write the next batch of rows into, with one vector per column reported by
		/// `CopyFromBindInput::GetColumnCount`. It starts out empty on every invocation: write the rows, then give the
		/// batch its row count with `Vector::SetSize` on the chunk's first vector, which the engine propagates to the
		/// others. Leaving it empty ends the read.
		/// @return A borrowed chunk, valid only for the callback duration.
		auto GetOutputChunk() const -> DataChunk;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyFromExecInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetLocalStateInternal() const;
		void *GetUserDataInternal() const;
	};

	/// What the `COPY ... FROM` progress callback works with. Borrowed, valid only for the callback duration.
	class CopyFromProgressInput {
		friend detail::Factory;

	public:
		/// The bind data set via `CopyFromBindInput::SetBindData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		/// The global state set via `CopyFromInitGlobalInput::SetGlobalState`. This callback runs while the read is
		/// running, so the state must be read in a thread-safe way.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetGlobalState() const -> T & {
			return *static_cast<T *>(GetGlobalStateInternal());
		}

		/// The user data set via `CopyFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// Reports how far the read has advanced, as a fraction between 0.0 and 1.0; values outside that range are
		/// clamped. A callback that returns without calling this reports no progress.
		/// @param progress The fraction of the read that is complete.
		auto SetProgress(double progress) -> void;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		CopyFromProgressInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetGlobalStateInternal() const;
		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Cast Function
//----------------------------------------------------------------------------------------------------------------------

/// The mode a cast runs in, which decides what a conversion failure means.
enum class CastMode : uint8_t {
	/// A regular cast. Throwing from the exec callback aborts the query.
	NORMAL = 0,
	/// A "try" cast (SQL TRY_CAST). The rows the callback could not convert should be left NULL in the output; an
	/// exception thrown anyway is swallowed and those rows stay NULL.
	TRY = 1,
};

/// A user-defined cast between two types, built up with the setters and made live with `Register`.
/// Create one against the `Connection` or `Extension` it will be registered in, describe it (source type, target
/// type, exec callback), then call `Register`. The function object may be destroyed after registration; the
/// registered cast lives on.
///
/// A cast is keyed by its (source, target) type pair rather than by a name, and is reached from SQL through CAST and
/// TRY_CAST -- and, when it declares a non-negative implicit cast cost, through the binder converting argument types
/// on its own. The exec callback converts a whole batch at a time; whether a per-row failure aborts the query or
/// becomes a NULL follows from `ExecInput::GetMode`.
class CastFunction final : public detail::Handle<CastFunction> {
	friend detail::Factory;

public:
	class ExecInput;

	/// Called for every batch of values; must fill the output vector. Required.
	using ExecCallback = void (*)(ExecInput &input);

	CastFunction(CastFunction &&) noexcept = default;
	CastFunction &operator=(CastFunction &&) noexcept = default;

	~CastFunction() override;

	/// Creates a cast that `Register` adds to the connection's database.
	static auto Create(const Connection &conn) -> CastFunction;
	/// Creates a cast that `Register` adds through the loading extension.
	static auto Create(const Extension &extension) -> CastFunction;

	/// Sets the type the cast converts from. Must be a fully defined concrete type.
	auto SetSourceType(const LogicalType &type) & -> CastFunction &;

	/// Sets the type the cast converts to. Must be a fully defined concrete type.
	auto SetTargetType(const LogicalType &type) & -> CastFunction &;

	/// Sets what it costs to apply this cast implicitly. The binder uses the cost to choose between candidate
	/// implicit casts: a lower non-negative cost makes this cast more likely to be picked. Built-in widening casts
	/// sit in the [0, 20] range. A negative cost -- the default -- keeps the cast out of implicit conversion
	/// entirely, so it is reached only through an explicit CAST or TRY_CAST.
	/// @param cost The cost, or a negative value to disable implicit casting.
	auto SetImplicitCastCost(int64_t cost) & -> CastFunction &;

	/// Constructs user data of type `T`, carried by the registered cast and freed at engine teardown; read it from the
	/// exec callback via `ExecInput::GetUserData<T>`. Consumed by `Register`: set it again before re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> CastFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetExecCallback(ExecCallback callback) & -> CastFunction &;

	/// Registers the cast in the database it was created against, replacing whatever cast was registered for the same
	/// type pair. The function object remains valid and may be adjusted and registered again; user data set via
	/// `SetUserData` is consumed by the first `Register`.
	/// @throws InvalidInputException When the source type, target type, or exec callback is missing, or either type is
	/// not concrete.
	auto Register() -> void;

private:
	explicit CastFunction(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	ExecCallback exec_callback = nullptr;
	detail::UserData user_data;

public:
	/// What the exec callback works with. Borrowed, valid only for the callback duration.
	class ExecInput {
		friend detail::Factory;

	public:
		/// The user data set via `CastFunction::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// How many rows this execution must convert: the callback writes exactly this many entries to the output
		/// vector. May be less than a full vector; a constant input is converted as a single row and the engine
		/// expands the result.
		auto GetRowCount() const -> idx_t;

		/// The input vector holding the values to convert.
		auto GetInput() const -> Vector;

		/// The output vector to fill.
		auto GetOutput() const -> Vector;

		/// The mode this cast runs in. In `CastMode::TRY` a failed row should be left NULL rather than reported by
		/// throwing.
		auto GetMode() const -> CastMode;

		/// The execution context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		ExecInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// File System
//----------------------------------------------------------------------------------------------------------------------

/// How `FileSystem::OpenFile` opens a file. Not a bitmask -- apply these one at a time with
/// `FileOpenOptions::SetFlag`, or pass several as a braced list to `FileSystem::OpenFile`.
enum class FileFlags : uint8_t {
	/// Not a flag. The zero value, so an uninitialized variable does not name a behaviour; applying it is an error.
	INVALID = 0,
	/// Open the file with "read" capabilities.
	READ = 1,
	/// Open the file with "write" capabilities.
	WRITE = 2,
	/// Create the file if it does not exist, and open it as it is if it does.
	/// The FILE_ prefix matches the engine's flag names and keeps CREATE clear of the `<windows.h>` macro.
	FILE_CREATE = 3,
	/// Create the file if it does not exist, and truncate it to empty if it does. To fail instead of truncating,
	/// combine `FILE_CREATE` with `EXCLUSIVE_CREATE`.
	FILE_CREATE_NEW = 4,
	/// Open the file in "append" mode.
	APPEND = 5,
	/// Fail if the file already exists. A modifier on `FILE_CREATE`, and meaningless without it.
	EXCLUSIVE_CREATE = 6,
	/// The file will be read and written at explicit offsets from several threads at once. Pass it whenever `ReadAt`
	/// or `WriteAt` are used concurrently, so that a file system which would otherwise assume sequential access does
	/// not.
	PARALLEL_ACCESS = 7,
};

/// An open file, obtained from `FileSystem::OpenFile`. Closes on destruction.
/// Only usable while the `FileSystem` it came from is still valid.
class FileHandle final : public detail::Handle<FileHandle> {
	friend detail::Factory;

public:
	FileHandle(FileHandle &&) noexcept = default;
	FileHandle &operator=(FileHandle &&) noexcept = default;

	~FileHandle() override;

	/// Flushes buffered writes to persistent storage, which is what makes them durable across a crash. Closing or
	/// destroying the handle flushes as well.
	void Sync();

	/// Closes the file, releasing its operating-system resources. The handle stays valid but can no longer be used
	/// to read, write or seek.
	void Close();

	/// Moves the read/write position to an absolute byte offset from the start of the file. Seeking past the end is
	/// allowed; reading from there yields nothing.
	void Seek(idx_t position);

	/// The current read/write position, as a byte offset from the start of the file.
	auto Tell() const -> idx_t;

	/// The total size of the file in bytes.
	auto Size() const -> idx_t;

	/// Reads up to `size` bytes from the current position, advancing it by however many were read.
	/// @return How many bytes were read. Fewer than asked for is normal at the end of the file, and zero means
	/// there is nothing left; neither is an error.
	auto Read(void *buffer, idx_t size) -> idx_t;

	/// Writes up to `size` bytes at the current position, advancing it by however many were written.
	/// @return How many bytes were written.
	auto Write(const void *buffer, idx_t size) -> idx_t;

	/// Reads exactly `size` bytes from `location`, leaving the file's position alone. Unlike `Read`, a short read is
	/// an error rather than a result, so there is no count to return. Safe to call from several threads at once when
	/// the file was opened with `FileFlags::PARALLEL_ACCESS`.
	/// @throws Exception When the file ends before `size` bytes have been read.
	void ReadAt(void *buffer, idx_t size, idx_t location);

	/// Writes exactly `size` bytes at `location`, leaving the file's position alone and extending the file when the
	/// offset is past its end. Safe to call from several threads at once when the file was opened with
	/// `FileFlags::PARALLEL_ACCESS` and the threads write disjoint ranges.
	void WriteAt(const void *buffer, idx_t size, idx_t location);

private:
	explicit FileHandle(void *impl);
};

/// How a file is opened: the flags, plus any values the file system handling the path cares about.
/// Created from the `FileSystem` it will be used with, and reusable across any number of opens.
class FileOpenOptions final : public detail::Handle<FileOpenOptions> {
	friend detail::Factory;

public:
	FileOpenOptions(FileOpenOptions &&) noexcept = default;
	FileOpenOptions &operator=(FileOpenOptions &&) noexcept = default;

	~FileOpenOptions() override;

	/// Creates an empty set of options for `fs`. Flags must be set before they can open anything.
	static auto Create(const FileSystem &fs) -> FileOpenOptions;

	/// Applies one flag. Additive, and applying the same flag twice is harmless; at least one flag is required
	/// before the options can open anything. There is no way to take a flag back -- build a fresh set instead.
	/// @throws InvalidInputException When the value is `FileFlags::INVALID` or not a flag at all.
	auto SetFlag(FileFlags flag) & -> FileOpenOptions &;

	/// Attaches a named value, a hint for whichever file system ends up handling the path. What a name means is that
	/// file system's business, and one it does not recognize is ignored. Setting the same name again replaces it.
	auto SetValue(std::string_view name, const Value &value) & -> FileOpenOptions &;

private:
	explicit FileOpenOptions(void *impl);
};

/// The file system DuckDB itself reads and writes through, so files open the way the engine would open them --
/// including through virtual and remote file systems registered by other extensions.
/// Borrowed from a `Context` or `Connection`, and valid only for as long as that is.
class FileSystem final : public detail::Handle<FileSystem> {
	friend detail::Factory;

public:
	FileSystem(FileSystem &&) noexcept = default;
	FileSystem &operator=(FileSystem &&) noexcept = default;

	~FileSystem() override;

	/// Opens a file with nothing but flags, which is what most opens need.
	/// @param path The path to open, routed the way the engine would route it.
	/// @param flags How to open it, e.g. `{FileFlags::WRITE, FileFlags::FILE_CREATE}`; at least one is required.
	/// @throws Exception When the file cannot be opened.
	auto OpenFile(const std::string &path, std::initializer_list<FileFlags> flags) const -> FileHandle;

	/// Opens a file with a prepared set of options, for when the file system needs values as well as flags.
	/// @throws Exception When the file cannot be opened, or the options carry no flags.
	auto OpenFile(const std::string &path, const FileOpenOptions &options) const -> FileHandle;

	/// Creates an empty set of options for this file system, the same as `FileOpenOptions::Create(*this)`.
	auto CreateOpenOptions() const -> FileOpenOptions;

private:
	explicit FileSystem(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Qualified Name
//----------------------------------------------------------------------------------------------------------------------

/// The ordered path of identifiers that names a database object.
/// The path is the whole representation: partial qualification is expressed by having fewer parts (`{"t"}`,
/// `{"s", "t"}`, `{"c", "s", "t"}`), never by empty placeholders, and the last part is always the object itself.
/// Whether a two-part name means catalog.object or schema.object is decided by resolution, not by the name.
///
/// Owned, so a name obtained from somewhere transient -- a replacement scan callback, say -- can be kept for as long
/// as you like.
class QualifiedName final : public detail::Handle<QualifiedName> {
	friend detail::Factory;

public:
	QualifiedName(QualifiedName &&) noexcept = default;
	QualifiedName &operator=(QualifiedName &&) noexcept = default;

	~QualifiedName() override;

	/// Parses SQL text into a qualified name: dots separate parts, and a double-quoted part may contain dots and
	/// doubled interior quotes.
	/// @throws Exception When the text does not parse, or yields no parts or more than three.
	static auto Parse(std::string_view text) -> QualifiedName;

	/// Builds a name from its parts, outermost first, so the last one is the object name.
	/// @param parts Between one and three non-empty parts.
	/// @throws InvalidInputException When there are no parts, more than three, or any is empty.
	static auto Create(const std::vector<std::string> &parts) -> QualifiedName;

	/// How many parts the name has; always at least one.
	auto GetPartCount() const -> idx_t;

	/// One part, outermost first. The view is valid until this name is destroyed.
	/// @param index Part index in [0, GetPartCount()).
	/// @throws Exception When the index is out of range.
	auto GetPart(idx_t index) const -> std::string_view;

	/// The object name: the last part. The view is valid until this name is destroyed.
	auto GetName() const -> std::string_view;

	/// The name as SQL text, quoting each part only where the identifier requires it, so it parses back to an equal
	/// name.
	auto Render() const -> std::string;

	/// Whether two names have the same parts, compared case-insensitively the way the engine compares identifiers.
	auto Equals(const QualifiedName &other) const -> bool;

	/// A hash consistent with `Equals`. Not stable across processes or versions: for in-process lookup only.
	auto Hash() const -> uint64_t;

private:
	explicit QualifiedName(void *impl);
};

inline bool operator==(const QualifiedName &lhs, const QualifiedName &rhs) {
	return lhs.Equals(rhs);
}
inline bool operator!=(const QualifiedName &lhs, const QualifiedName &rhs) {
	return !lhs.Equals(rhs);
}

//----------------------------------------------------------------------------------------------------------------------
// Table Description
//----------------------------------------------------------------------------------------------------------------------

/// An owned snapshot of one base table, taken by `Connection::DescribeTable`: where the name resolved, the table's
/// columns in declared order (generated columns included), and per-column catalog facts. Later DDL does not update it.
class TableDescription final : public detail::Handle<TableDescription> {
	friend detail::Factory;

public:
	TableDescription(TableDescription &&) noexcept = default;
	TableDescription &operator=(TableDescription &&) noexcept = default;

	~TableDescription() override;

	/// The fully resolved name: the catalog, schema and table the lookup landed on, with the casing the table was
	/// created with.
	auto GetQualifiedName() const -> QualifiedName;

	/// How many columns the table has, generated columns included.
	auto GetColumnCount() const -> idx_t;

	/// An owned description of one column, independent of this table description.
	/// @param index Column index in [0, GetColumnCount()).
	/// @throws Exception When the index is out of range.
	auto GetColumn(idx_t index) const -> ColumnDescription;

	/// Whether the catalog the table lives in was attached read-only.
	auto IsReadOnly() const -> bool;

private:
	explicit TableDescription(void *impl);
};

/// An owned snapshot of one column of a described table, from `TableDescription::GetColumn`: its name, type and
/// catalog facts. Independent of the table description it came from.
class ColumnDescription final : public detail::Handle<ColumnDescription> {
	friend detail::Factory;

public:
	ColumnDescription(ColumnDescription &&) noexcept = default;
	ColumnDescription &operator=(ColumnDescription &&) noexcept = default;

	~ColumnDescription() override;

	/// The column name, with the casing it was declared with.
	/// @return A view borrowed from this description, valid until it is destroyed.
	auto GetName() const -> std::string_view;

	/// An owned copy of the column type.
	auto GetType() const -> LogicalType;

	/// Whether the column declares a default expression. Generated columns report false.
	auto HasDefault() const -> bool;

	/// Whether the column is generated: computed by the engine and not writable.
	auto HasGenerated() const -> bool;

private:
	explicit ColumnDescription(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Replacement Scan
//----------------------------------------------------------------------------------------------------------------------

/// A user-defined replacement scan, built up with the setters and made live with `Register`.
/// Create one against the `Connection`, `Database` or `Extension` it will be registered on, set its callback and
/// user data, then call `Register`. The scan object may be destroyed after registration; the registered scan lives
/// on until its scope ends.
///
/// The binder consults replacement scans when a table name cannot be resolved in the catalog; this is what makes
/// `SELECT * FROM 'file.parquet'` work. The callback inspects the unresolved name and either claims it, by naming a
/// table function to call, a `ColumnDataCollection` to read or a query to run instead, or declines it by claiming
/// nothing, which lets the next registered scan try. When no scan claims the name, the usual "table does not exist"
/// error is raised. A callback reports failure by throwing; the exception surfaces as the query's error.
///
/// Scope follows the constructor. A scan created against a `Connection` is visible only to that connection, is
/// released when it closes, and is consulted before every database-wide scan, including the built-in file scans. A
/// scan created against a `Database` or `Extension` is visible to every connection to that database and lives until
/// it closes; registering one is not thread-safe against queries binding on other connections, so do it during
/// extension load or before issuing queries. A registered scan cannot be unregistered.
class ReplacementScan final : public detail::Handle<ReplacementScan> {
	friend detail::Factory;

public:
	class Input;

	/// Called once per table reference the catalog could not resolve; claims it or declines it. Required.
	using Callback = void (*)(Input &input);

	ReplacementScan(ReplacementScan &&) noexcept = default;
	ReplacementScan &operator=(ReplacementScan &&) noexcept = default;

	~ReplacementScan() override;

	/// Creates a scan that `Register` adds to the connection, visible only there.
	static auto Create(const Connection &conn) -> ReplacementScan;
	/// Creates a scan that `Register` adds to the database, visible to every connection.
	static auto Create(const Database &db) -> ReplacementScan;
	/// Creates a scan that `Register` adds through the loading extension, visible to every connection.
	static auto Create(const Extension &extension) -> ReplacementScan;

	auto SetCallback(Callback callback) & -> ReplacementScan &;

	/// Constructs user data of type `T`, carried by the registered scan and freed when its scope ends; read it from
	/// the callback via `Input::GetUserData<T>`. Consumed by `Register`.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> ReplacementScan & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	/// Registers the scan on the target it was created against. Scans are consulted in registration order within
	/// their scope, connection-scoped ones before database-wide ones, and the first to claim a name wins. A scan can
	/// be registered only once.
	/// @throws InvalidInputException When the callback is missing, or the scan is already registered.
	auto Register() -> void;

private:
	explicit ReplacementScan(void *impl);

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

	Callback callback = nullptr;
	detail::UserData user_data;

public:
	/// What the callback works with. Borrowed, valid only for the callback duration, as are the name views it hands
	/// out.
	class Input {
		friend detail::Factory;

	public:
		/// The user data set via `ReplacementScan::SetUserData`.
		/// @throws InvalidInputException When none was set.
		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(GetUserDataInternal());
		}

		/// The name the catalog could not resolve, as written in the query. An unqualified reference has a single
		/// part; for a file-backed one that part is the path. Owned, so it can outlive the callback.
		auto GetName() const -> QualifiedName;

		/// Claims the reference by naming a table function to call instead; its arguments are then supplied with
		/// `AddArgument` and `AddNamedArgument`. Parts are matched case-insensitively, and a qualified name targets a
		/// function in a particular schema or catalog. The name is not resolved here: an unknown function fails
		/// later, when the replacement is bound. The three claim forms, `SetFunctionName`, `SetCollection` and
		/// `SetSubquery`, are mutually exclusive.
		/// @throws InvalidInputException When a different claim form was already used.
		auto SetFunctionName(const QualifiedName &name) -> void;

		/// `SetFunctionName` for the common case of an unqualified function.
		auto SetFunctionName(std::string_view name) -> void;

		/// Appends a positional argument to the claimed table function. Positional arguments are passed in the order
		/// they are added, before any named ones.
		/// @throws InvalidInputException Unless `SetFunctionName` was called first.
		auto AddArgument(const Value &value) -> void;

		/// Appends a named argument to the claimed table function, the equivalent of `name := value`.
		/// @throws InvalidInputException Unless `SetFunctionName` was called first.
		auto AddNamedArgument(std::string_view name, const Value &value) -> void;

		/// Claims the reference by naming a collection to read instead. The collection is borrowed: it must stay
		/// alive, and must not be cleared, reset or destroyed, for as long as any result reading it is live. A
		/// `PreparedStatement` over a claimed name extends that further: it captures the borrow in its plan and,
		/// since a collection claim reads no database, reuses that plan without consulting the callback again
		/// (`PreparedStatement::ReusesPlan` reports true). Destroy such statements before releasing the collection.
		/// The three claim forms, `SetFunctionName`, `SetCollection` and `SetSubquery`, are mutually exclusive.
		/// @param collection The collection to read.
		/// @param column_names Names for its columns, in order; empty names them col1..colN.
		/// @throws InvalidInputException When the names do not match the collection's columns, or a different claim
		/// form was already used.
		auto SetCollection(const ColumnDataCollection &collection, const std::vector<std::string> &column_names = {})
		    -> void;

		/// Claims the reference by naming a query to run instead. The text is parsed here and must contain exactly
		/// one SELECT statement. The three claim forms, `SetFunctionName`, `SetCollection` and `SetSubquery`, are
		/// mutually exclusive.
		/// @throws Exception When the text does not parse, or is not a single SELECT.
		/// @throws InvalidInputException When a different claim form was already used.
		auto SetSubquery(std::string_view sql) -> void;

		/// Sets the alias the claimed replacement is bound under. Optional: an alias written in the query takes
		/// precedence, and without either the reference's own table name is used.
		auto SetAlias(std::string_view alias) -> void;

		/// The binding context. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;

	private:
		Input(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Scalar Executor
//----------------------------------------------------------------------------------------------------------------------

namespace detail {

/// @internal
/// Names `Vector` once per element of a type pack, to declare one owned child vector per tuple element.
template <class T>
using VectorPerField = Vector;

/// @internal
/// Reads one argument row as a `T`. The primary template covers the primitive types; the specializations add the
/// composed forms: `std::reference_wrapper<const T>` reads in place, `std::optional<T>` absorbs NULLs, and
/// `std::tuple<Ts...>` reads a STRUCT/TUPLE vector's fields through its child vectors, recursively.
/// `IsValid` reports whether a row can produce a value: false when a non-optional part of it is NULL.
template <class T>
struct VectorReader {
	VectorView view;

	explicit VectorReader(const Vector &vec) : view(vec.GetView()) {
	}
	bool AllValid() const {
		return view.AllValid();
	}
	bool IsValid(idx_t row) const {
		return view.IsValid(row);
	}
	T Get(idx_t row) const {
		return view.Data<T>()[view.SelAt(row)];
	}
};

template <class T>
struct VectorReader<std::reference_wrapper<const T>> {
	VectorView view;

	explicit VectorReader(const Vector &vec) : view(vec.GetView()) {
	}
	bool AllValid() const {
		return view.AllValid();
	}
	bool IsValid(idx_t row) const {
		return view.IsValid(row);
	}
	std::reference_wrapper<const T> Get(idx_t row) const {
		return std::cref(view.Data<T>()[view.SelAt(row)]);
	}
};

template <class T>
struct VectorReader<std::optional<T>> {
	VectorReader<T> inner;

	explicit VectorReader(const Vector &vec) : inner(vec) {
	}
	bool AllValid() const {
		return inner.AllValid();
	}
	// A NULL is a value here (nullopt), so the row always participates.
	bool IsValid(idx_t) const {
		return true;
	}
	std::optional<T> Get(idx_t row) const {
		if (!inner.IsValid(row)) {
			return std::nullopt;
		}
		return inner.Get(row);
	}
};

template <class... Ts>
struct VectorReader<std::tuple<Ts...>> {
	VectorView view;
	std::tuple<VectorReader<Ts>...> children;

	explicit VectorReader(const Vector &vec)
	    : view(vec.GetView()), children(MakeChildren(vec, std::index_sequence_for<Ts...> {})) {
	}
	bool AllValid() const {
		return AllValidImpl(std::index_sequence_for<Ts...> {});
	}
	bool IsValid(idx_t row) const {
		// The row's struct entry must be non-NULL, and so must every non-optional field of it. The children index
		// by entry: their rows align with this vector's elements, so the selection applies once, here.
		return view.IsValid(row) && IsValidImpl(view.SelAt(row), std::index_sequence_for<Ts...> {});
	}
	std::tuple<Ts...> Get(idx_t row) const {
		return GetImpl(view.SelAt(row), std::index_sequence_for<Ts...> {});
	}

private:
	template <size_t... Is>
	static std::tuple<VectorReader<Ts>...> MakeChildren(const Vector &vec, std::index_sequence<Is...>) {
		return std::tuple<VectorReader<Ts>...> {VectorReader<Ts>(vec.GetChild(Is))...};
	}
	template <size_t... Is>
	bool AllValidImpl(std::index_sequence<Is...>) const {
		return view.AllValid() && (... && std::get<Is>(children).AllValid());
	}
	template <size_t... Is>
	bool IsValidImpl(idx_t entry, std::index_sequence<Is...>) const {
		return (... && std::get<Is>(children).IsValid(entry));
	}
	template <size_t... Is>
	std::tuple<Ts...> GetImpl(idx_t entry, std::index_sequence<Is...>) const {
		return std::tuple<Ts...> {std::get<Is>(children).Get(entry)...};
	}
};

/// @internal
/// Writes one result row as a `T` into a FLAT vector, mirroring `VectorReader`'s type forms: primitives write the
/// element, `std::optional<T>` turns nullopt into a NULL row, and `std::tuple<Ts...>` writes a STRUCT/TUPLE vector's
/// fields through its child vectors, recursively. NULLs go through the validity mask directly (fetched on first use, so
/// the all-valid path allocates no mask): the result's size is set by the engine only after the callback, so the
/// size-checked `Vector::SetNull` is not usable here. `ResetValidity` readies the masks before a loop that may write
/// NULLs; nulling a tuple row nulls its fields too, keeping the engine's nested-NULL invariant.
template <class T>
struct VectorWriter {
	Vector &vec;
	T *data;
	ValidityMask mask {nullptr};

	explicit VectorWriter(Vector &vec) : vec(vec), data(vec.GetDataMutable<T>()) {
	}
	void ResetValidity(idx_t count) {
		Mask().SetAllValid(count);
	}
	void Set(idx_t row, const T &value) {
		data[row] = value;
	}
	void SetNull(idx_t row) {
		Mask().SetInvalid(row);
	}

private:
	ValidityMask &Mask() {
		if (!mask.words) {
			mask = vec.GetValidityMutable();
		}
		return mask;
	}
};

template <class T>
struct VectorWriter<std::optional<T>> {
	VectorWriter<T> inner;

	explicit VectorWriter(Vector &vec) : inner(vec) {
	}
	void ResetValidity(idx_t count) {
		inner.ResetValidity(count);
	}
	void Set(idx_t row, const std::optional<T> &value) {
		if (value) {
			inner.Set(row, *value);
		} else {
			inner.SetNull(row);
		}
	}
	void SetNull(idx_t row) {
		inner.SetNull(row);
	}
};

template <class... Ts>
struct VectorWriter<std::tuple<Ts...>> {
	Vector &vec;
	std::tuple<VectorPerField<Ts>...> child_vectors;
	std::tuple<VectorWriter<Ts>...> children;
	ValidityMask mask {nullptr};

	explicit VectorWriter(Vector &vec)
	    : vec(vec), child_vectors(MakeChildVectors(vec, std::index_sequence_for<Ts...> {})),
	      children(MakeChildWriters(child_vectors, std::index_sequence_for<Ts...> {})) {
	}
	void ResetValidity(idx_t count) {
		Mask().SetAllValid(count);
		ResetValidityImpl(count, std::index_sequence_for<Ts...> {});
	}
	void Set(idx_t row, const std::tuple<Ts...> &value) {
		SetImpl(row, value, std::index_sequence_for<Ts...> {});
	}
	void SetNull(idx_t row) {
		// A NULL struct entry requires NULL fields, so the row nulls recursively.
		Mask().SetInvalid(row);
		SetNullImpl(row, std::index_sequence_for<Ts...> {});
	}

private:
	template <size_t... Is>
	static std::tuple<VectorPerField<Ts>...> MakeChildVectors(Vector &vec, std::index_sequence<Is...>) {
		return std::tuple<VectorPerField<Ts>...> {vec.GetChild(Is)...};
	}
	template <size_t... Is>
	static std::tuple<VectorWriter<Ts>...> MakeChildWriters(std::tuple<VectorPerField<Ts>...> &vectors,
	                                                        std::index_sequence<Is...>) {
		return std::tuple<VectorWriter<Ts>...> {VectorWriter<Ts>(std::get<Is>(vectors))...};
	}
	template <size_t... Is>
	void ResetValidityImpl(idx_t count, std::index_sequence<Is...>) {
		(std::get<Is>(children).ResetValidity(count), ...);
	}
	template <size_t... Is>
	void SetImpl(idx_t row, const std::tuple<Ts...> &value, std::index_sequence<Is...>) {
		(std::get<Is>(children).Set(row, std::get<Is>(value)), ...);
	}
	template <size_t... Is>
	void SetNullImpl(idx_t row, std::index_sequence<Is...>) {
		(std::get<Is>(children).SetNull(row), ...);
	}
	ValidityMask &Mask() {
		if (!mask.words) {
			mask = vec.GetValidityMutable();
		}
		return mask;
	}
};

} // namespace detail

/// Row-at-a-time execution helper for exec callbacks: give it the value types and a callable and it handles the
/// vector plumbing -- argument layout (selection vectors, constants), NULL propagation, and writing the result. For a
/// function this covers, the whole exec callback is one call:
///
///     void AddExec(ScalarFunction::ExecInput &input) {
///         ScalarExecutor::Execute<int64_t, int64_t, int64_t>(input, [](int64_t a, int64_t b) { return a + b; });
///     }
///
/// The type list is the result type followed by one type per argument; a nullary function passes just the result
/// type. Each type is composed from these forms:
///
/// - A primitive type (`VectorView::Data`'s element types) passes or returns the value itself.
/// - `std::reference_wrapper<const T>` (arguments only) passes a reference into the vector instead of a copy.
/// - `std::optional<T>` makes NULL part of the value: an argument arrives as nullopt when its row is NULL instead of
///   nulling the row, and a result of nullopt makes the row NULL. Meaningful for arguments only when the function
///   uses special NULL handling; under the default the engine assumes NULL in, NULL out.
/// - `std::tuple<Ts...>` maps a STRUCT/TUPLE vector to its fields, read from and written to the child vectors; the
///   forms nest, so a field can itself be optional, a reference, or a tuple.
///
/// Rows follow SQL's default NULL handling: a row where any non-optional part of any argument is NULL yields NULL,
/// and the callable is not invoked for it. Functions over strings or other unrepresented types drive the vectors
/// directly instead.
class ScalarExecutor {
public:
	/// Fills the exec callback's result vector by invoking `fun` once per participating row.
	/// @tparam RESULT The result type; `fun` must return it (or something convertible).
	/// @tparam ARGS One type per argument, in signature order; `fun` is invoked with one value of each.
	/// @param input The exec callback's input.
	/// @param fun The callable computing one row: `RESULT(ARGS...)`.
	/// @throws InvalidInputException When the call carries a different number of arguments than `ARGS` lists.
	template <class RESULT, class... ARGS, class FUN>
	static void Execute(ScalarFunction::ExecInput &input, FUN fun) {
		if (input.GetArgCount() != sizeof...(ARGS)) {
			throw InvalidInputException("ScalarExecutor::Execute: the call carries " +
			                            std::to_string(input.GetArgCount()) + " arguments but the type list names " +
			                            std::to_string(sizeof...(ARGS)));
		}
		auto result = input.GetResult();
		ExecuteImpl<RESULT, ARGS...>(input, result, input.GetRowCount(), fun, std::index_sequence_for<ARGS...> {});
	}

	/// The vector-level form, for vectors that come from somewhere other than an exec callback (e.g. a `DataChunk`):
	/// one vector parameter per type in `ARGS`, so passing the wrong number of vectors is a compile error.
	/// @tparam RESULT The result type; `fun` must return it (or something convertible).
	/// @tparam ARGS One type per argument vector, in order; `fun` is invoked with one value of each.
	/// @param args One vector per argument, each covering at least `count` rows.
	/// @param result The FLAT vector to fill; must have room for `count` rows.
	/// @param count How many rows to produce.
	/// @param fun The callable computing one row: `RESULT(ARGS...)`.
	template <class RESULT, class... ARGS, class FUN>
	static void Execute(detail::VectorPerArg<ARGS>... args, Vector &result, idx_t count, FUN fun) {
		std::tuple<detail::VectorReader<ARGS>...> readers {detail::VectorReader<ARGS>(args)...};
		detail::VectorWriter<RESULT> writer(result);
		Run(readers, writer, count, fun, std::index_sequence_for<ARGS...> {});
	}

private:
	template <class RESULT, class... ARGS, class FUN, size_t... Is>
	static void ExecuteImpl(ScalarFunction::ExecInput &input, Vector &result, idx_t count, FUN &fun,
	                        std::index_sequence<Is...> seq) {
		std::tuple<detail::VectorReader<ARGS>...> readers {detail::VectorReader<ARGS>(input.GetArg(Is))...};
		detail::VectorWriter<RESULT> writer(result);
		Run(readers, writer, count, fun, seq);
	}

	template <class RESULT, class... ARGS, class FUN, size_t... Is>
	static void Run(const std::tuple<detail::VectorReader<ARGS>...> &readers, detail::VectorWriter<RESULT> &writer,
	                idx_t count, FUN &fun, std::index_sequence<Is...>) {
		if ((... && std::get<Is>(readers).AllValid())) {
			for (idx_t i = 0; i < count; i++) {
				writer.Set(i, fun(std::get<Is>(readers).Get(i)...));
			}
			return;
		}

		// Some argument row is NULL: propagate row by row through the result's mask.
		writer.ResetValidity(count);
		for (idx_t i = 0; i < count; i++) {
			if ((... && std::get<Is>(readers).IsValid(i))) {
				writer.Set(i, fun(std::get<Is>(readers).Get(i)...));
			} else {
				writer.SetNull(i);
			}
		}
	}
};

inline auto Context::CreateType() -> TypeBuilder<Context> {
	return TypeBuilder(*this);
}

inline auto Connection::CreateType() -> TypeBuilder<Connection> {
	return TypeBuilder(*this);
}

inline auto Connection::Execute(const SqlStatement &statement, const std::vector<Value> &parameters) -> QueryResult {
	return Execute(statement, parameters.data(), parameters.size());
}

inline auto PreparedStatement::Execute(const std::vector<Value> &parameters) -> QueryResult {
	return Execute(parameters.data(), parameters.size());
}

} // namespace cxx
} // namespace duckdb
