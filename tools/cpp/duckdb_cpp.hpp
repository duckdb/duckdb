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
#include <optional>
#include <tuple>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <limits>

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

struct TypeParam;
struct NamedParam;

enum class LogicalTypeId : uint32_t;

template <class CTX>
class TypeBuilder;

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
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params = {}) const -> LogicalType;

	/// The id-keyed twin of `CreateType`: the id resolves to its canonical name and binds like it.
	/// @param id The type's id. Without parameters, only ids that name a complete type on their own are accepted;
	/// parameterized kinds such as LIST or DECIMAL require parameters.
	/// @param params The type's parameters, as in the name-keyed overload.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params = {}) const -> LogicalType;

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

	/// `Context::ParseType` outside a callback.
	/// @param text A type as SQL spells it, e.g. "DECIMAL(18, 3)" or "STRUCT(a INTEGER, b VARCHAR)".
	auto ParseType(std::string_view text) -> LogicalType;

	/// `Context::CreateType` outside a callback.
	/// @param name The type's unqualified name, e.g. "LIST" or "DECIMAL".
	/// @param params The type's parameters, in the order SQL takes them. A `TypeParam` with an empty name is
	/// positional.
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params = {}) -> LogicalType;

	/// The id-keyed twin of `CreateType`: the id resolves to its canonical name and binds like it.
	/// @param id The type's id. Without parameters, only ids that name a complete type on their own are accepted;
	/// parameterized kinds such as LIST or DECIMAL require parameters.
	/// @param params The type's parameters, as in the name-keyed overload.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params = {}) -> LogicalType;

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
	/// again. Outstanding append and scan states are invalidated: create new ones.
	auto Reset() -> void;

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

} // namespace cxx
} // namespace duckdb
