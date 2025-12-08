//
//  DuckDB
//  https://github.com/duckdb/duckdb-swift
//
//  Copyright © 2018-2024 Stichting DuckDB Foundation
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to
//  deal in the Software without restriction, including without limitation the
//  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
//  sell copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
//  IN THE SOFTWARE.

#if compiler(>=6.0)
internal import Cduckdb
#else
@_implementationOnly import Cduckdb
#endif
import Foundation

/// An object representing a DuckDB prepared statement
///
/// A prepared statement is a parameterized query. The query is prepared with
/// question marks (`?`) or dollar symbols (`$1`) indicating the parameters of
/// the query. Values can then be bound to these parameters, after which the
/// prepared statement can be executed using those parameters. A single query
/// can be prepared once and executed many times.
///
/// Prepared statements are useful to:
///
///   - Easily supply parameters to functions while avoiding string
///     concatenation/SQL injection attacks.
///   - Speed up queries that will be executed many times with different
///     parameters.
///
/// The following example creates a prepared statement that allows parameters
/// to be bound in two positions within a 'select' statement. The prepared
/// statement is finally executed by calling ``PreparedStatement/execute()``.
///
/// ```swift
///   let connection: Connection = ...
///   let statement = try PreparedStatement(
///     connection: connection,
///     query: "SELECT $1 from $2"
///   )
///   try statement.bind("last_name")
///   try statement.bind("employees")
///   // executes 'SELECT last_name from employees'
///   let result = try statement.execute()
/// ```
public final class PreparedStatement {
  
  /// The number of parameters to which values can be bound
  public var parameterCount: Int { Int(duckdb_nparams(ptr.pointee)) }

  private let connection: Connection
  private let ptr = UnsafeMutablePointer<duckdb_prepared_statement?>.allocate(capacity: 1)

  /// Creates a new prepared statement for a given connection and query
  ///
  /// The query is prepared with question marks (`?`) or dollar symbols (`$1`)
  /// indicating the parameters of the query.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter connection: the connection on which the prepared stement will
  ///   execute
  /// - Parameter query: the parameterized query
  /// - Throws: ``DatabaseError/preparedStatementFailedToInitialize(reason:)``
  ///   if there is a problem with the query or connection
  public init(connection: Connection, query: String) throws {
    self.connection = connection
    let status = query.withCString { queryStPtr in
      connection.withCConnection { duckdb_prepare($0, queryStPtr, ptr) }
    }
    guard .success == status else {
      throw DatabaseError.preparedStatementFailedToInitialize(reason: preparedStatementError())
    }
  }

  deinit {
    duckdb_destroy_prepare(ptr)
    ptr.deallocate()
  }
  
  /// Executes the prepared statement
  ///
  /// Issues the parameterized query to the database using the values previously
  /// bound via the `bind(_:at:)` set of functions.
  ///
  /// - Throws: ``DatabaseError/preparedStatementQueryError(reason:)``
  public func execute() throws -> ResultSet {
    try ResultSet(prepared: self)
  }
  
  func parameterType(at index: Int) -> DatabaseType {
    let cparamtype = duckdb_param_type(ptr.pointee, DBInt(index))
    return cparamtype.asTypeID
  }
  
  func clearBindings() {
    duckdb_clear_bindings(ptr.pointee)
  }
  
  func withCPreparedStatement<T>(_ body: (duckdb_prepared_statement?) throws -> T) rethrows -> T {
    try body(ptr.pointee)
  }
}

public extension PreparedStatement {
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Bool?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_boolean(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Int8?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_int8(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Int16?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_int16(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Int32?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_int32(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Int64?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_int64(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: IntHuge?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_hugeint(ptr.pointee, .init(index), .init(value)) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: UIntHuge?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_uhugeint(ptr.pointee, .init(index), .init(value)) }
  }

  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Decimal?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_decimal(ptr.pointee, .init(index), try .init(value)) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: UInt8?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_uint8(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: UInt16?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_uint16(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: UInt32?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_uint32(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: UInt64?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_uint64(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Float?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_float(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Double?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_double(ptr.pointee, .init(index), value) }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Date?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_date(ptr.pointee, .init(index), .init(date: value)) }
  }

  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Time?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand { duckdb_bind_time(ptr.pointee, .init(index), .init(time: value)) }
  }

  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Timestamp?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand {
      duckdb_bind_timestamp(ptr.pointee, .init(index), .init(timestamp: value))
    }
  }

  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Interval?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand {
      duckdb_bind_interval(ptr.pointee, .init(index), .init(interval: value))
    }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: String?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    let data = value.data(using: .utf8)!
    try withThrowingCommand {
      data.withUnsafeBytes { dataPtr in
        duckdb_bind_varchar_length(
          ptr.pointee, .init(index), dataPtr.baseAddress, .init(dataPtr.count))
      }
    }
  }
  
  /// Binds a value of the given type at the specified parameter index
  ///
  /// Sets the value that will be used for the next call to ``execute()``.
  ///
  /// - Important: Prepared statement parameters use one-based indexing
  /// - Parameter value: the value to bind
  /// - Parameter index: the one-based parameter index
  /// - Throws: ``DatabaseError/preparedStatementFailedToBindParameter(reason:)``
  ///   if there is a type-mismatch between the value being bound and the
  ///   underlying column type
  func bind(_ value: Data?, at index: Int) throws {
    guard let value = try unwrapValueOrBindNull(value, at: index) else { return }
    try withThrowingCommand {
      value.withUnsafeBytes { dataPtr in
        duckdb_bind_blob(
          ptr.pointee, .init(index), dataPtr.baseAddress, .init(dataPtr.count))
      }
    }
  }
}

private extension PreparedStatement {
  
  func bindNullValue(at index: Int) throws {
    try withThrowingCommand { duckdb_bind_null(ptr.pointee, .init(index)) }
  }
  
  func unwrapValueOrBindNull<T>(_ value: T?, at index: Int) throws -> T? {
    guard let value else {
      try bindNullValue(at: index)
      return nil
    }
    return value
  }
  
  func withThrowingCommand(_ body: () throws -> duckdb_state) throws {
    let state = try body()
    guard state == .success else {
      throw DatabaseError.preparedStatementFailedToBindParameter(reason: preparedStatementError())
    }
  }
  
  func preparedStatementError() -> String? {
    duckdb_prepare_error(ptr.pointee).map(String.init(cString:))
  }
}
