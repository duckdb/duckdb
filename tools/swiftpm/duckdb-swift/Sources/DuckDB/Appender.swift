//
//  DuckDB
//  https://github.com/duckdb/duckdb-swift
//
//  Copyright © 2018-2023 Stichting DuckDB Foundation
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

@_implementationOnly import Cduckdb
import Foundation

/// An object that efficiently appends data to a DuckDB table
///
/// Appenders are the most efficient way of loading data into DuckDB from within
/// Swift, and are recommended for fast data loading. The appender is much
/// faster than using prepared statements or individual INSERT INTO statements.
///
/// Appends are made in row-wise format. For every column an `append(_:)` call
/// should be made after which the row should be finished by calling
/// ``endRow()``.
///
/// The following example shows two rows being appended to a table with a two
/// column layout.
///
/// ```swift
/// do {
///   let database = try Database(store: .inMemory)
///   let connection = try database.connect()
///   try connection.execute("CREATE TABLE people(id INTEGER, name VARCHAR)")
///   let appender = try Appender(connection: connection, table: "people")
///   // add first row
///   try appender.append(Int32(1))
///   try appender.append("Mark")
///   try appender.endRow()
///   // add second row
///   try appender.append(Int32(2))
///   try appender.append("Hannes")
///   try appender.endRow()
///   // flush rows to table
///   try appender.flush()
/// }
/// catch {
///   // handle error
/// }
/// ```
public final class Appender {

  private let connection: Connection
  private let ptr = UnsafeMutablePointer<duckdb_appender?>.allocate(capacity: 1)

  /// Creates a new appender
  ///
  /// Instantiates an ``Appender`` through which table rows can be efficiently
  /// added
  ///
  /// - Parameter connection: the connection through which rows should be
  ///   appended
  /// - Parameter schema: the database schema (defaults to `main`)
  /// - Parameter table: the table to append to
  /// - Throws: ``DatabaseError/appenderFailedToInitialize(reason:)`` if the
  ///   appender failed to instantiate
  public init(connection: Connection, schema: String? = nil, table: String) throws {
    self.connection = connection
    let status = schema.withOptionalCString { schemaStrPtr in
      table.withCString { tableStrPtr in
        connection.withCConnection { duckdb_appender_create($0, schemaStrPtr, tableStrPtr, ptr) }
      }
    }
    guard .success == status else {
      throw DatabaseError.appenderFailedToInitialize(reason: appenderError())
    }
  }

  deinit {
    try? flush()
    duckdb_appender_destroy(ptr)
    ptr.deallocate()
  }
  
  /// Signals the end of the current row
  ///
  /// After all columns for a row have been appended, ``endRow()`` must be
  /// called to indicate that the row is ready to be added to the database.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToEndRow(reason:)``
  ///   if the row could not be completed in its current state
  public func endRow() throws {
    let status = duckdb_appender_end_row(ptr.pointee)
    guard .success == status else {
      throw DatabaseError.appenderFailedToEndRow(reason: appenderError())
    }
  }
  
  /// Flushes pending rows to the database
  ///
  /// To optimize performance, the appender writes rows to the database in
  /// batches. Use `flush()` to immediately write any rows that are pending
  /// insertion.
  ///
  /// - Throws: ``DatabaseError/appenderFailedToFlush(reason:)`` if the pending
  ///   rows failed to be written to the database
  public func flush() throws {
    let status = duckdb_appender_flush(ptr.pointee)
    guard .success == status else {
      throw DatabaseError.appenderFailedToFlush(reason: appenderError())
    }
  }
}

public extension Appender {
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Bool?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_bool(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Int8?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_int8(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Int16?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_int16(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Int32?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_int32(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Int64?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_int64(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: IntHuge?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_hugeint(ptr.pointee, .init(value)) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: UInt8?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_uint8(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: UInt16?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_uint16(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: UInt32?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_uint32(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: UInt64?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_uint64(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Float?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_float(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Double?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_double(ptr.pointee, value) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Date?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_date(ptr.pointee, .init(date: value)) }
  }

  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Time?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand { duckdb_append_time(ptr.pointee, .init(time: value)) }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Timestamp?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand {
      duckdb_append_timestamp(ptr.pointee, .init(timestamp: value))
    }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Interval?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand {
      duckdb_append_interval(ptr.pointee, .init(interval: value))
    }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: String?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    let data = value.data(using: .utf8)!
    try withThrowingCommand {
      data.withUnsafeBytes { dataPtr in
        duckdb_append_varchar_length(ptr.pointee, dataPtr.baseAddress, .init(dataPtr.count))
      }
    }
  }
  
  /// Appends a value for the current row of the given type
  ///
  /// Appends are made in row-wise format. For every column, an `append(_:)`
  /// call should be made, after which the row should be finished by calling
  /// ``endRow()``.
  ///
  /// - Parameter value: the value to append
  /// - Throws: ``DatabaseError/appenderFailedToAppendItem(reason:)``
  ///   if a value of this type was not expected in the appender's current state
  func append(_ value: Data?) throws {
    guard let value = try unwrapValueOrAppendNull(value) else { return }
    try withThrowingCommand {
      value.withUnsafeBytes { dataPtr in
        duckdb_append_blob( ptr.pointee, dataPtr.baseAddress, .init(dataPtr.count)) }
    }
  }
}

private extension Appender {
  
  func appendNullValue() throws {
    try withThrowingCommand { duckdb_append_null(ptr.pointee) }
  }
  
  func unwrapValueOrAppendNull<T>(_ value: T?) throws -> T? {
    guard let value else {
      try appendNullValue()
      return nil
    }
    return value
  }
  
  func withThrowingCommand(_ body: () throws -> duckdb_state) throws {
    let state = try body()
    guard state == .success else {
      throw DatabaseError.appenderFailedToAppendItem(reason: appenderError())
    }
  }
  
  func appenderError() -> String? {
    duckdb_appender_error(ptr.pointee).map(String.init(cString:))
  }
}
