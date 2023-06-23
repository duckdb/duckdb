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

/// A DuckDB database error
public enum DatabaseError: Error {
  /// Provided decimal overflows the internal database representaion
  case decimalUnrepresentable
  /// Supplied item could not be appended
  case appenderFailedToAppendItem(reason: String?)
  /// Row could not be ended with the appender in its current state
  case appenderFailedToEndRow(reason: String?)
  /// Appender could not be flushed in its current state
  case appenderFailedToFlush(reason: String?)
  /// Failed to instantiate appender
  case appenderFailedToInitialize(reason: String?)
  /// Failed to instantiate connection
  case connectionFailedToInitialize
  /// Failed to set flag on database configuration
  case configurationFailedToSetFlag
  /// Failed to execute query on connection
  case connectionQueryError(reason: String?)
  /// Failed to instantiate database
  case databaseFailedToInitialize(reason: String?)
  /// Failed to instantiate prepared statement
  case preparedStatementFailedToInitialize(reason: String?)
  /// Failed to bound value to prepared statement
  case preparedStatementFailedToBindParameter(reason: String?)
  /// Failed to execute prepared statement query
  case preparedStatementQueryError(reason: String?)
  /// Value of type could not be found
  case valueNotFound(Any.Type)
  /// Type does not match underlying database type
  case typeMismatch(Any.Type)
}
