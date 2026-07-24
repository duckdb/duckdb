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

/// A type that can be used as the element type of a DuckDB ARRAY column.
///
/// Conforming types can be bound to prepared statement parameters as fixed-size
/// arrays via ``PreparedStatement/bind(_:at:)-swift.type.method``.
protocol DuckDBArrayElement {
  /// The DuckDB logical type ID for this element type.
  static var duckdbType: duckdb_type { get }
  /// Creates a `duckdb_value` representing this value.
  func createDuckDBValue() -> duckdb_value?
}

// MARK: - Numeric Conformances

extension Float: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_FLOAT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_float(self) }
}

extension Double: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_DOUBLE }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_double(self) }
}

extension Int8: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_TINYINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_int8(self) }
}

extension Int16: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_SMALLINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_int16(self) }
}

extension Int32: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_INTEGER }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_int32(self) }
}

extension Int64: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_BIGINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_int64(self) }
}

extension UInt8: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_UTINYINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_uint8(self) }
}

extension UInt16: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_USMALLINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_uint16(self) }
}

extension UInt32: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_UINTEGER }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_uint32(self) }
}

extension UInt64: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_UBIGINT }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_uint64(self) }
}

// MARK: - Bool

extension Bool: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_BOOLEAN }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_bool(self) }
}

// MARK: - String

extension String: DuckDBArrayElement {
  static var duckdbType: duckdb_type { DUCKDB_TYPE_VARCHAR }
  func createDuckDBValue() -> duckdb_value? { duckdb_create_varchar(self) }
}
