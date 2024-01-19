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

@_implementationOnly import Cduckdb

/// The underlying database type of a DuckDB column
///
/// DuckDB is a strongly typed database system. As such, every column has a
/// single type specified. This type is constant over the entire column. That is
/// to say, a column with an underlying type of ``DatabaseType/integer`` will
/// only contain ``DatabaseType/integer`` values.
///
/// DuckDB also supports columns of composite types. For example, it is possible
/// to define an array of integers (`INT[]`), which can be cast to `[Int32]`
/// within Swift using ``Column/cast(to:)-4376d``. It is also possible to
/// define database types as arbitrary structs (`ROW(i INTEGER, j VARCHAR)`),
/// which can be cast to their `Decodable` matching Swift type in the same way.
public struct DatabaseType: RawRepresentable, Hashable, Equatable {
  public let rawValue: UInt32
  public init(rawValue: UInt32) {
    self.rawValue = rawValue
  }
}

// MARK: - Public Types

public extension DatabaseType {
  /// Boolean type castable to `Bool`
  static let boolean = DatabaseType(rawValue: DUCKDB_TYPE_BOOLEAN.rawValue)
  /// Integer type castable to `Int8`
  static let tinyint = DatabaseType(rawValue: DUCKDB_TYPE_TINYINT.rawValue)
  /// Integer type castable to `Int16`
  static let smallint = DatabaseType(rawValue: DUCKDB_TYPE_SMALLINT.rawValue)
  /// Integer type castable to `Int32`
  static let integer = DatabaseType(rawValue: DUCKDB_TYPE_INTEGER.rawValue)
  /// Integer type castable to `Int64`
  static let bigint = DatabaseType(rawValue: DUCKDB_TYPE_BIGINT.rawValue)
  /// Integer type castable to `HugeInt`
  static let hugeint = DatabaseType(rawValue: DUCKDB_TYPE_HUGEINT.rawValue)
  /// Integer type castable to `UHugeInt`
  static let uhugeint = DatabaseType(rawValue: DUCKDB_TYPE_UHUGEINT.rawValue)
  /// Unsigned integer type castable to `UInt8`
  static let utinyint = DatabaseType(rawValue: DUCKDB_TYPE_UTINYINT.rawValue)
  /// Unsigned integer type castable to `UInt16`
  static let usmallint = DatabaseType(rawValue: DUCKDB_TYPE_USMALLINT.rawValue)
  /// Unsigned integer type castable to `UInt32`
  static let uinteger = DatabaseType(rawValue: DUCKDB_TYPE_UINTEGER.rawValue)
  /// Unsigned integer type castable to `UInt64`
  static let ubigint = DatabaseType(rawValue: DUCKDB_TYPE_UBIGINT.rawValue)
  /// Floating point type castable to `Float`
  static let float = DatabaseType(rawValue: DUCKDB_TYPE_FLOAT.rawValue)
  /// Floating point type castable to `Double`
  static let double = DatabaseType(rawValue: DUCKDB_TYPE_DOUBLE.rawValue)
  /// Timestamp type castable to `Timestamp`
  static let timestamp = DatabaseType(rawValue: DUCKDB_TYPE_TIMESTAMP.rawValue)
  /// Timestamp(TZ) type castable to `Timestamp`
  static let timestampTz = DatabaseType(rawValue: DUCKDB_TYPE_TIMESTAMP_TZ.rawValue)
  /// Date type castable to `Date`
  static let date = DatabaseType(rawValue: DUCKDB_TYPE_DATE.rawValue)
  /// Time type castable to `Time`
  static let time = DatabaseType(rawValue: DUCKDB_TYPE_TIME.rawValue)
  /// Time(TZ) type castable to `Time`
  static let timeTz = DatabaseType(rawValue: DUCKDB_TYPE_TIME_TZ.rawValue)
  /// Interval type castable to `Interval`
  static let interval = DatabaseType(rawValue: DUCKDB_TYPE_INTERVAL.rawValue)
  /// String type castable to `String`
  static let varchar = DatabaseType(rawValue: DUCKDB_TYPE_VARCHAR.rawValue)
  /// Data type castable to `Data`
  static let blob = DatabaseType(rawValue: DUCKDB_TYPE_BLOB.rawValue)
  /// Decimal type castable to `Decimal`
  static let decimal = DatabaseType(rawValue: DUCKDB_TYPE_DECIMAL.rawValue)
  /// Timestamp type castable to `Timestamp`
  static let timestampS = DatabaseType(rawValue: DUCKDB_TYPE_TIMESTAMP_S.rawValue)
  /// Timestamp type castable to `Timestamp`
  static let timestampMS = DatabaseType(rawValue: DUCKDB_TYPE_TIMESTAMP_MS.rawValue)
  /// Timestamp type castable to `Timestamp`
  static let timestampNS = DatabaseType(rawValue: DUCKDB_TYPE_TIMESTAMP_NS.rawValue)
  /// Enum type castable to suitable `Decodable` conforming types
  static let `enum` = DatabaseType(rawValue: DUCKDB_TYPE_ENUM.rawValue)
  /// Array type castable to suitable `Decodable` conforming types
  static let list = DatabaseType(rawValue: DUCKDB_TYPE_LIST.rawValue)
  /// Struct type castable to suitable `Decodable` conforming types
  static let `struct` = DatabaseType(rawValue: DUCKDB_TYPE_STRUCT.rawValue)
  /// Dictionary type castable to suitable `Decodable` conforming types
  static let map = DatabaseType(rawValue: DUCKDB_TYPE_MAP.rawValue)
  /// Enum type castable to suitable `Decodable` conforming types
  static let union = DatabaseType(rawValue: DUCKDB_TYPE_UNION.rawValue)
  /// UUID type castable to `UUID`
  static let uuid = DatabaseType(rawValue: DUCKDB_TYPE_UUID.rawValue)
}

// MARK: - Internal Types

extension DatabaseType {
  static let invalid = DatabaseType(rawValue: DUCKDB_TYPE_INVALID.rawValue)
}

extension DatabaseType: CustomStringConvertible {
  
  public var description: String {
    switch self {
    case .boolean: return "\(Self.self).boolean"
    case .tinyint: return "\(Self.self).tinyint"
    case .smallint: return "\(Self.self).smallint"
    case .integer: return "\(Self.self).integer"
    case .bigint: return "\(Self.self).bigint"
    case .utinyint: return "\(Self.self).utinyint"
    case .usmallint: return "\(Self.self).usmallint"
    case .uinteger: return "\(Self.self).uinteger"
    case .ubigint: return "\(Self.self).ubigint"
    case .float: return "\(Self.self).float"
    case .double: return "\(Self.self).double"
    case .timestamp: return "\(Self.self).timestamp"
    case .timestampTz: return "\(Self.self).timestampTZ"
    case .date: return "\(Self.self).date"
    case .time: return "\(Self.self).time"
    case .timeTz: return "\(Self.self).timeTZ"
    case .interval: return "\(Self.self).interval"
    case .hugeint: return "\(Self.self).hugeint"
    case .uhugeint: return "\(Self.self).uhugeint"
    case .varchar: return "\(Self.self).varchar"
    case .blob: return "\(Self.self).blob"
    case .decimal: return "\(Self.self).decimal"
    case .timestampS: return "\(Self.self).timestampS"
    case .timestampMS: return "\(Self.self).timestampMS"
    case .timestampNS: return "\(Self.self).timestampNS"
    case .`enum`: return "\(Self.self).enum"
    case .list: return "\(Self.self).list"
    case .`struct`: return "\(Self.self).struct"
    case .map: return "\(Self.self).map"
    case .union: return "\(Self.self).union"
    case .uuid: return "\(Self.self).uuid"
    case .invalid: return "\(Self.self).invalid"
    default: return "\(Self.self).unknown - id: (\(self.rawValue))"
    }
  }
}
