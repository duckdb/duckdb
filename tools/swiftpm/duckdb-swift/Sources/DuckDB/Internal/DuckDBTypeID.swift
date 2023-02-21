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

struct DuckDBTypeID: RawRepresentable, Equatable {
  let rawValue: UInt32
  init(rawValue: UInt32) {
    self.rawValue = rawValue
  }
}

extension DuckDBTypeID {
  // invalid data type
  static let invalid = DuckDBTypeID(rawValue: DUCKDB_TYPE_INVALID.rawValue)
  // bool
  static let boolean = DuckDBTypeID(rawValue: DUCKDB_TYPE_BOOLEAN.rawValue)
  // int8_t
  static let tinyint = DuckDBTypeID(rawValue: DUCKDB_TYPE_TINYINT.rawValue)
  // int16_t
  static let smallint = DuckDBTypeID(rawValue: DUCKDB_TYPE_SMALLINT.rawValue)
  // int32_t
  static let integer = DuckDBTypeID(rawValue: DUCKDB_TYPE_INTEGER.rawValue)
  // int64_t
  static let bigint = DuckDBTypeID(rawValue: DUCKDB_TYPE_BIGINT.rawValue)
  // uint8_t
  static let utinyint = DuckDBTypeID(rawValue: DUCKDB_TYPE_UTINYINT.rawValue)
  // uint16_t
  static let usmallint = DuckDBTypeID(rawValue: DUCKDB_TYPE_USMALLINT.rawValue)
  // uint32_t
  static let uinteger = DuckDBTypeID(rawValue: DUCKDB_TYPE_UINTEGER.rawValue)
  // uint64_t
  static let ubigint = DuckDBTypeID(rawValue: DUCKDB_TYPE_UBIGINT.rawValue)
  // float
  static let float = DuckDBTypeID(rawValue: DUCKDB_TYPE_FLOAT.rawValue)
  // double
  static let double = DuckDBTypeID(rawValue: DUCKDB_TYPE_DOUBLE.rawValue)
  // duckdb_timestamp, in microseconds
  static let timestamp = DuckDBTypeID(rawValue: DUCKDB_TYPE_TIMESTAMP.rawValue)
  // duckdb_date
  static let date = DuckDBTypeID(rawValue: DUCKDB_TYPE_DATE.rawValue)
  // duckdb_time
  static let time = DuckDBTypeID(rawValue: DUCKDB_TYPE_TIME.rawValue)
  // duckdb_interval
  static let interval = DuckDBTypeID(rawValue: DUCKDB_TYPE_INTERVAL.rawValue)
  // duckdb_hugeint
  static let hugeint = DuckDBTypeID(rawValue: DUCKDB_TYPE_HUGEINT.rawValue)
  // const char*
  static let varchar = DuckDBTypeID(rawValue: DUCKDB_TYPE_VARCHAR.rawValue)
  // duckdb_blob
  static let blob = DuckDBTypeID(rawValue: DUCKDB_TYPE_BLOB.rawValue)
  // decimal
  static let decimal = DuckDBTypeID(rawValue: DUCKDB_TYPE_DECIMAL.rawValue)
  // duckdb_timestamp, in seconds
  static let timestamp_s = DuckDBTypeID(rawValue: DUCKDB_TYPE_TIMESTAMP_S.rawValue)
  // duckdb_timestamp, in milliseconds
  static let timestamp_ms = DuckDBTypeID(rawValue: DUCKDB_TYPE_TIMESTAMP_MS.rawValue)
  // duckdb_timestamp, in nanoseconds
  static let timestamp_ns = DuckDBTypeID(rawValue: DUCKDB_TYPE_TIMESTAMP_NS.rawValue)
  // enum type, only useful as logical type
  static let `enum` = DuckDBTypeID(rawValue: DUCKDB_TYPE_ENUM.rawValue)
  // list type, only useful as logical type
  static let list = DuckDBTypeID(rawValue: DUCKDB_TYPE_LIST.rawValue)
  // struct type, only useful as logical type
  static let `struct` = DuckDBTypeID(rawValue: DUCKDB_TYPE_STRUCT.rawValue)
  // map type, only useful as logical type
  static let map = DuckDBTypeID(rawValue: DUCKDB_TYPE_MAP.rawValue)
  // union type, only useful as logical type
  static let union = DuckDBTypeID(rawValue: DUCKDB_TYPE_UNION.rawValue)
  // duckdb_hugeint
  static let uuid = DuckDBTypeID(rawValue: DUCKDB_TYPE_UUID.rawValue)
  // const char*
//  static let json = DUCKDB_TYPE(DUCKDB_TYPE_JSON.rawValue)
}

extension DuckDBTypeID: CustomStringConvertible {
  
  public var description: String {
    switch self {
    case .boolean: return "boolean"
    case .tinyint: return "tinyint"
    case .smallint: return "smallint"
    case .integer: return "integer"
    case .bigint: return "bigint"
    case .utinyint: return "utinyint"
    case .usmallint: return "usmallint"
    case .uinteger: return "uinteger"
    case .ubigint: return "ubigint"
    case .float: return "float"
    case .double: return "double"
    case .timestamp: return "timestamp"
    case .date: return "date"
    case .time: return "time"
    case .interval: return "interval"
    case .hugeint: return "hugeint"
    case .varchar: return "varchar"
    case .blob: return "blob"
    case .decimal: return "decimal"
    case .timestamp_s: return "timestamp_s"
    case .timestamp_ms: return "timestamp_ms"
    case .timestamp_ns: return "timestamp_ns"
    case .`enum`: return "enum"
    case .list: return "list"
    case .`struct`: return "struct"
    case .map: return "map"
    case .union: return "union"
    case .uuid: return "uuid"
//    case .json: return "json"
    case .invalid: return "invalid"
    default: return "unknown (\(self.rawValue))"
    }
  }
}
