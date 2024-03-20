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

protocol PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { get }
}

extension Bool: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .boolean }
}

extension Int8: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .tinyint }
}

extension Int16: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .smallint }
}

extension Int32: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .integer }
}

extension Int64: PrimitiveDatabaseValue  {
  static var representedDatabaseTypeID: DatabaseType { .bigint }
}

extension UInt8: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .utinyint }
}

extension UInt16: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .usmallint }
}

extension UInt32: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .uinteger }
}

extension UInt64: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .ubigint }
}

extension Float: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .float }
}

extension Double: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DatabaseType { .double }
}
