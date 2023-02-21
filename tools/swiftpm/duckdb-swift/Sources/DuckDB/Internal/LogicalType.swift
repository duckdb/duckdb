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

final class LogicalType {
  
  private let ptr = UnsafeMutablePointer<duckdb_logical_type?>.allocate(capacity: 1)
  
  init(result: QueryResult, index: DBInt) {
    self.ptr.pointee = result.withCResult { duckdb_column_logical_type($0, index) }
  }
  
  deinit {
    duckdb_destroy_logical_type(ptr)
    ptr.deallocate()
  }
  
  var dataType: DBTypeID {
    let ctypeid = duckdb_get_type_id(ptr.pointee)
    return ctypeid.asTypeID
  }
  
  func withCLogicalType<T>(_ body: (duckdb_logical_type?) throws -> T) rethrows -> T {
    try body(ptr.pointee)
  }
}

// MARK: - Decimal

extension LogicalType {
  
  struct DecimalProperties {
    let width: UInt8
    let scale: UInt8
    let storageType: DBTypeID
  }
  
  var decimalProperties: DecimalProperties? {
    guard dataType == .decimal else { return nil }
    let internalStorageType = duckdb_decimal_internal_type(ptr.pointee)
    return .init(
      width: duckdb_decimal_width(ptr.pointee),
      scale: duckdb_decimal_scale(ptr.pointee),
      storageType: internalStorageType.asTypeID
    )
  }
}
