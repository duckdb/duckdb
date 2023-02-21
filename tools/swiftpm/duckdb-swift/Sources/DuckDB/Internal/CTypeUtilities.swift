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

extension duckdb_state {
  static let success = duckdb_state(0)
  static let failure = duckdb_state(1)
}

extension duckdb_pending_state {
  static let ready = duckdb_pending_state(0)
  static let notReady = duckdb_pending_state(1)
  static let error = duckdb_pending_state(2)
}

extension UnsafePointer where Pointee == duckdb_string {
  
  private static let inlineLimit = UInt32(12)
  
  var contents: String {
    let contentsSize = UnsafeRawPointer(self).load(as: UInt32.self)
    let strPtr: UnsafeRawPointer
    if contentsSize <= Self.inlineLimit {
      strPtr = UnsafeRawPointer(self).advanced(by: MemoryLayout<UInt32>.stride)
    }
    else {
      let strPtrPtr = UnsafeRawPointer(self).advanced(by: MemoryLayout<UInt64>.stride)
      strPtr = strPtrPtr.load(as: UnsafeRawPointer.self)
    }
    let stringData = Data(bytes: strPtr, count: Int(contentsSize))
    return String(data: stringData, encoding:.utf8)!
  }
}
