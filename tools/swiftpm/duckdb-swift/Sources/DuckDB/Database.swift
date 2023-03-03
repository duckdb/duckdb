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

public typealias DBInt = UInt64

public final class Database {
  
  public enum Store {
    case file(at: URL)
    case inMemory
  }
  
  private let ptr = UnsafeMutablePointer<duckdb_database?>.allocate(capacity: 1)
  
  public convenience init(store: Store = .inMemory) throws {
    var fileURL: URL?
    if case .file(let url) = store {
      guard url.isFileURL else {
        throw DatabaseError.databaseFailedToInitialize(
          reason: "provided URL for database store file must be local")
      }
      fileURL = url
    }
    try self.init(path: fileURL?.path, config: nil)
  }
  
  private init(path: String?, config: duckdb_config?) throws {
    let outError = UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>.allocate(capacity: 1)
    defer { outError.deallocate() }
    let status = path.withOptionalCString { strPtr in
      duckdb_open_ext(strPtr, ptr, config, outError)
    }
    guard status == .success else {
      let error = outError.pointee.map { ptr in
        defer { duckdb_free(ptr) }
        return String(cString: ptr)
      }
      throw DatabaseError.databaseFailedToInitialize(reason: error)
    }
  }
  
  deinit {
    duckdb_close(ptr)
    ptr.deallocate()
  }
  
  public func connect() throws -> Connection {
    try .init(database: self)
  }
  
  func withCDatabase<T>(_ body: (duckdb_database?) throws -> T) rethrows -> T {
    try body(ptr.pointee)
  }
}
