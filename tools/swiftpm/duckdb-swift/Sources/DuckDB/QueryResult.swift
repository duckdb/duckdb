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

public final class QueryResult {
  
  public var chunkCount: DBInt { duckdb_result_chunk_count(ptr.pointee) }
  public var columnCount: DBInt { duckdb_column_count(ptr) }
  
  lazy private (set) var rowCount = {
    guard chunkCount > 0 else { return DBInt(0) }
    let lastChunk = dataChunk(at: chunkCount - 1)
    return (chunkCount - 1) * Self.vectorSize + lastChunk.count
  }()
  
  private let ptr = UnsafeMutablePointer<duckdb_result>.allocate(capacity: 1)
  
  init(connection: Connection, sql: String) throws {
    let status = sql.withCString { queryStrPtr in
      connection.withCConnection { duckdb_query($0, queryStrPtr, ptr) }
    }
    guard status == .success else {
      let error = duckdb_result_error(ptr).map(String.init(cString:))
      throw DatabaseError.connectionQueryError(reason: error)
    }
  }
  
  init(prepared: PreparedStatement) throws {
    let status = prepared.withCPreparedStatement { duckdb_execute_prepared($0, ptr) }
    guard status == .success else {
      let error = duckdb_result_error(ptr).map(String.init(cString:))
      throw DatabaseError.preparedStatementQueryError(reason: error)
    }
  }
  
  deinit {
    duckdb_destroy_result(ptr)
    ptr.deallocate()
  }
  
  public subscript(_ columnIndex: DBInt) -> Column<Void> {
    precondition(columnIndex < columnCount)
    return Column(result: self, columnIndex: columnIndex)
  }
  
  public func columnName(at index: DBInt) -> String {
    String(cString: duckdb_column_name(ptr, index))
  }
  
  public func index(forColumnName columnName: String) -> DBInt? {
    for i in 0..<columnCount {
      if self.columnName(at: i) == columnName {
        return i
      }
    }
    return nil
  }
  
  func columnDataType(at index: DBInt) -> DBTypeID {
    let dataType = duckdb_column_type(ptr, index)
    return DBTypeID(rawValue: dataType.rawValue)
  }
  
  func withCResult<T>(_ body: (UnsafeMutablePointer<duckdb_result>) throws -> T) rethrows -> T {
    try body(ptr)
  }
}

// MARK: - Type Casting Transformers

extension QueryResult {
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Void.Type
  ) -> (DBInt) -> Void? {
    transformer(forColumn: columnIndex, to: type) { $0.unwrapNull() ? nil : () }
  }
  
  func transformer<T: PrimitiveDatabaseValue>(
    forColumn columnIndex: DBInt, to type: T.Type
  ) -> (DBInt) -> T? {
    transformer(
      forColumn: columnIndex, to: type, fromType: T.representedDatabaseTypeID
    ) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: IntHuge.Type
  ) -> (DBInt) -> IntHuge? {
    transformer(forColumn: columnIndex, to: type, fromType: .hugeint) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: String.Type
  ) -> (DBInt) -> String? {
    transformer(forColumn: columnIndex, to: type, fromType: .varchar) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: UUID.Type
  ) -> (DBInt) -> UUID? {
    transformer(forColumn: columnIndex, to: type, fromType: .uuid) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Time.Type
  ) -> (DBInt) -> Time? {
    transformer(forColumn: columnIndex, to: type, fromType: .time) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Date.Type
  ) -> (DBInt) -> Date? {
    transformer(forColumn: columnIndex, to: type, fromType: .date) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Timestamp.Type
  ) -> (DBInt) -> Timestamp? {
    let columnTypes = [DBTypeID.timestamp_s, .timestamp_ms, .timestamp, .timestamp_ns]
    return transformer(
      forColumn: columnIndex, to: type, fromTypes: .init(columnTypes)
    ) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Interval.Type
  ) -> (DBInt) -> Interval? {
    transformer(forColumn: columnIndex, to: type, fromType: .interval) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Data.Type
  ) -> (DBInt) -> Data? {
    transformer(forColumn: columnIndex, to: type, fromType: .blob) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Decimal.Type
  ) -> (DBInt) -> Decimal? {
    transformer(forColumn: columnIndex, to: type, fromType: .decimal) { try? $0.unwrap(type) }
  }
  
  func decodableTransformer<T: Decodable>(
    forColumn columnIndex: DBInt, to type: T.Type
  ) -> (DBInt) -> T? {
    transformer(forColumn: columnIndex, to: type) { element in
      do {
        return try VectorElementDecoder.default.decode(T?.self, element: element)
      }
      catch {
        print("decoding failed with error: \(error)")
        return nil
      }
    }
  }
}

// MARK: - Data Extraction Utilities

private extension QueryResult {
  
  static let vectorSize = DBInt(duckdb_vector_size())
  
  func dataChunk(at index: DBInt) -> DataChunk {
    precondition(index < chunkCount, "data chunk out of bounds")
    return DataChunk(result: self, index: index)
  }
  
  func transformer<T>(
    forColumn columnIndex: DBInt,
    to type: T.Type,
    fromType columnType: DBTypeID,
    _ body: @escaping (Vector.Element) -> T?
  ) -> (DBInt) -> T? {
    transformer(forColumn: columnIndex, to: type, fromTypes: .init([columnType]), body)
  }
  
  func transformer<T>(
    forColumn columnIndex: DBInt,
    to type: T.Type,
    fromTypes columnTypes: Set<DBTypeID>? = nil,
    _ body: @escaping (Vector.Element) -> T?
  ) -> (DBInt) -> T? {
    if let columnTypes {
      let columnDataType = columnDataType(at: columnIndex)
      guard columnTypes.contains(columnDataType) else {
        let columnTypeString = columnDataType.description.uppercased()
        assertionFailure("unsupported type conversion from \(columnTypeString) to \(type)")
        return { _ in nil }
      }
    }
    return { [self] itemIndex in
      let chunkIndex = itemIndex / Self.vectorSize
      let rowIndex = itemIndex % Self.vectorSize
      let chunk = dataChunk(at: chunkIndex)
      return chunk.withVector(at: columnIndex) { vector in
        body(vector[Int(rowIndex)])
      }
    }
  }
}

// MARK: - Debug Description

extension QueryResult: CustomDebugStringConvertible {
  
  public var debugDescription: String {
    let summary = "chunks: \(chunkCount); rows: \(rowCount); columns: \(columnCount); layout:"
    var columns = [String]()
    for i in 0..<columnCount {
      let name = columnName(at: i)
      let type = columnDataType(at: i).description.uppercased()
      columns.append("\t\(name) \(type)")
    }
    return "<\(Self.self): { \(summary) (\n\(columns.joined(separator: ",\n"))\n);>"
  }
}
