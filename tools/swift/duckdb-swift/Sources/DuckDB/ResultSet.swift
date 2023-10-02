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

/// An object representing a DuckDB result set
///
/// A DuckDB result set contains the data returned from the database after a
/// successful query.
///
/// A result set is organized into vertical table slices called columns. Each
/// column of the result set is accessible by calling the ``subscript(_:)``
/// method of the result.
///
/// Elements of a column can be accessed by casting the column to the native
/// Swift type that matches the underlying database column type. See ``Column``
/// for further discussion.
public final class ResultSet: Sendable {
  
  /// The number of chunks in the result set
  public var chunkCount: DBInt { duckdb_result_chunk_count(ptr.pointee) }
  /// The number of columns in the result set
  public var columnCount: DBInt { duckdb_column_count(ptr) }
  
  /// The total number of rows in the result set
  public var rowCount: DBInt {
    guard chunkCount > 0 else { return DBInt(0) }
    let lastChunk = dataChunk(at: chunkCount - 1)
    return (chunkCount - 1) * Vector.vectorSize + lastChunk.count
  }
  
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
  
  /// Returns a `Void` typed column for the given column index
  ///
  /// A `Void` typed column can be cast to a column matching the underlying
  /// database representation using ``Column/cast(to:)-4376d``. See ``Column``
  /// for further discussion.
  ///
  /// - Parameter columnIndex: the index of the column in the result set
  /// - Returns: a `Void` typed column
  public func column(at columnIndex: DBInt) -> Column<Void> {
    precondition(columnIndex < columnCount)
    return Column(result: self, columnIndex: columnIndex)
  }
  
  /// The underlying column name for the given column index
  ///
  /// - Parameter columnIndex: the index of the column in the result set
  /// - Returns: the name of the column
  public func columnName(at columnIndex: DBInt) -> String {
    String(cString: duckdb_column_name(ptr, columnIndex))
  }
  
  /// The index of the given column name
  ///
  /// - Parameter columnName: the name of the column in the result set
  /// - Returns: the index of the column
  /// - Complexity: O(n)
  public func index(forColumnName columnName: String) -> DBInt? {
    for i in 0..<columnCount {
      if self.columnName(at: i) == columnName {
        return i
      }
    }
    return nil
  }
  
  func columnDataType(at index: DBInt) -> DatabaseType {
    let dataType = duckdb_column_type(ptr, index)
    return DatabaseType(rawValue: dataType.rawValue)
  }

  func columnLogicalType(at index: DBInt) -> LogicalType {
    return LogicalType { duckdb_column_logical_type(ptr, index) }
  }
  
  func withCResult<T>(_ body: (UnsafeMutablePointer<duckdb_result>) throws -> T) rethrows -> T {
    try body(ptr)
  }
}

// MARK: - Type Casting Transformers

extension ResultSet {
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Void.Type
  ) -> @Sendable (DBInt) -> Void? {
    transformer(forColumn: columnIndex, to: type) { $0.unwrapNull() ? nil : () }
  }
  
  func transformer<T: PrimitiveDatabaseValue>(
    forColumn columnIndex: DBInt, to type: T.Type
  ) -> @Sendable (DBInt) -> T? {
    transformer(
      forColumn: columnIndex, to: type, fromType: T.representedDatabaseTypeID
    ) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Int.Type
  ) -> @Sendable (DBInt) -> Int? {
    transformer(forColumn: columnIndex, to: type) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: UInt.Type
  ) -> @Sendable (DBInt) -> UInt? {
    transformer(forColumn: columnIndex, to: type) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: IntHuge.Type
  ) -> @Sendable (DBInt) -> IntHuge? {
    transformer(forColumn: columnIndex, to: type, fromType: .hugeint) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: String.Type
  ) -> @Sendable (DBInt) -> String? {
    transformer(forColumn: columnIndex, to: type, fromType: .varchar) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: UUID.Type
  ) -> @Sendable (DBInt) -> UUID? {
    transformer(forColumn: columnIndex, to: type, fromType: .uuid) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Time.Type
  ) -> @Sendable (DBInt) -> Time? {
    transformer(forColumn: columnIndex, to: type, fromType: .time) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Date.Type
  ) -> @Sendable (DBInt) -> Date? {
    transformer(forColumn: columnIndex, to: type, fromType: .date) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Timestamp.Type
  ) -> @Sendable (DBInt) -> Timestamp? {
    let columnTypes = [DatabaseType.timestampS, .timestampMS, .timestamp, .timestampNS]
    return transformer(
      forColumn: columnIndex, to: type, fromTypes: .init(columnTypes)
    ) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Interval.Type
  ) -> @Sendable (DBInt) -> Interval? {
    transformer(forColumn: columnIndex, to: type, fromType: .interval) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Data.Type
  ) -> @Sendable (DBInt) -> Data? {
    transformer(forColumn: columnIndex, to: type, fromType: .blob) { try? $0.unwrap(type) }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Decimal.Type
  ) -> @Sendable (DBInt) -> Decimal? {
    transformer(forColumn: columnIndex, to: type, fromType: .decimal) { try? $0.unwrap(type) }
  }
  
  func decodableTransformer<T: Decodable>(
    forColumn columnIndex: DBInt, to type: T.Type
  ) -> @Sendable (DBInt) -> T? {
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

private extension ResultSet {
  
  func dataChunk(at index: DBInt) -> DataChunk {
    precondition(index < chunkCount, "data chunk out of bounds")
    return DataChunk(result: self, index: index)
  }
  
  func transformer<T>(
    forColumn columnIndex: DBInt,
    to type: T.Type,
    fromType columnType: DatabaseType,
    _ body: @escaping (Vector.Element) -> T?
  ) -> @Sendable (DBInt) -> T? {
    transformer(forColumn: columnIndex, to: type, fromTypes: .init([columnType]), body)
  }
  
  func transformer<T>(
    forColumn columnIndex: DBInt,
    to type: T.Type,
    fromTypes columnTypes: Set<DatabaseType>? = nil,
    _ body: @escaping (Vector.Element) -> T?
  ) -> @Sendable (DBInt) -> T? {
    if let columnTypes {
      let columnDataType = columnDataType(at: columnIndex)
      guard columnTypes.contains(columnDataType) else {
        let columnTypeString = columnDataType.description.uppercased()
        assertionFailure("unsupported type conversion from \(columnTypeString) to \(type)")
        return { _ in nil }
      }
    }
    return { [self] itemIndex in
      let chunkIndex = itemIndex / Vector.vectorSize
      let rowIndex = itemIndex % Vector.vectorSize
      let chunk = dataChunk(at: chunkIndex)
      return chunk.withVector(at: columnIndex) { vector in
        body(vector[Int(rowIndex)])
      }
    }
  }
}

// MARK: - Collection conformance

extension ResultSet: RandomAccessCollection {
  
  public typealias Element = Column<Void>
  
  public struct Iterator: IteratorProtocol {
    
    private let result: ResultSet
    private var position: DBInt
    
    init(result: ResultSet) {
      self.result = result
      self.position = result.startIndex
    }
    
    public mutating func next() -> Element? {
      guard position < result.endIndex else { return nil }
      defer { position += 1 }
      return .some(result[position])
    }
  }
  
  public var startIndex: DBInt { 0 }
  public var endIndex: DBInt { columnCount }
  
  public subscript(position: DBInt) -> Column<Void> {
    column(at: position)
  }
  
  public func makeIterator() -> Iterator {
    Iterator(result: self)
  }
  
  public func index(after i: DBInt) -> DBInt { i + 1 }
  public func index(before i: DBInt) -> DBInt { i - 1 }
}

// MARK: - Debug Description

extension ResultSet: CustomDebugStringConvertible {
  
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
