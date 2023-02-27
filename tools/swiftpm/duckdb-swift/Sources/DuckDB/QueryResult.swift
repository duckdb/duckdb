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
      throw DatabaseError.queryError(reason: error)
    }
  }
  
  init(prepared: PreparedStatement) throws {
    let status = prepared.withCPreparedStatement { duckdb_execute_prepared($0, ptr) }
    guard status == .success else {
      let error = duckdb_result_error(ptr).map(String.init(cString:))
      throw DatabaseError.queryError(reason: error)
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
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      return isValidItem(at: path) ? () : nil
    }
  }
  
  func transformer<T: PrimitiveDatabaseValue>(
    forColumn columnIndex: DBInt, to type: T.Type
  ) -> (DBInt) -> T? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == T.representedDatabaseTypeID else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(T.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: T.self, at: path) { $0 }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: IntHuge.Type
  ) -> (DBInt) -> IntHuge? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .hugeint else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(IntHuge.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_hugeint.self, at: path) { $0.asIntHuge }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: String.Type
  ) -> (DBInt) -> String? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .varchar else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(String.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_string.self, at: path) { $0.asString }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: UUID.Type
  ) -> (DBInt) -> UUID? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .uuid else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(UUID.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_hugeint.self, at: path) { $0.asUUID }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Time.Type
  ) -> (DBInt) -> Time? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .time else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Time.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_time.self, at: path) { $0.asTime }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Date.Type
  ) -> (DBInt) -> Date? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .date else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Date.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_date.self, at: path) { $0.asDate }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Timestamp.Type
  ) -> (DBInt) -> Timestamp? {
    let columnDataType = columnDataType(at: columnIndex)
    let columnTypes = [DBTypeID.timestamp_s, .timestamp_ms, .timestamp, .timestamp_ns]
    guard columnTypes.contains(columnDataType) else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Timestamp.self)")
      return { _ in nil }
    }
    let scaleF: (duckdb_timestamp) -> duckdb_timestamp
    switch columnDataType {
    case .timestamp_s:
      scaleF = { duckdb_timestamp(micros: $0.micros * 1_000_000) }
    case .timestamp_ms:
      scaleF = { duckdb_timestamp(micros: $0.micros * 1_000) }
    case .timestamp_ns:
      scaleF = { duckdb_timestamp(micros: $0.micros / 1_000) }
    default:
      scaleF = { $0 }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_timestamp.self, at: path) { cTimestamp in
        let scaled = scaleF(cTimestamp)
        return scaled.asTimestamp
      }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Interval.Type
  ) -> (DBInt) -> Interval? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .interval else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Interval.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_interval.self, at: path) { $0.asInterval }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Data.Type
  ) -> (DBInt) -> Data? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .blob else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Data.self)")
      return { _ in nil }
    }
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: duckdb_blob.self, at: path) { $0.asData }
    }
  }
  
  func transformer(
    forColumn columnIndex: DBInt, to type: Decimal.Type
  ) -> (DBInt) -> Decimal? {
    let columnDataType = columnDataType(at: columnIndex)
    guard columnDataType == .decimal else {
      let columnTypeString = columnDataType.description.uppercased()
      assertionFailure("unsupported type conversion from \(columnTypeString) to \(Decimal.self)")
      return { _ in nil }
    }
    let logicalType = LogicalType(result: self, index: columnIndex)
    guard let props = logicalType.decimalProperties else {
      fatalError("expected decimal logical type")
    }
    switch props.storageType {
    case .tinyint:
      return decimalTransformer(
        forColumn: columnIndex, storageType: Int8.self, scale: props.scale)
    case .smallint:
      return decimalTransformer(
        forColumn: columnIndex, storageType: Int16.self, scale: props.scale)
    case .integer:
      return decimalTransformer(
        forColumn: columnIndex, storageType: Int32.self, scale: props.scale)
    case .bigint:
      return decimalTransformer(
        forColumn: columnIndex, storageType: Int64.self, scale: props.scale)
    case .hugeint:
      return decimalTransformer(
        forColumn: columnIndex, storageType: IntHuge.self, scale: props.scale)
    case let unexpectedInternalType:
      fatalError("unexpected internal decimal type: \(unexpectedInternalType)")
    }
  }
  
  private func decimalTransformer<T: DecimalStorageType>(
    forColumn columnIndex: DBInt, storageType: T.Type, scale: UInt8
  ) -> (DBInt) -> Decimal? {
    return { [self] itemIndex in
      let path = Self.path(forColumn: columnIndex, item: itemIndex)
      guard isValidItem(at: path) else { return nil }
      return withAssumedCType(of: T.self, at: path) { decimalStorage in
        let storageValue = decimalStorage.asDecimal
        let sign = storageValue.sign
        let exponent = -Int(scale)
        let significand = abs(storageValue)
        return Decimal(sign: sign, exponent: exponent, significand: significand)
      }
    }
  }
}

// MARK: - Data Extraction Utilities

private extension QueryResult {
  
  func dataChunk(at index: DBInt) -> DataChunk {
    precondition(index < chunkCount, "data chunk out of bounds")
    return DataChunk(result: self, index: index)
  }
  
  func isValidItem(at path: Path) -> Bool {
    let chunk = dataChunk(at: path.chunk)
    return chunk.withCVector(at: path.column) { vector in
      let validityMasks = duckdb_vector_get_validity(vector)
      guard let validityMasks else { return true }
      let validityMasksBuffer = UnsafeBufferPointer(
        start: validityMasks, count: Int(duckdb_vector_size() / 64))
      let validityEntryIndex = path.row / 64
      let validityBitIndex = path.row % 64
      let validityMask = validityMasksBuffer[Int(validityEntryIndex)]
      let validityBit = (DBInt(1) << validityBitIndex)
      return validityMask & validityBit != 0
    }
  }
  
  func withAssumedCType<T, Result>(
    of type: T.Type, at path: Path, _ body: (T) throws -> Result
  ) rethrows -> Result {
    let chunk = dataChunk(at: path.chunk)
    return try chunk.withCVector(at: path.column) { vector in
      let dataPtr = duckdb_vector_get_data(vector)!
      let itemDataPtr = dataPtr.assumingMemoryBound(to: T.self)
      let pointer = itemDataPtr.advanced(by: Int(path.row))
      return try body(pointer[0])
    }
  }
}

// MARK: - Path Utilities

private extension QueryResult {
  
  struct Path {
    let column: DBInt
    let chunk: DBInt
    let row: DBInt
  }
  
  static let vectorSize = DBInt(duckdb_vector_size())
  
  static func path(forColumn column: DBInt, item itemIndex: DBInt) -> Path {
    let chunkIndex = itemIndex / Self.vectorSize
    let rowIndex = itemIndex % Self.vectorSize
    return Path(column: column, chunk: chunkIndex, row: rowIndex)
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
