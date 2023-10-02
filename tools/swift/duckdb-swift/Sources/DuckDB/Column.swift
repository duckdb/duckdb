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

/// A DuckDB result set column
///
/// DuckDB columns represent a vertical slice of a result set table. All DuckDB
/// columns have an underlying database type (accessed via the
/// ``underlyingDatabaseType`` member) which determine the native Swift types to
/// which the column can be cast to.
///
/// When columns are initially retrieved from a ``ResultSet`` through its
/// ``ResultSet/subscript(_:)`` accessor they have an element type of `Void`.
/// Only after a column is cast to a matching native type can its elements be
/// accessed.
///
/// For example, a column with an underlying database type of
/// ``DatabaseType/varchar`` can be cast to type `String`:
///
/// ```swift
/// // casts the first column in a result set to string
/// let column = result[0].cast(to: String.self)
/// ```
///
/// The documentation for each ``DatabaseType`` member specifies which native
/// Swift types a column may be cast to.
///
/// As a column is a Swift `Collection` type, once a column has been
/// successfully cast its elements can be accessed in the same way as any other
/// Swift collection type.
///
/// ```swift
/// for element in column {
///   print("element: \(element)")
/// }
/// ```
public struct Column<DataType> {
  
  private let result: ResultSet
  private let columnIndex: DBInt
  private let itemAt: @Sendable (DBInt) -> DataType?
  
  init(result: ResultSet, columnIndex: DBInt) where DataType == Void {
    let transformer = result.transformer(forColumn: columnIndex, to: Void.self)
    self.init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  init(result: ResultSet, columnIndex: DBInt, itemAt: @escaping @Sendable (DBInt) -> DataType?) {
    self.result = result
    self.columnIndex = columnIndex
    self.itemAt = itemAt
  }
  
  /// The name of the table column
  public var name: String {
    result.columnName(at: columnIndex)
  }
  
  /// The native Swift type to which the column has been cast
  public var dataType: DataType.Type {
    DataType.self
  }
  
  /// The underlying primitive database type of the column
  public var underlyingDatabaseType: DatabaseType {
    result.columnDataType(at: columnIndex)
  }

  /// The underlying logical type of the column
  public var underlyingLogicalType: LogicalType {
    result.columnLogicalType(at: columnIndex)
  }
}

// MARK: - Type Casting

public extension Column {
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Void.Type) -> Column<Void> {
    .init(result: result, columnIndex: columnIndex)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Bool.Type) -> Column<Bool> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Warning: Implicit conversion of a DuckDB integer column value greater
  ///   than `Int.max` or less than `Int.min` is a programmer error and will
  ///   result in a runtime precondition failure
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Int.Type) -> Column<Int> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Int8.Type) -> Column<Int8> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Int16.Type) -> Column<Int16> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Int32.Type) -> Column<Int32> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Int64.Type) -> Column<Int64> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: IntHuge.Type) -> Column<IntHuge> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Warning: Implicit conversion of a DuckDB integer column value greater
  ///   than `UInt.max` is a programmer error and will result in a runtime
  ///   precondition failure
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UInt.Type) -> Column<UInt> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UInt8.Type) -> Column<UInt8> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UInt16.Type) -> Column<UInt16> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UInt32.Type) -> Column<UInt32> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UInt64.Type) -> Column<UInt64> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Float.Type) -> Column<Float> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Double.Type) -> Column<Double> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: String.Type) -> Column<String> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: UUID.Type) -> Column<UUID> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Time.Type) -> Column<Time> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Date.Type) -> Column<Date> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Timestamp.Type) -> Column<Timestamp> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Interval.Type) -> Column<Interval> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Data.Type) -> Column<Data> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast(to type: Decimal.Type) -> Column<Decimal> {
    let transformer = result.transformer(forColumn: columnIndex, to: type)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  /// Casts the column to the given type
  ///
  /// A column cast always succeeds but if there is a type-mismatch between
  /// the given type and the column's underlying database type, returned
  /// elements will always be equal to `nil`.
  ///
  /// - Parameter type: the native Swift type to cast to
  /// - Returns: a typed DuckDB result set ``Column``
  func cast<T: Decodable>(to type: T.Type) -> Column<T> {
    let transformer = result.decodableTransformer(forColumn: columnIndex, to: T.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
}

// MARK: - Collection conformance

extension Column: RandomAccessCollection {
  
  public typealias Element = DataType?
  
  public struct Iterator: IteratorProtocol {
    
    private let column: Column
    private var position: DBInt
    
    init(column: Column) {
      self.column = column
      self.position = column.startIndex
    }
    
    public mutating func next() -> Element? {
      guard position < column.endIndex else { return nil }
      defer { position += 1 }
      return .some(column[position])
    }
  }
  
  public var startIndex: DBInt { 0 }
  public var endIndex: DBInt { result.rowCount }
  
  public subscript(position: DBInt) -> DataType? {
    itemAt(position)
  }
  
  public func makeIterator() -> Iterator {
    Iterator(column: self)
  }
  
  public func index(after i: DBInt) -> DBInt { i + 1 }
  public func index(before i: DBInt) -> DBInt { i - 1 }
}

// MARK: - Sendable conformance

extension Column: Sendable where DataType: Sendable {}

// MARK: - Identifiable conformance

extension Column: Identifiable {
  public var id: String { name }
}
