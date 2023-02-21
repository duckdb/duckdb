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

public struct Column<DataType> {
  
  private let result: QueryResult
  private let columnIndex: DBInt
  private let itemAt: (DBInt) -> DataType?
  
  init(result: QueryResult, columnIndex: DBInt) where DataType == Void {
    let transformer = result.transformer(forColumn: columnIndex, to: Void.self)
    self.init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  init(result: QueryResult, columnIndex: DBInt, itemAt: @escaping (DBInt) -> DataType?) {
    self.result = result
    self.columnIndex = columnIndex
    self.itemAt = itemAt
  }
}

// MARK: - Type Casting

public extension Column {
  
  func cast(to type: Void.Type) -> Column<Void> {
    .init(result: result, columnIndex: columnIndex)
  }
  
  func cast(to type: Bool.Type) -> Column<Bool> {
    let transformer = result.transformer(forColumn: columnIndex, to: Bool.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Int8.Type) -> Column<Int8> {
    let transformer = result.transformer(forColumn: columnIndex, to: Int8.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Int16.Type) -> Column<Int16> {
    let transformer = result.transformer(forColumn: columnIndex, to: Int16.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Int32.Type) -> Column<Int32> {
    let transformer = result.transformer(forColumn: columnIndex, to: Int32.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Int64.Type) -> Column<Int64> {
    let transformer = result.transformer(forColumn: columnIndex, to: Int64.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: IntHuge.Type) -> Column<IntHuge> {
    let transformer = result.transformer(forColumn: columnIndex, to: IntHuge.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: UInt8.Type) -> Column<UInt8> {
    let transformer = result.transformer(forColumn: columnIndex, to: UInt8.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: UInt16.Type) -> Column<UInt16> {
    let transformer = result.transformer(forColumn: columnIndex, to: UInt16.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: UInt32.Type) -> Column<UInt32> {
    let transformer = result.transformer(forColumn: columnIndex, to: UInt32.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: UInt64.Type) -> Column<UInt64> {
    let transformer = result.transformer(forColumn: columnIndex, to: UInt64.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Float.Type) -> Column<Float> {
    let transformer = result.transformer(forColumn: columnIndex, to: Float.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Double.Type) -> Column<Double> {
    let transformer = result.transformer(forColumn: columnIndex, to: Double.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: String.Type) -> Column<String> {
    let transformer = result.transformer(forColumn: columnIndex, to: String.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: UUID.Type) -> Column<UUID> {
    let transformer = result.transformer(forColumn: columnIndex, to: UUID.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Time.Type) -> Column<Time> {
    let transformer = result.transformer(forColumn: columnIndex, to: Time.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Date.Type) -> Column<Date> {
    let transformer = result.transformer(forColumn: columnIndex, to: Date.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Timestamp.Type) -> Column<Timestamp> {
    let transformer = result.transformer(forColumn: columnIndex, to: Timestamp.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Interval.Type) -> Column<Interval> {
    let transformer = result.transformer(forColumn: columnIndex, to: Interval.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Data.Type) -> Column<Data> {
    let transformer = result.transformer(forColumn: columnIndex, to: Data.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
  
  func cast(to type: Decimal.Type) -> Column<Decimal> {
    let transformer = result.transformer(forColumn: columnIndex, to: Decimal.self)
    return .init(result: result, columnIndex: columnIndex, itemAt: transformer)
  }
}

// MARK: - Collection conformance

extension Column: Collection {
  
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
