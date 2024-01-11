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

#if canImport(TabularData)

import TabularData

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
public extension TabularData.Column {
  
  /// Creates a tabular data column initialized from a DuckDB column
  init(_ column: DuckDB.Column<WrappedElement>) {
    self.init(name: column.name, contents: column)
  }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
public extension TabularData.AnyColumn {
  
  /// Creates a type-erased tabular data column from a DuckDB column
  init<T>(_ column: DuckDB.Column<T>) {
    self = TabularData.Column(column).eraseToAnyColumn()
  }
}

#endif
