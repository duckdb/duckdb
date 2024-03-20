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

@_implementationOnly import Cduckdb

/// A date in the Gregorian calendar
///
/// A date specifies a combination of year, month and day. DuckDB follows the
/// SQL standard’s lead by counting dates exclusively in the Gregorian calendar,
/// even for years before that calendar was in use.
public struct Date: Hashable, Equatable, Codable, Sendable {
  /// days since the unix date epoch `1970-01-01`
  public var days: Int32
}

public extension Date {
  
  /// The components of ``Date`` decomposed into its constituent parts
  ///
  /// A type to facilate the conversion between nominal units of years, months
  /// and days into the underlying DuckDB ``Date`` representation of days since
  /// `1970-01-01`.
  struct Components: Hashable, Equatable {
    public var year: Int32
    public var month: Int8
    public var day: Int8
  }
  
  /// Creates a new instance from the given date components
  ///
  /// - Parameter components: the date components of the instance
  init(components: Components) {
    let cdatestruct = duckdb_to_date(duckdb_date_struct(components: components))
    self = cdatestruct.asDate
  }
  
  /// Date components
  var components: Components { Components(self) }
}

private extension Date.Components {
  
  init(_ date: Date) {
    let cdate = duckdb_date(date: date)
    let cdatestruct = duckdb_from_date(cdate)
    self = cdatestruct.asDateComponents
  }
}
