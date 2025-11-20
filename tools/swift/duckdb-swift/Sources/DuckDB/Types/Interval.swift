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

#if compiler(>=6.0)
internal import Cduckdb
#else
@_implementationOnly import Cduckdb
#endif

/// A period of time
///
/// Intervals represent a period of time. This period can be measured in a
/// variety of units, for example years, days, or seconds.
public struct Interval: Hashable, Equatable, Sendable {
  public var months: Int32
  public var days: Int32
  public var microseconds: Int64
}

extension Interval {
  
  init(
    years: Int32,
    months: Int32,
    days: Int32,
    hours: Int32,
    minutes: Int32,
    seconds: Int32,
    microseconds: Int64
  ) {
    let hours_ms = Int64(hours) * 60 * 60 * 1_000_000
    let minutes_ms = Int64(minutes) * 60 * 1_000_000
    let seconds_ms = Int64(seconds) * 1_000_000
    self.init(
      months: (years * 12) + months,
      days: days,
      microseconds: hours_ms + minutes_ms + seconds_ms + microseconds
    )
  }
}
