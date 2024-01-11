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

/// A point in absolute time
///
/// Time represents points in absolute time, usually called instants.
/// DuckDB represents instants as the number of microseconds (µs) since
/// `1970-01-01 00:00:00+00`.
public struct Time: Hashable, Equatable, Sendable {
  /// microseconds (µs) since `1970-01-01 00:00:00+00`.
  public var microseconds: Int64
}

public extension Time {
  
  /// The components of ``Time`` decomposed into its constituent parts
  ///
  /// A type to facilate the conversion between nominal units of hours,
  /// minutes, seconds and microseconds into the underlying DuckDB
  /// ``Time`` representation of microseconds (µs) since
  /// `1970-01-01 00:00:00+00`.
  struct Components: Hashable, Equatable {
    public var hour: Int8
    public var minute: Int8
    public var second: Int8
    public var microsecond: Int32
  }
  
  /// Creates a new instance from the given time components
  ///
  /// - Parameter components: the time components of the instance
  init(components: Components) {
    let ctimestruct = duckdb_to_time(duckdb_time_struct(components: components))
    self = ctimestruct.asTime
  }
  
  /// Time components
  var components: Components { Components(self) }
}

private extension Time.Components {
  
  init(_ time: Time) {
    let ctime = duckdb_time(time: time)
    let ctimestruct = duckdb_from_time(ctime)
    self = ctimestruct.asTimeComponents
  }
}
