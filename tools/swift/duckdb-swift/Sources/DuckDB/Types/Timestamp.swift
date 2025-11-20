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

/// A point in absolute time
///
/// Timestamps represent points in absolute time, usually called instants.
/// DuckDB represents instants as the number of microseconds (µs) since
/// `1970-01-01 00:00:00+00`.
public struct Timestamp: Hashable, Equatable, Codable, Sendable {
  /// microseconds (µs) since `1970-01-01 00:00:00+00`.
  public var microseconds: Int64
}

public extension Timestamp {
  
  /// The components of a ``Timestamp`` decomposed into its constituent parts
  ///
  /// A type to facilate the conversion between nominal units of year, month,
  /// day, hours, minutes, seconds and microseconds into the underlying DuckDB
  /// ``Timestamp`` representation of microseconds (µs) since
  /// `1970-01-01 00:00:00+00`.
  struct Components: Hashable, Equatable {
    /// Date components
    public var date: Date.Components
    /// Time components
    public var time: Time.Components
  }
  
  /// Creates a new instance from the given timestamp components
  ///
  /// - Parameter components: the components of the timestamp to be instantiated
  init(components: Components) {
    let ctimestampstruct = duckdb_to_timestamp(duckdb_timestamp_struct(components: components))
    self = ctimestampstruct.asTimestamp
  }
  
  /// Timestamp components
  var components: Components { Components(self) }
}

private extension Timestamp.Components {
  
  init(_ timestamp: Timestamp) {
    let ctimestamp = duckdb_timestamp(timestamp: timestamp)
    let ctimestampstruct = duckdb_from_timestamp(ctimestamp)
    self = ctimestampstruct.asTimestampComponents
  }
}

extension Timestamp.Components {
  
  init(
    year: Int32,
    month: Int8,
    day: Int8,
    hour: Int8,
    minute: Int8,
    second: Int8,
    microsecond: Int32
  ) {
    self.date = .init(year: year, month: month, day: day)
    self.time = .init(hour: hour, minute: minute, second: second, microsecond: microsecond)
  }
}
