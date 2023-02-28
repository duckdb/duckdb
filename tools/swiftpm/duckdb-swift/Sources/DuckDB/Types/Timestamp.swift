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

public struct Timestamp: Hashable, Equatable {
  var microseconds: Int64
}

public extension Timestamp {
  
  struct Components: Hashable, Equatable {
    public var date: Date.Components
    public var time: Time.Components
  }
  
  init(components: Components) {
    let ctimestampstruct = duckdb_to_timestamp(duckdb_timestamp_struct(components: components))
    self = ctimestampstruct.asTimestamp
  }
  
  var components: Components { Components(self) }
}

public extension Timestamp.Components {
  
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
