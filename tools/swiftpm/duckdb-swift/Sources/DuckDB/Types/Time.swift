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

public struct Time: Hashable, Equatable {
  var microseconds: Int64
}

public extension Time {
  
  struct Components: Hashable, Equatable {
    public var hour: Int8
    public var minute: Int8
    public var second: Int8
    public var microsecond: Int32
  }
  
  init(components: Components) {
    let ctimestruct = duckdb_to_time(duckdb_time_struct(components: components))
    self = ctimestruct.asTime
  }
  
  var components: Components { Components(self) }
}

public extension Time.Components {
  
  init(_ time: Time) {
    let ctime = duckdb_time(time: time)
    let ctimestruct = duckdb_from_time(ctime)
    self = ctimestruct.asTimeComponents
  }
}
