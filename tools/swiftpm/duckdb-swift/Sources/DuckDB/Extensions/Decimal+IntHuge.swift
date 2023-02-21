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

import Foundation

public extension Decimal {
  
  init(_ source: IntHuge) {
    let (lower, upper, negative) = source.absoluteUInt64sWithSign
    let mantissa: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)
    var length = UInt32(4)
    mantissa.0 = UInt16(truncatingIfNeeded: lower >> 0)
    mantissa.1 = UInt16(truncatingIfNeeded: lower >> 16)
    mantissa.2 = UInt16(truncatingIfNeeded: lower >> 32)
    mantissa.3 = UInt16(truncatingIfNeeded: lower >> 48)
    if upper > 0 {
      length = 8
      mantissa.4 = UInt16(truncatingIfNeeded: upper >> 0)
      mantissa.5 = UInt16(truncatingIfNeeded: upper >> 16)
      mantissa.6 = UInt16(truncatingIfNeeded: upper >> 32)
      mantissa.7 = UInt16(truncatingIfNeeded: upper >> 48)
    }
    else {
      mantissa.4 = 0
      mantissa.5 = 0
      mantissa.6 = 0
      mantissa.7 = 0
    }
    self = .init(
      _exponent: 0,
      _length: length,
      _isNegative: negative ? 1 : 0,
      _isCompact: 0,
      _reserved: 0,
      _mantissa: mantissa
    )
  }
}

fileprivate extension IntHuge {
  
  var absoluteUInt64sWithSign: (lower: UInt64, upper: UInt64, negative: Bool) {
    if upper < 0 {
      let lower = ~self.lower &+ 1
      var upper = UInt64(~self.upper)
      if lower == 0 { upper += 1 }
      return (lower, upper, true)
    }
    else {
      return (lower, UInt64(upper), false)
    }
  }
}
