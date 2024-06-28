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

import Foundation

public extension Decimal {
  
  init(_ source: IntHuge) {
    let magnitude =  source.magnitude
    let mantissa: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)
    let significantBitCount = magnitude.bitWidth - magnitude.leadingZeroBitCount
    let length = (significantBitCount + (UInt16.bitWidth - 1)) / UInt16.bitWidth
    mantissa.0 = UInt16(truncatingIfNeeded: magnitude >> (0 * 16))
    mantissa.1 = UInt16(truncatingIfNeeded: magnitude >> (1 * 16))
    mantissa.2 = UInt16(truncatingIfNeeded: magnitude >> (2 * 16))
    mantissa.3 = UInt16(truncatingIfNeeded: magnitude >> (3 * 16))
    mantissa.4 = UInt16(truncatingIfNeeded: magnitude >> (4 * 16))
    mantissa.5 = UInt16(truncatingIfNeeded: magnitude >> (5 * 16))
    mantissa.6 = UInt16(truncatingIfNeeded: magnitude >> (6 * 16))
    mantissa.7 = UInt16(truncatingIfNeeded: magnitude >> (7 * 16))
    self = .init(
      _exponent: 0,
      _length: UInt32(length),
      _isNegative: source.signum() < 0 ? 1 : 0,
      _isCompact: 0,
      _reserved: 0,
      _mantissa: mantissa
    )
  }
  
  init(_ source: UIntHuge) {
    let mantissa: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)
    let significantBitCount = source.bitWidth - source.leadingZeroBitCount
    let length = (significantBitCount + (UInt16.bitWidth - 1)) / UInt16.bitWidth
    mantissa.0 = UInt16(truncatingIfNeeded: source >> (0 * 16))
    mantissa.1 = UInt16(truncatingIfNeeded: source >> (1 * 16))
    mantissa.2 = UInt16(truncatingIfNeeded: source >> (2 * 16))
    mantissa.3 = UInt16(truncatingIfNeeded: source >> (3 * 16))
    mantissa.4 = UInt16(truncatingIfNeeded: source >> (4 * 16))
    mantissa.5 = UInt16(truncatingIfNeeded: source >> (5 * 16))
    mantissa.6 = UInt16(truncatingIfNeeded: source >> (6 * 16))
    mantissa.7 = UInt16(truncatingIfNeeded: source >> (7 * 16))
    self = .init(
      _exponent: 0,
      _length: UInt32(length),
      _isNegative: 0,
      _isCompact: 0,
      _reserved: 0,
      _mantissa: mantissa
    )
  }
}
