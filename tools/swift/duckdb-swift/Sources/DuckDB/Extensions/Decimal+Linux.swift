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

#if os(Linux)

import Foundation

public typealias Mantissa = (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)

extension Decimal {
  public init(
    _exponent: Int32 = 0,
    _length: UInt32,
    _isNegative: UInt32 = 0,
    _isCompact: UInt32,
    _reserved: UInt32 = 0,
    _mantissa: Mantissa
  ) {
    let length: UInt8 = (UInt8(truncatingIfNeeded: _length) & 0xF) << 4
    let isNegative: UInt8 = UInt8(truncatingIfNeeded: _isNegative & 0x1) == 0 ? 0 : 0b00001000
    let isCompact: UInt8 = UInt8(truncatingIfNeeded: _isCompact & 0x1) == 0 ? 0 : 0b00000100
    let reservedLeft: UInt8 = UInt8(truncatingIfNeeded: (_reserved & 0x3FFFF) >> 16)
    self = unsafeBitCast(
      UnsafeDecimal(
        storage: .init(
          exponent: Int8(truncatingIfNeeded: _exponent),
          lengthFlagsAndReserved: length | isNegative | isCompact | reservedLeft,
          reserved: UInt16(truncatingIfNeeded: _reserved & 0xFFFF),
          mantissa: _mantissa
        )
      ),
      to: Decimal.self
    )
  }

  public var _length: UInt32 {
    UInt32(self.storage.lengthFlagsAndReserved >> 4)
  }

  public var _mantissa: Mantissa {
    self.storage.mantissa
  }

  private struct UnsafeDecimal {
    struct Storage {
      var exponent: Int8
      var lengthFlagsAndReserved: UInt8
      var reserved: UInt16
      var mantissa: Mantissa
    }

    var storage: Storage
  }

  private var storage: UnsafeDecimal.Storage {
    unsafeBitCast(self, to: UnsafeDecimal.self).storage
  }
}

#endif
