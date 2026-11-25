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
import XCTest
@testable import DuckDB

final class DecimalUtilityTests: XCTestCase {
  
  func test_decimal_int_huge_min() throws {
    let minInt128 = Decimal(string: "\(IntHuge.min)")!
    XCTAssertEqual(Decimal(IntHuge.min), minInt128)
  }
  
  func test_decimal_int_huge_max() throws {
    let maxInt128 = Decimal(string: "\(IntHuge.max)")!
    XCTAssertEqual(Decimal(IntHuge.max), maxInt128)
  }
  
  func test_decimal_uint_huge_min() throws {
    let minUInt128 = Decimal(string: "\(UIntHuge.min)")!
    XCTAssertEqual(Decimal(UIntHuge.min), minUInt128)
  }
  
  func test_decimal_uint_huge_max() throws {
    let maxUInt128 = Decimal(string: "\(UIntHuge.max)")!
    XCTAssertEqual(Decimal(UIntHuge.max), maxUInt128)
  }

  /// Values that set every 16-bit word of the 128 bits, and their negations
  func test_decimal_huge_digits() throws {
    let values: [UIntHuge] = [
      UIntHuge(UInt64(0x0123_4567_89AB_CDEF)) << 64 + UIntHuge(UInt64(0xFEDC_BA98_7654_3210)),
      UIntHuge(1) << 64,
      UIntHuge(1) << 64 - 1,
      UIntHuge(UInt64.max) << 64,
      UIntHuge(IntHuge.max),
    ]
    for value in values {
      XCTAssertEqual(Decimal(value).description, "\(value)")
      XCTAssertEqual(Decimal(value), Decimal(string: "\(value)"))
      let signed = IntHuge(truncatingIfNeeded: value)
      XCTAssertEqual(Decimal(signed).description, "\(signed)")
      XCTAssertEqual(Decimal(-1 - IntHuge(value >> 1)).description, "\(-1 - IntHuge(value >> 1))")
    }
  }

  /// Values of up to 64 bits take a shorter path than wider ones
  func test_decimal_huge_around_64_bits() throws {
    let twoToThe64th = UIntHuge(1) << 64
    let magnitudes: [UIntHuge] = [
      0, 1, UIntHuge(UInt64.max) - 1, twoToThe64th - 1, twoToThe64th, twoToThe64th + 1,
      twoToThe64th + UIntHuge(UInt64.max), twoToThe64th << 1, (twoToThe64th << 1) - 1, (twoToThe64th << 1) + 1,
    ]
    for magnitude in magnitudes {
      XCTAssertEqual(Decimal(magnitude).description, "\(magnitude)")
      XCTAssertEqual(Decimal(magnitude), Decimal(string: "\(magnitude)"))
      let positive = IntHuge(magnitude)
      XCTAssertEqual(Decimal(positive).description, "\(positive)")
      let negative = -positive
      XCTAssertEqual(Decimal(negative).description, "\(negative)")
      XCTAssertEqual(Decimal(negative), Decimal(string: "\(negative)"))
      XCTAssertEqual(Decimal(negative).isSignMinus, magnitude != 0)
    }
  }
}

