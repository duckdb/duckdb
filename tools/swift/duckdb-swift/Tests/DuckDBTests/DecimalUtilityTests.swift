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
}

