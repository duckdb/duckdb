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

final class WideDecimalTests: XCTestCase {

  // 2^128 - 1: the largest significand of a Foundation Decimal
  static let largestSignificand = "340282366920938463463374607431768211455"
  // 2^128
  static let smallestOverflow = "340282366920938463463374607431768211456"

  // MARK: - Reading

  func test_default_session_arithmetic_result() throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT 1.5::DECIMAL(38,2) + 1.5::DECIMAL(38,2)")
    XCTAssertEqual(result[0].underlyingDatabaseType, .decimal)
    let properties = try XCTUnwrap(result[0].underlyingLogicalType.decimalProperties)
    XCTAssertEqual(properties.width, 39)
    XCTAssertEqual(properties.scale, 2)
    XCTAssertEqual(properties.storageType, .bignum)
    XCTAssertEqual(result[0].cast(to: Decimal.self)[0], Decimal(string: "3.00"))
    XCTAssertEqual(result[0].cast(to: String.self)[0], "3.00")
  }

  /// A session with `max_decimal_width = 38` still takes a bound value of
  /// more than 38 digits exactly, as a wide DECIMAL; the limit applies to
  /// what the query then computes from it.
  func test_bind_wide_value_in_compatibility_session() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("SET max_decimal_width = 38")
    let wide: [(value: Decimal, type: String, text: String)] = [
      // a significand of 39 digits
      (Decimal(string: "170141183460469231731687303715884105727")!, "DECIMAL(39,0)",
       "170141183460469231731687303715884105727"),
      (Decimal(sign: .minus, exponent: 38, significand: 1), "DECIMAL(39,0)",
       "-1" + String(repeating: "0", count: 38)),
      // more than 38 fractional digits
      (Decimal(sign: .plus, exponent: -40, significand: 1), "DECIMAL(40,40)",
       "." + String(repeating: "0", count: 39) + "1"),
    ]
    let select = try PreparedStatement(connection: connection, query: "SELECT $1, typeof($1), ($1)::VARCHAR")
    let addition = try PreparedStatement(connection: connection, query: "SELECT $1 + 1")
    for (value, type, text) in wide {
      try select.bind(value, at: 1)
      let result = try select.execute()
      XCTAssertEqual(result[1].cast(to: String.self)[0], type)
      XCTAssertEqual(result[2].cast(to: String.self)[0], text)
      XCTAssertEqual(result[0].cast(to: String.self)[0], text)
      XCTAssertEqual(result[0].cast(to: Decimal.self)[0], value)
      // arithmetic that needs a wider decimal is refused by the database
      try addition.bind(value, at: 1)
      XCTAssertThrowsError(try addition.execute(), "\(value)") { error in
        guard case DatabaseError.preparedStatementQueryError(let reason) = error else {
          return XCTFail("unexpected error: \(error)")
        }
        XCTAssertTrue(reason?.contains("the maximum DECIMAL width is 38") == true, "\(String(describing: reason))")
      }
    }
    // a value of up to 38 digits binds as before
    try select.bind(Decimal(string: "-1.5")!, at: 1)
    let narrow = try select.execute()
    XCTAssertEqual(narrow[1].cast(to: String.self)[0], "DECIMAL(38,1)")
    XCTAssertEqual(narrow[0].cast(to: Decimal.self)[0], Decimal(string: "-1.5"))
    // a value that does not fit the target column is refused, not truncated
    try connection.execute("CREATE TABLE t1(d DECIMAL(38,10))")
    let insert = try PreparedStatement(connection: connection, query: "INSERT INTO t1 VALUES ($1)")
    try insert.bind(wide[0].value, at: 1)
    XCTAssertThrowsError(try insert.execute()) { error in
      guard case DatabaseError.preparedStatementQueryError(let reason) = error else {
        return XCTFail("unexpected error: \(error)")
      }
      XCTAssertTrue(reason?.contains("value is out of range") == true, "\(String(describing: reason))")
    }
    XCTAssertEqual(try connection.query("SELECT count(*) FROM t1")[0].cast(to: Int.self)[0], 0)
  }

  func test_compatibility_session_arithmetic_result() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("SET max_decimal_width = 38")
    let result = try connection.query("SELECT 1.5::DECIMAL(38,2) + 1.5::DECIMAL(38,2)")
    let properties = try XCTUnwrap(result[0].underlyingLogicalType.decimalProperties)
    XCTAssertEqual(properties.width, 38)
    XCTAssertEqual(properties.storageType, .hugeint)
    XCTAssertEqual(result[0].cast(to: Decimal.self)[0], Decimal(string: "3.00"))
    XCTAssertEqual(result[0].cast(to: String.self)[0], "3.00")
  }

  func test_exact_digits_as_string() throws {
    let nines = String(repeating: "9", count: 300)
    let cases: [(width: Int, scale: Int, value: String)] = [
      (39, 0, String(nines.prefix(39))),
      (39, 0, "-" + String(nines.prefix(39))),
      (39, 2, "-0.05"),
      // like the engine, no zero in front of the decimal point of a type without integer digits
      (39, 39, "." + String(nines.prefix(39))),
      (40, 2, "0"),
      (76, 10, String(nines.prefix(66)) + "." + String(nines.prefix(10))),
      (76, 10, "-1234567890123456789012345678901234567890.0123456789"),
      (77, 3, String(nines.prefix(74)) + "." + String(nines.prefix(3))),
      (77, 3, "-" + String(nines.prefix(74)) + "." + String(nines.prefix(3))),
      (255, 0, String(nines.prefix(255))),
      (255, 100, "-" + String(nines.prefix(155)) + "." + String(nines.prefix(100))),
      (255, 255, "-." + String(repeating: "0", count: 254) + "1"),
    ]
    let connection = try Database(store: .inMemory).connect()
    for (width, scale, value) in cases {
      let type = "DECIMAL(\(width),\(scale))"
      let result = try connection.query("SELECT v, v::VARCHAR FROM (SELECT '\(value)'::\(type) AS v)")
      XCTAssertEqual(result[0].underlyingDatabaseType, .decimal, type)
      let properties = try XCTUnwrap(result[0].underlyingLogicalType.decimalProperties)
      XCTAssertEqual(Int(properties.width), width, type)
      XCTAssertEqual(Int(properties.scale), scale, type)
      let text = try XCTUnwrap(result[0].cast(to: String.self).element(at: 0), type)
      // the engine's own rendering is the oracle
      XCTAssertEqual(text, result[1].cast(to: String.self)[0], type)
      // every digit that went in comes out
      XCTAssertEqual(text.filter { $0 != "0" && $0 != "." }, value.filter { $0 != "0" && $0 != "." }, type)
    }
  }

  func test_values_that_fit_decimal_exactly() throws {
    let cases: [(type: String, value: String)] = [
      ("DECIMAL(39,2)", "3.00"),
      ("DECIMAL(39,2)", "-0.05"),
      ("DECIMAL(39,0)", Self.largestSignificand),
      ("DECIMAL(39,0)", "-" + Self.largestSignificand),
      ("DECIMAL(50,11)", "-3402823669209384634633746074.31768211455"),
      ("DECIMAL(76,10)", "0"),
      ("DECIMAL(76,10)", "-12345678901234567890.0123456789"),
      ("DECIMAL(77,3)", "1" + String(repeating: "0", count: 70)),
      ("DECIMAL(255,100)", "-" + Self.largestSignificand + String(repeating: "0", count: 100)),
      ("DECIMAL(255,128)", "0." + String(repeating: "0", count: 127) + "1"),
      ("DECIMAL(255,200)", "0"),
    ]
    let connection = try Database(store: .inMemory).connect()
    for (type, value) in cases {
      let result = try connection.query("SELECT '\(value)'::\(type)")
      let decimal = try XCTUnwrap(result[0].cast(to: Decimal.self).element(at: 0), "\(type) \(value)")
      XCTAssertEqual(decimal, result[0].cast(to: Decimal.self)[0])
      // compare through the digits of the Decimal: an independent check of the conversion
      XCTAssertEqual(Self.plainString(decimal), Self.trimmed(value), "\(type) \(value)")
      XCTAssertEqual(decimal.isSignMinus, value.hasPrefix("-"))
    }
  }

  /// The exponent of a `Decimal` ends at 127, and Foundation keeps trailing
  /// zeros in the exponent: a value with up to 127 trailing zeros converts,
  /// one with more does not
  func test_values_with_many_trailing_zeros() throws {
    let fitting: [(type: String, value: String, expected: Decimal)] = [
      ("DECIMAL(128,0)", "1" + String(repeating: "0", count: 127),
       Decimal(sign: .plus, exponent: 127, significand: 1)),
      ("DECIMAL(130,0)", "-907" + String(repeating: "0", count: 127),
       Decimal(sign: .minus, exponent: 127, significand: 907)),
      // the significand uses all of the 128 bits
      ("DECIMAL(166,0)", Self.largestSignificand + String(repeating: "0", count: 127),
       Decimal(sign: .plus, exponent: 127, significand: Decimal(string: Self.largestSignificand)!)),
      ("DECIMAL(140,2)", "-123456789" + String(repeating: "0", count: 127) + ".00",
       Decimal(sign: .minus, exponent: 127, significand: 123_456_789)),
    ]
    let connection = try Database(store: .inMemory).connect()
    for (type, value, expected) in fitting {
      let result = try connection.query("SELECT '\(value)'::\(type)")
      let decimal = try XCTUnwrap(result[0].cast(to: Decimal.self).element(at: 0), "\(type) \(value)")
      XCTAssertFalse(decimal.isNaN, "\(type) \(value)")
      XCTAssertEqual(decimal, expected, "\(type) \(value)")
      XCTAssertEqual(decimal.isSignMinus, value.hasPrefix("-"))
      XCTAssertEqual(Self.plainString(decimal), Self.trimmed(value), "\(type) \(value)")
      XCTAssertEqual(result[0].cast(to: String.self)[0], value)
    }
    let notFitting: [(type: String, value: String)] = [
      ("DECIMAL(129,0)", "1" + String(repeating: "0", count: 128)),
      ("DECIMAL(131,0)", "1" + String(repeating: "0", count: 130)),
      ("DECIMAL(131,0)", "-1" + String(repeating: "0", count: 130)),
      ("DECIMAL(138,0)", "-123456789" + String(repeating: "0", count: 129)),
      ("DECIMAL(165,0)", String(Self.largestSignificand.prefix(36)) + String(repeating: "0", count: 129)),
    ]
    for (type, value) in notFitting {
      let result = try connection.query("SELECT '\(value)'::\(type)")
      let column = result[0].cast(to: Decimal.self)
      XCTAssertNil(column[0], "\(type) \(value)")
      XCTAssertThrowsError(try column.element(at: 0), "\(type) \(value)") { error in
        guard case DatabaseError.decimalNotConvertible(let reason) = error else {
          return XCTFail("unexpected error: \(error)")
        }
        XCTAssertEqual(reason?.contains(value), true, reason ?? "")
      }
      XCTAssertEqual(try result[0].cast(to: String.self).element(at: 0), value)
    }
  }

  func test_values_that_do_not_fit_decimal() throws {
    let cases: [(type: String, value: String)] = [
      ("DECIMAL(39,0)", Self.smallestOverflow),
      ("DECIMAL(39,0)", "-" + Self.smallestOverflow),
      ("DECIMAL(40,1)", "-" + Self.smallestOverflow + ".1"),
      ("DECIMAL(76,10)", String(repeating: "9", count: 66) + ".9999999999"),
      ("DECIMAL(77,3)", "-" + String(repeating: "7", count: 74) + ".777"),
      // more than 127 trailing zeros and too many digits to shift
      ("DECIMAL(255,0)", "123456789012345678901234567890" + String(repeating: "0", count: 140)),
      // exponent below -128
      ("DECIMAL(255,129)", "0." + String(repeating: "0", count: 128) + "1"),
    ]
    let connection = try Database(store: .inMemory).connect()
    for (type, value) in cases {
      let result = try connection.query("SELECT '\(value)'::\(type)")
      let column = result[0].cast(to: Decimal.self)
      XCTAssertNil(column[0], "\(type) \(value)")
      XCTAssertThrowsError(try column.element(at: 0), "\(type) \(value)") { error in
        guard case DatabaseError.decimalNotConvertible(let reason) = error else {
          return XCTFail("unexpected error: \(error)")
        }
        // the error names the exact value
        XCTAssertEqual(reason?.contains(value), true, reason ?? "")
      }
      XCTAssertEqual(try result[0].cast(to: String.self).element(at: 0), value)
    }
  }

  func test_null_values() throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("""
      SELECT v::DECIMAL(60,4) FROM (VALUES ('-1.5'), (NULL), ('\(Self.smallestOverflow)'), (NULL)) t(v)
      """)
    let decimals = result[0].cast(to: Decimal.self)
    let strings = result[0].cast(to: String.self)
    XCTAssertEqual(Array(decimals), [Decimal(string: "-1.5"), nil, nil, nil])
    XCTAssertEqual(Array(strings), ["-1.5000", nil, Self.smallestOverflow + ".0000", nil])
    XCTAssertNil(try decimals.element(at: 1))
    XCTAssertNil(try strings.element(at: 1))
    XCTAssertThrowsError(try decimals.element(at: 2))
    XCTAssertNil(try decimals.element(at: 3))
    XCTAssertEqual(Array(result[0].cast(to: Void.self)).map { $0 == nil }, [false, true, false, true])
  }

  func test_width_above_255() throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT v, v::VARCHAR FROM (SELECT '-1.5'::DECIMAL(300,2) AS v)")
    XCTAssertEqual(result[0].underlyingDatabaseType, .decimal)
    // the C API reports neither width nor scale of such a type
    let properties = try XCTUnwrap(result[0].underlyingLogicalType.decimalProperties)
    XCTAssertEqual(properties.width, 0)
    XCTAssertEqual(properties.storageType, .bignum)
    XCTAssertNil(result[0].cast(to: Decimal.self)[0])
    XCTAssertNil(result[0].cast(to: String.self)[0])
    XCTAssertThrowsError(try result[0].cast(to: Decimal.self).element(at: 0)) { error in
      guard case DatabaseError.decimalNotConvertible = error else {
        return XCTFail("unexpected error: \(error)")
      }
    }
    XCTAssertThrowsError(try result[0].cast(to: String.self).element(at: 0)) { error in
      guard case DatabaseError.decimalNotConvertible = error else {
        return XCTFail("unexpected error: \(error)")
      }
    }
    // a cast in SQL reaches the digits
    XCTAssertEqual(result[1].cast(to: String.self)[0], "-1.50")
  }

  func test_many_rows() throws {
    // unscaled values from one byte up to past the size that is stored inline
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("""
      SELECT v, v::VARCHAR, i FROM (
        SELECT i, CASE WHEN i % 7 = 0 THEN NULL
          ELSE ((CASE WHEN i % 2 = 0 THEN '-' ELSE '' END) || (i % 9 + 1)::VARCHAR
            || repeat('0', (i % 60)::BIGINT) || '.' || (i % 1000)::VARCHAR)::DECIMAL(70,3) END AS v
        FROM range(5000) r(i)
      ) ORDER BY i
      """)
    let strings = result[0].cast(to: String.self)
    let decimals = result[0].cast(to: Decimal.self)
    let expected = result[1].cast(to: String.self)
    XCTAssertEqual(result.rowCount, 5000)
    for row in 0..<result.rowCount {
      XCTAssertEqual(strings[row], expected[row])
      guard let text = expected[row] else {
        XCTAssertNil(decimals[row])
        continue
      }
      // at most 39 digits apart from trailing integer zeros: Decimal(string:) parses it exactly
      if Self.trimmed(text).filter(\.isNumber).count <= 38 {
        XCTAssertEqual(decimals[row], Decimal(string: text), text)
      }
      else if let decimal = decimals[row] {
        XCTAssertEqual(Self.plainString(decimal), Self.trimmed(text))
      }
    }
  }

  func test_narrow_decimals_as_string() throws {
    let connection = try Database(store: .inMemory).connect()
    for column in ["dec_4_1", "dec_9_4", "dec_18_6", "dec38_10"] {
      let result = try connection.query("SELECT \(column), \(column)::VARCHAR FROM test_all_types()")
      XCTAssertEqual(Array(result[0].cast(to: String.self)), Array(result[1].cast(to: String.self)), column)
      XCTAssertEqual(result[0].cast(to: String.self)[2], nil)
    }
    let result = try connection.query("SELECT v, v::VARCHAR FROM (SELECT '-0.050'::DECIMAL(5,3) AS v)")
    XCTAssertEqual(result[0].cast(to: String.self)[0], "-0.050")
    let fraction = try connection.query("SELECT v, v::VARCHAR FROM (SELECT '-0.5'::DECIMAL(5,5) AS v)")
    XCTAssertEqual(fraction[0].cast(to: String.self)[0], fraction[1].cast(to: String.self)[0])
    XCTAssertEqual(fraction[0].cast(to: String.self)[0], "-.50000")
  }

  struct Amount: Equatable, Decodable {
    let text: String
  }

  func test_decodable_struct_members() throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("""
      SELECT {'text': v} FROM (
        SELECT '-1234567890123456789012345678901234567.50'::DECIMAL(45,2) AS v
        UNION ALL SELECT '\(Self.smallestOverflow).25'::DECIMAL(45,2)
      )
      """)
    let column = result[0].cast(to: Amount.self)
    XCTAssertEqual(column[0], Amount(text: "-1234567890123456789012345678901234567.50"))
    XCTAssertEqual(column[1], Amount(text: Self.smallestOverflow + ".25"))
  }

  // MARK: - Storage decoding

  func test_storage_decoding() throws {
    let zero = try WideDecimal(bignumStorage: Data([0x80, 0x00, 0x01, 0x00]))
    XCTAssertEqual(zero.string(width: 40, scale: 0), "0")
    XCTAssertEqual(zero.string(width: 40, scale: 3), "0.000")
    XCTAssertEqual(zero.decimal(scale: 250), Decimal(0))
    let positive = try WideDecimal(bignumStorage: Data([0x80, 0x00, 0x02, 0x01, 0x00]))
    XCTAssertEqual(positive.string(width: 40, scale: 0), "256")
    XCTAssertEqual(positive.string(width: 40, scale: 5), "0.00256")
    XCTAssertEqual(positive.decimal(scale: 5), Decimal(string: "0.00256"))
    let negative = try WideDecimal(bignumStorage: Data([0x7F, 0xFF, 0xFD, 0xFE, 0xFF]))
    XCTAssertEqual(negative.string(width: 40, scale: 0), "-256")
    XCTAssertEqual(negative.string(width: 40, scale: 2), "-2.56")
    XCTAssertEqual(negative.decimal(scale: 2), Decimal(string: "-2.56"))
    for malformed in [Data(), Data([0x80, 0x00, 0x01]), Data([0x80, 0x00, 0x02, 0x01])] {
      XCTAssertThrowsError(try WideDecimal(bignumStorage: malformed)) { error in
        guard case DatabaseError.decimalNotConvertible = error else {
          return XCTFail("unexpected error: \(error)")
        }
      }
    }
  }

  // MARK: - Writing

  func test_bind_small_fraction() throws {
    // 39 to 128 fractional digits: more than a DECIMAL of 38 digits holds
    let values = [
      Decimal(sign: .plus, exponent: -50, significand: 1),
      Decimal(sign: .minus, exponent: -128, significand: 123456789),
      Decimal(sign: .plus, exponent: -39, significand: Decimal(string: "12345678901234567890123456789012345678")!),
    ]
    let expected = [
      "." + String(repeating: "0", count: 49) + "1",
      "-." + String(repeating: "0", count: 119) + "123456789",
      ".012345678901234567890123456789012345678",
    ]
    let connection = try Database(store: .inMemory).connect()
    let statement = try PreparedStatement(connection: connection, query: "SELECT $1, ($1)::VARCHAR")
    for (value, text) in zip(values, expected) {
      try statement.bind(value, at: 1)
      let result = try statement.execute()
      XCTAssertEqual(result[0].underlyingDatabaseType, .decimal)
      XCTAssertEqual(result[1].cast(to: String.self)[0], text)
      XCTAssertEqual(result[0].cast(to: String.self)[0], text)
      XCTAssertEqual(result[0].cast(to: Decimal.self)[0], value)
    }
  }

  func test_bind_positive_exponent() throws {
    let connection = try Database(store: .inMemory).connect()
    let statement = try PreparedStatement(connection: connection, query: "SELECT $1, ($1)::VARCHAR")
    try statement.bind(Decimal(sign: .minus, exponent: 3, significand: 5), at: 1)
    var result = try statement.execute()
    XCTAssertEqual(result[1].cast(to: String.self)[0], "-5000")
    XCTAssertEqual(result[0].cast(to: Decimal.self)[0], Decimal(-5000))
    // 39 digits: one more than the widest fixed-size DECIMAL
    try statement.bind(Decimal(sign: .plus, exponent: 38, significand: 1), at: 1)
    result = try statement.execute()
    XCTAssertEqual(result[1].cast(to: String.self)[0], "1" + String(repeating: "0", count: 38))
    XCTAssertEqual(result[0].cast(to: Decimal.self)[0], Decimal(sign: .plus, exponent: 38, significand: 1))
    XCTAssertEqual(result[0].underlyingLogicalType.decimalProperties?.width, 39)
  }

  func test_bind_keeps_every_digit_or_throws() throws {
    let connection = try Database(store: .inMemory).connect()
    let statement = try PreparedStatement(connection: connection, query: "SELECT $1")
    // trailing zeros are not digits that can be lost: the value stays within 38 digits
    let small = Decimal(sign: .plus, exponent: -40, significand: 1_000_000)
    try statement.bind(small, at: 1)
    let result = try statement.execute()
    XCTAssertEqual(result[0].underlyingLogicalType.decimalProperties?.width, 38)
    XCTAssertEqual(result[0].cast(to: Decimal.self)[0], small)
    let unrepresentable = [
      // the unscaled value is handed over as a signed 128-bit integer
      Decimal(sign: .plus, exponent: -8, significand: Decimal(string: Self.largestSignificand)!),
      Decimal(sign: .minus, exponent: 0, significand: Decimal(string: Self.largestSignificand)!),
      Decimal(sign: .plus, exponent: 39, significand: 1),
      Decimal(sign: .plus, exponent: 127, significand: 1),
    ]
    for value in unrepresentable {
      XCTAssertThrowsError(try statement.bind(value, at: 1), "\(value)") { error in
        guard case DatabaseError.decimalUnrepresentable = error else {
          return XCTFail("unexpected error: \(error)")
        }
      }
    }
  }

  func test_bind_into_wide_column() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("CREATE TABLE t1(d DECIMAL(60,20))")
    let statement = try PreparedStatement(connection: connection, query: "INSERT INTO t1 VALUES ($1)")
    let values = [Decimal(string: "-12345678901234567.890123456789"), Decimal(string: "0.00000000000000000001"), nil]
    for value in values {
      try statement.bind(value, at: 1)
      _ = try statement.execute()
    }
    let result = try connection.query("SELECT d FROM t1")
    XCTAssertEqual(Array(result[0].cast(to: Decimal.self)), values)
    XCTAssertEqual(Array(result[0].cast(to: String.self)), [
      "-12345678901234567.89012345678900000000", "0.00000000000000000001", nil,
    ])
  }

  func test_append_into_wide_column() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("CREATE TABLE t1(d DECIMAL(50,5))")
    let appender = try Appender(connection: connection, table: "t1")
    let values = ["-" + String(repeating: "9", count: 45) + ".99999", "0.00001", nil]
    for value in values {
      try appender.append(value)
      try appender.endRow()
    }
    try appender.flush()
    let result = try connection.query("SELECT d FROM t1")
    XCTAssertEqual(Array(result[0].cast(to: String.self)), values)
    XCTAssertEqual(Array(result[0].cast(to: Decimal.self)), [nil, Decimal(string: "0.00001"), nil])
  }
}

private extension WideDecimalTests {

  /// Removes insignificant zeros: trailing ones of the fraction (and its decimal point)
  static func trimmed(_ text: String) -> String {
    guard text.contains(".") else { return text == "-0" ? "0" : text }
    var result = Substring(text)
    while result.hasSuffix("0") { result.removeLast() }
    if result.hasSuffix(".") { result.removeLast() }
    return result == "-0" ? "0" : String(result)
  }

  /// The digits of a Decimal without exponent notation, built from its public parts only
  static func plainString(_ decimal: Decimal) -> String {
    guard decimal != 0 else { return "0" }
    var digits = Array("\(decimal.significand)".filter(\.isNumber))
    let exponent = decimal.exponent
    if exponent >= 0 {
      digits.append(contentsOf: repeatElement("0", count: exponent))
    }
    else {
      if digits.count <= -exponent {
        digits.insert(contentsOf: repeatElement("0", count: -exponent - digits.count + 1), at: 0)
      }
      digits.insert(".", at: digits.count + exponent)
    }
    return trimmed((decimal.isSignMinus ? "-" : "") + String(digits))
  }
}
