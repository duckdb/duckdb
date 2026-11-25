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

/// The unscaled value of a `DECIMAL` of more than 38 digits
///
/// DuckDB stores these as `BIGNUM` blobs: three header bytes holding the
/// number of data bytes with the top bit set, followed by the magnitude in
/// big-endian order. Header and data bytes of a negative value are bitwise
/// complemented.
struct WideDecimal {

  private static let headerSize = 3
  private static let asciiZero = UInt8(ascii: "0")

  /// Whether the value is below zero
  let isNegative: Bool
  /// ASCII decimal digits of the magnitude of the unscaled value, without
  /// leading zeros. Zero is a single `0` digit.
  let digits: [UInt8]

  init(bignumStorage storage: Data) throws {
    let bytes = [UInt8](storage)
    guard bytes.count > Self.headerSize else {
      throw DatabaseError.decimalNotConvertible(reason: "malformed DECIMAL storage: \(bytes.count) bytes")
    }
    let isNegative = bytes[0] & 0x80 == 0
    let mask: UInt8 = isNegative ? 0xFF : 0x00
    let header = bytes[..<Self.headerSize].reduce(0) { ($0 << 8) | Int($1 ^ mask) }
    guard header & 0x7FFFFF == bytes.count - Self.headerSize else {
      throw DatabaseError.decimalNotConvertible(reason: "malformed DECIMAL storage: unexpected header")
    }
    self.isNegative = isNegative
    self.digits = Self.decimalDigits(magnitude: bytes[Self.headerSize...].map { $0 ^ mask })
  }

  /// The value as text with every digit, in the format of DuckDB's `VARCHAR`
  /// cast of a `DECIMAL`
  func string(width: Int, scale: Int) -> String {
    Self.string(digits: digits, isNegative: isNegative, width: width, scale: scale)
  }

  /// The value as a Foundation `Decimal`, or `nil` if a `Decimal` cannot hold
  /// it exactly
  ///
  /// A `Decimal` has a 128-bit significand and a decimal exponent in the range
  /// of an `Int8`. Foundation keeps trailing zeros in the exponent, not in the
  /// significand, so a value with more than 127 trailing zeros has no
  /// `Decimal` that the public API creates reliably: with swift-foundation
  /// `Decimal(sign: .plus, exponent: 127, significand: 1000)` is `NaN`.
  func decimal(scale: Int) -> Decimal? {
    var significandDigits = digits[...]
    while significandDigits.count > 1, significandDigits.last == Self.asciiZero {
      significandDigits.removeLast()
    }
    var significand = UIntHuge(0)
    for digit in significandDigits {
      guard let next = Self.appending(digit: digit - Self.asciiZero, to: significand) else { return nil }
      significand = next
    }
    guard significand != 0 else { return Decimal(0) }
    let exponent = digits.count - significandDigits.count - scale
    guard exponent >= Int(Int8.min), exponent <= Int(Int8.max) else { return nil }
    let decimal = Decimal(
      sign: isNegative ? .minus : .plus, exponent: exponent, significand: Decimal(significand))
    // never hand out a value that Foundation could not represent
    return decimal.isNaN ? nil : decimal
  }
}

extension WideDecimal {

  /// Formats the unscaled value of a `DECIMAL` given as its sign and the ASCII
  /// decimal digits of its magnitude
  static func string<Digits: Collection>(
    digits: Digits, isNegative: Bool, width: Int, scale: Int
  ) -> String where Digits.Element == UInt8 {
    var characters = [UInt8]()
    characters.reserveCapacity(Swift.max(digits.count, scale + 1) + 2)
    if isNegative {
      characters.append(UInt8(ascii: "-"))
    }
    // a value below one keeps a single zero in front of the decimal point, unless the type has no integer digits
    let integerDigitCount = width > scale ? 1 : 0
    let padding = Swift.max(0, scale + integerDigitCount - digits.count)
    characters.append(contentsOf: repeatElement(asciiZero, count: padding))
    characters.append(contentsOf: digits)
    if scale > 0 {
      characters.insert(UInt8(ascii: "."), at: characters.count - scale)
    }
    return String(decoding: characters, as: UTF8.self)
  }
}

private extension WideDecimal {

  static func appending(digit: UInt8, to value: UIntHuge) -> UIntHuge? {
    let (product, productOverflow) = value.multipliedReportingOverflow(by: UInt64(10))
    let (sum, sumOverflow) = product.addingReportingOverflow(UIntHuge(digit))
    return productOverflow || sumOverflow ? nil : sum
  }

  /// Converts a big-endian magnitude to its ASCII decimal digits
  static func decimalDigits<Magnitude: Collection>(
    magnitude: Magnitude
  ) -> [UInt8] where Magnitude.Element == UInt8 {
    // 32-bit limbs, most significant first
    var limbs = [UInt32]()
    limbs.reserveCapacity(magnitude.count / 4 + 1)
    var pending = magnitude.count % 4 == 0 ? 4 : magnitude.count % 4
    var limb = UInt32(0)
    for byte in magnitude {
      limb = (limb << 8) | UInt32(byte)
      pending -= 1
      if pending == 0 {
        limbs.append(limb)
        limb = 0
        pending = 4
      }
    }
    // repeatedly divide by 10^9; the remainders are the digits, least
    // significant group first
    let groupBase = UInt64(1_000_000_000)
    let groupDigitCount = 9
    var groups = [UInt32]()
    var start = limbs.firstIndex { $0 != 0 } ?? limbs.endIndex
    while start < limbs.endIndex {
      var remainder = UInt64(0)
      for index in start..<limbs.endIndex {
        let current = (remainder << 32) | UInt64(limbs[index])
        limbs[index] = UInt32(current / groupBase)
        remainder = current % groupBase
      }
      groups.append(UInt32(remainder))
      while start < limbs.endIndex, limbs[start] == 0 {
        start += 1
      }
    }
    guard let leadingGroup = groups.last else { return [asciiZero] }
    var digits = Array(String(leadingGroup).utf8)
    digits.reserveCapacity(groups.count * groupDigitCount)
    for group in groups.dropLast().reversed() {
      let groupDigits = Array(String(group).utf8)
      digits.append(contentsOf: repeatElement(asciiZero, count: groupDigitCount - groupDigits.count))
      digits.append(contentsOf: groupDigits)
    }
    return digits
  }
}
