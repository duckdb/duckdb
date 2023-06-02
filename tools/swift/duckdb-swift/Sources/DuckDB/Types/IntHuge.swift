//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2014 - 2021 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
// This source file has been modified from the original found within the
// Swift.org open source project:
// https://github.com/apple/swift/blob/main/stdlib/public/core/Int128.swift.gyb
// 
// If and when the Swift.org open source project releases its final version
// of Int128, this altered version can be deprecated and replaced with a 
// typealias.
//
//===----------------------------------------------------------------------===//

/// A 128-bit unsigned integer type.
public struct UIntHuge {
  public typealias High = UInt64
  public typealias Low = UInt64

  /// The low part of the value.
  public var low: Low

  /// The high part of the value.
  public var high: High

  /// Creates a new instance from the given tuple of high and low parts.
  ///
  /// - Parameter value: The tuple to use as the source of the new instance's
  ///   high and low parts.
  public init(_ value: (high: High, low: Low)) {
    self.low = value.low
    self.high = value.high
  }

  public init(high: High, low: Low) {
    self.low = low
    self.high = high
  }

  public init() {
    self.init(high: 0, low: 0)
  }

  public init(bitPattern v: IntHuge) {
    self.init(high: High(bitPattern: v.high), low: v.low)
  }

  public static var zero: Self { Self(high: 0, low: 0) }
  public static var one: Self { Self(high: 0, low: 1) }
}

extension UIntHuge: CustomStringConvertible {
  public var description: String {
    String(self, radix: 10)
  }
}

extension UIntHuge: CustomDebugStringConvertible {
  public var debugDescription: String {
    description
  }
}

extension UIntHuge: Equatable {
  public static func == (_ lhs: Self, _ rhs: Self) -> Bool {
    return (lhs.high, lhs.low) == (rhs.high, rhs.low)
  }
}

extension UIntHuge: Comparable {
  public static func < (_ lhs: Self, _ rhs: Self) -> Bool {
    (lhs.high, lhs.low) < (rhs.high, rhs.low)
  }
}

extension UIntHuge: Hashable {
  public func hash(into hasher: inout Hasher) {
    hasher.combine(low)
    hasher.combine(high)
  }
}

extension UIntHuge {
  public var components: (high: High, low: Low) {
    @inline(__always) get { (high, low) }
    @inline(__always) set { (self.high, self.low) = (newValue.high, newValue.low) }
  }
}

extension UIntHuge: AdditiveArithmetic {
  public static func - (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.subtractingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in -")
    return result
  }

  public static func -= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.subtractingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in -=")
    lhs = result
  }

  public static func + (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.addingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in +")
    return result
  }

  public static func += (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.addingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in +=")
    lhs = result
  }
}

extension UIntHuge: Numeric {
  public typealias Magnitude = UIntHuge

  public var magnitude: Magnitude {
    return self
  }

  public init(_ magnitude: Magnitude) {
    self.init(high: High(magnitude.high), low: magnitude.low)
  }

  public init<T: BinaryInteger>(_ source: T) {
    guard let result = Self(exactly: source) else {
      preconditionFailure("Value is outside the representable range")
    }
    self = result
  }

  public init?<T: BinaryInteger>(exactly source: T) {
    // Can't represent a negative 'source' if Self is unsigned.
    guard Self.isSigned || source >= 0 else {
      return nil
    }

    // Is 'source' entirely representable in Low?
    if let low = Low(exactly: source.magnitude) {
      self.init(source._isNegative ? (~0, low._twosComplement) : (0, low))
    } else {
      // At this point we know source.bitWidth > High.bitWidth, or else we
      // would've taken the first branch.
      let lowInT = source & T(~0 as Low)
      let highInT = source >> Low.bitWidth

      let low = Low(lowInT)
      guard let high = High(exactly: highInT) else {
        return nil
      }
      self.init(high: high, low: low)
    }
  }

  public static func * (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in *")
    return result
  }

  public static func *= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in *=")
    lhs = result
  }
}

public extension UIntHuge {
  struct Words {
    internal var _value: UIntHuge

    internal init(_ value: UIntHuge) {
      self._value = value
    }
  }
}

extension UIntHuge.Words: RandomAccessCollection {
  public typealias Element = UInt
  public typealias Index = Int
  public typealias Indices = Range<Int>
  public typealias SubSequence = Slice<Self>

  public var count: Int { 128 / UInt.bitWidth }
  public var startIndex: Int { 0 }
  public var endIndex: Int { count }
  public var indices: Indices { startIndex ..< endIndex }
  public func index(after i: Int) -> Int { i + 1 }
  public func index(before i: Int) -> Int { i - 1 }

  public subscript(position: Int) -> UInt {
    get {
      precondition(position >= 0 && position < endIndex,
        "Word index out of range")
      let shift = position &* UInt.bitWidth
      assert(shift < UIntHuge.bitWidth)

      let r = _wideMaskedShiftRight(
        _value.components, UInt64(truncatingIfNeeded: shift))
      return r.low._lowWord
    }
  }
}

extension UIntHuge: FixedWidthInteger {
  @_transparent
  public var _lowWord: UInt {
    low._lowWord
  }

  public var words: Words {
    Words(self)
  }

  public static var isSigned: Bool {
    false
  }

  public static var max: Self {
    self.init(high: High.max, low: Low.max)
  }

  public static var min: Self {
    self.init(high: High.min, low: Low.min)
  }

  public static var bitWidth: Int { 128 }

  public func addingReportingOverflow(
    _ rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let (r, o) = _wideAddReportingOverflow22(self.components, rhs.components)
    return (Self(r), o)
  }

  public func subtractingReportingOverflow(
    _ rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let (r, o) = _wideSubtractReportingOverflow22(
      self.components, rhs.components)
    return (Self(r), o)
  }

  public func multipliedReportingOverflow(
    by rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let h1 = self.high.multipliedReportingOverflow(by: rhs.low)
    let h2 = self.low.multipliedReportingOverflow(by: rhs.high)
    let h3 = h1.partialValue.addingReportingOverflow(h2.partialValue)
    let (h, l) = self.low.multipliedFullWidth(by: rhs.low)
    let high = h3.partialValue.addingReportingOverflow(h)
    let overflow = (
      (self.high != 0 && rhs.high != 0)
      || h1.overflow || h2.overflow || h3.overflow || high.overflow)
    return (Self(high: high.partialValue, low: l), overflow)
  }

  /// Returns the product of this value and the given 64-bit value, along with a
  /// Boolean value indicating whether overflow occurred in the operation.
  public func multipliedReportingOverflow(
    by other: UInt64
  ) -> (partialValue: Self, overflow: Bool) {
    let h1 = self.high.multipliedReportingOverflow(by: other)
    let (h2, l) = self.low.multipliedFullWidth(by: other)
    let high = h1.partialValue.addingReportingOverflow(h2)
    let overflow = h1.overflow || high.overflow
    return (Self(high: high.partialValue, low: l), overflow)
  }

  public func multiplied(by other: UInt64) -> Self {
    let r = multipliedReportingOverflow(by: other)
    precondition(!r.overflow, "Overflow in multiplication")
    return r.partialValue
  }

  public func quotientAndRemainder(
    dividingBy other: Self
  ) -> (quotient: Self, remainder: Self) {
    let (q, r) = _wideDivide22(
      self.magnitude.components, by: other.magnitude.components)
    let quotient = Self.Magnitude(q)
    let remainder = Self.Magnitude(r)
    return (quotient, remainder)
  }

  public func dividedReportingOverflow(
    by other: Self
  ) -> (partialValue: Self, overflow: Bool) {
    if other == Self.zero {
      return (self, true)
    }
    if Self.isSigned && other == -1 && self == .min {
      return (self, true)
    }
    return (quotientAndRemainder(dividingBy: other).quotient, false)
  }

  public func remainderReportingOverflow(
    dividingBy other: Self
  ) -> (partialValue: Self, overflow: Bool) {
    if other == Self.zero {
      return (self, true)
    }
    if Self.isSigned && other == -1 && self == .min {
      return (0, true)
    }
    return (quotientAndRemainder(dividingBy: other).remainder, false)
  }

  public func multipliedFullWidth(
    by other: Self
  ) -> (high: Self, low: Magnitude) {
    let isNegative = Self.isSigned && (self._isNegative != other._isNegative)

    func sum(_ x: Low, _ y: Low) -> (high: Low, low: Low) {
      let (sum, overflow) = x.addingReportingOverflow(y)
      return (overflow ? 1 : 0, sum)
    }

    func sum(_ x: Low, _ y: Low, _ z: Low) -> (high: Low, low: Low) {
      let s1 = sum(x, y)
      let s2 = sum(s1.low, z)
      return (s1.high &+ s2.high, s2.low)
    }

    func sum(
      _ x0: Low, _ x1: Low, _ x2: Low, _ x3: Low
    ) -> (high: Low, low: Low) {
      let s1 = sum(x0, x1)
      let s2 = sum(x2, x3)
      let s = sum(s1.low, s2.low)
      return (s1.high &+ s2.high &+ s.high, s.low)
    }

    let lhs = self.magnitude
    let rhs = other.magnitude

    let a = rhs.low.multipliedFullWidth(by: lhs.low)
    let b = rhs.low.multipliedFullWidth(by: lhs.high)
    let c = rhs.high.multipliedFullWidth(by: lhs.low)
    let d = rhs.high.multipliedFullWidth(by: lhs.high)

    let mid1 = sum(a.high, b.low, c.low)
    let mid2 = sum(b.high, c.high, mid1.high, d.low)

    let high = UIntHuge(
      high: High(d.high &+ mid2.high), // Note: this addition will never wrap
      low: mid2.low)
    let low = UIntHuge(
      high: mid1.low,
      low: a.low)

    if isNegative {
      let (lowComplement, overflow) = (~low).addingReportingOverflow(.one)
      return (~high + (overflow ? 1 : 0), lowComplement)
    } else {
      return (high, low)
    }
  }

  public func dividingFullWidth(
    _ dividend: (high: Self, low: Self.Magnitude)
  ) -> (quotient: Self, remainder: Self) {
    let (q, r) = _wideDivide42(
      (dividend.high.components, dividend.low.components),
      by: self.components)
    return (Self(q), Self(r))
  }

  public static prefix func ~(x: Self) -> Self {
    Self(high: ~x.high, low: ~x.low)
  }

  public static func &= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low &= rhs.low
    lhs.high &= rhs.high
  }

  public static func |= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low |= rhs.low
    lhs.high |= rhs.high
  }

  public static func ^= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low ^= rhs.low
    lhs.high ^= rhs.high
  }

  public static func <<= (_ lhs: inout Self, _ rhs: Self) {
    if Self.isSigned && rhs._isNegative {
      lhs >>= 0 - rhs
      return
    }

    // Shift is larger than this type's bit width.
    if rhs.high != High.zero || rhs.low >= Self.bitWidth {
      lhs = 0
      return
    }

    lhs &<<= rhs
  }

  public static func >>= (_ lhs: inout Self, _ rhs: Self) {
    if Self.isSigned && rhs._isNegative {
      lhs <<= 0 - rhs
      return
    }

    // Shift is larger than this type's bit width.
    if rhs.high != High.zero || rhs.low >= Self.bitWidth {
      lhs = lhs._isNegative ? ~0 : 0
      return
    }

    lhs &>>= rhs
  }

  public static func &<< (lhs: Self, rhs: Self) -> Self {
    Self(_wideMaskedShiftLeft(lhs.components, rhs.low))
  }

  public static func &>> (lhs: Self, rhs: Self) -> Self {
    Self(_wideMaskedShiftRight(lhs.components, rhs.low))
  }

  public static func &<<= (lhs: inout Self, rhs: Self) {
    _wideMaskedShiftLeft(&lhs.components, rhs.low)
  }

  public static func &>>= (lhs: inout Self, rhs: Self) {
    _wideMaskedShiftRight(&lhs.components, rhs.low)
  }

  public static func / (
    _ lhs: Self, _ rhs: Self
  ) -> Self {
    var lhs = lhs
    lhs /= rhs
    return lhs
  }

  public static func /= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.dividedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in /=")
    lhs = result
  }

  public static func % (
    _ lhs: Self, _ rhs: Self
  ) -> Self {
    var lhs = lhs
    lhs %= rhs
    return lhs
  }

  public static func %= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.remainderReportingOverflow(dividingBy: rhs)
    precondition(!overflow, "Overflow in %=")
    lhs = result
  }

  public init(_truncatingBits bits: UInt) {
    low = Low(_truncatingBits: bits)
    high = High(_truncatingBits: bits >> UInt(Low.bitWidth))
  }

  public init(integerLiteral x: Int64) {
    self.init(x)
  }

  public var leadingZeroBitCount: Int {
    (high == High.zero
      ? High.bitWidth + low.leadingZeroBitCount
      : high.leadingZeroBitCount)
  }

  public var trailingZeroBitCount: Int {
    (low == Low.zero
      ? Low.bitWidth + high.trailingZeroBitCount
      : low.trailingZeroBitCount)
  }

  public var nonzeroBitCount: Int {
    high.nonzeroBitCount + low.nonzeroBitCount
  }

  public var byteSwapped: Self {
    Self(
      high: High(truncatingIfNeeded: low.byteSwapped),
      low: Low(truncatingIfNeeded: high.byteSwapped))
  }
}

extension UIntHuge: Sendable {}
/// A 128-bit signed integer type.
public struct IntHuge {
  public typealias High = Int64
  public typealias Low = UInt64

  /// The low part of the value.
  public var low: Low

  /// The high part of the value.
  public var high: High

  /// Creates a new instance from the given tuple of high and low parts.
  ///
  /// - Parameter value: The tuple to use as the source of the new instance's
  ///   high and low parts.
  public init(_ value: (high: High, low: Low)) {
    self.low = value.low
    self.high = value.high
  }

  public init(high: High, low: Low) {
    self.low = low
    self.high = high
  }

  public init() {
    self.init(high: 0, low: 0)
  }

  public init(bitPattern v: UIntHuge) {
    self.init(high: High(bitPattern: v.high), low: v.low)
  }

  public static var zero: Self { Self(high: 0, low: 0) }
  public static var one: Self { Self(high: 0, low: 1) }
}

extension IntHuge: CustomStringConvertible {
  public var description: String {
    String(self, radix: 10)
  }
}

extension IntHuge: CustomDebugStringConvertible {
  public var debugDescription: String {
    description
  }
}

extension IntHuge: Equatable {
  public static func == (_ lhs: Self, _ rhs: Self) -> Bool {
    return (lhs.high, lhs.low) == (rhs.high, rhs.low)
  }
}

extension IntHuge: Comparable {
  public static func < (_ lhs: Self, _ rhs: Self) -> Bool {
    (lhs.high, lhs.low) < (rhs.high, rhs.low)
  }
}

extension IntHuge: Hashable {
  public func hash(into hasher: inout Hasher) {
    hasher.combine(low)
    hasher.combine(high)
  }
}

extension IntHuge {
  public var components: (high: High, low: Low) {
    @inline(__always) get { (high, low) }
    @inline(__always) set { (self.high, self.low) = (newValue.high, newValue.low) }
  }
}

extension IntHuge: AdditiveArithmetic {
  public static func - (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.subtractingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in -")
    return result
  }

  public static func -= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.subtractingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in -=")
    lhs = result
  }

  public static func + (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.addingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in +")
    return result
  }

  public static func += (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.addingReportingOverflow(rhs)
    precondition(!overflow, "Overflow in +=")
    lhs = result
  }
}

extension IntHuge: Numeric {
  public typealias Magnitude = UIntHuge

  public var magnitude: Magnitude {
    var result = UIntHuge(bitPattern: self)
    guard high._isNegative else { return result }
    result.high = ~result.high
    result.low = ~result.low
    return result.addingReportingOverflow(.one).partialValue
  }

  public init(_ magnitude: Magnitude) {
    self.init(high: High(magnitude.high), low: magnitude.low)
  }

  public init<T: BinaryInteger>(_ source: T) {
    guard let result = Self(exactly: source) else {
      preconditionFailure("Value is outside the representable range")
    }
    self = result
  }

  public init?<T: BinaryInteger>(exactly source: T) {
    // Can't represent a negative 'source' if Self is unsigned.
    guard Self.isSigned || source >= 0 else {
      return nil
    }

    // Is 'source' entirely representable in Low?
    if let low = Low(exactly: source.magnitude) {
      self.init(source._isNegative ? (~0, low._twosComplement) : (0, low))
    } else {
      // At this point we know source.bitWidth > High.bitWidth, or else we
      // would've taken the first branch.
      let lowInT = source & T(~0 as Low)
      let highInT = source >> Low.bitWidth

      let low = Low(lowInT)
      guard let high = High(exactly: highInT) else {
        return nil
      }
      self.init(high: high, low: low)
    }
  }

  public static func * (_ lhs: Self, _ rhs: Self) -> Self {
    let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in *")
    return result
  }

  public static func *= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in *=")
    lhs = result
  }
}

public extension IntHuge {
  struct Words {
    internal var _value: IntHuge

    internal init(_ value: IntHuge) {
      self._value = value
    }
  }
}

extension IntHuge.Words: RandomAccessCollection {
  public typealias Element = UInt
  public typealias Index = Int
  public typealias Indices = Range<Int>
  public typealias SubSequence = Slice<Self>

  public var count: Int { 128 / UInt.bitWidth }
  public var startIndex: Int { 0 }
  public var endIndex: Int { count }
  public var indices: Indices { startIndex ..< endIndex }
  public func index(after i: Int) -> Int { i + 1 }
  public func index(before i: Int) -> Int { i - 1 }

  public subscript(position: Int) -> UInt {
    get {
      precondition(position >= 0 && position < endIndex,
        "Word index out of range")
      let shift = position &* UInt.bitWidth
      assert(shift < IntHuge.bitWidth)

      let r = _wideMaskedShiftRight(
        _value.components, UInt64(truncatingIfNeeded: shift))
      return r.low._lowWord
    }
  }
}

extension IntHuge: FixedWidthInteger {
  @_transparent
  public var _lowWord: UInt {
    low._lowWord
  }

  public var words: Words {
    Words(self)
  }

  public static var isSigned: Bool {
    true
  }

  public static var max: Self {
    self.init(high: High.max, low: Low.max)
  }

  public static var min: Self {
    self.init(high: High.min, low: Low.min)
  }

  public static var bitWidth: Int { 128 }

  public func addingReportingOverflow(
    _ rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let (r, o) = _wideAddReportingOverflow22(self.components, rhs.components)
    return (Self(r), o)
  }

  public func subtractingReportingOverflow(
    _ rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let (r, o) = _wideSubtractReportingOverflow22(
      self.components, rhs.components)
    return (Self(r), o)
  }

  public func multipliedReportingOverflow(
    by rhs: Self
  ) -> (partialValue: Self, overflow: Bool) {
    let isNegative = (self._isNegative != rhs._isNegative)
    let (p, overflow) = self.magnitude.multipliedReportingOverflow(
      by: rhs.magnitude)
    let r = IntHuge(bitPattern: isNegative ? p._twosComplement : p)
    return (r, overflow || (isNegative != r._isNegative))
  }

  /// Returns the product of this value and the given 64-bit value, along with a
  /// Boolean value indicating whether overflow occurred in the operation.
  public func multipliedReportingOverflow(
    by other: UInt64
  ) -> (partialValue: Self, overflow: Bool) {
    let isNegative = self._isNegative
    let (p, overflow) = self.magnitude.multipliedReportingOverflow(by: other)
    let r = IntHuge(bitPattern: isNegative ? p._twosComplement : p)
    return (r, overflow || (isNegative != r._isNegative))
  }

  public func multiplied(by other: UInt64) -> Self {
    let r = multipliedReportingOverflow(by: other)
    precondition(!r.overflow, "Overflow in multiplication")
    return r.partialValue
  }

  public func quotientAndRemainder(
    dividingBy other: Self
  ) -> (quotient: Self, remainder: Self) {
    let (q, r) = _wideDivide22(
      self.magnitude.components, by: other.magnitude.components)
    let quotient = Self.Magnitude(q)
    let remainder = Self.Magnitude(r)
    let isNegative = (self.high._isNegative != other.high._isNegative)
    let quotient_ = (isNegative
      ? quotient == Self.min.magnitude ? Self.min : 0 - Self(quotient)
      : Self(quotient))
    let remainder_ = (self.high._isNegative
      ? 0 - Self(remainder)
      : Self(remainder))
    return (quotient_, remainder_)
  }

  public func dividedReportingOverflow(
    by other: Self
  ) -> (partialValue: Self, overflow: Bool) {
    if other == Self.zero {
      return (self, true)
    }
    if Self.isSigned && other == -1 && self == .min {
      return (self, true)
    }
    return (quotientAndRemainder(dividingBy: other).quotient, false)
  }

  public func remainderReportingOverflow(
    dividingBy other: Self
  ) -> (partialValue: Self, overflow: Bool) {
    if other == Self.zero {
      return (self, true)
    }
    if Self.isSigned && other == -1 && self == .min {
      return (0, true)
    }
    return (quotientAndRemainder(dividingBy: other).remainder, false)
  }

  public func multipliedFullWidth(
    by other: Self
  ) -> (high: Self, low: Magnitude) {
    let isNegative = Self.isSigned && (self._isNegative != other._isNegative)

    func sum(_ x: Low, _ y: Low) -> (high: Low, low: Low) {
      let (sum, overflow) = x.addingReportingOverflow(y)
      return (overflow ? 1 : 0, sum)
    }

    func sum(_ x: Low, _ y: Low, _ z: Low) -> (high: Low, low: Low) {
      let s1 = sum(x, y)
      let s2 = sum(s1.low, z)
      return (s1.high &+ s2.high, s2.low)
    }

    func sum(
      _ x0: Low, _ x1: Low, _ x2: Low, _ x3: Low
    ) -> (high: Low, low: Low) {
      let s1 = sum(x0, x1)
      let s2 = sum(x2, x3)
      let s = sum(s1.low, s2.low)
      return (s1.high &+ s2.high &+ s.high, s.low)
    }

    let lhs = self.magnitude
    let rhs = other.magnitude

    let a = rhs.low.multipliedFullWidth(by: lhs.low)
    let b = rhs.low.multipliedFullWidth(by: lhs.high)
    let c = rhs.high.multipliedFullWidth(by: lhs.low)
    let d = rhs.high.multipliedFullWidth(by: lhs.high)

    let mid1 = sum(a.high, b.low, c.low)
    let mid2 = sum(b.high, c.high, mid1.high, d.low)

    let high = IntHuge(
      high: High(d.high &+ mid2.high), // Note: this addition will never wrap
      low: mid2.low)
    let low = UIntHuge(
      high: mid1.low,
      low: a.low)

    if isNegative {
      let (lowComplement, overflow) = (~low).addingReportingOverflow(.one)
      return (~high + (overflow ? 1 : 0), lowComplement)
    } else {
      return (high, low)
    }
  }

  public func dividingFullWidth(
    _ dividend: (high: Self, low: Self.Magnitude)
  ) -> (quotient: Self, remainder: Self) {
    let m = _wideMagnitude22(dividend)
    let (quotient, remainder) = self.magnitude.dividingFullWidth(m)

    let isNegative = (self.high._isNegative != dividend.high.high._isNegative)
    let quotient_ = (isNegative
      ? (quotient == Self.min.magnitude ? Self.min : 0 - Self(quotient))
      : Self(quotient))
    let remainder_ = (dividend.high.high._isNegative
      ? 0 - Self(remainder)
      : Self(remainder))
    return (quotient_, remainder_)
  }

  public static prefix func ~(x: Self) -> Self {
    Self(high: ~x.high, low: ~x.low)
  }

  public static func &= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low &= rhs.low
    lhs.high &= rhs.high
  }

  public static func |= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low |= rhs.low
    lhs.high |= rhs.high
  }

  public static func ^= (_ lhs: inout Self, _ rhs: Self) {
    lhs.low ^= rhs.low
    lhs.high ^= rhs.high
  }

  public static func <<= (_ lhs: inout Self, _ rhs: Self) {
    if Self.isSigned && rhs._isNegative {
      lhs >>= 0 - rhs
      return
    }

    // Shift is larger than this type's bit width.
    if rhs.high != High.zero || rhs.low >= Self.bitWidth {
      lhs = 0
      return
    }

    lhs &<<= rhs
  }

  public static func >>= (_ lhs: inout Self, _ rhs: Self) {
    if Self.isSigned && rhs._isNegative {
      lhs <<= 0 - rhs
      return
    }

    // Shift is larger than this type's bit width.
    if rhs.high != High.zero || rhs.low >= Self.bitWidth {
      lhs = lhs._isNegative ? ~0 : 0
      return
    }

    lhs &>>= rhs
  }

  public static func &<< (lhs: Self, rhs: Self) -> Self {
    Self(_wideMaskedShiftLeft(lhs.components, rhs.low))
  }

  public static func &>> (lhs: Self, rhs: Self) -> Self {
    Self(_wideMaskedShiftRight(lhs.components, rhs.low))
  }

  public static func &<<= (lhs: inout Self, rhs: Self) {
    _wideMaskedShiftLeft(&lhs.components, rhs.low)
  }

  public static func &>>= (lhs: inout Self, rhs: Self) {
    _wideMaskedShiftRight(&lhs.components, rhs.low)
  }

  public static func / (
    _ lhs: Self, _ rhs: Self
  ) -> Self {
    var lhs = lhs
    lhs /= rhs
    return lhs
  }

  public static func /= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.dividedReportingOverflow(by: rhs)
    precondition(!overflow, "Overflow in /=")
    lhs = result
  }

  public static func % (
    _ lhs: Self, _ rhs: Self
  ) -> Self {
    var lhs = lhs
    lhs %= rhs
    return lhs
  }

  public static func %= (_ lhs: inout Self, _ rhs: Self) {
    let (result, overflow) = lhs.remainderReportingOverflow(dividingBy: rhs)
    precondition(!overflow, "Overflow in %=")
    lhs = result
  }

  public init(_truncatingBits bits: UInt) {
    low = Low(_truncatingBits: bits)
    high = High(_truncatingBits: bits >> UInt(Low.bitWidth))
  }

  public init(integerLiteral x: Int64) {
    self.init(x)
  }

  public var leadingZeroBitCount: Int {
    (high == High.zero
      ? High.bitWidth + low.leadingZeroBitCount
      : high.leadingZeroBitCount)
  }

  public var trailingZeroBitCount: Int {
    (low == Low.zero
      ? Low.bitWidth + high.trailingZeroBitCount
      : low.trailingZeroBitCount)
  }

  public var nonzeroBitCount: Int {
    high.nonzeroBitCount + low.nonzeroBitCount
  }

  public var byteSwapped: Self {
    Self(
      high: High(truncatingIfNeeded: low.byteSwapped),
      low: Low(truncatingIfNeeded: high.byteSwapped))
  }
}

extension IntHuge: Sendable {}

extension BinaryInteger {
  @inline(__always)
  fileprivate var _isNegative: Bool { self < Self.zero }
}

extension FixedWidthInteger {
  @inline(__always)
  fileprivate var _twosComplement: Self {
    ~self &+ 1
  }
}

private typealias _Wide2<F: FixedWidthInteger> =
  (high: F, low: F.Magnitude)

private typealias _Wide3<F: FixedWidthInteger> =
  (high: F, mid: F.Magnitude, low: F.Magnitude)

private typealias _Wide4<F: FixedWidthInteger> =
  (high: _Wide2<F>, low: (high: F.Magnitude, low: F.Magnitude))

private func _wideMagnitude22<F: FixedWidthInteger>(
  _ v: _Wide2<F>
) -> _Wide2<F.Magnitude> {
  var result = (high: F.Magnitude(truncatingIfNeeded: v.high), low: v.low)
  guard F.isSigned && v.high._isNegative else { return result }
  result.high = ~result.high
  result.low = ~result.low
  return _wideAddReportingOverflow22(result, (high: 0, low: 1)).partialValue
}

private func _wideAddReportingOverflow22<F: FixedWidthInteger>(
  _ lhs: _Wide2<F>, _ rhs: _Wide2<F>
) -> (partialValue: _Wide2<F>, overflow: Bool) {
  let (low, lowOverflow) = lhs.low.addingReportingOverflow(rhs.low)
  let (high, highOverflow) = lhs.high.addingReportingOverflow(rhs.high)
  let overflow = highOverflow || high == F.max && lowOverflow
  let result = (high: high &+ (lowOverflow ? 1 : 0), low: low)
  return (partialValue: result, overflow: overflow)
}

private func _wideAdd22<F: FixedWidthInteger>(
  _ lhs: inout _Wide2<F>, _ rhs: _Wide2<F>
) {
  let (result, overflow) = _wideAddReportingOverflow22(lhs, rhs)
  precondition(!overflow, "Overflow in +")
  lhs = result
}

private func _wideAddReportingOverflow33<F: FixedWidthInteger>(
  _ lhs: _Wide3<F>, _ rhs: _Wide3<F>
) -> (
  partialValue: _Wide3<F>,
  overflow: Bool
) {
  let (low, lowOverflow) =
    _wideAddReportingOverflow22((lhs.mid, lhs.low), (rhs.mid, rhs.low))
  let (high, highOverflow) = lhs.high.addingReportingOverflow(rhs.high)
  let result = (high: high &+ (lowOverflow ? 1 : 0), mid: low.high, low: low.low)
  let overflow = highOverflow || (high == F.max && lowOverflow)
  return (partialValue: result, overflow: overflow)
}

private func _wideSubtractReportingOverflow22<F: FixedWidthInteger>(
  _ lhs: _Wide2<F>, _ rhs: _Wide2<F>
) -> (partialValue: (high: F, low: F.Magnitude), overflow: Bool) {
  let (low, lowOverflow) = lhs.low.subtractingReportingOverflow(rhs.low)
  let (high, highOverflow) = lhs.high.subtractingReportingOverflow(rhs.high)
  let result = (high: high &- (lowOverflow ? 1 : 0), low: low)
  let overflow = highOverflow || high == F.min && lowOverflow
  return (partialValue: result, overflow: overflow)
}

private func _wideSubtract22<F: FixedWidthInteger>(
  _ lhs: inout _Wide2<F>, _ rhs: _Wide2<F>
) {
  let (result, overflow) = _wideSubtractReportingOverflow22(lhs, rhs)
  precondition(!overflow, "Overflow in -")
  lhs = result
}

private func _wideSubtractReportingOverflow33<F: FixedWidthInteger>(
  _ lhs: _Wide3<F>, _ rhs: _Wide3<F>
) -> (
  partialValue: _Wide3<F>,
  overflow: Bool
) {
  let (low, lowOverflow) =
    _wideSubtractReportingOverflow22((lhs.mid, lhs.low), (rhs.mid, rhs.low))
  let (high, highOverflow) = lhs.high.subtractingReportingOverflow(rhs.high)
  let result = (high: high &- (lowOverflow ? 1 : 0), mid: low.high, low: low.low)
  let overflow = highOverflow || (high == F.min && lowOverflow)
  return (partialValue: result, overflow: overflow)
}

private func _wideMaskedShiftLeft<F: FixedWidthInteger>(
  _ lhs: _Wide2<F>, _ rhs: F.Magnitude
) -> _Wide2<F> {
  let bitWidth = F.bitWidth + F.Magnitude.bitWidth
  assert(bitWidth.nonzeroBitCount == 1)

  // Mask rhs by the bit width of the wide value.
  let rhs = rhs & F.Magnitude(bitWidth &- 1)

  guard rhs < F.Magnitude.bitWidth else {
    let s = rhs &- F.Magnitude(F.Magnitude.bitWidth)
    return (high: F(truncatingIfNeeded: lhs.low &<< s), low: 0)
  }

  guard rhs != F.Magnitude.zero else { return lhs }
  var high = lhs.high &<< F(rhs)
  let rollover = F.Magnitude(F.bitWidth) &- rhs
  high |= F(truncatingIfNeeded: lhs.low &>> rollover)
  let low = lhs.low &<< rhs
  return (high, low)
}

private func _wideMaskedShiftLeft<F: FixedWidthInteger>(
  _ lhs: inout _Wide2<F>, _ rhs: F.Magnitude
) {
  lhs = _wideMaskedShiftLeft(lhs, rhs)
}

private func _wideMaskedShiftRight<F: FixedWidthInteger>(
  _ lhs: _Wide2<F>, _ rhs: F.Magnitude
) -> _Wide2<F> {
  let bitWidth = F.bitWidth + F.Magnitude.bitWidth
  assert(bitWidth.nonzeroBitCount == 1)

  // Mask rhs by the bit width of the wide value.
  let rhs = rhs & F.Magnitude(bitWidth &- 1)

  guard rhs < F.bitWidth else {
    let s = F(rhs &- F.Magnitude(F.bitWidth))
    return (
      high: lhs.high._isNegative ? ~0 : 0,
      low: F.Magnitude(truncatingIfNeeded: lhs.high &>> s))
  }

  guard rhs != F.zero else { return lhs }
  var low = lhs.low &>> rhs
  let rollover = F(F.bitWidth) &- F(rhs)
  low |= F.Magnitude(truncatingIfNeeded: lhs.high &<< rollover)
  let high = lhs.high &>> rhs
  return (high, low)
}

private func _wideMaskedShiftRight<F: FixedWidthInteger>(
  _ lhs: inout _Wide2<F>, _ rhs: F.Magnitude
) {
  lhs = _wideMaskedShiftRight(lhs, rhs)
}

/// Returns the quotient and remainder after dividing a triple-width magnitude
/// `lhs` by a double-width magnitude `rhs`.
///
/// This operation is conceptually that described by Burnikel and Ziegler
/// (1998).
private func _wideDivide32<F: FixedWidthInteger & UnsignedInteger>(
  _ lhs: _Wide3<F>, by rhs: _Wide2<F>
) -> (quotient: F, remainder: _Wide2<F>) {
  // The following invariants are guaranteed to hold by dividingFullWidth or
  // quotientAndRemainder before this function is invoked:
  assert(lhs.high != F.zero)
  assert(rhs.high.leadingZeroBitCount == 0)
  assert((high: lhs.high, low: lhs.mid) < rhs)

  // Estimate the quotient with a 2/1 division using just the top digits.
  var quotient = (lhs.high == rhs.high
    ? F.max
    : rhs.high.dividingFullWidth((high: lhs.high, low: lhs.mid)).quotient)

  // Compute quotient * rhs.
  // TODO: This could be performed more efficiently.
  let p1 = quotient.multipliedFullWidth(by: F(rhs.low))
  let p2 = quotient.multipliedFullWidth(by: rhs.high)
  let product = _wideAddReportingOverflow33(
    (high: F.zero, mid: F.Magnitude(p1.high), low: p1.low),
    (high: p2.high, mid: p2.low, low: .zero)).partialValue

  // Compute the remainder after decrementing quotient as necessary.
  var remainder = lhs

  while remainder < product {
    quotient = quotient &- 1
    remainder = _wideAddReportingOverflow33(
      remainder,
      (high: F.zero, mid: F.Magnitude(rhs.high), low: rhs.low)).partialValue
  }
  remainder = _wideSubtractReportingOverflow33(remainder, product).partialValue
  assert(remainder.high == 0)
  return (quotient, (high: F(remainder.mid), low: remainder.low))
}

/// Returns the quotient and remainder after dividing a double-width
/// magnitude `lhs` by a double-width magnitude `rhs`.
private func _wideDivide22<F: FixedWidthInteger & UnsignedInteger>(
  _ lhs: _Wide2<F>, by rhs: _Wide2<F>
) -> (quotient: _Wide2<F>, remainder: _Wide2<F>) {
  guard _fastPath(rhs > (F.zero, F.Magnitude.zero)) else {
    fatalError("Division by zero")
  }
  guard rhs < lhs else {
    if _fastPath(rhs > lhs) { return (quotient: (0, 0), remainder: lhs) }
    return (quotient: (0, 1), remainder: (0, 0))
  }

  if lhs.high == F.zero {
    let (quotient, remainder) =
      lhs.low.quotientAndRemainder(dividingBy: rhs.low)
    return ((0, quotient), (0, remainder))
  }

  if rhs.high == F.zero {
    let (x, a) = lhs.high.quotientAndRemainder(dividingBy: F(rhs.low))
    let (y, b) = (a == F.zero
      ? lhs.low.quotientAndRemainder(dividingBy: rhs.low)
      : rhs.low.dividingFullWidth((F.Magnitude(a), lhs.low)))
    return (quotient: (high: x, low: y), remainder: (high: 0, low: b))
  }

  // Left shift both rhs and lhs, then divide and right shift the remainder.
  let shift = F.Magnitude(rhs.high.leadingZeroBitCount)
  let rollover = F.Magnitude(F.bitWidth + F.Magnitude.bitWidth) &- shift
  let rhs = _wideMaskedShiftLeft(rhs, shift)
  let high = _wideMaskedShiftRight(lhs, rollover).low
  let lhs = _wideMaskedShiftLeft(lhs, shift)
  let (quotient, remainder) = _wideDivide32(
    (F(high), F.Magnitude(lhs.high), lhs.low), by: rhs)
  return (
    quotient: (high: 0, low: F.Magnitude(quotient)),
    remainder: _wideMaskedShiftRight(remainder, shift))
}

/// Returns the quotient and remainder after dividing a quadruple-width
/// magnitude `lhs` by a double-width magnitude `rhs`.
private func _wideDivide42<F: FixedWidthInteger & UnsignedInteger>(
  _ lhs: _Wide4<F>, by rhs: _Wide2<F>
) -> (quotient: _Wide2<F>, remainder: _Wide2<F>) {
  guard _fastPath(rhs > (F.zero, F.Magnitude.zero)) else {
    fatalError("Division by zero")
  }
  guard _fastPath(rhs >= lhs.high) else {
    fatalError("Division results in an overflow")
  }

  if lhs.high == (F.zero, F.Magnitude.zero) {
    return _wideDivide22((high: F(lhs.low.high), low: lhs.low.low), by: rhs)
  }

  if rhs.high == F.zero {
    let a = F.Magnitude(lhs.high.high) % rhs.low
    let b = (a == F.Magnitude.zero
      ? lhs.high.low % rhs.low
      : rhs.low.dividingFullWidth((a, lhs.high.low)).remainder)
    let (x, c) = (b == F.Magnitude.zero
      ? lhs.low.high.quotientAndRemainder(dividingBy: rhs.low)
      : rhs.low.dividingFullWidth((b, lhs.low.high)))
    let (y, d) = (c == F.Magnitude.zero
      ? lhs.low.low.quotientAndRemainder(dividingBy: rhs.low)
      : rhs.low.dividingFullWidth((c, lhs.low.low)))
    return (quotient: (high: F(x), low: y), remainder: (high: 0, low: d))
  }

  // Left shift both rhs and lhs, then divide and right shift the remainder.
  let shift = F.Magnitude(rhs.high.leadingZeroBitCount)
  let rollover = F.Magnitude(F.bitWidth + F.Magnitude.bitWidth) &- shift
  let rhs = _wideMaskedShiftLeft(rhs, shift)

  let lh1 = _wideMaskedShiftLeft(lhs.high, shift)
  let lh2 = _wideMaskedShiftRight(lhs.low, rollover)
  let lhs = (
    high: (high: lh1.high | F(lh2.high), low: lh1.low | lh2.low),
    low: _wideMaskedShiftLeft(lhs.low, shift))

  if
    lhs.high.high == F.Magnitude.zero,
    (high: F(lhs.high.low), low: lhs.low.high) < rhs
  {
    let (quotient, remainder) = _wideDivide32(
      (F(lhs.high.low), lhs.low.high, lhs.low.low),
      by: rhs)
    return (
      quotient: (high: 0, low: F.Magnitude(quotient)),
      remainder: _wideMaskedShiftRight(remainder, shift))
  }
  let (x, a) = _wideDivide32(
    (lhs.high.high, lhs.high.low, lhs.low.high), by: rhs)
  let (y, b) = _wideDivide32((a.high, a.low, lhs.low.low), by: rhs)
  return (
    quotient: (high: x, low: F.Magnitude(y)),
    remainder: _wideMaskedShiftRight(b, shift))
}


extension UIntHuge: UnsignedInteger {}
extension IntHuge: SignedNumeric, SignedInteger {}


extension IntHuge {
  internal func dividedBy1e18() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: 664613997892457936, low: 8336148766501648893)
    var q = self.multipliedFullWidth(by: m).high
    q &>>= 55
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000000000000000000 as IntHuge)
    return (q, r)
  }
}
extension IntHuge {
  internal func dividedBy1e15() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: -8062150356639896359, low: 1125115960621402641)
    var q = self.multipliedFullWidth(by: m).high
    q &+= self
    q &>>= 49
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000000000000000 as IntHuge)
    return (q, r)
  }
}
extension IntHuge {
  internal func dividedBy1e12() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: 2535301200456458802, low: 18325113820324532597)
    var q = self.multipliedFullWidth(by: m).high
    q &>>= 37
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000000000000 as IntHuge)
    return (q, r)
  }
}
extension IntHuge {
  internal func dividedBy1e9() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: 4951760157141521099, low: 11003425581274142745)
    var q = self.multipliedFullWidth(by: m).high
    q &>>= 28
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000000000 as IntHuge)
    return (q, r)
  }
}
extension IntHuge {
  internal func dividedBy1e6() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: 604462909807314587, low: 6513323971497958161)
    var q = self.multipliedFullWidth(by: m).high
    q &>>= 15
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000000 as IntHuge)
    return (q, r)
  }
}
extension IntHuge {
  internal func dividedBy1e3() -> (quotient: Self, remainder: Self) {
    let m = IntHuge(high: 4722366482869645213, low: 12838933875301847925)
    var q = self.multipliedFullWidth(by: m).high
    q &>>= 8
    // Add 1 to q if self is negative
    q &+= IntHuge(bitPattern: UIntHuge(bitPattern: self) &>> 127)
    let r = self &- q &* (1000 as IntHuge)
    return (q, r)
  }
}
