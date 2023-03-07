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
import Foundation

// MARK: - Type Layouts

struct duckdb_list_entry_t {
  let offset: UInt64
  let length: UInt64
}

extension duckdb_state {
  static let success = duckdb_state(0)
  static let failure = duckdb_state(1)
}

extension duckdb_pending_state {
  static let ready = duckdb_pending_state(0)
  static let notReady = duckdb_pending_state(1)
  static let error = duckdb_pending_state(2)
}

// MARK: - Type ID

extension duckdb_type {
  var asTypeID: DatabaseType { .init(rawValue: rawValue) }
}

// MARK: - String

extension duckdb_string {
  
  private static let inlineLimit = UInt32(12)
  
  var asString: String {
    withUnsafePointer(to: self) { ptr in
      let contentsSize = UnsafeRawPointer(ptr).load(as: UInt32.self)
      let strPtr: UnsafeRawPointer
      if contentsSize <= Self.inlineLimit {
        strPtr = UnsafeRawPointer(ptr).advanced(by: MemoryLayout<UInt32>.stride)
      }
      else {
        let strPtrPtr = UnsafeRawPointer(ptr).advanced(by: MemoryLayout<UInt64>.stride)
        strPtr = strPtrPtr.load(as: UnsafeRawPointer.self)
      }
      let stringData = Data(bytes: strPtr, count: Int(contentsSize))
      return String(data: stringData, encoding:.utf8)!
    }
  }
}

// MARK: - Huge Int

extension duckdb_hugeint {
  
  init(_ source: IntHuge) {
    self = duckdb_hugeint(lower: source.low, upper: source.high)
  }
  
  var asIntHuge: IntHuge { .init(high: upper, low: lower) }
  
  var asUIntHuge: UIntHuge {
    let high = IntHuge(upper) + IntHuge(Int64.max) + 1
    return UIntHuge(high) << 64 + UIntHuge(lower)
  }
  
  var asUUID: UUID {
    let value = asUIntHuge
    let uuid = UUID(
      uuid: uuid_t(
        UInt8(truncatingIfNeeded: value >> (8 * 15)),
        UInt8(truncatingIfNeeded: value >> (8 * 14)),
        UInt8(truncatingIfNeeded: value >> (8 * 13)),
        UInt8(truncatingIfNeeded: value >> (8 * 12)),
        UInt8(truncatingIfNeeded: value >> (8 * 11)),
        UInt8(truncatingIfNeeded: value >> (8 * 10)),
        UInt8(truncatingIfNeeded: value >> (8 * 9)),
        UInt8(truncatingIfNeeded: value >> (8 * 8)),
        UInt8(truncatingIfNeeded: value >> (8 * 7)),
        UInt8(truncatingIfNeeded: value >> (8 * 6)),
        UInt8(truncatingIfNeeded: value >> (8 * 5)),
        UInt8(truncatingIfNeeded: value >> (8 * 4)),
        UInt8(truncatingIfNeeded: value >> (8 * 3)),
        UInt8(truncatingIfNeeded: value >> (8 * 2)),
        UInt8(truncatingIfNeeded: value >> (8 * 1)),
        UInt8(truncatingIfNeeded: value >> (8 * 0))
      )
    )
    return uuid
  }
}

// MARK: - Time

extension duckdb_time {
  init(time: Time) { self = duckdb_time(micros: time.microseconds) }
  var asTime: Time { Time(microseconds: micros) }
}

extension duckdb_time_struct {
  
  init(components: Time.Components) {
    self = duckdb_time_struct(
      hour: components.hour,
      min: components.minute,
      sec: components.second,
      micros: components.microsecond
    )
  }
  
  var asTimeComponents: Time.Components {
    .init(hour: hour, minute: min, second: sec, microsecond: micros)
  }
}

// MARK: - Date

extension duckdb_date {
  init(date: Date) { self = duckdb_date(days: date.days) }
  var asDate: Date { Date(days: days) }
}

extension duckdb_date_struct {
  
  init(components: Date.Components) {
    self = duckdb_date_struct(year: components.year, month: components.month, day: components.day)
  }
  
  var asDateComponents: Date.Components {
    .init(year: year, month: month, day: day)
  }
}

// MARK: - Timestamp

extension duckdb_timestamp {
  init(timestamp: Timestamp) { self = duckdb_timestamp(micros: timestamp.microseconds) }
  var asTimestamp: Timestamp { Timestamp(microseconds: micros) }
}

extension duckdb_timestamp_struct {
  
  init(components: Timestamp.Components) {
    self = duckdb_timestamp_struct(
      date: duckdb_date_struct(components: components.date),
      time: duckdb_time_struct(components: components.time)
    )
  }
  
  var asTimestampComponents: Timestamp.Components {
    .init(date: date.asDateComponents, time: time.asTimeComponents)
  }
}

// MARK: - Interval

extension duckdb_interval {
  
  init(interval: Interval) {
    self = duckdb_interval(
      months: interval.months, days: interval.days, micros: interval.microseconds)
  }
  
  var asInterval: Interval {
    Interval(months: months, days: days, microseconds: micros)
  }
}

// MARK: - Blob

extension duckdb_blob {
  
  private static let inlineLimit = UInt32(12)
  
  var asData: Data {
    withUnsafePointer(to: self) { ptr in
      let contentsSize = UnsafeRawPointer(ptr).load(as: UInt32.self)
      let blobPtr: UnsafeRawPointer
      if contentsSize <= Self.inlineLimit {
        blobPtr = UnsafeRawPointer(ptr).advanced(by: MemoryLayout<UInt32>.stride)
      }
      else {
        let blobPtrPtr = UnsafeRawPointer(ptr).advanced(by: MemoryLayout<UInt64>.stride)
        blobPtr = blobPtrPtr.load(as: UnsafeRawPointer.self)
      }
      return Data(bytes: blobPtr, count: Int(contentsSize))
    }
  }
}

// MARK: - Decimal

extension duckdb_decimal {
  
  private static let scaleLimit = 38
  
  init(_ source: Decimal) throws {    
    let mantissaLimit = source.isSignMinus ? 1 + UIntHuge(IntHuge.max) : UIntHuge(IntHuge.max)
    var scale: Int
    var mantissa: UIntHuge
    if source.exponent > 0 {
      let exponent = UIntHuge(source.exponent)
      let (out, overflow) = source.hugeMantissa.multipliedReportingOverflow(by: exponent)
      guard overflow == false else { throw DatabaseError.decimalUnrepresentable }
      mantissa = out
      scale = 0
    }
    else {
      scale = -source.exponent
      mantissa = source.hugeMantissa
      while scale > Self.scaleLimit {
        mantissa /= 10
        scale -= 1
      }
      while scale > 0, mantissa > mantissaLimit {
        mantissa /= 10
        scale -= 1
      }
    }
    guard mantissa <= mantissaLimit else {
      throw DatabaseError.decimalUnrepresentable
    }
    guard mantissa > 0 else {
      self = duckdb_decimal(width: 0, scale: 0, value: .init(lower: 0, upper: 0))
      return
    }
    let value = source.isSignMinus
      ? IntHuge.min + IntHuge(mantissaLimit - mantissa) : IntHuge(mantissa)
    self = duckdb_decimal(width: 38, scale: .init(scale), value: .init(value))
  }
}

fileprivate extension Decimal {
  
  var hugeMantissa: UIntHuge {
    let components = [
      _mantissa.0, _mantissa.1, _mantissa.2, _mantissa.3,
      _mantissa.4, _mantissa.5, _mantissa.6, _mantissa.7
    ]
    var mantissa = UIntHuge(0)
    for i in 0..<Int(_length) {
      mantissa += UIntHuge(components[i]) << (i * 16)
    }
    return mantissa
  }
}

// MARK: - Vector

extension duckdb_vector {
  
  var logicalType: LogicalType {
    LogicalType { duckdb_vector_get_column_type(self) }
  }
}
