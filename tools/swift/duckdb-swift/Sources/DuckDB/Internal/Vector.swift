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

#if compiler(>=6.0)
internal import Cduckdb
#else
@_implementationOnly import Cduckdb
#endif
import Foundation

struct Vector {
  
  static let vectorSize = DBInt(duckdb_vector_size())
  
  let count: Int
  let offset: Int
  let logicalType: LogicalType
  private let cvector: duckdb_vector
  
  init(_ cvector: duckdb_vector, count: Int, offset: Int = 0, logicalType: LogicalType? = nil) {
    self.count = count
    self.offset = offset
    self.cvector = cvector
    self.logicalType = logicalType ?? cvector.logicalType
  }
  
  func withCVector<T>(_ body: (duckdb_vector) throws -> T) rethrows -> T {
    try body(cvector)
  }
}

extension Vector {
  
  func unwrapNull(at index: Int) -> Bool {
    precondition(index < count, "vector index out of bounds")
    let offsetIndex = offset + index
    let validityMasksPtr = duckdb_vector_get_validity(cvector)
    guard let validityMasksPtr else { return false }
    let validityMaskEntryIndex = offsetIndex / 64
    let validityMaskEntryPtr = (validityMasksPtr + validityMaskEntryIndex)
    let validityBitIndex = offsetIndex % 64
    let validityBit = (DBInt(1) << validityBitIndex)
    return validityMaskEntryPtr.pointee & validityBit == 0
  }
  
  func unwrap(_ type: Int.Type, at index: Int) throws -> Int {
    switch logicalType.dataType {
    case .tinyint:
      return Int(try unwrap(Int8.self, at: index))
    case .smallint:
      return Int(try unwrap(Int16.self, at: index))
    case .integer:
      return Int(try unwrap(Int32.self, at: index))
    case .bigint:
      return Int(try unwrap(Int64.self, at: index))
    default:
      throw DatabaseError.typeMismatch(Int.self)
    }
  }
  
  func unwrap(_ type: UInt.Type, at index: Int) throws -> UInt {
    switch logicalType.dataType {
    case .utinyint:
      return UInt(try unwrap(UInt8.self, at: index))
    case .usmallint:
      return UInt(try unwrap(UInt16.self, at: index))
    case .uinteger:
      return UInt(try unwrap(UInt32.self, at: index))
    case .ubigint:
      return UInt(try unwrap(UInt64.self, at: index))
    default:
      throw DatabaseError.typeMismatch(Int.self)
    }
  }
  
  func unwrap<T: PrimitiveDatabaseValue>(_ type: T.Type, at index: Int) throws -> T {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: T.representedDatabaseTypeID)
    return unsafelyUnwrapElement(as: T.self, at: index)
  }
  
  func unwrap(_ type: String.Type, at index: Int) throws -> String {
    if logicalType.dataType == .decimal {
      // the exact digits of a decimal of up to 255 digits; a wider one throws
      return try unwrapDecimalString(at: index)
    }
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .varchar)
    return unsafelyUnwrapElement(as: duckdb_string.self, at: index) { $0.asString }
  }
  
  func unwrap(_ type: IntHuge.Type, at index: Int) throws -> IntHuge {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .hugeint)
    return unsafelyUnwrapElement(as: duckdb_hugeint.self, at: index) { $0.asIntHuge }
  }
  
  func unwrap(_ type: UIntHuge.Type, at index: Int) throws -> UIntHuge {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .uhugeint)
    return unsafelyUnwrapElement(as: duckdb_uhugeint.self, at: index) { $0.asUIntHuge }
  }
  
  func unwrap(_ type: UUID.Type, at index: Int) throws -> UUID {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .uuid)
    return unsafelyUnwrapElement(as: duckdb_hugeint.self, at: index) { $0.asUUID }
  }
  
  func unwrap(_ type: Time.Type, at index: Int) throws -> Time {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .time)
    return unsafelyUnwrapElement(as: duckdb_time.self, at: index) { $0.asTime }
  }
  
  func unwrap(_ type: TimeTz.Type, at index: Int) throws -> TimeTz {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .timeTz)
    return unsafelyUnwrapElement(as: duckdb_time_tz.self, at: index) { $0.asTime }
  }

  func unwrap(_ type: Date.Type, at index: Int) throws -> Date {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .date)
    return unsafelyUnwrapElement(as: duckdb_date.self, at: index) { $0.asDate }
  }
  
  func unwrap(_ type: Interval.Type, at index: Int) throws -> Interval {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .interval)
    return unsafelyUnwrapElement(as: duckdb_interval.self, at: index) { $0.asInterval }
  }
  
  func unwrap(_ type: Timestamp.Type, at index: Int) throws -> Timestamp {
    let columnTypes = [DatabaseType.timestampS, .timestampMS, .timestamp, .timestampTz, .timestampNS]
    try assertNonNullTypeMatch(of: type, at: index, withColumnTypes: .init(columnTypes))
    return unsafelyUnwrapElement(as: duckdb_timestamp.self, at: index) { ctimestamp in
      switch logicalType.dataType {
      case .timestampS:
        let scaled = duckdb_timestamp(micros: ctimestamp.micros * 1_000_000)
        return scaled.asTimestamp
      case .timestampMS:
        let scaled = duckdb_timestamp(micros: ctimestamp.micros * 1_000)
        return scaled.asTimestamp
      case .timestampNS:
        let scaled = duckdb_timestamp(micros: ctimestamp.micros / 1_000)
        return scaled.asTimestamp
      default:
        return ctimestamp.asTimestamp
      }
    }
  }
  
  func unwrap(_ type: Data.Type, at index: Int) throws -> Data {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .blob)
    return unsafelyUnwrapElement(as: duckdb_blob.self, at: index) { $0.asData }
  }
  
  func unwrap(_ type: Decimal.Type, at index: Int) throws -> Decimal {
    try assertNonNullTypeMatch(of: type, at: index, withColumnType: .decimal)
    guard let props = logicalType.decimalProperties else {
      throw DatabaseError.typeMismatch(type)
    }
    switch props.storageType {
    case .tinyint:
      return unwrapDecimal(withUnderlyingType: Int8.self, scale: props.scale, at: index)
    case .smallint:
      return unwrapDecimal(withUnderlyingType: Int16.self, scale: props.scale, at: index)
    case .integer:
      return unwrapDecimal(withUnderlyingType: Int32.self, scale: props.scale, at: index)
    case .bigint:
      return unwrapDecimal(withUnderlyingType: Int64.self, scale: props.scale, at: index)
    case .hugeint:
      return unwrapDecimal(withUnderlyingType: IntHuge.self, scale: props.scale, at: index)
    case .bignum:
      let (value, scale) = try unwrapWideDecimal(properties: props, at: index)
      guard let decimal = value.decimal(scale: scale) else {
        let digits = value.string(width: Int(props.width), scale: scale)
        throw DatabaseError.decimalNotConvertible(
          reason: "DECIMAL(\(props.width),\(props.scale)) value \(digits) does not fit "
            + "a Foundation Decimal exactly; cast the column to String for its digits")
      }
      return decimal
    default:
      throw DatabaseError.typeMismatch(type)
    }
  }

  /// The exact digits of a decimal, in the format of DuckDB's `VARCHAR` cast
  func unwrapDecimalString(at index: Int) throws -> String {
    try assertNonNullTypeMatch(of: String.self, at: index, withColumnType: .decimal)
    guard let props = logicalType.decimalProperties else {
      throw DatabaseError.typeMismatch(String.self)
    }
    switch props.storageType {
    case .tinyint:
      return unwrapDecimalString(withUnderlyingType: Int8.self, width: props.width, scale: props.scale, at: index)
    case .smallint:
      return unwrapDecimalString(withUnderlyingType: Int16.self, width: props.width, scale: props.scale, at: index)
    case .integer:
      return unwrapDecimalString(withUnderlyingType: Int32.self, width: props.width, scale: props.scale, at: index)
    case .bigint:
      return unwrapDecimalString(withUnderlyingType: Int64.self, width: props.width, scale: props.scale, at: index)
    case .hugeint:
      return unwrapDecimalString(withUnderlyingType: IntHuge.self, width: props.width, scale: props.scale, at: index)
    case .bignum:
      let (value, scale) = try unwrapWideDecimal(properties: props, at: index)
      return value.string(width: Int(props.width), scale: scale)
    default:
      throw DatabaseError.typeMismatch(String.self)
    }
  }

  func unwrapDecimalString<T: FixedWidthInteger>(
    withUnderlyingType storageType: T.Type, width: UInt8, scale: UInt8, at index: Int
  ) -> String {
    unsafelyUnwrapElement(as: T.self, at: index) { unscaled in
      WideDecimal.string(
        digits: String(unscaled.magnitude).utf8, isNegative: unscaled < 0, width: Int(width), scale: Int(scale))
    }
  }

  /// Decodes the unscaled value of a decimal of more than 38 digits, which
  /// DuckDB stores as a `BIGNUM`
  func unwrapWideDecimal(
    properties: LogicalType.DecimalProperties, at index: Int
  ) throws -> (value: WideDecimal, scale: Int) {
    guard properties.width > 0 else {
      // DuckDB's C API reports width and scale as UInt8, and both as zero for
      // a type that does not fit: the position of the decimal point is unknown
      throw DatabaseError.decimalNotConvertible(
        reason: "the scale of a DECIMAL of more than 255 digits is not available; cast it to VARCHAR in the query")
    }
    let storage = unsafelyUnwrapElement(as: duckdb_blob.self, at: index) { $0.asData }
    return (try WideDecimal(bignumStorage: storage), Int(properties.scale))
  }
  
  func unwrapDecimal<T: DecimalStorageType>(
    withUnderlyingType storageType: T.Type, scale: UInt8, at index: Int
  ) -> Decimal {
    unsafelyUnwrapElement(as: T.self, at: index) { decimalStorage in
      let storageValue = decimalStorage.asDecimal
      let sign = storageValue.sign
      let exponent = -Int(scale)
      let significand = abs(storageValue)
      return Decimal(sign: sign, exponent: exponent, significand: significand)
    }
  }
  
  func unsafelyUnwrapElement<T>(as type: T.Type, at index: Int) -> T {
    unsafelyUnwrapElement(as: type, at: index) { $0 }
  }
  
  func unsafelyUnwrapElement<T, U>(
    as type: T.Type, at index: Int, transform: (T) -> U
  ) -> U {
    precondition(index < count, "vector index out of bounds")
    let offsetIndex = offset + index
    let dataPtr = duckdb_vector_get_data(cvector)!
    let itemDataPtr = dataPtr.assumingMemoryBound(to: T.self)
    return transform(itemDataPtr.advanced(by: offsetIndex)[0])
  }
  
  func assertNonNullTypeMatch<T>(
    of type: T.Type, at index: Int, withColumnType columnType: DatabaseType
  ) throws {
    try assertNonNullTypeMatch(of: type, at: index, withColumnTypes: .init([columnType]))
  }
  
  func assertNonNullTypeMatch<T>(
    of type: T.Type, at index: Int, withColumnTypes columnTypes: Set<DatabaseType>
  ) throws {
    guard unwrapNull(at: index) == false else {
      throw DatabaseError.valueNotFound(type)
    }
    guard columnTypes.contains(logicalType.underlyingDataType) else {
      throw DatabaseError.typeMismatch(type)
    }
  }
}

// MARK: - Collection Conformance

extension Vector: Collection {
  
  struct Element {
    let vector: Vector
    let index: Int
  }
  
  public struct Iterator: IteratorProtocol {
    
    private let vector: Vector
    private var position: Int
    
    init(_ vector: Vector) {
      self.vector = vector
      self.position = 0
    }
    
    public mutating func next() -> Element? {
      guard position < vector.count else { return nil }
      defer { position += 1 }
      return vector[position]
    }
  }
  
  public var startIndex: Int { 0 }
  public var endIndex: Int { count }
  
  public subscript(position: Int) -> Element { Element(vector: self, index: position) }
  public func makeIterator() -> Iterator { Iterator(self) }
  
  public func index(after i: Int) -> Int { i + 1 }
  public func index(before i: Int) -> Int { i - 1 }
}

// MARK: - Element Accessors

extension Vector.Element {
  
  var dataType: DatabaseType { vector.logicalType.dataType }
  var logicalType: LogicalType { vector.logicalType }
  
  func unwrapNull() -> Bool { vector.unwrapNull(at: index) }
  func unwrap(_ type: Int.Type) throws -> Int { try vector.unwrap(type, at: index) }
  func unwrap(_ type: UInt.Type) throws -> UInt { try vector.unwrap(type, at: index) }
  func unwrap<T: PrimitiveDatabaseValue>(_ type: T.Type) throws -> T { try vector.unwrap(type, at: index) }
  func unwrap(_ type: String.Type) throws -> String { try vector.unwrap(type, at: index) }
  func unwrap(_ type: IntHuge.Type) throws -> IntHuge { try vector.unwrap(type, at: index) }
  func unwrap(_ type: UIntHuge.Type) throws -> UIntHuge { try vector.unwrap(type, at: index) }
  func unwrap(_ type: UUID.Type) throws -> UUID { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Time.Type) throws -> Time { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Date.Type) throws -> Date { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Interval.Type) throws -> Interval  { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Timestamp.Type) throws -> Timestamp  { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Data.Type) throws -> Data  { try vector.unwrap(type, at: index) }
  func unwrap(_ type: Decimal.Type) throws -> Decimal  { try vector.unwrap(type, at: index) }
  func unwrap(_ type: TimeTz.Type) throws -> TimeTz { try vector.unwrap(type, at: index) }
  func unwrapDecodable<T: Decodable>(_ type: T.Type) throws -> T {
    try VectorElementDecoder.default.decode(T.self, element: self)
  }
}

// MARK: - Map Contents accessors

extension Vector.Element {
  
  struct MapContent {
    let keyVector: Vector
    let valueVector: Vector
  }
  
  var childVector: Vector? {
    guard let child = duckdb_list_vector_get_child(vector.cvector) else { return nil }
    let count = duckdb_list_vector_get_size(vector.cvector)
    let info = vector.unsafelyUnwrapElement(as: duckdb_list_entry_t.self, at: vector.offset + index)
    precondition(info.offset + info.length <= count)
    return Vector(child, count: Int(info.length), offset: Int(info.offset))
  }
  
  var mapContents: MapContent? {
    guard dataType == .map else { return nil }
    guard let childVector = childVector else { return nil }
    guard let keys = duckdb_struct_vector_get_child(childVector.cvector, 0) else { return nil }
    guard let values = duckdb_struct_vector_get_child(childVector.cvector, 1) else { return nil }
    let keyVector = Vector(keys, count: childVector.count, offset: childVector.offset)
    let valueVector = Vector(values, count: childVector.count, offset: childVector.offset)
    return MapContent(keyVector: keyVector, valueVector: valueVector)
  }
}

// MARK: - Struct Contents accesors

extension Vector.Element {
  
  struct StructMemberContent {
    let name: String
    let vector: Vector
  }
  
  var structContents: [StructMemberContent]? {
    guard let properties = vector.logicalType.structMemberProperties else { return nil }
    var content = [StructMemberContent]()
    for (i, member) in properties.enumerated() {
      let memberCVector = duckdb_struct_vector_get_child(vector.cvector, DBInt(i))!
      let memberVector = Vector(memberCVector, count: vector.count, offset: vector.offset)
      content.append(.init(name: member.name, vector: memberVector))
    }
    return content
  }
}


