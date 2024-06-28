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

@_implementationOnly import Cduckdb
import Foundation

// MARK: - Top Level Decoder

final class VectorElementDecoder {
  
  static let `default` = VectorElementDecoder()
  
  func decode<T: Decodable>(_ type: T.Type, element: Vector.Element) throws -> T {
    try T(from: VectorElementDataDecoder(element: element))
  }
}

// MARK: - Vector Decoder

fileprivate struct VectorElementDataDecoder: Decoder {
  
  struct VectorDecoderCodingKey: CodingKey {
    
    let stringValue: String
    let intValue: Int?
    
    init(stringValue: String) {
      self.intValue = nil
      self.stringValue = stringValue
    }
    
    init(intValue: Int) {
      self.intValue = intValue
      self.stringValue = "\(intValue)"
    }
  }
  
  let codingPath: [CodingKey]
  let element: Vector.Element
  let userInfo: [CodingUserInfoKey : Any]
  
  init(element: Vector.Element, codingPath: [CodingKey] = []) {
    self.codingPath = codingPath
    self.element = element
    self.userInfo = [CodingUserInfoKeys.logicalTypeCodingUserInfoKey: element.logicalType]
  }
  
  func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
    switch element.dataType {
    case .map:
      return .init(
        try KeyedValueContainer<Key>.createMapContainer(decoder: self, element: element)
      )
    case .struct:
      return .init(
        try KeyedValueContainer<Key>.createStructContainer(decoder: self, element: element)
      )
    default:
      let columnType = element.dataType
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Cannot create keyed decoding container for column type \(columnType)"
      )
      throw DecodingError.typeMismatch(type, context)
    }
  }
  
  func unkeyedContainer() throws -> UnkeyedDecodingContainer {
    switch element.dataType {
    case .list:
      return try UnkeyedValueContainer.createListContainer(decoder: self, element: element)
    default:
      let columnType = element.dataType
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Cannot create unkeyed decoding container for column type \(columnType)"
      )
      throw DecodingError.typeMismatch(UnkeyedDecodingContainer.self, context)
    }
  }
  
  func singleValueContainer() -> SingleValueDecodingContainer {
    SingleValueContainer(decoder: self, codingPath: codingPath, element: element)
  }
}

// MARK: - Single Value Container

extension VectorElementDataDecoder {
  
  struct SingleValueContainer: SingleValueDecodingContainer {
    
    let decoder: VectorElementDataDecoder
    var codingPath: [CodingKey]
    let element: Vector.Element
    
    init(decoder: VectorElementDataDecoder, codingPath: [CodingKey], element: Vector.Element) {
      self.decoder = decoder
      self.codingPath = codingPath
      self.element = element
    }
    
    // Protocol conforming `decode(_:)` implementations
    
    func decodeNil() -> Bool {
      element.unwrapNull()
    }
    
    func decode(_ type: Int.Type) throws -> Int {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Int8.Type) throws -> Int8 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Int16.Type) throws -> Int16 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Int32.Type) throws -> Int32 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Int64.Type) throws -> Int64 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UInt.Type) throws -> UInt {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UInt8.Type) throws -> UInt8 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UInt16.Type) throws -> UInt16 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UInt32.Type) throws -> UInt32 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UInt64.Type) throws -> UInt64 {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Float.Type) throws -> Float {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Double.Type) throws -> Double {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: String.Type) throws -> String {
      try attemptDecode { try element.unwrap(type) }
    }
    
    // Special case `decode(_:)` implementations
    
    func decode(_ type: IntHuge.Type) throws -> IntHuge {
      try attemptDecode { try element.unwrap(type) }
    }

	func decode(_ type: UIntHuge.Type) throws -> UIntHuge {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Decimal.Type) throws -> Decimal {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Data.Type) throws -> Data {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: UUID.Type) throws -> UUID {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Date.Type) throws -> Date {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Time.Type) throws -> Time {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: TimeTz.Type) throws -> TimeTz {
      try attemptDecode { try element.unwrap(type) }
    }

    func decode(_ type: Timestamp.Type) throws -> Timestamp {
      try attemptDecode { try element.unwrap(type) }
    }
    
    func decode(_ type: Interval.Type) throws -> Interval {
      try attemptDecode { try element.unwrap(type) }
    }
    
    // Generic decode
    
    func decode<T: Decodable>(_ type: T.Type) throws -> T {
      switch type {
      case is IntHuge.Type:
        return try decode(IntHuge.self) as! T
      case is UIntHuge.Type:
        return try decode(UIntHuge.self) as! T
      case is Decimal.Type:
        return try decode(Decimal.self) as! T
      case is Data.Type:
        return try decode(Data.self) as! T
      case is UUID.Type:
        return try decode(UUID.self) as! T
      case is Date.Type:
        return try decode(Date.self) as! T
      case is Time.Type:
        return try decode(Time.self) as! T
      case is Timestamp.Type:
        return try decode(Timestamp.self) as! T
      case is Interval.Type:
        return try decode(Interval.self) as! T
      case is TimeTz.Type:
        return try decode(TimeTz.self) as! T
      default:
        return try T(from: decoder)
      }
    }
    
    private func attemptDecode<T>(_ body: () throws -> T) throws -> T {
      do {
        return try body()
      }
      catch DatabaseError.valueNotFound(let type) {
        let context = DecodingError.Context(
          codingPath: codingPath,
          debugDescription: "Expected value of type \(type) – found null value instead"
        )
        throw DecodingError.valueNotFound(type, context)
      }
      catch DatabaseError.typeMismatch(let type) {
        let columnType = element.dataType
        let context = DecodingError.Context(
          codingPath: codingPath,
          debugDescription: "Expected to decode \(type) but found \(columnType) instead."
        )
        throw DecodingError.typeMismatch(type, context)
      }
    }
  }
}

// MARK: - Unkeyed Container

extension VectorElementDataDecoder {
  
  struct UnkeyedValueContainer: UnkeyedDecodingContainer {
    
    let decoder: VectorElementDataDecoder
    let codingPath: [CodingKey]
    let vector: Vector
    let count: Int?
    var currentIndex = 0
    
    var isAtEnd: Bool { currentIndex >= vector.count }
    
    init(
      decoder: VectorElementDataDecoder,
      codingPath: [CodingKey],
      vector: Vector
    ) {
      self.decoder = decoder
      self.codingPath = codingPath
      self.vector = vector
      self.count = vector.count
    }
    
    func decodeNil() -> Bool {
      vector[currentIndex].unwrapNull()
    }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
      let decoder = try superDecoder()
      let value = try T(from: decoder)
      currentIndex += 1
      return value
    }
    
    func nestedContainer<NestedKey: CodingKey>(
      keyedBy type: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> {
      let decoder = try superDecoder()
      return try decoder.container(keyedBy: type)
    }
    
    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
      let decoder = try superDecoder()
      return try decoder.unkeyedContainer()
    }
    
    func superDecoder() throws -> Decoder {
      var codingPath = decoder.codingPath
      codingPath.append(VectorDecoderCodingKey(intValue: currentIndex))
      return VectorElementDataDecoder(element: vector[currentIndex], codingPath: codingPath)
    }
  }
}

// MARK: - Keyed Container

extension VectorElementDataDecoder {
  
  struct KeyedValueContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    
    struct Properties {
      let keyMap: [String: KeyPath]
      let vectors: [Vector]
    }
    
    struct KeyPath {
      let key: Key
      let vectorIndex: Int
      let rowIndex: Int
    }
    
    let decoder: VectorElementDataDecoder
    let codingPath: [CodingKey]
    let keyMap: [String: KeyPath]
    let vectors: [Vector]
    
    init(
      decoder: VectorElementDataDecoder,
      codingPath: [CodingKey],
      keyMap: [String: KeyPath],
      vectors: [Vector]
    ) {
      self.decoder = decoder
      self.codingPath = codingPath
      self.keyMap = keyMap
      self.vectors = vectors
    }
    
    var allKeys: [Key]  { keyMap.values.map { $0.key } }
    
    func contains(_ key: Key) -> Bool {
      keyMap[key.stringValue] != nil
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
      let path = try unwrapPath(forKey: key)
      let vector = vectors[path.vectorIndex]
      return vector[path.rowIndex].unwrapNull()
    }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
      let decoder = try superDecoder(forKey: key)
      return try T(from: decoder)
    }
    
    func nestedContainer<NestedKey: CodingKey>(
      keyedBy type: NestedKey.Type, forKey key: Key
    ) throws -> KeyedDecodingContainer<NestedKey> {
      let decoder = try superDecoder(forKey: key)
      return try decoder.container(keyedBy: type)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
      let decoder = try superDecoder(forKey: key)
      return try decoder.unkeyedContainer()
    }
    
    func superDecoder() throws -> Decoder {
      VectorElementDataDecoder(element: decoder.element, codingPath: decoder.codingPath)
    }
    
    func superDecoder(forKey key: Key) throws -> Decoder {
      let path = try unwrapPath(forKey: key)
      let vector = vectors[path.vectorIndex]
      var codingPath = decoder.codingPath
      codingPath.append(VectorDecoderCodingKey(stringValue: key.stringValue))
      return VectorElementDataDecoder(element: vector[path.rowIndex], codingPath: codingPath)
    }
    
    private func unwrapPath(forKey key: Key) throws -> KeyPath {
      guard let path = keyMap[key.stringValue] else {
        let context = DecodingError.Context(
          codingPath: codingPath,
          debugDescription: "Key not found"
        )
        throw DecodingError.keyNotFound(key, context)
      }
      return path
    }
  }
}

// MARK: - Container Factories

fileprivate extension VectorElementDataDecoder.KeyedValueContainer {
  
  static func createMapContainer(
    decoder: VectorElementDataDecoder, element: Vector.Element
  ) throws -> Self {
    guard element.unwrapNull() == false else {
      let context = DecodingError.Context(
        codingPath: decoder.codingPath,
        debugDescription: "Cannot get keyed decoding container – found null value instead"
      )
      throw DecodingError.valueNotFound(Self.self, context)
    }
    guard element.dataType == .map else {
      let columnType = element.dataType
      let context = DecodingError.Context(
        codingPath: decoder.codingPath,
        debugDescription: "Expected map column type, found \(columnType) column type instead."
      )
      throw DecodingError.typeMismatch(Self.self, context)
    }
    guard let mapContent = element.mapContents else {
      fatalError("Internal consistency error. Expected map content in vector.")
    }
    let keys: [Key]
    switch mapContent.keyVector.logicalType.dataType {
    case .varchar:
      let stringKeys = try mapContent.keyVector.map { try $0.unwrap(String.self) }
      keys = try stringKeys.map { try createKey(stringValue: $0) }
    case .tinyint, .smallint, .integer, .bigint:
      let intKeys = try mapContent.keyVector.map { try $0.unwrap(Int.self) }
      keys = try intKeys.map { try createKey(intValue: $0) }
    default:
      let context = DecodingError.Context(
        codingPath: decoder.codingPath,
        debugDescription: "Cannot decode map with non string/integer key type"
      )
      throw DecodingError.typeMismatch(Key.self, context)
    }
    let vectors = [mapContent.valueVector]
    let mapKeys = keys.map(\.stringValue)
    let mapValues = keys.enumerated().map { KeyPath(key: $0.1, vectorIndex: 0, rowIndex: $0.0) }
    let keyMap = Dictionary(uniqueKeysWithValues: zip(mapKeys, mapValues))
    return Self(decoder: decoder, codingPath: decoder.codingPath, keyMap: keyMap, vectors: vectors)
  }
  
  static func createStructContainer(
    decoder: VectorElementDataDecoder, element: Vector.Element
  ) throws -> Self {
    let codingPath = decoder.codingPath
    let dataType = element.dataType
    guard element.unwrapNull() == false else {
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Cannot get keyed decoding container – found null value instead"
      )
      throw DecodingError.valueNotFound(Self.self, context)
    }
    guard dataType == .struct else {
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Expected struct column type, found \(dataType) column type instead."
      )
      throw DecodingError.typeMismatch(Self.self, context)
    }
    guard let structContents = element.structContents else {
      fatalError("Internal consistency error. Expected struct content in vector.")
    }
    let keys = try structContents.map(\.name).map(Self.createKey(stringValue:))
    let vectors = structContents.map(\.vector)
    var keyMap = [String: KeyPath]()
    for (i, key) in keys.enumerated() {
      keyMap[key.stringValue] = .init(key: key, vectorIndex: i, rowIndex: element.index)
    }
    return Self(decoder: decoder, codingPath: codingPath, keyMap: keyMap, vectors: vectors)
  }
  
  private static func createKey(stringValue: String) throws -> Key {
    guard let key = Key(stringValue: stringValue) else {
      let context = DecodingError.Context(
        codingPath: [],
        debugDescription: "Cannot instatiate key of type \(Key.self) with string: \(stringValue)"
      )
      throw DecodingError.typeMismatch(Key.self, context)
    }
    return key
  }
  
  private static func createKey(intValue: Int) throws -> Key {
    guard let key = Key(intValue: intValue) else {
      let context = DecodingError.Context(
        codingPath: [],
        debugDescription: "Cannot instatiate key of type \(Key.self) with integer: \(intValue)"
      )
      throw DecodingError.typeMismatch(Key.self, context)
    }
    return key
  }
}

fileprivate extension VectorElementDataDecoder.UnkeyedValueContainer {
  
  static func createListContainer(
    decoder: VectorElementDataDecoder, element: Vector.Element
  ) throws -> Self {
    let codingPath = decoder.codingPath
    let dataType = element.dataType
    guard element.unwrapNull() == false else {
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Cannot get unkeyed decoding container – found null value instead"
      )
      throw DecodingError.valueNotFound(Self.self, context)
    }
    guard dataType == .list else {
      let context = DecodingError.Context(
        codingPath: codingPath,
        debugDescription: "Expected list column type, found \(dataType) column type instead."
      )
      throw DecodingError.typeMismatch(Self.self, context)
    }
    guard let childVector = element.childVector else {
      fatalError("Internal consistency error. Expected list content in vector.")
    }
    return Self(decoder: decoder, codingPath: codingPath, vector: childVector)
  }
}
