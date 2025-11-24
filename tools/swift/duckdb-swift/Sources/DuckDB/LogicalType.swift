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

public final class LogicalType {
  
  private let ptr = UnsafeMutablePointer<duckdb_logical_type?>.allocate(capacity: 1)
  
  init(_ body: () -> duckdb_logical_type?) {
    self.ptr.pointee = body()
  }
  
  deinit {
    duckdb_destroy_logical_type(ptr)
    ptr.deallocate()
  }

  /// The primitive type represented by this logical type.
  public var dataType: DatabaseType {
    let ctypeid = duckdb_get_type_id(ptr.pointee)
    return ctypeid.asTypeID
  }

  /// The primitive type representing the internal storage type, which is equivalent
  /// to ``dataType``, except for when the type is an enum.
  public var underlyingDataType: DatabaseType {
    guard dataType == .enum else { return dataType }
    let ctypeid = duckdb_enum_internal_type(ptr.pointee)
    return ctypeid.asTypeID
  }
}

// MARK: - Decimal

public extension LogicalType {

  /// Properties associated with a decimal type
  struct DecimalProperties {
    let width: UInt8
    let scale: UInt8
    let storageType: DatabaseType
  }

  /// Properties associated with a decimal type. For all other types, returns `nil`
  var decimalProperties: DecimalProperties? {
    guard dataType == .decimal else { return nil }
    let internalStorageType = duckdb_decimal_internal_type(ptr.pointee)
    return .init(
      width: duckdb_decimal_width(ptr.pointee),
      scale: duckdb_decimal_scale(ptr.pointee),
      storageType: internalStorageType.asTypeID
    )
  }
}

// MARK: - Struct

public extension LogicalType {
  
  static let structCompatibleTypes = [DatabaseType.struct, .map]

  /// Properties associated with a struct type
  struct StructMemberProperties {
    let name: String
    let type: LogicalType
  }

  /// Properties associated with a struct type. For all other types, returns `nil`
  var structMemberProperties: [StructMemberProperties]? {
    guard Self.structCompatibleTypes.contains(dataType) else { return nil }
    let memberCount = duckdb_struct_type_child_count(ptr.pointee)
    var properties = [StructMemberProperties]()
    properties.reserveCapacity(Int(memberCount))
    for i in 0..<memberCount {
      let cStr = duckdb_struct_type_child_name(ptr.pointee, i)!
      properties.append(StructMemberProperties(
        name: String(cString: cStr),
        type: LogicalType { duckdb_struct_type_child_type(ptr.pointee, i) }
      ))
      duckdb_free(cStr)
    }
    return properties
  }
}

// MARK: Union

public extension LogicalType {

  /// Properties associated with a union type
  struct UnionMemberProperties {
    let name: String
    let type: LogicalType
  }

  /// Properties associated with a union type. For all other types, returns `nil`
  var unionMemberProperties: [UnionMemberProperties]? {
    guard dataType == .union else { return nil }
    let memberCount = duckdb_union_type_member_count(ptr.pointee)
    var properties = [UnionMemberProperties]()
    properties.reserveCapacity(Int(memberCount))
    for i in 0..<memberCount {
      let cStr = duckdb_union_type_member_name(ptr.pointee, i)!
      properties.append(UnionMemberProperties(
        name: String(cString: cStr),
        type: LogicalType { duckdb_union_type_member_type(ptr.pointee, i) }
      ))
      duckdb_free(cStr)
    }
    return properties
  }
}

// MARK: - List

public extension LogicalType {

  /// Child type of a list type. For all other types, returns `nil`
  var listChildType: LogicalType? {
    guard dataType == .list else { return nil }
    return LogicalType { duckdb_list_type_child_type(ptr.pointee) }
  }
}

// MARK: - Map

public extension LogicalType {

  /// Key type of a map type. For all other types, returns `nil`
  var mapKeyType: LogicalType? {
    guard dataType == .map else { return nil }
    return LogicalType { duckdb_map_type_key_type(ptr.pointee) }
  }

  /// Value type of a map type. For all other types, returns `nil`
  var mapValueType: LogicalType? {
    guard dataType == .map else { return nil }
    return LogicalType { duckdb_map_type_value_type(ptr.pointee) }
  }
}
