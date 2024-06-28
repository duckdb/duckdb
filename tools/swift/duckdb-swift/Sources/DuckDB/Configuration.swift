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

public extension Database {
  
  /// An object representing a DuckDB database configuration
  ///
  /// Configuration options can be provided to change different settings of the
  /// database system. Note that many of these settings can be changed later on
  /// using PRAGMA statements as well. The configuration object should be
  /// created, filled with values using ``setValue(_:forKey:)`` and passed to
  /// ``Database/init(store:configuration:)``
  ///
  /// The following example sets up an in-memory database with some
  /// configuration options set
  ///
  /// ```swift
  /// do {
    /// let configuration = Configuration()
  ///   configuration.setValue("READ_WRITE", forKey: "access_mode")
  ///   configuration.setValue("8", forKey: "threads")
  ///   configuration.setValue("8GB", forKey: "max_memory")
  ///   configuration.setValue("DESC", forKey: "default_order")
  ///   let database = try Database(store: .inMemory, configuration: configuration)
  ///   let connection = try database.connect()
  /// }
  /// catch {
  ///   // handle error
  /// }
  /// ```
  /// Use ``Database/Configuration/options`` to see the full list of available
  /// configuration options.
  final class Configuration {
    
    private let ptr = UnsafeMutablePointer<duckdb_config?>.allocate(capacity: 1)
    
    /// Creates a DuckDB configuration
    public init() {
      let status = duckdb_create_config(ptr)
      guard status == .success else { fatalError("malloc failure") }
    }
    
    deinit {
      duckdb_destroy_config(ptr)
      ptr.deallocate()
    }
    
    func withCConfiguration<T>(_ body: (duckdb_config?) throws -> T) rethrows -> T {
      try body(ptr.pointee)
    }
    
    /// Set a value on the configuration object
    ///
    /// Applies a configuration option to the configuration object. Use
    /// ``Database/Configuration/options`` to see the full list of available
    /// configuration options.
    ///
    /// - Parameter key: the name of the option being set
    /// - Parameter value: the desired value of the option being set
    /// - Throws: ``DatabaseError/configurationFailedToSetFlag`` if the option
    ///   is not available or the value is malformed
    public func setValue(_ value: String, forKey key: String) throws {
      try value.withCString { valueCStr in
        try key.withCString { keyCStr in
          let status = duckdb_set_config(ptr.pointee, keyCStr, valueCStr)
          guard .success == status else {
            throw DatabaseError.configurationFailedToSetFlag
          }
        }
      }
    }
  }
}

// MARK: - Option

public extension Database.Configuration {
  
  /// A DuckDB database configuration option information type
  struct OptionInfo {
    /// The name/key of the option
    public let name: String
    /// An overview of the option and its potential values
    public let description: String
  }
  
  /// A list of all available database configuration options with their
  /// descriptions
  static var options: [OptionInfo] {
    let count = duckdb_config_count()
    var options = [OptionInfo]()
    let outName = UnsafeMutablePointer<UnsafePointer<CChar>?>.allocate(capacity: 1)
    let outDesc = UnsafeMutablePointer<UnsafePointer<CChar>?>.allocate(capacity: 1)
    defer {
      outName.deallocate()
      outDesc.deallocate()
    }
    for i in 0..<count {
      duckdb_get_config_flag(i, outName, outDesc)
      if let nameCStr = outName.pointee, let descCStr = outDesc.pointee {
        let name = String(cString: nameCStr)
        let desc = String(cString: descCStr)
        options.append(OptionInfo(name: name, description: desc))
      }
      outName.pointee = nil
      outDesc.pointee = nil
    }
    return options
  }
}
