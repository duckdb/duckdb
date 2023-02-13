@_implementationOnly import Cduckdb
import TabularData
import Foundation

enum DuckDBError: Error {
    case runtimeError(String)
}


public enum DuckDBInternalError: Error {
    case typeError
}


public final class Database {
    public init(_ filename: String = ":memory:") throws {
        let error_msg  = UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>.allocate(capacity: 1)
        // TODO config
        let open_res = duckdb_open_ext(filename, &_handle, nil, error_msg)
        if (open_res != DuckDBSuccess) {
            var message = ""
            if (Int(bitPattern: error_msg.pointee) != 0) {
                message = String(cString: error_msg.pointee.unsafelyUnwrapped)
            }
            throw DuckDBError.runtimeError(message)
        }
    }
    
    public func connect() throws -> Connection  {
        return try Connection(self)
    }
    
    deinit {
        duckdb_close(&_handle);
    }
    
    fileprivate var _handle: duckdb_database?
}

public final class Connection {
    public init(_ db: Database) throws {
        let connect_res = duckdb_connect(db._handle, &_handle)
        if (connect_res != DuckDBSuccess) {
            throw DuckDBError.runtimeError("Failed to connect?")
        }
    }
    
    public func prepare(_ sql:String) throws -> Statement {
        return try Statement(self, sql)
    }
    
    @available(macOS,introduced:12)
    @available(iOS,introduced:15)
    public func execute(_ sql:String) throws -> DataFrame {
        return try Statement(self, sql).run()
    }
    
    
    deinit {
        duckdb_disconnect(&_handle);
    }
    
    fileprivate var _handle: duckdb_connection?
}

let DUCKDB_SIZEOF_STRING_T = 16
let DUCKDB_INLINE_LIMIT = 12




public final class Statement {
    public init(_ con: Connection, _ sql: String) throws {
        let prepare_res = duckdb_prepare(con._handle, sql, &_stmt_handle)
        if (prepare_res != DuckDBSuccess) {
            throw DuckDBError.runtimeError(String(cString: duckdb_prepare_error(_stmt_handle).unsafelyUnwrapped))
        }
        _res_handle = UnsafeMutablePointer<duckdb_result>.allocate(capacity: 1)
    }
    
    @available(macOS,introduced:12)
    @available(iOS,introduced:15)
    static private func fill<T>( _ vec: duckdb_vector?, _ count: Int, _ value_class: T.Type, _ out:inout AnyColumn ) {
        let data_ptr = duckdb_vector_get_data(vec)
        let validity = duckdb_vector_get_validity(vec)
        for offset in 0..<count {
            if (duckdb_validity_row_is_valid(validity, UInt64(offset))) {
                out.append(data_ptr.unsafelyUnwrapped.load(fromByteOffset: offset * MemoryLayout<T>.size, as: value_class))
            } else {
                out.append(nil)
            }
        }
    }
    
    static private func convert_string(_ ptr : UnsafeMutableRawPointer) -> String {
        let int_len = MemoryLayout<UInt32>.size
        let str_len = Int(ptr.load(as: UInt32.self))
        let str_ptr = (str_len <= DUCKDB_INLINE_LIMIT) ? ptr.advanced(by: int_len) : ptr.advanced(by: 2 * int_len).load(as: UnsafeMutableRawPointer.self)
        return(String(data: Data(bytes: str_ptr, count: str_len), encoding:.utf8)!)
    }
    
    // TODO optionally take prepared statement parameters
    @available(macOS,introduced:12)
    @available(iOS,introduced:15)
    public func run() throws -> DataFrame  {
        let execute_res = duckdb_execute_prepared(_stmt_handle, _res_handle)
        if (execute_res != DuckDBSuccess) {
            throw DuckDBError.runtimeError(String(cString: duckdb_result_error(_res_handle).unsafelyUnwrapped))
        }
        
        if (duckdb_row_count(_res_handle) == 0) {
            return DataFrame()
        }
        var res_columns = Array<AnyColumn>()
        
        for col_idx in 0..<duckdb_column_count(_res_handle) {
            let name = String(cString: duckdb_column_name(_res_handle, col_idx))
            let count = Int(duckdb_row_count(_res_handle))
            let duckdb_type = duckdb_column_type(_res_handle, col_idx)
            var res_col:AnyColumn
            switch (duckdb_type) {
            case DUCKDB_TYPE_BOOLEAN:
                res_col = Column<Bool>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_TINYINT:
                res_col = Column<Int8>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_UTINYINT:
                res_col = Column<UInt8>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_SMALLINT:
                res_col = Column<Int16>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_USMALLINT:
                res_col = Column<UInt16>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_INTEGER:
                res_col = Column<Int32>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_UINTEGER:
                res_col = Column<UInt32>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_BIGINT:
                res_col = Column<Int64>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_UBIGINT:
                res_col = Column<UInt64>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_FLOAT:
                res_col = Column<Float>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_DOUBLE:
                res_col = Column<Double>.init (name: name, capacity: count).eraseToAnyColumn()
            case DUCKDB_TYPE_VARCHAR:
                res_col = Column<String>.init (name: name, capacity: count).eraseToAnyColumn()
            default:
                // TODO other types
                throw DuckDBInternalError.typeError
            }
            
            for chunk_idx in 0..<duckdb_result_chunk_count(_res_handle.pointee) {
                var chunk = duckdb_result_get_chunk(_res_handle.pointee, chunk_idx)
                let chunk_len = Int(duckdb_data_chunk_get_size(chunk))
                let vec = duckdb_data_chunk_get_vector(chunk, col_idx)
                switch (duckdb_type) {
                case DUCKDB_TYPE_BOOLEAN:
                    Statement.fill(vec, chunk_len, Bool.self, &res_col)
                case DUCKDB_TYPE_TINYINT:
                    Statement.fill(vec, chunk_len, Int8.self, &res_col)
                case DUCKDB_TYPE_UTINYINT:
                    Statement.fill(vec, chunk_len, UInt8.self, &res_col)
                case DUCKDB_TYPE_SMALLINT:
                    Statement.fill(vec, chunk_len, Int16.self, &res_col)
                case DUCKDB_TYPE_USMALLINT:
                    Statement.fill(vec, chunk_len, UInt16.self, &res_col)
                case DUCKDB_TYPE_INTEGER:
                    Statement.fill(vec, chunk_len, Int32.self, &res_col)
                case DUCKDB_TYPE_UINTEGER:
                    Statement.fill(vec, chunk_len, UInt32.self, &res_col)
                case DUCKDB_TYPE_BIGINT:
                    Statement.fill(vec, chunk_len, Int64.self, &res_col)
                case DUCKDB_TYPE_UBIGINT:
                    Statement.fill(vec, chunk_len, UInt64.self, &res_col)
                case DUCKDB_TYPE_FLOAT:
                    Statement.fill(vec, chunk_len, Float.self, &res_col)
                case DUCKDB_TYPE_DOUBLE:
                    Statement.fill(vec, chunk_len, Double.self, &res_col)
                case DUCKDB_TYPE_VARCHAR:
                    let data_ptr = duckdb_vector_get_data(vec).unsafelyUnwrapped
                    let validity = duckdb_vector_get_validity(vec).unsafelyUnwrapped
                    for offset in 0..<chunk_len {
                        if (duckdb_validity_row_is_valid(validity, UInt64(offset))) {
                            res_col.append(Statement.convert_string(data_ptr.advanced(by: offset * DUCKDB_SIZEOF_STRING_T)))
                        } else {
                            res_col.append(nil)
                        }
                    }
                default:
                    throw DuckDBInternalError.typeError
                }
                duckdb_destroy_data_chunk(&chunk)
            }
            res_columns.append(res_col)
        }
        
        return DataFrame.init(columns: res_columns)
    }
    
    deinit {
        duckdb_destroy_result(_res_handle)
        duckdb_destroy_prepare(&_stmt_handle);
    }
    
    fileprivate var _stmt_handle: duckdb_prepared_statement?
    fileprivate var _res_handle: UnsafeMutablePointer<duckdb_result>
}

