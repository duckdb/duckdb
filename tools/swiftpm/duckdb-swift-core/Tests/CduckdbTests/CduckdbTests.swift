import XCTest
@testable import Cduckdb

final class CduckdbTests: XCTestCase {
  
    func test_db_connect() throws {
      var db: duckdb_database?
      var conn: duckdb_connection?
      var status = duckdb_state(0)
      status = duckdb_open(nil, &db)
      XCTAssertEqual(status.rawValue, 0)
      status = duckdb_connect(db, &conn)
      XCTAssertEqual(status.rawValue, 0)
      duckdb_disconnect(&conn)
      duckdb_close(&db)
    }
}
