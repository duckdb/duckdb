#ifndef ODBC_FETCH_HPP
#define ODBC_FETCH_HPP

#include "duckdb.hpp"
// #include <cstdint>
#include <sqltypes.h>

namespace duckdb {

class OdbcHandleStmt;

enum class FetchOrientation : uint8_t {
    COLUMN = 0,
    ROW = 1
};

class OdbcFetch {
public:
    FetchOrientation orientation;
    SQLULEN rows_to_fetch;
   	SQLPOINTER row_length;
    SQLUSMALLINT *row_status_buff;

public:
    OdbcFetch(): orientation(FetchOrientation::COLUMN), rows_to_fetch(1) {
    }
    ~OdbcFetch();

    SQLRETURN Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

private:
    SQLRETURN ColumnWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

    SQLRETURN RowWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

};
} // namespace duckdb

#endif // ODBC_FETCH_HPP
