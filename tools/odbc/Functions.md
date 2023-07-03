# ODBC Supported Functions

## SQLAllocHandle
Allocates a handle.

```c
SQLRETURN SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE * OutputHandlePtr);
```

### Parameters
- `HandleType`: The type of [handle](README.md#handles) to be allocated.
- `InputHandle`: The handle of the input object associated with the handle to be allocated.
- `OutputHandlePtr`: A pointer to the handle to be allocated.

## SQLBindCol
Binds a buffer to a column in the result set.

```c
SQLRETURN SQLBindCol(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN *       StrLen_or_IndPtr);
```

### Parameters
- `StatementHandle`: The statement handle.
- `ColumnNumber`: The number of the column in the result set.
- `TargetType`: The C data type of the buffer.
- `TargetValuePtr`: A pointer to the buffer.
- `BufferLength`: The length of the buffer.
- `StrLen_or_IndPtr`: A pointer to a buffer that contains the length of the data in the buffer.

## SQLBindParameter
