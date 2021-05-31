#pragma once

enum class SQLiteTypeValue : uint8_t {
	INTEGER = 0,
    FLOAT   = 1,
    TEXT    = 2,
    BLOB    = 3,
    NUL    = 4
};
