//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/version_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

class DataTable;
class VersionChunk;

struct VersionInfo {
	DataTable *table;
	VersionChunk *chunk;
	union {
		index_t entry;
		VersionInfo *pointer;
	} prev;
	VersionInfo *next;
	transaction_t version_number;
	data_ptr_t tuple_data;
};

}
