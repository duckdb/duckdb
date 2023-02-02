//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/drop_database_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class TableFunctionRef;

struct DropDatabaseExtensionData {
	virtual ~DropDatabaseExtensionData() {
	}
};

typedef unique_ptr<TableFunctionRef> (*drop_database_t)(ClientContext &context, const string &database_name,
                                                        DropDatabaseExtensionData *data);

struct DropDatabaseExtension {
	explicit DropDatabaseExtension(drop_database_t function, unique_ptr<DropDatabaseExtensionData> data_p = nullptr)
	    : function(function), data(std::move(data_p)) {
	}

	drop_database_t function;
	unique_ptr<DropDatabaseExtensionData> data;
};

} // namespace duckdb
