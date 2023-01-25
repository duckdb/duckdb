//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/create_database_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class TableFunctionRef;

struct CreateDatabaseExtensionData {
	virtual ~CreateDatabaseExtensionData() {
	}
};

typedef unique_ptr<TableFunctionRef> (*create_database_t)(ClientContext &context, const string &extension_name,
                                                          const string &database_name, const string &source_path,
                                                          CreateDatabaseExtensionData *data);

struct CreateDatabaseExtension {
	explicit CreateDatabaseExtension(create_database_t function,
	                                 unique_ptr<CreateDatabaseExtensionData> data_p = nullptr)
	    : function(function), data(move(data_p)) {
	}

	create_database_t function;
	unique_ptr<CreateDatabaseExtensionData> data;
};

} // namespace duckdb
