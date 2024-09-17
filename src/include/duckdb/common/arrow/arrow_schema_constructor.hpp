//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_schema_constructor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

struct ArrowSchemaConstructor {
private:
	//===--------------------------------------------------------------------===//
	// ArrowSchema 'private_data'
	//===--------------------------------------------------------------------===//
	struct DuckDBArrowSchemaHolder {
		// unused in children
		vector<ArrowSchema> children;
		// unused in children
		vector<ArrowSchema *> children_ptrs;
		//! used for nested structures
		list<vector<ArrowSchema>> nested_children;
		list<vector<ArrowSchema *>> nested_children_ptr;
		//! This holds strings created to represent decimal types
		vector<unsafe_unique_array<char>> owned_type_names;
		vector<unsafe_unique_array<char>> owned_column_names;
		//! This holds any values created for metadata info
		vector<unsafe_unique_array<char>> metadata_info;
	};

public:
	ArrowSchemaConstructor(const ClientProperties &options, bool add_metadata = true);

public:
	void Construct(ArrowSchema &out, const vector<LogicalType> &types, const vector<string> &names);

private:
	void SetArrowFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type);
	void SetArrowMapFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type);
	void InitializeChild(ArrowSchema &child, DuckDBArrowSchemaHolder &root_holder, const string &name = "");
	static void ReleaseDuckDBArrowSchema(ArrowSchema *schema);

private:
	const ClientProperties &options;
	//! Whether the produced ArrowSchema should have arrow extension type metadata
	bool add_metadata;
};

} // namespace duckdb
