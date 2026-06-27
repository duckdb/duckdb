//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo();

public:
	//! The qualified name encodes the full path as [catalog, parent_schemas..., new_schema, <empty name>]. The empty
	//! trailing name keeps the new schema in the Schema() slot (so catalog/schema serialization stays correct). These
	//! helpers interpret it. For a top-level schema the path is [catalog, new_schema, <empty name>].
	//! The name of the schema being created (the last component of the path)
	const Identifier &SchemaName() const;
	//! The catalog the schema is created in (empty if the path carries no catalog component)
	const Identifier &SchemaCatalog() const;
	//! The chain of parent schemas (outermost first); empty for a top-level schema
	vector<Identifier> ParentSchemas() const;
	//! Whether this is a nested schema (i.e. it has at least one parent schema)
	bool IsNested() const;

public:
	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
