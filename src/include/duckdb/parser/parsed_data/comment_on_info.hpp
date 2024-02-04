//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/comment_on_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

struct CommentOnInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::COMMENT_ON_INFO;

public:
	CommentOnInfo();

	//! Catalog type to comment on
	CatalogType type;

	//! The catalog name of the entry to comment on
	string catalog;
	//! The schema name of the entry to comment on
	string schema;
	//! The name of the entry to comment on
	string name;

	//! The comment, can be NULL or a string
	Value comment;

public:
	unique_ptr<CommentOnInfo> Copy() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
