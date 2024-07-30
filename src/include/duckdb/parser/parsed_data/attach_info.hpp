//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/attach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"

namespace duckdb {

struct AttachInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ATTACH_INFO;

public:
	AttachInfo() : ParseInfo(TYPE) {
	}

	//! The alias of the attached database
	string name;
	//! The path to the attached database
	string path;
	//! Set of (key, value) options
	unordered_map<string, Value> options;
	//! What to do on create conflict
	OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;

public:
	//! Returns the block allocation size, which is the allocation size of blocks for this attached database file.
	//! Returns DConstants::INVALID_INDEX, if not provided. This is NOT the actual memory available on a block
	//! (block_size), even though the corresponding option we expose to the user is called "block_size".
	optional_idx GetBlockAllocSize() const;
	//! Copies this AttachInfo and returns an unique pointer to the new AttachInfo.
	unique_ptr<AttachInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
