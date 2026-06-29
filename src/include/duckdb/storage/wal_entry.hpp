//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/wal_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//===--------------------------------------------------------------------===//
// WAL entries
//===--------------------------------------------------------------------===//
// These structs hold the serialized payload of a single WAL entry (the fields following the WALType marker).
// Their Serialize/Deserialize implementations are generated from storage/serialization/wal.json. The WAL replay
// logic (which performs the catalog/storage side effects) operates on the deserialized structs.

struct WALCheckpoint {
	MetaBlockPointer meta_block;

	void Serialize(Serializer &serializer) const;
	static WALCheckpoint Deserialize(Deserializer &deserializer);
};

struct WALCreateTable {
	unique_ptr<CreateInfo> table;

	void Serialize(Serializer &serializer) const;
	static WALCreateTable Deserialize(Deserializer &deserializer);
};

struct WALDropTable {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropTable Deserialize(Deserializer &deserializer);
};

struct WALCreateSchema {
	// legacy top-level schema name (serialized for storage versions older than v2.0.0)
	Identifier schema;
	// the schema as a QualifiedName (parent schemas form the path, the schema name is the name); v2.0.0 onwards
	QualifiedName qualified_name;

	void Serialize(Serializer &serializer) const;
	static WALCreateSchema Deserialize(Deserializer &deserializer);
};

struct WALDropSchema {
	// legacy top-level schema name (serialized for storage versions older than v2.0.0)
	Identifier schema;
	// the schema as a QualifiedName (parent schemas form the path, the schema name is the name); v2.0.0 onwards
	QualifiedName qualified_name;

	void Serialize(Serializer &serializer) const;
	static WALDropSchema Deserialize(Deserializer &deserializer);
};

struct WALCreateView {
	unique_ptr<CreateInfo> view;

	void Serialize(Serializer &serializer) const;
	static WALCreateView Deserialize(Deserializer &deserializer);
};

struct WALDropView {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropView Deserialize(Deserializer &deserializer);
};

struct WALCreateSequence {
	unique_ptr<CreateInfo> sequence;

	void Serialize(Serializer &serializer) const;
	static WALCreateSequence Deserialize(Deserializer &deserializer);
};

struct WALDropSequence {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropSequence Deserialize(Deserializer &deserializer);
};

struct WALSequenceValue {
	Identifier schema;
	Identifier name;
	uint64_t usage_count;
	int64_t counter;
	// the last value produced by the sequence; only serialized from storage version v2.0.0 onwards, and omitted when
	// unset (so older readers can still replay sequence values that do not carry a last_value)
	optional<int64_t> last_value;

	void Serialize(Serializer &serializer) const;
	static WALSequenceValue Deserialize(Deserializer &deserializer);
};

struct WALCreateMacro {
	unique_ptr<CreateInfo> macro;

	void Serialize(Serializer &serializer) const;
	static WALCreateMacro Deserialize(Deserializer &deserializer);
};

struct WALDropMacro {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropMacro Deserialize(Deserializer &deserializer);
};

struct WALCreateTableMacro {
	unique_ptr<CreateInfo> table_macro;

	void Serialize(Serializer &serializer) const;
	static WALCreateTableMacro Deserialize(Deserializer &deserializer);
};

struct WALDropTableMacro {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropTableMacro Deserialize(Deserializer &deserializer);
};

struct WALCreateType {
	unique_ptr<CreateInfo> type;

	void Serialize(Serializer &serializer) const;
	static WALCreateType Deserialize(Deserializer &deserializer);
};

struct WALDropType {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropType Deserialize(Deserializer &deserializer);
};

struct WALCreateTrigger {
	unique_ptr<CreateInfo> trigger;

	void Serialize(Serializer &serializer) const;
	static WALCreateTrigger Deserialize(Deserializer &deserializer);
};

struct WALDropTrigger {
	Identifier schema;
	Identifier name;
	Identifier table;

	void Serialize(Serializer &serializer) const;
	static WALDropTrigger Deserialize(Deserializer &deserializer);
};

struct WALDropIndex {
	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const;
	static WALDropIndex Deserialize(Deserializer &deserializer);
};

struct WALUseTable {
	Identifier schema;
	Identifier table;

	void Serialize(Serializer &serializer) const;
	static WALUseTable Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
