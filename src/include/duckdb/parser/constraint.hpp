//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
class FieldWriter;
class FieldReader;

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//
enum class ConstraintType : uint8_t {
	INVALID = 0,     // invalid constraint type
	NOT_NULL = 1,    // NOT NULL constraint
	CHECK = 2,       // CHECK constraint
	UNIQUE = 3,      // UNIQUE constraint
	FOREIGN_KEY = 4, // FOREIGN KEY constraint
};

enum class ForeignKeyType : uint8_t {
	FK_TYPE_PRIMARY_KEY_TABLE = 0,   // main table
	FK_TYPE_FOREIGN_KEY_TABLE = 1,   // referencing table
	FK_TYPE_SELF_REFERENCE_TABLE = 2 // self refrencing table
};

struct ForeignKeyInfo {
	ForeignKeyType type;
	string schema;
	//! if type is FK_TYPE_FOREIGN_KEY_TABLE, means main key table, if type is FK_TYPE_PRIMARY_KEY_TABLE, means foreign
	//! key table
	string table;
	//! The set of main key table's column's index
	vector<storage_t> pk_keys;
	//! The set of foreign key table's column's index
	vector<storage_t> fk_keys;

	const vector<storage_t> &GetKeys() const {
		if (type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE) {
			return pk_keys;
		} else {
			return fk_keys;
		}
	}

	bool operator==(const ForeignKeyInfo &other) const {
		if (type != other.type)
			return false;
		if (schema != other.schema)
			return false;
		if (table != other.table)
			return false;
		if (pk_keys != other.pk_keys)
			return false;
		if (fk_keys != other.fk_keys)
			return false;
		return true;
	}
};

//! Constraint is the base class of any type of table constraint.
class Constraint {
public:
	DUCKDB_API explicit Constraint(ConstraintType type);
	DUCKDB_API virtual ~Constraint();

	ConstraintType type;

public:
	DUCKDB_API virtual string ToString() const = 0;
	DUCKDB_API void Print() const;

	DUCKDB_API virtual unique_ptr<Constraint> Copy() const = 0;
	//! Serializes a Constraint to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) const;
	//! Serializes a Constraint to a stand-alone binary blob
	DUCKDB_API virtual void Serialize(FieldWriter &writer) const = 0;
	//! Deserializes a blob back into a Constraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb

namespace std {

template <>
struct hash<duckdb::ForeignKeyInfo> {
	template <class X>
	static size_t compute_hash(const X &x) {
		return hash<X>()(x);
	}

	size_t operator()(const duckdb::ForeignKeyInfo &j) const {
		D_ASSERT(j.pk_keys.size() > 0);
		D_ASSERT(j.fk_keys.size() > 0);
		return compute_hash((size_t)j.type) + compute_hash(j.schema) + compute_hash(j.table) +
		       compute_hash(j.pk_keys[0]) + compute_hash(j.fk_keys[0]);
	}
};

} // namespace std
