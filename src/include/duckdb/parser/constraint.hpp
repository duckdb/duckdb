//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
class FieldWriter;
class FieldReader;

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//
enum class ConstraintType : uint8_t {
	INVALID = 0,    // invalid constraint type
	NOT_NULL = 1,   // NOT NULL constraint
	CHECK = 2,      // CHECK constraint
	UNIQUE = 3,     // UNIQUE constraint
	FOREIGN_KEY = 4 // FOREIGN KEY constraint
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
