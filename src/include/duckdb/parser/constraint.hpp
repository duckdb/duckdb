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
	DUCKDB_API void Print();

	DUCKDB_API virtual unique_ptr<Constraint> Copy() = 0;
	//! Serializes a Constraint to a stand-alone binary blob
	DUCKDB_API virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a Constraint, returns NULL if
	//! deserialization is not possible
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &source);
};
} // namespace duckdb
