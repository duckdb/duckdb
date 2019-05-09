//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//
enum class ConstraintType : uint8_t {
	INVALID = 0,     // invalid constraint type
	NOT_NULL = 1,    // NOT NULL constraint
	CHECK = 2,       // CHECK constraint
	PRIMARY_KEY = 3, // PRIMARY KEY constraint
	UNIQUE = 4,      // UNIQUE constraint
	FOREIGN_KEY = 5, // FOREIGN KEY constraint
	DUMMY = 6        // Dummy constraint used by parser
};

//! Constraint is the base class of any type of table constraint.
class Constraint {
public:
	Constraint(ConstraintType type) : type(type){};
	virtual ~Constraint() {
	}

	ConstraintType type;

public:
	virtual string ToString() const = 0;
	void Print();

	unique_ptr<Constraint> Copy();
	//! Serializes a Constraint to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a Constraint, returns NULL if
	//! deserialization is not possible
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};
} // namespace duckdb
