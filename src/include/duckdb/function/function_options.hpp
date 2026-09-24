//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! An option a function receives through its "**kwargs" parameter
class FunctionOptionDefinition {
public:
	FunctionOptionDefinition(Identifier name, LogicalType type);

	//! The name the function receives the option under
	Identifier name;
	//! Other names a caller can pass the option by
	vector<Identifier> aliases;
	//! The type a passed value is cast to - ANY passes it through as-is, for the function to check
	LogicalType type;

public:
	DUCKDB_API bool operator==(const FunctionOptionDefinition &other) const;
	DUCKDB_API bool operator!=(const FunctionOptionDefinition &other) const;
	DUCKDB_API string ToString() const;
};

//! The options a function receives through its "**kwargs" parameter
class FunctionOptionSchema {
public:
	//! Adds an option - a call that leaves it out does not pass it, the function decides what that means
	DUCKDB_API FunctionOptionSchema &Add(Identifier name, LogicalType type);
	//! Adds another name for the option added last
	DUCKDB_API FunctionOptionSchema &Alias(Identifier alias);

	//! A schema holding the options of this one followed by those of the other
	DUCKDB_API FunctionOptionSchema Merge(const FunctionOptionSchema &other) const;

	//! The option a name or an alias refers to, or nullptr if the schema declares no such name
	DUCKDB_API optional_ptr<const FunctionOptionDefinition> Find(const Identifier &name) const;
	//! The options in the order they were added
	DUCKDB_API const vector<FunctionOptionDefinition> &GetOptions() const;
	//! Every name and alias the schema declares
	DUCKDB_API vector<Identifier> GetNames() const;

	DUCKDB_API bool operator==(const FunctionOptionSchema &other) const;
	DUCKDB_API bool operator!=(const FunctionOptionSchema &other) const;
	DUCKDB_API hash_t Hash() const;

	//! Adding does not check the names - this does, once the schema is complete
	//! @throws InvalidInputException if a name or an alias is declared twice
	DUCKDB_API void Verify() const;

private:
	vector<FunctionOptionDefinition> options;
};

} // namespace duckdb
