//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// dsdgen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_cpp.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace tpcds {

struct DSDGenWrapper {
	//! Throws when dsdgen cannot generate the given scale factor
	static void ValidateScaleFactor(double scale);

	static uint32_t QueriesCount();
	//! Gets the specified TPC-DS Query number as a string
	static std::string GetQuery(int query);
	//! Gets the specified TPC-DS Query number with the substitution parameters for the given scale factor
	static std::string GetQuery(int query, double sf);
	//! Returns the CSV answer of a TPC-DS query
	static std::string GetAnswer(double sf, int query);
};

struct DSDGenOptions {
	double sf = 0;
	//! Where to create the tables; the current catalog and schema when not given
	std::optional<std::string> catalog;
	std::optional<std::string> schema;
	std::string suffix;
	bool overwrite = false;
	bool keys = false;
};

//! Creates the TPC-DS tables in the transaction of a context, and fills them from every thread scanning dsdgen
class DSDGenGenerator {
public:
	virtual ~DSDGenGenerator();

	//! Creates the tables, and plans the generation of their rows
	static std::unique_ptr<DSDGenGenerator> Create(duckdb::cxx::Context &context, const DSDGenOptions &options);

	//! How many threads can generate rows at the same time
	virtual duckdb::cxx::idx_t MaxThreads() const = 0;
	//! Takes the next step of generating and inserting rows, returning false when the calling thread has no work left.
	//! Called by every thread scanning dsdgen.
	virtual bool Generate(duckdb::cxx::Context &context) = 0;
	//! Returns the progress as a fraction in [0, 1]
	virtual double Progress() const = 0;
};

} // namespace tpcds
