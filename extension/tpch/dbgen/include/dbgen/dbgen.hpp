//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// dbgen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_cpp.hpp"

#include <memory>
#include <string>
#include <vector>

namespace tpch {

using duckdb::cxx::idx_t;

struct DBGenWrapper {
	//! Gets the specified TPC-H Query number as a string
	static std::string GetQuery(int query);
	//! Gets the specified TPC-H Query number with the substitution parameters for the given scale factor
	static std::string GetQuery(int query, double sf);
	//! Returns the CSV answer of a TPC-H query
	static std::string GetAnswer(double sf, int query);
};

//! The schema of one of the eight TPC-H tables
struct TPCHTableSchema {
	const char *name;
	idx_t column_count;
	const char *const *columns;
	//! The column types as SQL type names
	const char *const *types;
};

static constexpr idx_t TPCH_TABLE_COUNT = 8;

//! The TPC-H tables, in the order dbgen creates them
const TPCHTableSchema &GetTPCHTableSchema(idx_t table);

//! A slice of the rows of a table: the rows of a table are those of its work items, in step order
struct DBGenWorkItem {
	int children;
	int step;
	//! The number of rows of the generator's driving table, e.g. orders for lineitem
	idx_t count;
};

//! Splits the generation of a scale factor into work items that can be generated in parallel and in any order
class DBGenGenerator {
public:
	//! Loads the dbgen distributions unless another generator already did: they are process-wide
	DBGenGenerator(double sf, int children, int step);
	~DBGenGenerator();

	DBGenGenerator(const DBGenGenerator &) = delete;
	DBGenGenerator &operator=(const DBGenGenerator &) = delete;

	//! The work items of a table (an index into GetTPCHTableSchema), in step order
	const std::vector<DBGenWorkItem> &WorkItems(idx_t table) const;

private:
	friend class DBGenScanner;
	struct State;
	std::unique_ptr<State> state;
};

//! Generates the rows of one work item of one table, chunk by chunk
class DBGenScanner {
public:
	DBGenScanner(const DBGenGenerator &generator, idx_t table, const DBGenWorkItem &item);
	~DBGenScanner();

	DBGenScanner(const DBGenScanner &) = delete;
	DBGenScanner &operator=(const DBGenScanner &) = delete;

	//! Fills the chunk with the next rows, and returns the number of rows of the driving table they cover. Leaves the
	//! chunk empty once the work item is done.
	idx_t Fill(duckdb::cxx::DataChunk &chunk);

private:
	struct State;
	std::unique_ptr<State> state;
};

} // namespace tpch
