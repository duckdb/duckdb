#pragma once

#include "duckdb_cpp.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace tpcds {

using duckdb::cxx::idx_t;

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
	int first_column;
	int64_t *null_bitmap;
};

#define CALL_CENTER   0
#define DBGEN_VERSION 24

enum class TPCDSAppendType : uint8_t {
	INT32,
	INT64,
	VARCHAR,
	BOOLEAN,
	DATE,
	DECIMAL16,
	DECIMAL32,
	DECIMAL64,
	DECIMAL128
};

//! The columns of a TPC-DS table, as DuckDB types and as the generator writes them
struct TPCDSTableLayout {
	TPCDSTableLayout(duckdb::cxx::Factory &factory, const char *const *type_names, idx_t column_count);

	std::vector<duckdb::cxx::LogicalType> types;
	std::vector<TPCDSAppendType> append_types;
	std::vector<uint32_t> decimal_scales;
};

//! Buffers the rows the generator produces for one table, until they are inserted
struct tpcds_append_information {
	tpcds_append_information(duckdb::cxx::Factory &factory, const TPCDSTableLayout &layout);

	tpcds_table_def table_def;

	bool IsNull(int nColumn);
	void BeginRow();
	void EndRow();
	TPCDSAppendType ActiveAppendType() const;
	uint32_t ActiveDecimalScale() const;
	void AppendNull();
	void AppendVarchar(const char *value);
	void AppendKey(int64_t value);
	void AppendInteger(int32_t value);
	void AppendIntegerDecimal(int32_t value);
	void AppendBoolean(int32_t value);
	void AppendDate(int32_t days);
	void AppendDecimal(int64_t value);

	//! Moves the rows of the chunk being filled into the buffer
	void FlushChunk();
	//! The buffered rows, complete after FlushChunk
	const duckdb::cxx::ColumnDataCollection &GetBuffer() const {
		return buffer;
	}
	//! The rows buffered or in the chunk being filled
	idx_t RowCount() const {
		return buffer.GetRowCount() + row;
	}
	//! Empties the buffer after its rows were inserted
	void ResetBuffer();

private:
	void PrepareChunk();
	idx_t NextColumn();

	const TPCDSTableLayout &layout;
	duckdb::cxx::DataChunk chunk;
	duckdb::cxx::ColumnDataCollection buffer;
	std::unique_ptr<duckdb::cxx::ColumnDataCollection::AppendState> append_state;
	idx_t capacity;
	//! The columns of the chunk, with their data, heap and validity, taken again whenever it is cleared
	std::vector<duckdb::cxx::Vector> vectors;
	std::vector<void *> column_data;
	std::vector<std::unique_ptr<duckdb::cxx::Arena>> heaps;
	std::vector<uint64_t *> validity;
	idx_t row = 0;
	idx_t active_row = static_cast<idx_t>(-1);
	idx_t active_col = 0;
};

} // namespace tpcds
