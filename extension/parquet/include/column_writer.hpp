//===----------------------------------------------------------------------===//
//                         DuckDB
//
// column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_types.h"

namespace duckdb {
class MemoryStream;
class ParquetWriter;
class ColumnWriterPageState;
class BasicColumnWriterState;
struct ChildFieldIDs;

class ColumnWriterState {
public:
	virtual ~ColumnWriterState();

	unsafe_vector<uint16_t> definition_levels;
	unsafe_vector<uint16_t> repetition_levels;
	vector<bool> is_empty;
	idx_t null_count = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class ColumnWriterStatistics {
public:
	virtual ~ColumnWriterStatistics();

	virtual bool HasStats();
	virtual string GetMin();
	virtual string GetMax();
	virtual string GetMinValue();
	virtual string GetMaxValue();

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class ColumnWriter {

public:
	ColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path, idx_t max_repeat,
	             idx_t max_define, bool can_have_nulls);
	virtual ~ColumnWriter();

	ParquetWriter &writer;
	idx_t schema_idx;
	vector<string> schema_path;
	idx_t max_repeat;
	idx_t max_define;
	bool can_have_nulls;

public:
	//! Create the column writer for a specific type recursively
	static unique_ptr<ColumnWriter>
	CreateWriterRecursive(ClientContext &context, vector<duckdb_parquet::SchemaElement> &schemas, ParquetWriter &writer,
	                      const LogicalType &type, const string &name, vector<string> schema_path,
	                      optional_ptr<const ChildFieldIDs> field_ids, idx_t max_repeat = 0, idx_t max_define = 1,
	                      bool can_have_nulls = true);

	virtual unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) = 0;

	//! indicates whether the write need to analyse the data before preparing it
	virtual bool HasAnalyze() {
		return false;
	}

	virtual void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) {
		throw NotImplementedException("Writer does not need analysis");
	}

	//! Called after all data has been passed to Analyze
	virtual void FinalizeAnalyze(ColumnWriterState &state) {
		throw NotImplementedException("Writer does not need analysis");
	}

	virtual void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) = 0;

	virtual void BeginWrite(ColumnWriterState &state) = 0;
	virtual void Write(ColumnWriterState &state, Vector &vector, idx_t count) = 0;
	virtual void FinalizeWrite(ColumnWriterState &state) = 0;

protected:
	void HandleDefineLevels(ColumnWriterState &state, ColumnWriterState *parent, const ValidityMask &validity,
	                        const idx_t count, const uint16_t define_value, const uint16_t null_value) const;
	void HandleRepeatLevels(ColumnWriterState &state_p, ColumnWriterState *parent, idx_t count, idx_t max_repeat) const;

	void CompressPage(MemoryStream &temp_writer, size_t &compressed_size, data_ptr_t &compressed_data,
	                  unique_ptr<data_t[]> &compressed_buf);
};

} // namespace duckdb
