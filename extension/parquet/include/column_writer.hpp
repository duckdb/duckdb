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
#include "parquet_column_schema.hpp"

namespace duckdb {
class MemoryStream;
class ParquetWriter;
class ColumnWriterPageState;
class PrimitiveColumnWriterState;
struct ChildFieldIDs;
class ResizeableBuffer;
class ParquetBloomFilter;

class ColumnWriterState {
public:
	virtual ~ColumnWriterState();

	unsafe_vector<uint16_t> definition_levels;
	unsafe_vector<uint16_t> repetition_levels;
	vector<bool> is_empty;
	idx_t parent_null_count = 0;
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

class ColumnWriterPageState {
public:
	virtual ~ColumnWriterPageState() {
	}

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
protected:
	static constexpr uint16_t PARQUET_DEFINE_VALID = UINT16_C(65535);

public:
	ColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path,
	             bool can_have_nulls);
	virtual ~ColumnWriter();

	ParquetWriter &writer;
	const ParquetColumnSchema &column_schema;
	vector<string> schema_path;
	bool can_have_nulls;

public:
	const LogicalType &Type() const {
		return column_schema.type;
	}
	const ParquetColumnSchema &Schema() const {
		return column_schema;
	}
	inline idx_t SchemaIndex() const {
		return column_schema.schema_index;
	}
	inline idx_t MaxDefine() const {
		return column_schema.max_define;
	}
	idx_t MaxRepeat() const {
		return column_schema.max_repeat;
	}

	static ParquetColumnSchema FillParquetSchema(vector<duckdb_parquet::SchemaElement> &schemas,
	                                             const LogicalType &type, const string &name,
	                                             optional_ptr<const ChildFieldIDs> field_ids, idx_t max_repeat = 0,
	                                             idx_t max_define = 1, bool can_have_nulls = true);
	//! Create the column writer for a specific type recursively
	static unique_ptr<ColumnWriter> CreateWriterRecursive(ClientContext &context, ParquetWriter &writer,
	                                                      const vector<duckdb_parquet::SchemaElement> &parquet_schemas,
	                                                      const ParquetColumnSchema &schema,
	                                                      vector<string> path_in_schema);

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
	                  AllocatedData &compressed_buf);
};

} // namespace duckdb
