//===----------------------------------------------------------------------===//
//                         DuckDB
//
// struct_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

class StructColumnReader : public ColumnReader {
public:
	StructColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                   idx_t max_define_p, idx_t max_repeat_p, vector<unique_ptr<ColumnReader>> child_readers_p)
	    : ColumnReader(reader, move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
	      child_readers(move(child_readers_p)) {
		D_ASSERT(type.id() == LogicalTypeId::STRUCT);
		D_ASSERT(!StructType::GetChildTypes(type).empty());
	};

	ColumnReader *GetChildReader(idx_t child_idx) {
		return child_readers[child_idx].get();
	}

	void InitializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override {
		for (auto &child : child_readers) {
			child->InitializeRead(columns, protocol_p);
		}
	}

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result) override {
		auto &struct_entries = StructVector::GetEntries(result);
		D_ASSERT(StructType::GetChildTypes(Type()).size() == struct_entries.size());

		idx_t read_count = num_values;
		for (idx_t i = 0; i < struct_entries.size(); i++) {
			auto child_num_values =
			    child_readers[i]->Read(num_values, filter, define_out, repeat_out, *struct_entries[i]);
			if (i == 0) {
				read_count = child_num_values;
			} else if (read_count != child_num_values) {
				throw std::runtime_error("Struct child row count mismatch");
			}
		}

		return read_count;
	}

	virtual void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

	idx_t GroupRowsAvailable() override {
		for (idx_t i = 0; i < child_readers.size(); i++) {
			if (child_readers[i]->Type().id() != LogicalTypeId::LIST) {
				return child_readers[i]->GroupRowsAvailable();
			}
		}
		return child_readers[0]->GroupRowsAvailable();
	}

	vector<unique_ptr<ColumnReader>> child_readers;
};

} // namespace duckdb
