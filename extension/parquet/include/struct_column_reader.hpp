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
		D_ASSERT(!type.child_types().empty());
	};

	ColumnReader *GetChildReader(idx_t child_idx) {
		return child_readers[child_idx].get();
	}

	void IntializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override {
		for (auto &child : child_readers) {
			child->IntializeRead(columns, protocol_p);
		}
	}

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result) override {
		result.Initialize(Type());

		for (idx_t i = 0; i < Type().child_types().size(); i++) {
			auto child_read = make_unique<Vector>();
			child_read->Initialize(Type().child_types()[i].second);
			auto child_num_values = child_readers[i]->Read(num_values, filter, define_out, repeat_out, *child_read);
			if (child_num_values != num_values) {
				throw std::runtime_error("Struct child row count mismatch");
			}
			StructVector::AddEntry(result, Type().child_types()[i].first, move(child_read));
		}

		return num_values;
	}

	virtual void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

	idx_t GroupRowsAvailable() override {
		return child_readers[0]->GroupRowsAvailable();
	}

	vector<unique_ptr<ColumnReader>> child_readers;
};

} // namespace duckdb
