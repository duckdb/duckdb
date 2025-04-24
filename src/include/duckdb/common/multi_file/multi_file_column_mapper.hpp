//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_column_mapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {
struct ResultColumnMapping;
struct ColumnMapper;

class MultiFileColumnMapper {
public:
	MultiFileColumnMapper(ClientContext &context, MultiFileReader &multi_file_reader, MultiFileReaderData &reader_data,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters,
	                      const OpenFileInfo &initial_file, const MultiFileReaderBindData &bind_data,
	                      const virtual_column_map_t &virtual_columns);

public:
	ReaderInitializeType CreateMapping();

	void ThrowColumnNotFoundError(const string &global_column_name) const;

private:
	ResultColumnMapping CreateColumnMapping();
	ResultColumnMapping CreateColumnMappingByMapper(const ColumnMapper &mapper);

	unique_ptr<TableFilterSet> CreateFilters(map<idx_t, reference<TableFilter>> &filters, ResultColumnMapping &mapping);
	ReaderInitializeType EvaluateConstantFilters(ResultColumnMapping &mapping,
	                                             map<idx_t, reference<TableFilter>> &remaining_filters);
	Value GetConstantValue(idx_t global_index);
	bool EvaluateFilterAgainstConstant(TableFilter &filter, const Value &constant);

private:
	ClientContext &context;
	MultiFileReader &multi_file_reader;
	MultiFileReaderData &reader_data;
	const vector<MultiFileColumnDefinition> &global_columns;
	const vector<ColumnIndex> &global_column_ids;
	optional_ptr<TableFilterSet> global_filters;
	const OpenFileInfo &initial_file;
	const MultiFileReaderBindData &bind_data;
	const virtual_column_map_t &virtual_columns;
};

} // namespace duckdb
