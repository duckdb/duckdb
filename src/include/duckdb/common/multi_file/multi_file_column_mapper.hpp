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

class MultiFileColumnMapper {
public:
	MultiFileColumnMapper(ClientContext &context, MultiFileReaderData &reader_data,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters,
	                      const string &initial_file, const MultiFileReaderBindData &bind_data,
	                      const virtual_column_map_t &virtual_columns);

public:
	ReaderInitializeType CreateMapping();

private:
	ResultColumnMapping CreateColumnMapping();
	ResultColumnMapping CreateColumnMappingByName();
	ResultColumnMapping CreateColumnMappingByFieldId();

	unique_ptr<TableFilterSet> CreateFilters(map<idx_t, reference<TableFilter>> &filters, ResultColumnMapping &mapping);
	ReaderInitializeType EvaluateConstantFilters(ResultColumnMapping &mapping,
	                                             map<idx_t, reference<TableFilter>> &remaining_filters);

	void PushColumnMapping(const LogicalType &global_type, const LogicalType &local_type,
	                       MultiFileLocalColumnId local_id, ResultColumnMapping &result,
	                       MultiFileGlobalIndex global_idx, const ColumnIndex &global_id);

private:
	ClientContext &context;
	MultiFileReaderData &reader_data;
	const vector<MultiFileColumnDefinition> &global_columns;
	const vector<ColumnIndex> &global_column_ids;
	optional_ptr<TableFilterSet> global_filters;
	const string &initial_file;
	const MultiFileReaderBindData &bind_data;
	const virtual_column_map_t &virtual_columns;
};

} // namespace duckdb
