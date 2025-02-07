#include "reader/cast_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast Column Reader
//===--------------------------------------------------------------------===//
CastColumnReader::CastColumnReader(unique_ptr<ColumnReader> child_reader_p, LogicalType target_type_p)
    : ColumnReader(child_reader_p->Reader(), std::move(target_type_p), child_reader_p->Schema(),
                   child_reader_p->FileIdx(), child_reader_p->MaxDefine(), child_reader_p->MaxRepeat()),
      child_reader(std::move(child_reader_p)) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

unique_ptr<BaseStatistics> CastColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	// casting stats is not supported (yet)
	return nullptr;
}

void CastColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                      TProtocol &protocol_p) {
	child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
}

idx_t CastColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	auto amount = child_reader->Read(num_values, define_out, repeat_out, intermediate_vector);
	string error_message;
	bool all_succeeded = VectorOperations::DefaultTryCast(intermediate_vector, result, amount, &error_message);
	if (!all_succeeded) {
		string extended_error;
		if (!reader.table_columns.empty()) {
			// COPY .. FROM
			extended_error = StringUtil::Format(
			    "In file \"%s\" the column \"%s\" has type %s, but we are trying to load it into column ",
			    reader.file_name, schema.name, intermediate_vector.GetType());
			if (FileIdx() < reader.table_columns.size()) {
				extended_error += "\"" + reader.table_columns[FileIdx()] + "\" ";
			}
			extended_error += StringUtil::Format("with type %s.", result.GetType());
			extended_error += "\nThis means the Parquet schema does not match the schema of the table.";
			extended_error += "\nPossible solutions:";
			extended_error += "\n* Insert by name instead of by position using \"INSERT INTO tbl BY NAME SELECT * FROM "
			                  "read_parquet(...)\"";
			extended_error += "\n* Manually specify which columns to insert using \"INSERT INTO tbl SELECT ... FROM "
			                  "read_parquet(...)\"";
		} else {
			// read_parquet() with multiple files
			extended_error = StringUtil::Format(
			    "In file \"%s\" the column \"%s\" has type %s, but we are trying to read it as type %s.",
			    reader.file_name, schema.name, intermediate_vector.GetType(), result.GetType());
			extended_error +=
			    "\nThis can happen when reading multiple Parquet files. The schema information is taken from "
			    "the first Parquet file by default. Possible solutions:\n";
			extended_error += "* Enable the union_by_name=True option to combine the schema of all Parquet files "
			                  "(duckdb.org/docs/data/multiple_files/combining_schemas)\n";
			extended_error += "* Use a COPY statement to automatically derive types from an existing table.";
		}
		throw ConversionException(
		    "In Parquet reader of file \"%s\": failed to cast column \"%s\" from type %s to %s: %s\n\n%s",
		    reader.file_name, schema.name, intermediate_vector.GetType(), result.GetType(), error_message,
		    extended_error);
	}
	return amount;
}

void CastColumnReader::Skip(idx_t num_values) {
	child_reader->Skip(num_values);
}

idx_t CastColumnReader::GroupRowsAvailable() {
	return child_reader->GroupRowsAvailable();
}

} // namespace duckdb
