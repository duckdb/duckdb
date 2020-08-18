#include "duckdb.hpp"
//#include "duckdb/common/arrow_cdata.hpp"
#include "arrow/c/bridge.h"
#include "arrow/api.h"
#include "parquet/arrow/reader.h"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"

// see https://github.com/apache/arrow/blob/master/docs/source/format/CDataInterface.rst
// see also https://arrow.apache.org/docs/format/Columnar.html

struct MyArrowTable {
	const char *tag = "THIS_IS_SPARTAA";
	vector<ArrowSchema *> schemas;
	vector<ArrowArray *> columns;
};

struct ArrowScanFunctionData : public duckdb::TableFunctionData {
	bool finished;
	MyArrowTable *table;
};

class ArrowScanFunction : public duckdb::TableFunction {
public:
	ArrowScanFunction()
	    : duckdb::TableFunction("parquet_scan", {duckdb::LogicalType::POINTER}, arrow_scan_bind, arrow_scan_function,
	                            nullptr) {
		supports_projection = true;
	}

private:
	static unique_ptr<duckdb::FunctionData> arrow_scan_bind(duckdb::ClientContext &context,
	                                                        vector<duckdb::Value> &inputs,
	                                                        unordered_map<string, duckdb::Value> &named_parameters,
	                                                        vector<duckdb::LogicalType> &return_types,
	                                                        vector<string> &names) {

		auto res = duckdb::make_unique<ArrowScanFunctionData>();
		res->table = (MyArrowTable *)inputs[0].GetValue<uintptr_t>();

		int col_idx = 0;
		for (auto &schema : res->table->schemas) {
			auto format = string(schema->format);
			if (format == "c") {
				return_types.push_back(duckdb::LogicalType::TINYINT);
			} else if (format == "s") {
				return_types.push_back(duckdb::LogicalType::SMALLINT);
            } else if (format == "i") {
                return_types.push_back(duckdb::LogicalType::INTEGER);
            } else if (format == "l") {
                return_types.push_back(duckdb::LogicalType::BIGINT);
            } else if (format == "f") {
                return_types.push_back(duckdb::LogicalType::FLOAT);
            } else if (format == "g") {
                return_types.push_back(duckdb::LogicalType::DOUBLE);
            } else if (format == "u") {
                return_types.push_back(duckdb::LogicalType::VARCHAR);
			} else {
				throw duckdb::NotImplementedException("Invalid Arrow type %s", format);
			}
			auto name = string(schema->name);
			if (name.empty()) {
				name = string("v") + to_string(col_idx);
			}
			names.push_back(name);
			col_idx++;
		}

		res->finished = false;
		return move(res);
	}

	static void arrow_scan_function(duckdb::ClientContext &context, vector<duckdb::Value> &input,
	                                duckdb::DataChunk &output, duckdb::FunctionData *dataptr) {
		auto &data = *((ArrowScanFunctionData *)dataptr);

		if (data.finished) {
			return;
		}

		auto nrow = data.table->columns[0]->length;

		// FIXME support scanning with offsets
		assert(nrow < STANDARD_VECTOR_SIZE);

		output.SetCardinality(data.table->columns[0]->length);
		for (duckdb::idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
            auto array = data.table->columns[col_idx];
            assert(nrow == array->length);

            // just memcpy nullmask
            auto n_bitmask_bytes = (nrow + 8 - 1) / 8;
			duckdb::nullmask_t new_nullmask;
			memcpy(&new_nullmask, array->buffers[0], n_bitmask_bytes);
            duckdb::FlatVector::SetNullmask(output.data[col_idx], new_nullmask.flip());

			switch (output.data[col_idx].type.id()) {
            case duckdb::LogicalTypeId::TINYINT:
            case duckdb::LogicalTypeId::SMALLINT:
            case duckdb::LogicalTypeId::INTEGER:
            case duckdb::LogicalTypeId::FLOAT:
            case duckdb::LogicalTypeId::DOUBLE:
            case duckdb::LogicalTypeId::BIGINT:
				duckdb::FlatVector::SetData(output.data[col_idx],
				                            (duckdb::data_ptr_t)array->buffers[1]);
				break;

			case duckdb::LogicalTypeId::VARCHAR: {
				auto offsets = (uint32_t *)array->buffers[1];
				auto cdata = (char *)array->buffers[2];

				for (int i = 0; i < nrow; i++) {
					// TODO this copies the string data for now :/
					duckdb::FlatVector::GetData<duckdb::string_t>(output.data[col_idx])[i] =
					    duckdb::StringVector::AddString(output.data[col_idx], cdata + offsets[i],
					                                    offsets[i + 1] - offsets[i]);
				}

				break;
			}
			default:
				throw runtime_error("Unsupported type " + output.data[col_idx].type.ToString());
			}


		}
		output.Verify();
		data.finished = true;
	}
};

static string ptr_to_string(void const *ptr) {
	std::ostringstream address;
	address << ptr;
	return address.str();
}

int main(int argc, char *argv[]) {

	arrow::StringBuilder sbuilder;
	sbuilder.Append("1");
	sbuilder.Append("2");
	sbuilder.Append("3222");
	sbuilder.AppendNull();
	sbuilder.Append("5");
	sbuilder.Append("6");
	sbuilder.Append("733");
	sbuilder.Append("8");

	std::shared_ptr<arrow::Array> sarray;
	auto sst = sbuilder.Finish(&sarray);
	if (!sst.ok()) {
		// ... do something on array building failure
	}

	arrow::Int64Builder builder;
	builder.Append(1);
	builder.Append(2);
	builder.Append(3);
	builder.AppendNull();
	builder.Append(5);
	builder.Append(6);
	builder.Append(7);
	builder.Append(8);

	std::shared_ptr<arrow::Array> array;
	auto st = builder.Finish(&array);
	if (!st.ok()) {
		// ... do something on array building failure
	}

	// c-land from here on out

	duckdb::DuckDB db;

	ArrowScanFunction scan_fun;
	duckdb::CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_arrow";

	duckdb::Connection conn(db);
	conn.context->transaction.BeginTransaction();
	db.catalog->CreateTableFunction(*conn.context, &cinfo);
	conn.context->transaction.Commit();

	ArrowArray c_array;
	ArrowSchema c_schema;
	ArrowArray c_sarray;
	ArrowSchema c_sschema;
	arrow::ExportArray(*array, &c_array, &c_schema);
	arrow::ExportArray(*sarray, &c_sarray, &c_sschema);

	// printf("%s %lld %d \n", c_sschema.format, c_sarray.length, c_sarray.n_buffers);

	assert(c_schema.children == nullptr);
	assert(c_schema.n_children == 0);

	assert(string(c_schema.format) == "l"); // TODO support other types

	assert(c_array.n_children == 0);
	assert(c_array.children == 0);

	assert(c_array.dictionary == nullptr); // TODO support this
	assert(c_array.offset == 0);           // TODO this can be non-zero

	// if c_array.null_count == 0, we might have only one n_buffers, in which case the data is in buffers[0] instead of
	// buffers[1]

	assert(c_array.n_buffers == 2);
	// lets try to read those buffers

	MyArrowTable arrow_table;
	arrow_table.schemas.push_back(&c_schema);
	arrow_table.columns.push_back(&c_array);

	arrow_table.schemas.push_back(&c_sschema);
	arrow_table.columns.push_back(&c_sarray);

	auto res = conn.TableFunction("read_arrow", {duckdb::Value::POINTER((uintptr_t)&arrow_table)})
	               ->Query("arrow", "SELECT * FROM arrow");
	res->Print();

	// TODO release whenever scan function completes?
	c_schema.release(&c_schema);
	c_array.release(&c_array);

	c_sschema.release(&c_sschema);
	c_sarray.release(&c_sarray);


    //arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input = ...;

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    if (!st.ok()) {
        // Handle error instantiating file reader...
    }

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    st = arrow_reader->ReadTable(&table);
    if (!st.ok()) {
        // Handle error reading Parquet data...
    }

	return 0;
}
