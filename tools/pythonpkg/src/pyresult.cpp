#include "duckdb_python/array_wrapper.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/arrow.hpp"

#include "datetime.h" // from Python

namespace duckdb {

void DuckDBPyResult::Initialize(py::handle &m) {
	py::class_<DuckDBPyResult>(m, "DuckDBPyResult")
	    .def("close", &DuckDBPyResult::Close)
	    .def("fetchone", &DuckDBPyResult::Fetchone)
	    .def("fetchall", &DuckDBPyResult::Fetchall)
	    .def("fetchnumpy", &DuckDBPyResult::FetchNumpy)
	    .def("fetchdf", &DuckDBPyResult::FetchDF)
	    .def("fetch_df", &DuckDBPyResult::FetchDF)
	    .def("fetch_df_chunk", &DuckDBPyResult::FetchDFChunk)
	    .def("fetch_arrow_table", &DuckDBPyResult::FetchArrowTable)
	    .def("arrow", &DuckDBPyResult::FetchArrowTable)
	    .def("df", &DuckDBPyResult::FetchDF);

	PyDateTime_IMPORT;
}

template <class SRC>
static SRC FetchScalar(Vector &src_vec, idx_t offset) {
	auto src_ptr = FlatVector::GetData<SRC>(src_vec);
	return src_ptr[offset];
}

py::object DuckDBPyResult::Fetchone() {
	if (!result) {
		throw std::runtime_error("result closed");
	}
	if (!current_chunk || chunk_offset >= current_chunk->size()) {
		current_chunk = result->Fetch();
		chunk_offset = 0;
	}
	if (!current_chunk || current_chunk->size() == 0) {
		return py::none();
	}
	py::tuple res(result->types.size());

	for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
		auto &mask = FlatVector::Validity(current_chunk->data[col_idx]);
		if (!mask.RowIsValid(chunk_offset)) {
			res[col_idx] = py::none();
			continue;
		}
		auto val = current_chunk->data[col_idx].GetValue(chunk_offset);
		switch (result->types[col_idx].id()) {
		case LogicalTypeId::BOOLEAN:
			res[col_idx] = val.GetValue<bool>();
			break;
		case LogicalTypeId::TINYINT:
			res[col_idx] = val.GetValue<int8_t>();
			break;
		case LogicalTypeId::SMALLINT:
			res[col_idx] = val.GetValue<int16_t>();
			break;
		case LogicalTypeId::INTEGER:
			res[col_idx] = val.GetValue<int32_t>();
			break;
		case LogicalTypeId::BIGINT:
			res[col_idx] = val.GetValue<int64_t>();
			break;
		case LogicalTypeId::UTINYINT:
			res[col_idx] = val.GetValue<uint8_t>();
			break;
		case LogicalTypeId::USMALLINT:
			res[col_idx] = val.GetValue<uint16_t>();
			break;
		case LogicalTypeId::UINTEGER:
			res[col_idx] = val.GetValue<uint32_t>();
			break;
		case LogicalTypeId::UBIGINT:
			res[col_idx] = val.GetValue<uint64_t>();
			break;
		case LogicalTypeId::HUGEINT: {
			auto hugeint_str = val.GetValue<string>();
			res[col_idx] = PyLong_FromString((char *)hugeint_str.c_str(), nullptr, 10);
			break;
		}
		case LogicalTypeId::FLOAT:
			res[col_idx] = val.GetValue<float>();
			break;
		case LogicalTypeId::DOUBLE:
			res[col_idx] = val.GetValue<double>();
			break;
		case LogicalTypeId::DECIMAL:
			res[col_idx] = val.CastAs(LogicalType::DOUBLE).GetValue<double>();
			break;
		case LogicalTypeId::VARCHAR:
			res[col_idx] = val.GetValue<string>();
			break;
		case LogicalTypeId::BLOB:
			res[col_idx] = py::bytes(val.GetValueAsIs<string>());
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_SEC: {
			D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT64);
			auto timestamp = val.GetValueUnsafe<timestamp_t>();
			if (result->types[col_idx].id() == LogicalTypeId::TIMESTAMP_MS) {
				timestamp = Timestamp::FromEpochMs(timestamp.value);
			} else if (result->types[col_idx].id() == LogicalTypeId::TIMESTAMP_NS) {
				timestamp = Timestamp::FromEpochNanoSeconds(timestamp.value);
			} else if (result->types[col_idx].id() == LogicalTypeId::TIMESTAMP_SEC) {
				timestamp = Timestamp::FromEpochSeconds(timestamp.value);
			}
			int32_t year, month, day, hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(timestamp, date, time);
			Date::Convert(date, year, month, day);
			Time::Convert(time, hour, min, sec, micros);
			res[col_idx] = PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros);
			break;
		}
		case LogicalTypeId::TIME: {
			D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT64);

			int32_t hour, min, sec, microsec;
			auto time = val.GetValueUnsafe<dtime_t>();
			duckdb::Time::Convert(time, hour, min, sec, microsec);
			res[col_idx] = PyTime_FromTime(hour, min, sec, microsec);
			break;
		}
		case LogicalTypeId::DATE: {
			D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT32);

			auto date = val.GetValueUnsafe<date_t>();
			int32_t year, month, day;
			duckdb::Date::Convert(date, year, month, day);
			res[col_idx] = PyDate_FromDate(year, month, day);
			break;
		}

		default:
			throw std::runtime_error("unsupported type: " + result->types[col_idx].ToString());
		}
	}
	chunk_offset++;
	return move(res);
}

py::list DuckDBPyResult::Fetchall() {
	py::list res;
	while (true) {
		auto fres = Fetchone();
		if (fres.is_none()) {
			break;
		}
		res.append(fres);
	}
	return res;
}

py::dict DuckDBPyResult::FetchNumpy(bool stream) {
	if (!result) {
		throw std::runtime_error("result closed");
	}

	// iterate over the result to materialize the data needed for the NumPy arrays
	idx_t initial_capacity = STANDARD_VECTOR_SIZE * 2;
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		// materialized query result: we know exactly how much space we need
		auto &materialized = (MaterializedQueryResult &)*result;
		initial_capacity = materialized.collection.Count();
	}

	NumpyResultConversion conversion(result->types, initial_capacity);
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		auto &materialized = (MaterializedQueryResult &)*result;
		if (!stream) {
			for (auto &chunk : materialized.collection.Chunks()) {
				conversion.Append(*chunk);
			}
			materialized.collection.Reset();
		} else {
			conversion.Append(*materialized.Fetch());
		}
	} else {
		if (!stream) {
			while (true) {
				auto chunk = result->FetchRaw();
				if (!chunk || chunk->size() == 0) {
					// finished
					break;
				}
				conversion.Append(*chunk);
			}
		} else {
			auto chunk = result->FetchRaw();
			if (chunk && chunk->size() > 0) {
				conversion.Append(*chunk);
			}
		}
	}

	// now that we have materialized the result in contiguous arrays, construct the actual NumPy arrays
	py::dict res;
	for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
		res[result->names[col_idx].c_str()] = conversion.ToArray(col_idx);
	}
	return res;
}

py::object DuckDBPyResult::FetchDF() {
	return py::module::import("pandas").attr("DataFrame").attr("from_dict")(FetchNumpy());
}

py::object DuckDBPyResult::FetchDFChunk() {
	return py::module::import("pandas").attr("DataFrame").attr("from_dict")(FetchNumpy(true));
}

py::object DuckDBPyResult::FetchArrowTable() {
	if (!result) {
		throw std::runtime_error("result closed");
	}
	py::gil_scoped_acquire acquire;
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

	auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
	auto from_batches_func = pyarrow_lib_module.attr("Table").attr("from_batches");
	auto schema_import_func = pyarrow_lib_module.attr("Schema").attr("_import_from_c");
	ArrowSchema schema;
	result->ToArrowSchema(&schema);
	auto schema_obj = schema_import_func((uint64_t)&schema);

	py::list batches;
	while (true) {
		auto data_chunk = result->Fetch();
		if (!data_chunk || data_chunk->size() == 0) {
			break;
		}
		ArrowArray data;
		data_chunk->ToArrowArray(&data);
		ArrowSchema arrow_schema;
		result->ToArrowSchema(&arrow_schema);
		batches.append(batch_import_func((uint64_t)&data, (uint64_t)&arrow_schema));
	}
	return from_batches_func(batches, schema_obj);
}

py::list DuckDBPyResult::Description() {
	py::list desc(result->names.size());
	for (idx_t col_idx = 0; col_idx < result->names.size(); col_idx++) {
		py::tuple col_desc(7);
		col_desc[0] = py::str(result->names[col_idx]);
		col_desc[1] = py::none();
		col_desc[2] = py::none();
		col_desc[3] = py::none();
		col_desc[4] = py::none();
		col_desc[5] = py::none();
		col_desc[6] = py::none();
		desc[col_idx] = col_desc;
	}
	return desc;
}

void DuckDBPyResult::Close() {
	result = nullptr;
}

} // namespace duckdb
