#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_objects.hpp"

#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb_python/arrow/arrow_export_utils.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"

namespace duckdb {

DuckDBPyResult::DuckDBPyResult(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	if (!result) {
		throw InternalException("PyResult created without a result object");
	}
}

const vector<string> &DuckDBPyResult::GetNames() {
	if (!result) {
		throw InternalException("Calling GetNames without a result object");
	}
	return result->names;
}

const vector<LogicalType> &DuckDBPyResult::GetTypes() {
	if (!result) {
		throw InternalException("Calling GetTypes without a result object");
	}
	return result->types;
}

unique_ptr<DataChunk> DuckDBPyResult::FetchChunk() {
	if (!result) {
		throw InternalException("FetchChunk called without a result object");
	}
	return FetchNext(*result);
}

unique_ptr<DataChunk> DuckDBPyResult::FetchNext(QueryResult &query_result) {
	if (!result_closed && query_result.type == QueryResultType::STREAM_RESULT &&
	    !query_result.Cast<StreamQueryResult>().IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = query_result.Fetch();
	if (query_result.HasError()) {
		query_result.ThrowError();
	}
	return chunk;
}

unique_ptr<DataChunk> DuckDBPyResult::FetchNextRaw(QueryResult &query_result) {
	if (!result_closed && query_result.type == QueryResultType::STREAM_RESULT &&
	    !query_result.Cast<StreamQueryResult>().IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = query_result.FetchRaw();
	if (query_result.HasError()) {
		query_result.ThrowError();
	}
	return chunk;
}

Optional<py::tuple> DuckDBPyResult::Fetchone() {
	{
		py::gil_scoped_release release;
		if (!result) {
			throw InvalidInputException("result closed");
		}
		if (!current_chunk || chunk_offset >= current_chunk->size()) {
			current_chunk = FetchNext(*result);
			chunk_offset = 0;
		}
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
		res[col_idx] = PythonObject::FromValue(val, result->types[col_idx], result->client_properties);
	}
	chunk_offset++;
	return res;
}

py::list DuckDBPyResult::Fetchmany(idx_t size) {
	py::list res;
	for (idx_t i = 0; i < size; i++) {
		auto fres = Fetchone();
		if (fres.is_none()) {
			break;
		}
		res.append(fres);
	}
	return res;
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

py::dict DuckDBPyResult::FetchNumpy() {
	return FetchNumpyInternal();
}

void DuckDBPyResult::FillNumpy(py::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name) {
	if (result->types[col_idx].id() == LogicalTypeId::ENUM) {
		// first we (might) need to create the categorical type
		if (categories_type.find(col_idx) == categories_type.end()) {
			// Equivalent to: pandas.CategoricalDtype(['a', 'b'], ordered=True)
			categories_type[col_idx] = py::module::import("pandas").attr("CategoricalDtype")(categories[col_idx], true);
		}
		// Equivalent to: pandas.Categorical.from_codes(codes=[0, 1, 0, 1], dtype=dtype)
		res[name] = py::module::import("pandas")
		                .attr("Categorical")
		                .attr("from_codes")(conversion.ToArray(col_idx), py::arg("dtype") = categories_type[col_idx]);
	} else {
		res[name] = conversion.ToArray(col_idx);
	}
}

void InsertCategory(QueryResult &result, unordered_map<idx_t, py::list> &categories) {
	for (idx_t col_idx = 0; col_idx < result.types.size(); col_idx++) {
		auto &type = result.types[col_idx];
		if (type.id() == LogicalTypeId::ENUM) {
			// It's an ENUM type, in addition to converting the codes we must convert the categories
			if (categories.find(col_idx) == categories.end()) {
				auto &categories_list = EnumType::GetValuesInsertOrder(type);
				auto categories_size = EnumType::GetSize(type);
				for (idx_t i = 0; i < categories_size; i++) {
					categories[col_idx].append(py::cast(categories_list.GetValue(i).ToString()));
				}
			}
		}
	}
}

unique_ptr<NumpyResultConversion> DuckDBPyResult::InitializeNumpyConversion(bool pandas) {
	if (!result) {
		throw InvalidInputException("result closed");
	}

	idx_t initial_capacity = STANDARD_VECTOR_SIZE * 2ULL;
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		// materialized query result: we know exactly how much space we need
		auto &materialized = result->Cast<MaterializedQueryResult>();
		initial_capacity = materialized.RowCount();
	}

	auto conversion =
	    make_uniq<NumpyResultConversion>(result->types, initial_capacity, result->client_properties, pandas);
	return std::move(conversion);
}

py::dict DuckDBPyResult::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk,
                                            unique_ptr<NumpyResultConversion> conversion_p) {
	if (!result) {
		throw InvalidInputException("result closed");
	}
	if (!conversion_p) {
		conversion_p = InitializeNumpyConversion();
	}
	auto &conversion = *conversion_p;

	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		auto &materialized = result->Cast<MaterializedQueryResult>();
		for (auto &chunk : materialized.Collection().Chunks()) {
			conversion.Append(chunk);
		}
		InsertCategory(materialized, categories);
		materialized.Collection().Reset();
	} else {
		D_ASSERT(result->type == QueryResultType::STREAM_RESULT);
		if (!stream) {
			vectors_per_chunk = NumericLimits<idx_t>::Maximum();
		}
		auto &stream_result = result->Cast<StreamQueryResult>();
		for (idx_t count_vec = 0; count_vec < vectors_per_chunk; count_vec++) {
			if (!stream_result.IsOpen()) {
				break;
			}
			unique_ptr<DataChunk> chunk;
			{
				py::gil_scoped_release release;
				chunk = FetchNextRaw(stream_result);
			}
			if (!chunk || chunk->size() == 0) {
				//! finished
				break;
			}
			conversion.Append(*chunk);
			InsertCategory(stream_result, categories);
		}
	}

	// now that we have materialized the result in contiguous arrays, construct the actual NumPy arrays or categorical
	// types
	py::dict res;
	unordered_map<string, idx_t> names;
	for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
		if (names[result->names[col_idx]]++ == 0) {
			FillNumpy(res, col_idx, conversion, result->names[col_idx].c_str());
		} else {
			auto name = result->names[col_idx] + "_" + to_string(names[result->names[col_idx]]);
			while (names[name] > 0) {
				// This entry already exists
				name += "_" + to_string(names[name]);
			}
			names[name]++;
			FillNumpy(res, col_idx, conversion, name.c_str());
		}
	}
	return res;
}

// TODO: unify these with an enum/flag to indicate which conversions to do
void DuckDBPyResult::ChangeToTZType(PandasDataFrame &df) {
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::TIMESTAMP_TZ) {
			// first localize to UTC then convert to timezone_config
			auto utc_local = df[result->names[i].c_str()].attr("dt").attr("tz_localize")("UTC");
			df[result->names[i].c_str()] = utc_local.attr("dt").attr("tz_convert")(result->client_properties.time_zone);
		}
	}
}

// TODO: unify these with an enum/flag to indicate which conversions to perform
void DuckDBPyResult::ChangeDateToDatetime(PandasDataFrame &df) {
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::DATE) {
			df[result->names[i].c_str()] = df[result->names[i].c_str()].attr("dt").attr("date");
		}
	}
}

PandasDataFrame DuckDBPyResult::FrameFromNumpy(bool date_as_object, const py::handle &o) {
	auto df = py::cast<PandasDataFrame>(py::module::import("pandas").attr("DataFrame").attr("from_dict")(o));
	// Unfortunately we have to do a type change here for timezones since these types are not supported by numpy
	ChangeToTZType(df);
	if (date_as_object) {
		ChangeDateToDatetime(df);
	}
	return df;
}

PandasDataFrame DuckDBPyResult::FetchDF(bool date_as_object) {
	auto conversion = InitializeNumpyConversion(true);
	return FrameFromNumpy(date_as_object, FetchNumpyInternal(false, 1, std::move(conversion)));
}

PandasDataFrame DuckDBPyResult::FetchDFChunk(idx_t num_of_vectors, bool date_as_object) {
	auto conversion = InitializeNumpyConversion(true);
	return FrameFromNumpy(date_as_object, FetchNumpyInternal(true, num_of_vectors, std::move(conversion)));
}

py::dict DuckDBPyResult::FetchPyTorch() {
	auto result_dict = FetchNumpyInternal();
	auto from_numpy = py::module::import("torch").attr("from_numpy");
	for (auto &item : result_dict) {
		result_dict[item.first] = from_numpy(item.second);
	}
	return result_dict;
}

py::dict DuckDBPyResult::FetchTF() {
	auto result_dict = FetchNumpyInternal();
	auto convert_to_tensor = py::module::import("tensorflow").attr("convert_to_tensor");
	for (auto &item : result_dict) {
		result_dict[item.first] = convert_to_tensor(item.second);
	}
	return result_dict;
}

bool DuckDBPyResult::FetchArrowChunk(ChunkScanState &scan_state, py::list &batches, idx_t rows_per_batch) {
	ArrowArray data;
	idx_t count;
	auto &query_result = *result.get();
	{
		py::gil_scoped_release release;
		count = ArrowUtil::FetchChunk(scan_state, query_result.client_properties, rows_per_batch, &data);
	}
	if (count == 0) {
		return false;
	}
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, query_result.types, query_result.names,
	                              query_result.client_properties);
	TransformDuckToArrowChunk(arrow_schema, data, batches);
	return true;
}

py::list DuckDBPyResult::FetchAllArrowChunks(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("result closed");
	}
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

	py::list batches;
	QueryResultChunkScanState scan_state(*result.get());
	while (FetchArrowChunk(scan_state, batches, rows_per_batch)) {
	}
	return batches;
}

duckdb::pyarrow::Table DuckDBPyResult::FetchArrowTable(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	return pyarrow::ToArrowTable(result->types, result->names, FetchAllArrowChunks(rows_per_batch),
	                             result->client_properties);
}

duckdb::pyarrow::RecordBatchReader DuckDBPyResult::FetchRecordBatchReader(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	py::gil_scoped_acquire acquire;
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto record_batch_reader_func = pyarrow_lib_module.attr("RecordBatchReader").attr("_import_from_c");
	//! We have to construct an Arrow Array Stream
	ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(std::move(result), rows_per_batch);
	py::object record_batch_reader = record_batch_reader_func((uint64_t)&result_stream->stream); // NOLINT
	return py::cast<duckdb::pyarrow::RecordBatchReader>(record_batch_reader);
}

py::str GetTypeToPython(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return py::str("bool");
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL: {
		return py::str("NUMBER");
	}
	case LogicalTypeId::VARCHAR:
		return py::str("STRING");
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		return py::str("BINARY");
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		return py::str("DATETIME");
	}
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ: {
		return py::str("Time");
	}
	case LogicalTypeId::DATE: {
		return py::str("Date");
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		return py::str("dict");
	case LogicalTypeId::LIST: {
		return py::str("list");
	}
	case LogicalTypeId::INTERVAL: {
		return py::str("TIMEDELTA");
	}
	case LogicalTypeId::UUID: {
		return py::str("UUID");
	}
	default:
		return py::str(type.ToString());
	}
}

py::list DuckDBPyResult::GetDescription(const vector<string> &names, const vector<LogicalType> &types) {
	py::list desc;

	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		auto py_name = py::str(names[col_idx]);
		auto py_type = GetTypeToPython(types[col_idx]);
		desc.append(py::make_tuple(py_name, py_type, py::none(), py::none(), py::none(), py::none(), py::none()));
	}
	return desc;
}

void DuckDBPyResult::Close() {
	result = nullptr;
}

bool DuckDBPyResult::IsClosed() const {
	return result_closed;
}

} // namespace duckdb
