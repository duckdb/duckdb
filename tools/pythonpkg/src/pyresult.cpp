#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"

#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/stream_execution_result.hpp"
#include "duckdb_python/arrow/arrow_export_utils.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"

namespace duckdb {

DuckDBPyResult::DuckDBPyResult(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	if (!result) {
		throw InternalException("PyResult created without a result object");
	}
}

DuckDBPyResult::~DuckDBPyResult() {
	try {
		D_ASSERT(py::gil_check());
		py::gil_scoped_release gil;
		result.reset();
		current_chunk.reset();
	} catch (...) { // NOLINT
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
	if (query_result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = query_result.Cast<StreamQueryResult>();
		StreamExecutionResult execution_result;
		while (!StreamQueryResult::IsChunkReady(execution_result = stream_result.ExecuteTask())) {
			{
				py::gil_scoped_acquire gil;
				if (PyErr_CheckSignals() != 0) {
					throw std::runtime_error("Query interrupted");
				}
			}
			if (execution_result == StreamExecutionResult::BLOCKED) {
				stream_result.WaitForTask();
			}
		}
		if (execution_result == StreamExecutionResult::EXECUTION_CANCELLED) {
			throw InvalidInputException("The execution of the query was cancelled before it could finish, likely "
			                            "caused by executing a different query");
		}
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			stream_result.ThrowError();
		}
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
		D_ASSERT(py::gil_check());
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
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		auto pandas_categorical = import_cache.pandas.Categorical();
		auto categorical_dtype = import_cache.pandas.CategoricalDtype();
		if (!pandas_categorical || !categorical_dtype) {
			throw InvalidInputException("'pandas' is required for this operation but it was not installed");
		}

		// first we (might) need to create the categorical type
		if (categories_type.find(col_idx) == categories_type.end()) {
			// Equivalent to: pandas.CategoricalDtype(['a', 'b'], ordered=True)
			categories_type[col_idx] = categorical_dtype(categories[col_idx], true);
		}
		// Equivalent to: pandas.Categorical.from_codes(codes=[0, 1, 0, 1], dtype=dtype)
		res[name] = pandas_categorical.attr("from_codes")(conversion.ToArray(col_idx),
		                                                  py::arg("dtype") = categories_type[col_idx]);
		if (!conversion.ToPandas()) {
			res[name] = res[name].attr("to_numpy")();
		}
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
	return conversion;
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
				D_ASSERT(py::gil_check());
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
	auto names = result->names;
	QueryResult::DeduplicateColumns(names);
	for (idx_t col_idx = 0; col_idx < result->names.size(); col_idx++) {
		auto &name = names[col_idx];
		FillNumpy(res, col_idx, conversion, name.c_str());
	}
	return res;
}

// TODO: unify these with an enum/flag to indicate which conversions to do
void DuckDBPyResult::ChangeToTZType(PandasDataFrame &df) {
	auto names = df.attr("columns").cast<vector<string>>();

	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::TIMESTAMP_TZ) {
			// first localize to UTC then convert to timezone_config
			auto utc_local = df[names[i].c_str()].attr("dt").attr("tz_localize")("UTC");
			df.attr("__setitem__")(names[i].c_str(),
			                       utc_local.attr("dt").attr("tz_convert")(result->client_properties.time_zone));
		}
	}
}

static py::object ConvertNumpyDtype(py::handle numpy_array) {
	D_ASSERT(py::gil_check());
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	auto dtype = numpy_array.attr("dtype");
	if (!py::isinstance(numpy_array, import_cache.numpy.ma.masked_array())) {
		return dtype;
	}

	auto numpy_type = ConvertNumpyType(dtype);
	switch (numpy_type.type) {
	case NumpyNullableType::BOOL: {
		return import_cache.pandas.BooleanDtype()();
	}
	case NumpyNullableType::UINT_8: {
		return import_cache.pandas.UInt8Dtype()();
	}
	case NumpyNullableType::UINT_16: {
		return import_cache.pandas.UInt16Dtype()();
	}
	case NumpyNullableType::UINT_32: {
		return import_cache.pandas.UInt32Dtype()();
	}
	case NumpyNullableType::UINT_64: {
		return import_cache.pandas.UInt64Dtype()();
	}
	case NumpyNullableType::INT_8: {
		return import_cache.pandas.Int8Dtype()();
	}
	case NumpyNullableType::INT_16: {
		return import_cache.pandas.Int16Dtype()();
	}
	case NumpyNullableType::INT_32: {
		return import_cache.pandas.Int32Dtype()();
	}
	case NumpyNullableType::INT_64: {
		return import_cache.pandas.Int64Dtype()();
	}
	case NumpyNullableType::FLOAT_32:
	case NumpyNullableType::FLOAT_64:
	case NumpyNullableType::FLOAT_16: // there is no pandas.Float16Dtype
	default:
		return dtype;
	}
}

PandasDataFrame DuckDBPyResult::FrameFromNumpy(bool date_as_object, const py::handle &o) {
	D_ASSERT(py::gil_check());
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto pandas = import_cache.pandas();
	if (!pandas) {
		throw InvalidInputException("'pandas' is required for this operation but it was not installed");
	}

	py::object items = o.attr("items")();
	for (const py::handle &item : items) {
		// Each item is a tuple of (key, value)
		auto key_value = py::cast<py::tuple>(item);
		py::handle key = key_value[0];   // Access the first element (key)
		py::handle value = key_value[1]; // Access the second element (value)

		auto dtype = ConvertNumpyDtype(value);
		if (py::isinstance(value, import_cache.numpy.ma.masked_array())) {
			// o[key] = pd.Series(value.filled(pd.NA), dtype=dtype)
			auto series = pandas.attr("Series")(value.attr("data"), py::arg("dtype") = dtype);
			series.attr("__setitem__")(value.attr("mask"), import_cache.pandas.NA());
			o.attr("__setitem__")(key, series);
		}
	}

	PandasDataFrame df = py::cast<PandasDataFrame>(pandas.attr("DataFrame").attr("from_dict")(o));
	// Unfortunately we have to do a type change here for timezones since these types are not supported by numpy
	ChangeToTZType(df);

	auto names = df.attr("columns").cast<vector<string>>();
	D_ASSERT(result->ColumnCount() == names.size());
	if (date_as_object) {
		for (idx_t i = 0; i < result->ColumnCount(); i++) {
			if (result->types[i] == LogicalType::DATE) {
				df.attr("__setitem__")(names[i].c_str(), df[names[i].c_str()].attr("dt").attr("date"));
			}
		}
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

duckdb::pyarrow::Table DuckDBPyResult::FetchArrowTable(idx_t rows_per_batch, bool to_polars) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	auto names = result->names;
	if (to_polars) {
		QueryResult::DeduplicateColumns(names);
	}

	if (!result) {
		throw InvalidInputException("result closed");
	}
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

	py::list batches;
	if (result->type == QueryResultType::ARROW_RESULT) {
		auto &arrow_result = result->Cast<ArrowQueryResult>();
		auto arrays = arrow_result.ConsumeArrays();
		for (auto &array : arrays) {
			ArrowSchema arrow_schema;
			auto result_names = arrow_result.names;
			if (to_polars) {
				QueryResult::DeduplicateColumns(result_names);
			}
			ArrowArray data = array->arrow_array;
			array->arrow_array.release = nullptr;
			ArrowConverter::ToArrowSchema(&arrow_schema, arrow_result.types, result_names,
			                              arrow_result.client_properties);
			TransformDuckToArrowChunk(arrow_schema, data, batches);
		}
	} else {
		QueryResultChunkScanState scan_state(*result.get());
		while (true) {
			ArrowArray data;
			idx_t count;
			auto &query_result = *result.get();
			{
				D_ASSERT(py::gil_check());
				py::gil_scoped_release release;
				count = ArrowUtil::FetchChunk(scan_state, query_result.client_properties, rows_per_batch, &data,
				                              ArrowTypeExtensionData::GetExtensionTypes(
				                                  *query_result.client_properties.client_context, query_result.types));
			}
			if (count == 0) {
				break;
			}
			ArrowSchema arrow_schema;
			auto result_names = query_result.names;
			if (to_polars) {
				QueryResult::DeduplicateColumns(result_names);
			}
			ArrowConverter::ToArrowSchema(&arrow_schema, query_result.types, result_names,
			                              query_result.client_properties);
			TransformDuckToArrowChunk(arrow_schema, data, batches);
		}
	}

	return pyarrow::ToArrowTable(result->types, names, std::move(batches), result->client_properties);
}

ArrowArrayStream DuckDBPyResult::FetchArrowArrayStream(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(std::move(result), rows_per_batch);
	// The 'result_stream' is part of the 'private_data' of the ArrowArrayStream and its lifetime is bound to that of
	// the ArrowArrayStream.
	return result_stream->stream;
}

duckdb::pyarrow::RecordBatchReader DuckDBPyResult::FetchRecordBatchReader(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	py::gil_scoped_acquire acquire;
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto record_batch_reader_func = pyarrow_lib_module.attr("RecordBatchReader").attr("_import_from_c");
	auto stream = FetchArrowArrayStream(rows_per_batch);
	py::object record_batch_reader = record_batch_reader_func((uint64_t)&stream); // NOLINT
	return py::cast<duckdb::pyarrow::RecordBatchReader>(record_batch_reader);
}

static void ArrowArrayStreamPyCapsuleDestructor(PyObject *object) {
	auto data = PyCapsule_GetPointer(object, "arrow_array_stream");
	if (!data) {
		return;
	}
	auto stream = reinterpret_cast<ArrowArrayStream *>(data);
	if (stream->release) {
		stream->release(stream);
	}
	delete stream;
}

py::object DuckDBPyResult::FetchArrowCapsule(idx_t rows_per_batch) {
	auto stream_p = FetchArrowArrayStream(rows_per_batch);
	auto stream = new ArrowArrayStream();
	*stream = stream_p;
	return py::capsule(stream, "arrow_array_stream", ArrowArrayStreamPyCapsuleDestructor);
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
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL: {
		return py::str("NUMBER");
	}
	case LogicalTypeId::VARCHAR: {
		if (type.HasAlias() && type.GetAlias() == "JSON") {
			return py::str("JSON");
		} else {
			return py::str("STRING");
		}
	}
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
