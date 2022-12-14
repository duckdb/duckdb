#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_objects.hpp"

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

unique_ptr<DataChunk> DuckDBPyResult::FetchNext(QueryResult &result) {
	if (!result_closed && result.type == QueryResultType::STREAM_RESULT && !((StreamQueryResult &)result).IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = result.Fetch();
	if (result.HasError()) {
		result.ThrowError();
	}
	return chunk;
}

unique_ptr<DataChunk> DuckDBPyResult::FetchNextRaw(QueryResult &result) {
	if (!result_closed && result.type == QueryResultType::STREAM_RESULT && !((StreamQueryResult &)result).IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = result.FetchRaw();
	if (result.HasError()) {
		result.ThrowError();
	}
	return chunk;
}

py::object DuckDBPyResult::Fetchone() {
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
		res[col_idx] = PythonObject::FromValue(val, result->types[col_idx]);
	}
	chunk_offset++;
	return move(res);
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

py::dict DuckDBPyResult::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk) {
	if (!result) {
		throw InvalidInputException("result closed");
	}

	// iterate over the result to materialize the data needed for the NumPy arrays
	idx_t initial_capacity = STANDARD_VECTOR_SIZE * 2;
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		// materialized query result: we know exactly how much space we need
		auto &materialized = (MaterializedQueryResult &)*result;
		initial_capacity = materialized.RowCount();
	}

	NumpyResultConversion conversion(result->types, initial_capacity);
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		auto &materialized = (MaterializedQueryResult &)*result;
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
		auto stream_result = (StreamQueryResult *)result.get();
		for (idx_t count_vec = 0; count_vec < vectors_per_chunk; count_vec++) {
			if (!stream_result->IsOpen()) {
				break;
			}
			unique_ptr<DataChunk> chunk;
			{
				py::gil_scoped_release release;
				chunk = FetchNextRaw(*stream_result);
			}
			if (!chunk || chunk->size() == 0) {
				//! finished
				break;
			}
			conversion.Append(*chunk);
			InsertCategory(*stream_result, categories);
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
void DuckDBPyResult::ChangeToTZType(DataFrame &df) {
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::TIMESTAMP_TZ) {
			// first localize to UTC then convert to timezone_config
			auto utc_local = df[result->names[i].c_str()].attr("dt").attr("tz_localize")("UTC");
			df[result->names[i].c_str()] = utc_local.attr("dt").attr("tz_convert")(timezone_config);
		}
	}
}

// TODO: unify these with an enum/flag to indicate which conversions to perform
void DuckDBPyResult::ChangeDateToDatetime(DataFrame &df) {
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::DATE) {
			df[result->names[i].c_str()] = df[result->names[i].c_str()].attr("dt").attr("date");
		}
	}
}

DataFrame DuckDBPyResult::FrameFromNumpy(bool date_as_object, const py::handle &o) {
	auto df = py::cast<DataFrame>(py::module::import("pandas").attr("DataFrame").attr("from_dict")(o));
	// Unfortunately we have to do a type change here for timezones since these types are not supported by numpy
	ChangeToTZType(df);
	if (date_as_object) {
		ChangeDateToDatetime(df);
	}
	return df;
}

DataFrame DuckDBPyResult::FetchDF(bool date_as_object) {
	timezone_config = QueryResult::GetConfigTimezone(*result);
	return FrameFromNumpy(date_as_object, FetchNumpyInternal());
}

DataFrame DuckDBPyResult::FetchDFChunk(idx_t num_of_vectors, bool date_as_object) {
	if (timezone_config.empty()) {
		timezone_config = QueryResult::GetConfigTimezone(*result);
	}
	return FrameFromNumpy(date_as_object, FetchNumpyInternal(true, num_of_vectors));
}

void TransformDuckToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, py::list &batches) {
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
	batches.append(batch_import_func((uint64_t)&data, (uint64_t)&arrow_schema));
}

bool DuckDBPyResult::FetchArrowChunk(QueryResult *result, py::list &batches, idx_t chunk_size) {
	ArrowArray data;
	auto count = ArrowUtil::FetchChunk(result, chunk_size, &data);
	if (count == 0) {
		return false;
	}
	ArrowSchema arrow_schema;
	timezone_config = QueryResult::GetConfigTimezone(*result);
	ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
	TransformDuckToArrowChunk(arrow_schema, data, batches);
	return true;
}

py::object DuckDBPyResult::FetchAllArrowChunks(idx_t chunk_size) {
	if (!result) {
		throw InvalidInputException("result closed");
	}
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

	py::list batches;

	while (FetchArrowChunk(result.get(), batches, chunk_size)) {
	}
	return std::move(batches);
}

duckdb::pyarrow::Table DuckDBPyResult::FetchArrowTable(idx_t chunk_size) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	py::gil_scoped_acquire acquire;

	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto from_batches_func = pyarrow_lib_module.attr("Table").attr("from_batches");

	auto schema_import_func = pyarrow_lib_module.attr("Schema").attr("_import_from_c");
	ArrowSchema schema;

	timezone_config = QueryResult::GetConfigTimezone(*result);
	ArrowConverter::ToArrowSchema(&schema, result->types, result->names, timezone_config);

	auto schema_obj = schema_import_func((uint64_t)&schema);

	py::list batches = FetchAllArrowChunks(chunk_size);

	// We return an Arrow Table
	return py::cast<duckdb::pyarrow::Table>(from_batches_func(batches, schema_obj));
}

duckdb::pyarrow::RecordBatchReader DuckDBPyResult::FetchRecordBatchReader(idx_t chunk_size) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	py::gil_scoped_acquire acquire;
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto record_batch_reader_func = pyarrow_lib_module.attr("RecordBatchReader").attr("_import_from_c");
	//! We have to construct an Arrow Array Stream
	ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(move(result), chunk_size);
	py::object record_batch_reader = record_batch_reader_func((uint64_t)&result_stream->stream);
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
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return py::str("STRING");
	case LogicalTypeId::BLOB:
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
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return py::str("dict");
	case LogicalTypeId::LIST: {
		return py::str("list");
	}
	case LogicalTypeId::INTERVAL: {
		return py::str("TIMEDELTA");
	}
	case LogicalTypeId::USER:
	case LogicalTypeId::ENUM: {
		return py::str(type.ToString());
	}
	default:
		throw NotImplementedException("Unsupported type: \"%s\"", type.ToString());
	}
}

py::list DuckDBPyResult::Description() {
	const auto names = result->names;

	py::list desc(names.size());

	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		auto py_name = py::str(names[col_idx]);
		auto py_type = GetTypeToPython(result->types[col_idx]);
		desc[col_idx] = py::make_tuple(py_name, py_type, py::none(), py::none(), py::none(), py::none(), py::none());
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
