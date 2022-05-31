#include "rapi.hpp"
#include "typesr.hpp"
#include "altrepstring.hpp"

#include <R_ext/Utils.h>

#include "duckdb/common/arrow.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/common/result_arrow_wrapper.hpp"
#include "duckdb/main/stream_query_result.hpp"

using namespace duckdb;
using namespace cpp11::literals;

// converter for primitive types
template <class SRC, class DEST>
static void VectorToR(Vector &src_vec, size_t count, void *dest, uint64_t dest_offset, DEST na_val) {
	auto src_ptr = FlatVector::GetData<SRC>(src_vec);
	auto &mask = FlatVector::Validity(src_vec);
	auto dest_ptr = ((DEST *)dest) + dest_offset;
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] = !mask.RowIsValid(row_idx) ? na_val : src_ptr[row_idx];
	}
}

[[cpp11::register]] void rapi_release(duckdb::stmt_eptr_t stmt) {
	auto stmt_ptr = stmt.release();
	if (stmt_ptr) {
		delete stmt_ptr;
	}
}

[[cpp11::register]] cpp11::list rapi_prepare(duckdb::conn_eptr_t conn, std::string query) {
	if (!conn || !conn->conn) {
		cpp11::stop("rapi_prepare: Invalid connection");
	}

	auto statements = conn->conn->ExtractStatements(query.c_str());
	if (statements.empty()) {
		// no statements to execute
		cpp11::stop("rapi_prepare: No statements to execute");
	}
	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (idx_t i = 0; i + 1 < statements.size(); i++) {
		auto res = conn->conn->Query(move(statements[i]));
		if (!res->success) {
			cpp11::stop("rapi_prepare: Failed to execute statement %s\nError: %s", query.c_str(), res->error.c_str());
		}
	}
	auto stmt = conn->conn->Prepare(move(statements.back()));
	if (!stmt->success) {
		cpp11::stop("rapi_prepare: Failed to prepare query %s\nError: %s", query.c_str(), stmt->error.c_str());
	}

	cpp11::writable::list retlist;
	retlist.reserve(6);
	retlist.push_back({"str"_nm = query});

	auto stmtholder = new RStatement();
	stmtholder->stmt = move(stmt);

	retlist.push_back({"ref"_nm = stmt_eptr_t(stmtholder)});
	retlist.push_back({"type"_nm = StatementTypeToString(stmtholder->stmt->GetStatementType())});
	retlist.push_back({"names"_nm = cpp11::as_sexp(stmtholder->stmt->GetNames())});

	cpp11::writable::strings rtypes;

	for (auto &stype : stmtholder->stmt->GetTypes()) {
		string rtype = "";
		switch (stype.id()) {
		case LogicalTypeId::BOOLEAN:
			rtype = "logical";
			break;
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			rtype = "integer";
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP_NS:
			rtype = "POSIXct";
			break;
		case LogicalTypeId::DATE:
			rtype = "Date";
			break;
		case LogicalTypeId::TIME:
			rtype = "difftime";
			break;
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::DECIMAL:
			rtype = "numeric";
			break;
		case LogicalTypeId::VARCHAR:
			rtype = "character";
			break;
		case LogicalTypeId::BLOB:
			rtype = "raw";
			break;
		case LogicalTypeId::LIST:
			rtype = "list";
			break;
		case LogicalTypeId::ENUM:
			rtype = "factor";
			break;
		case LogicalTypeId::UNKNOWN:
			rtype = "unknown";
			break;
		default:
			cpp11::stop("rapi_prepare: Unknown column type for prepare: %s", stype.ToString().c_str());
			break;
		}
		rtypes.push_back(rtype);
	}

	retlist.push_back({"rtypes"_nm = rtypes});
	retlist.push_back({"n_param"_nm = stmtholder->stmt->n_param});

	return retlist;
}

[[cpp11::register]] cpp11::list rapi_bind(duckdb::stmt_eptr_t stmt, cpp11::list params, bool arrow) {
	if (!stmt || !stmt->stmt) {
		cpp11::stop("rapi_bind: Invalid statement");
	}

	stmt->parameters.clear();
	stmt->parameters.resize(stmt->stmt->n_param);

	if (stmt->stmt->n_param == 0) {
		cpp11::stop("rapi_bind: dbBind called but query takes no parameters");
	}

	if (params.size() != stmt->stmt->n_param) {
		cpp11::stop("rapi_bind: Bind parameters need to be a list of length %i", stmt->stmt->n_param);
	}

	R_len_t n_rows = Rf_length(params[0]);

	for (auto param = std::next(params.begin()); param != params.end(); ++param) {
		if (Rf_length(*param) != n_rows) {
			cpp11::stop("rapi_bind: Bind parameter values need to have the same length");
		}
	}

	if (n_rows != 1 && arrow) {
		cpp11::stop("rapi_bind: Bind parameter values need to have length one for arrow queries");
	}

	cpp11::writable::list out;
	out.reserve(n_rows);

	for (idx_t row_idx = 0; row_idx < (size_t)n_rows; ++row_idx) {
		for (idx_t param_idx = 0; param_idx < (idx_t)params.size(); param_idx++) {
			SEXP valsexp = params[(size_t)param_idx];
			auto val = RApiTypes::SexpToValue(valsexp, row_idx);
			stmt->parameters[param_idx] = val;
		}

		// No protection, assigned immediately
		out.push_back(rapi_execute(stmt, arrow));
	}

	return out;
}

static SEXP allocate(const LogicalType &type, RProtector &r_varvalue, idx_t nrows) {
	SEXP varvalue = NULL;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		varvalue = r_varvalue.Protect(NEW_LOGICAL(nrows));
		break;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER:
		varvalue = r_varvalue.Protect(NEW_INTEGER(nrows));
		break;
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
		varvalue = r_varvalue.Protect(NEW_NUMERIC(nrows));
		break;
	case LogicalTypeId::LIST:
		varvalue = r_varvalue.Protect(NEW_LIST(nrows));
		break;
	case LogicalTypeId::VARCHAR: {
		auto wrapper = new DuckDBAltrepStringWrapper();
		wrapper->length = nrows;

		cpp11::external_pointer<DuckDBAltrepStringWrapper> ptr(wrapper);
		varvalue = r_varvalue.Protect(R_new_altrep(AltrepString::rclass, ptr, R_NilValue));
		break;
	}

	case LogicalTypeId::BLOB:
		varvalue = r_varvalue.Protect(NEW_LIST(nrows));
		break;
	case LogicalTypeId::ENUM:
		varvalue = r_varvalue.Protect(NEW_INTEGER(nrows));
		break;
	default:
		cpp11::stop("rapi_execute: Unknown column type for execute: %s", type.ToString().c_str());
	}
	if (!varvalue) {
		throw std::bad_alloc();
	}
	return varvalue;
}

// Convert DuckDB's timestamp to R's timestamp (POSIXct). This is a represented as the number of seconds since the
// epoch, stored as a double.
template <LogicalTypeId>
double ConvertTimestampValue(int64_t timestamp);

template <>
double ConvertTimestampValue<LogicalTypeId::TIMESTAMP_SEC>(int64_t timestamp) {
	return static_cast<double>(timestamp);
}

template <>
double ConvertTimestampValue<LogicalTypeId::TIMESTAMP_MS>(int64_t timestamp) {
	return static_cast<double>(timestamp) / Interval::MSECS_PER_SEC;
}

template <>
double ConvertTimestampValue<LogicalTypeId::TIMESTAMP>(int64_t timestamp) {
	return static_cast<double>(timestamp) / Interval::MICROS_PER_SEC;
}

template <>
double ConvertTimestampValue<LogicalTypeId::TIMESTAMP_TZ>(int64_t timestamp) {
	return ConvertTimestampValue<LogicalTypeId::TIMESTAMP>(timestamp);
}

template <>
double ConvertTimestampValue<LogicalTypeId::TIMESTAMP_NS>(int64_t timestamp) {
	return static_cast<double>(timestamp) / Interval::NANOS_PER_SEC;
}

template <LogicalTypeId LT>
void ConvertTimestampVector(Vector &src_vec, size_t count, SEXP &dest, uint64_t dest_offset) {
	auto src_data = FlatVector::GetData<int64_t>(src_vec);
	auto &mask = FlatVector::Validity(src_vec);
	double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] = !mask.RowIsValid(row_idx) ? NA_REAL : ConvertTimestampValue<LT>(src_data[row_idx]);
	}

	// some dresssup for R
	SET_CLASS(dest, RStrings::get().POSIXct_POSIXt_str);
	Rf_setAttrib(dest, RStrings::get().tzone_sym, RStrings::get().UTC_str);
}

std::once_flag nanosecond_coercion_warning;

static void transform(Vector &src_vec, SEXP &dest, idx_t dest_offset, idx_t n) {
	switch (src_vec.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		VectorToR<int8_t, uint32_t>(src_vec, n, LOGICAL_POINTER(dest), dest_offset, NA_LOGICAL);
		break;
	case LogicalTypeId::UTINYINT:
		VectorToR<uint8_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
		break;
	case LogicalTypeId::TINYINT:
		VectorToR<int8_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
		break;
	case LogicalTypeId::USMALLINT:
		VectorToR<uint16_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
		break;
	case LogicalTypeId::SMALLINT:
		VectorToR<int16_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
		break;
	case LogicalTypeId::INTEGER:
		VectorToR<int32_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		ConvertTimestampVector<LogicalTypeId::TIMESTAMP_SEC>(src_vec, n, dest, dest_offset);
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		ConvertTimestampVector<LogicalTypeId::TIMESTAMP_MS>(src_vec, n, dest, dest_offset);
		break;
	case LogicalTypeId::TIMESTAMP:
		ConvertTimestampVector<LogicalTypeId::TIMESTAMP>(src_vec, n, dest, dest_offset);
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		ConvertTimestampVector<LogicalTypeId::TIMESTAMP_TZ>(src_vec, n, dest, dest_offset);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		ConvertTimestampVector<LogicalTypeId::TIMESTAMP_NS>(src_vec, n, dest, dest_offset);
		std::call_once(nanosecond_coercion_warning, Rf_warning,
		               "Coercing nanoseconds to a lower resolution may result in a loss of data.");
		break;
	case LogicalTypeId::DATE: {
		auto src_data = FlatVector::GetData<date_t>(src_vec);
		auto &mask = FlatVector::Validity(src_vec);
		double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			dest_ptr[row_idx] = !mask.RowIsValid(row_idx) ? NA_REAL : (double)int32_t(src_data[row_idx]);
		}

		// some dresssup for R
		SET_CLASS(dest, RStrings::get().Date_str);
		break;
	}
	case LogicalTypeId::TIME: {
		auto src_data = FlatVector::GetData<dtime_t>(src_vec);
		auto &mask = FlatVector::Validity(src_vec);
		double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			if (!mask.RowIsValid(row_idx)) {
				dest_ptr[row_idx] = NA_REAL;
			} else {
				dtime_t n = src_data[row_idx];
				dest_ptr[row_idx] = n.micros / Interval::MICROS_PER_SEC;
			}
		}

		// some dress-up for R
		SET_CLASS(dest, RStrings::get().difftime_str);
		Rf_setAttrib(dest, RStrings::get().units_sym, RStrings::get().secs_str);
		break;
	}
	case LogicalTypeId::UINTEGER:
		VectorToR<uint32_t, double>(src_vec, n, NUMERIC_POINTER(dest), dest_offset, NA_REAL);
		break;
	case LogicalTypeId::UBIGINT:
		VectorToR<uint64_t, double>(src_vec, n, NUMERIC_POINTER(dest), dest_offset, NA_REAL);
		break;
	case LogicalTypeId::BIGINT:
		VectorToR<int64_t, double>(src_vec, n, NUMERIC_POINTER(dest), dest_offset, NA_REAL);
		break;
	case LogicalTypeId::HUGEINT: {
		auto src_data = FlatVector::GetData<hugeint_t>(src_vec);
		auto &mask = FlatVector::Validity(src_vec);
		double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			if (!mask.RowIsValid(row_idx)) {
				dest_ptr[row_idx] = NA_REAL;
			} else {
				Hugeint::TryCast(src_data[row_idx], dest_ptr[row_idx]);
			}
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto &decimal_type = src_vec.GetType();
		double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
		auto dec_scale = DecimalType::GetScale(decimal_type);
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT16:
			RDecimalCastLoop<int16_t>(src_vec, n, dest_ptr, dec_scale);
			break;
		case PhysicalType::INT32:
			RDecimalCastLoop<int32_t>(src_vec, n, dest_ptr, dec_scale);
			break;
		case PhysicalType::INT64:
			RDecimalCastLoop<int64_t>(src_vec, n, dest_ptr, dec_scale);
			break;
		case PhysicalType::INT128:
			RDecimalCastLoop<hugeint_t>(src_vec, n, dest_ptr, dec_scale);
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for DECIMAL");
		}
		break;
	}
	case LogicalTypeId::FLOAT:
		VectorToR<float, double>(src_vec, n, NUMERIC_POINTER(dest), dest_offset, NA_REAL);
		break;

	case LogicalTypeId::DOUBLE:
		VectorToR<double, double>(src_vec, n, NUMERIC_POINTER(dest), dest_offset, NA_REAL);
		break;
	case LogicalTypeId::VARCHAR: {
		auto wrapper = (DuckDBAltrepStringWrapper *)R_ExternalPtrAddr(R_altrep_data1(dest));
		wrapper->vectors.emplace_back(LogicalType::VARCHAR, nullptr);
		wrapper->vectors.back().Reference(src_vec);
		break;
	}
	case LogicalTypeId::LIST: {
		// figure out the total and max element length of the list vector child
		auto src_data = ListVector::GetData(src_vec);
		auto &child_type = ListType::GetChildType(src_vec.GetType());
		Vector child_vector(child_type, nullptr);

		// actual loop over rows
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			if (!FlatVector::Validity(src_vec).RowIsValid(row_idx)) {
				SET_ELEMENT(dest, dest_offset + row_idx, Rf_ScalarLogical(NA_LOGICAL));
			} else {
				child_vector.Slice(ListVector::GetEntry(src_vec), src_data[row_idx].offset);

				RProtector ele_prot;
				// transform the list child vector to a single R SEXP
				auto list_element =
				    allocate(ListType::GetChildType(src_vec.GetType()), ele_prot, src_data[row_idx].length);
				transform(child_vector, list_element, 0, src_data[row_idx].length);

				// call R's own extract subset method
				SET_ELEMENT(dest, dest_offset + row_idx, list_element);
			}
		}
		break;
	}
	case LogicalTypeId::BLOB: {
		auto src_ptr = FlatVector::GetData<string_t>(src_vec);
		auto &mask = FlatVector::Validity(src_vec);
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			if (!mask.RowIsValid(row_idx)) {
				SET_VECTOR_ELT(dest, dest_offset + row_idx, Rf_ScalarLogical(NA_LOGICAL));
			} else {
				SEXP rawval = NEW_RAW(src_ptr[row_idx].GetSize());
				if (!rawval) {
					throw std::bad_alloc();
				}
				memcpy(RAW_POINTER(rawval), src_ptr[row_idx].GetDataUnsafe(), src_ptr[row_idx].GetSize());
				SET_VECTOR_ELT(dest, dest_offset + row_idx, rawval);
			}
		}
		break;
	}
	case LogicalTypeId::ENUM: {
		auto physical_type = src_vec.GetType().InternalType();

		switch (physical_type) {
		case PhysicalType::UINT8:
			VectorToR<uint8_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
			break;

		case PhysicalType::UINT16:
			VectorToR<uint16_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
			break;

		case PhysicalType::UINT32:
			VectorToR<uint8_t, uint32_t>(src_vec, n, INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
			break;

		default:
			cpp11::stop("rapi_execute: Unknown enum type for convert: %s", TypeIdToString(physical_type).c_str());
		}
		// increment by one cause R factor offsets start at 1
		auto dest_ptr = ((int32_t *)INTEGER_POINTER(dest)) + dest_offset;
		for (idx_t i = 0; i < n; i++) {
			if (dest_ptr[i] == NA_INTEGER) {
				continue;
			}
			dest_ptr[i]++;
		}

		auto &str_vec = EnumType::GetValuesInsertOrder(src_vec.GetType());
		auto size = EnumType::GetSize(src_vec.GetType());
		vector<string> str_c_vec(size);
		for (idx_t i = 0; i < size; i++) {
			str_c_vec[i] = str_vec.GetValue(i).ToString();
		}

		SET_LEVELS(dest, StringsToSexp(str_c_vec));
		SET_CLASS(dest, RStrings::get().factor_str);
		break;
	}
	default:
		cpp11::stop("rapi_execute: Unknown column type for convert: %s", src_vec.GetType().ToString().c_str());
		break;
	}
}

static SEXP duckdb_execute_R_impl(MaterializedQueryResult *result) {
	// step 2: create result data frame and allocate columns
	uint32_t ncols = result->types.size();
	if (ncols == 0) {
		return Rf_ScalarReal(0); // no need for protection because no allocation can happen afterwards
	}

	uint64_t nrows = result->collection.Count();
	cpp11::list retlist(NEW_LIST(ncols));
	SET_NAMES(retlist, StringsToSexp(result->names));

	for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
		// TODO move the protector to allocate?
		RProtector r_varvalue;
		auto varvalue = allocate(result->types[col_idx], r_varvalue, nrows);
		SET_VECTOR_ELT(retlist, col_idx, varvalue);
	}

	// at this point retlist is fully allocated and the only protected SEXP

	// step 3: set values from chunks
	uint64_t dest_offset = 0;
	idx_t chunk_idx = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		D_ASSERT(chunk->ColumnCount() == ncols);
		D_ASSERT(chunk->ColumnCount() == (idx_t)Rf_length(retlist));
		for (size_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
			SEXP dest = VECTOR_ELT(retlist, col_idx);
			transform(chunk->data[col_idx], dest, dest_offset, chunk->size());
		}
		dest_offset += chunk->size();
		chunk_idx++;
	}

	D_ASSERT(dest_offset == nrows);
	return retlist;
}

struct AppendableRList {
	AppendableRList() {
		the_list = r.Protect(NEW_LIST(capacity));
	}
	void PrepAppend() {
		if (size >= capacity) {
			capacity = capacity * 2;
			SEXP new_list = r.Protect(NEW_LIST(capacity));
			D_ASSERT(new_list);
			for (idx_t i = 0; i < size; i++) {
				SET_VECTOR_ELT(new_list, i, VECTOR_ELT(the_list, i));
			}
			the_list = new_list;
		}
	}

	void Append(SEXP val) {
		D_ASSERT(size < capacity);
		D_ASSERT(the_list != R_NilValue);
		SET_VECTOR_ELT(the_list, size++, val);
	}
	SEXP the_list;
	idx_t capacity = 1000;
	idx_t size = 0;
	RProtector r;
};

bool FetchArrowChunk(QueryResult *result, AppendableRList &batches_list, ArrowArray &arrow_data,
                     ArrowSchema &arrow_schema, SEXP batch_import_from_c, SEXP arrow_namespace, idx_t chunk_size) {

	auto data_chunk = ArrowUtil::FetchChunk(result, chunk_size);
	if (!data_chunk || data_chunk->size() == 0) {
		return false;
	}
	string timezone_config = QueryResult::GetConfigTimezone(*result);
	QueryResult::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
	data_chunk->ToArrowArray(&arrow_data);
	batches_list.PrepAppend();
	batches_list.Append(cpp11::safe[Rf_eval](batch_import_from_c, arrow_namespace));
	return true;
}

// Turn a DuckDB result set into an Arrow Table
[[cpp11::register]] SEXP rapi_execute_arrow(duckdb::rqry_eptr_t qry_res, int chunk_size) {
	if (qry_res->result->type == QueryResultType::STREAM_RESULT) {
		qry_res->result = ((StreamQueryResult *)qry_res->result.get())->Materialize();
	}
	auto result = qry_res->result.get();
	// somewhat dark magic below
	cpp11::function getNamespace = RStrings::get().getNamespace_sym;
	cpp11::sexp arrow_namespace(getNamespace(RStrings::get().arrow_str));

	// export schema setup
	ArrowSchema arrow_schema;
	cpp11::doubles schema_ptr_sexp(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_schema))));
	cpp11::sexp schema_import_from_c(Rf_lang2(RStrings::get().ImportSchema_sym, schema_ptr_sexp));

	// export data setup
	ArrowArray arrow_data;
	cpp11::doubles data_ptr_sexp(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_data))));
	cpp11::sexp batch_import_from_c(Rf_lang3(RStrings::get().ImportRecordBatch_sym, data_ptr_sexp, schema_ptr_sexp));
	// create data batches
	AppendableRList batches_list;

	while (FetchArrowChunk(result, batches_list, arrow_data, arrow_schema, batch_import_from_c, arrow_namespace,
	                       chunk_size)) {
	}

	SET_LENGTH(batches_list.the_list, batches_list.size);
	string timezone_config = QueryResult::GetConfigTimezone(*result);
	QueryResult::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
	cpp11::sexp schema_arrow_obj(cpp11::safe[Rf_eval](schema_import_from_c, arrow_namespace));

	// create arrow::Table
	cpp11::sexp from_record_batches(
	    Rf_lang3(RStrings::get().Table__from_record_batches_sym, batches_list.the_list, schema_arrow_obj));
	return cpp11::safe[Rf_eval](from_record_batches, arrow_namespace);
}

// Turn a DuckDB result set into an RecordBatchReader
[[cpp11::register]] SEXP rapi_record_batch(duckdb::rqry_eptr_t qry_res, int chunk_size) {
	// somewhat dark magic below
	cpp11::function getNamespace = RStrings::get().getNamespace_sym;
	cpp11::sexp arrow_namespace(getNamespace(RStrings::get().arrow_str));

	ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(move(qry_res->result), chunk_size);
	cpp11::sexp stream_ptr_sexp(
	    Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&result_stream->stream))));
	cpp11::sexp record_batch_reader(Rf_lang2(RStrings::get().ImportRecordBatchReader_sym, stream_ptr_sexp));
	return cpp11::safe[Rf_eval](record_batch_reader, arrow_namespace);
}

[[cpp11::register]] SEXP rapi_execute(duckdb::stmt_eptr_t stmt, bool arrow) {
	if (!stmt || !stmt->stmt) {
		cpp11::stop("rapi_execute: Invalid statement");
	}

	auto pending_query = stmt->stmt->PendingQuery(stmt->parameters, arrow);
	duckdb::PendingExecutionResult execution_result;
	do {
		execution_result = pending_query->ExecuteTask();
		R_CheckUserInterrupt();
	} while (execution_result == PendingExecutionResult::RESULT_NOT_READY);
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		cpp11::stop("rapi_execute: Failed to run query\nError: %s", pending_query->error.c_str());
	}
	auto generic_result = pending_query->Execute();
	if (!generic_result->success) {
		cpp11::stop("rapi_execute: Failed to run query\nError: %s", generic_result->error.c_str());
	}

	if (arrow) {
		auto query_result = new RQueryResult();
		query_result->result = move(generic_result);
		rqry_eptr_t query_resultsexp(query_result);
		return query_resultsexp;
	} else {
		D_ASSERT(generic_result->type == QueryResultType::MATERIALIZED_RESULT);
		MaterializedQueryResult *result = (MaterializedQueryResult *)generic_result.get();
		return duckdb_execute_R_impl(result);
	}
}
