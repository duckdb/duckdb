#include "altrepstring.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/arrow_wrapper.hpp"

using namespace duckdb;

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

struct RStatement {
	unique_ptr<PreparedStatement> stmt;
	vector<Value> parameters;
};

SEXP RApi::Release(SEXP stmtsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_release_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (stmtsexp) {
		R_ClearExternalPtr(stmtsexp);
		delete stmtholder;
	}
	return R_NilValue;
}

static SEXP duckdb_finalize_statement_R(SEXP stmtsexp) {
	return RApi::Release(stmtsexp);
}

SEXP RApi::Prepare(SEXP connsexp, SEXP querysexp) {
	RProtector r;
	if (TYPEOF(querysexp) != STRSXP || Rf_length(querysexp) != 1) {
		Rf_error("duckdb_prepare_R: Need single string parameter for query");
	}
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_prepare_R: Need external pointer parameter for connections");
	}

	char *query = (char *)CHAR(STRING_ELT(querysexp, 0));
	if (!query) {
		Rf_error("duckdb_prepare_R: No query");
	}

	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (!conn_wrapper || !conn_wrapper->conn) {
		Rf_error("duckdb_prepare_R: Invalid connection");
	}

	auto stmt = conn_wrapper->conn->Prepare(query);
	if (!stmt->success) {
		Rf_error("duckdb_prepare_R: Failed to prepare query %s\nError: %s", query, stmt->error.c_str());
	}

	auto stmtholder = new RStatement();
	stmtholder->stmt = move(stmt);

	SEXP retlist = r.Protect(NEW_LIST(6));

	SEXP stmtsexp = r.Protect(R_MakeExternalPtr(stmtholder, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(stmtsexp, (void (*)(SEXP))duckdb_finalize_statement_R);

	SET_NAMES(retlist, RStrings::get().str_ref_type_names_rtypes_n_param_str);

	SET_VECTOR_ELT(retlist, 0, querysexp);
	SET_VECTOR_ELT(retlist, 1, stmtsexp);

	SEXP stmt_type = RApi::StringsToSexp({StatementTypeToString(stmtholder->stmt->GetStatementType())});
	SET_VECTOR_ELT(retlist, 2, stmt_type);

	SEXP col_names = RApi::StringsToSexp(stmtholder->stmt->GetNames());
	SET_VECTOR_ELT(retlist, 3, col_names);

	vector<string> rtypes;

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
		case LogicalTypeId::TIMESTAMP:
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
		default:
			Rf_error("duckdb_prepare_R: Unknown column type for prepare: %s", stype.ToString().c_str());
			break;
		}
		rtypes.push_back(rtype);
	}

	SEXP rtypessexp = StringsToSexp(rtypes);
	SET_VECTOR_ELT(retlist, 4, rtypessexp);

	SET_VECTOR_ELT(retlist, 5, Rf_ScalarInteger(stmtholder->stmt->n_param));

	return retlist;
}

SEXP RApi::Bind(SEXP stmtsexp, SEXP paramsexp, SEXP arrowsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_bind_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (!stmtholder || !stmtholder->stmt) {
		Rf_error("duckdb_bind_R: Invalid statement");
	}

	stmtholder->parameters.clear();
	stmtholder->parameters.resize(stmtholder->stmt->n_param);

	if (stmtholder->stmt->n_param == 0) {
		Rf_error("duckdb_bind_R: dbBind called but query takes no parameters");
	}

	if (TYPEOF(paramsexp) != VECSXP || (idx_t)Rf_length(paramsexp) != stmtholder->stmt->n_param) {
		Rf_error("duckdb_bind_R: bind parameters need to be a list of length %i", stmtholder->stmt->n_param);
	}

	if (TYPEOF(arrowsexp) != LGLSXP) {
		Rf_error("duckdb_bind_R: Need logical for third parameter");
	}

	bool arrow_fetch = LOGICAL_POINTER(arrowsexp)[0] != 0;

	R_len_t n_rows = Rf_length(VECTOR_ELT(paramsexp, 0));

	for (idx_t param_idx = 1; param_idx < (idx_t)Rf_length(paramsexp); param_idx++) {
		SEXP valsexp = VECTOR_ELT(paramsexp, param_idx);
		if (Rf_length(valsexp) != n_rows) {
			Rf_error("duckdb_bind_R: bind parameter values need to have the same length");
		}
	}

	if (n_rows != 1 && arrow_fetch) {
		Rf_error("duckdb_bind_R: bind parameter values need to have length one for arrow queries");
	}

	RProtector r;
	auto out = r.Protect(NEW_LIST(n_rows));

	for (idx_t row_idx = 0; row_idx < (size_t)n_rows; ++row_idx) {
		for (idx_t param_idx = 0; param_idx < (idx_t)Rf_length(paramsexp); param_idx++) {
			SEXP valsexp = VECTOR_ELT(paramsexp, param_idx);
			auto val = RApiTypes::SexpToValue(valsexp, row_idx);
			stmtholder->parameters[param_idx] = val;
		}

		// No protection, assigned immediately
		auto exec_result = RApi::Execute(stmtsexp, arrowsexp);
		SET_VECTOR_ELT(out, row_idx, exec_result);
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
	case LogicalTypeId::TIMESTAMP:
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

		auto ptr = PROTECT(R_MakeExternalPtr((void *)wrapper, R_NilValue, R_NilValue));
		R_RegisterCFinalizer(ptr, AltrepString::Finalize);
		varvalue = r_varvalue.Protect(R_new_altrep(AltrepString::rclass, ptr, R_NilValue));
		UNPROTECT(1);
		break;
	}

	case LogicalTypeId::BLOB:
		varvalue = r_varvalue.Protect(NEW_LIST(nrows));
		break;
	case LogicalTypeId::ENUM:
		varvalue = r_varvalue.Protect(NEW_INTEGER(nrows));
		break;
	default:
		Rf_error("duckdb_execute_R: Unknown column type for execute: %s", type.ToString().c_str());
	}
	if (!varvalue) {
		throw std::bad_alloc();
	}
	return varvalue;
}

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
	case LogicalTypeId::TIMESTAMP: {
		auto src_data = FlatVector::GetData<timestamp_t>(src_vec);
		auto &mask = FlatVector::Validity(src_vec);
		double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
		for (size_t row_idx = 0; row_idx < n; row_idx++) {
			dest_ptr[row_idx] =
			    !mask.RowIsValid(row_idx) ? NA_REAL : (double)Timestamp::GetEpochSeconds(src_data[row_idx]);
		}

		// some dresssup for R
		SET_CLASS(dest, RStrings::get().POSIXct_POSIXt_str);
		Rf_setAttrib(dest, RStrings::get().tzone_sym, RStrings::get().UTC_str);
		break;
	}
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
				dest_ptr[row_idx] = n.micros / 1000000.0;
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
		RProtector list_prot;
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
			Rf_error("duckdb_execute_R: Unknown enum type for convert: %s", TypeIdToString(physical_type).c_str());
		}
		// increment by one cause R factor offsets start at 1
		auto dest_ptr = ((int32_t *)INTEGER_POINTER(dest)) + dest_offset;
		for (idx_t i = 0; i < n; i++) {
			if (dest_ptr[i] == NA_INTEGER) {
				continue;
			}
			dest_ptr[i]++;
		}

		RProtector r;
		auto &str_vec = EnumType::GetValuesInsertOrder(src_vec.GetType());
		auto size = EnumType::GetSize(src_vec.GetType());
		vector<string> str_c_vec(size);
		for (idx_t i = 0; i < size; i++) {
			str_c_vec[i] = str_vec.GetValue(i).ToString();
		}

		auto levels_sexp = r.Protect(RApi::StringsToSexp(str_c_vec));
		SET_LEVELS(dest, levels_sexp);
		SET_CLASS(dest, RStrings::get().factor_str);
		break;
	}
	default:
		Rf_error("duckdb_execute_R: Unknown column type for convert: %s", src_vec.GetType().ToString().c_str());
		break;
	}
}

static SEXP duckdb_execute_R_impl(MaterializedQueryResult *result) {
	RProtector r;
	// step 2: create result data frame and allocate columns
	uint32_t ncols = result->types.size();
	if (ncols == 0) {
		return Rf_ScalarReal(0); // no need for protection because no allocation can happen afterwards
	}

	uint64_t nrows = result->collection.Count();
	SEXP retlist = r.Protect(NEW_LIST(ncols));
	SET_NAMES(retlist, RApi::StringsToSexp(result->names));

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

struct RQueryResult {
	unique_ptr<QueryResult> result;
};

bool FetchArrowChunk(QueryResult *result, AppendableRList &batches_list, ArrowArray &arrow_data,
                     ArrowSchema &arrow_schema, SEXP &batch_import_from_c, SEXP &arrow_namespace) {
	if (result->type == QueryResultType::STREAM_RESULT) {
		auto stream_result = (StreamQueryResult *)result;
		if (!stream_result->IsOpen()) {
			return false;
		}
	}
	unique_ptr<DataChunk> data_chunk = result->Fetch();
	if (!data_chunk || data_chunk->size() == 0) {
		return false;
	}
	result->ToArrowSchema(&arrow_schema);
	data_chunk->ToArrowArray(&arrow_data);
	batches_list.PrepAppend();
	batches_list.Append(RApi::REvalRerror(batch_import_from_c, arrow_namespace));
	return true;
}

// Turn a DuckDB result set into an Arrow Table
SEXP RApi::DuckDBExecuteArrow(SEXP query_resultsexp, SEXP streamsexp, SEXP vector_per_chunksexp,
                              SEXP return_tablesexp) {
	RProtector r;
	RQueryResult *query_result_holder = (RQueryResult *)R_ExternalPtrAddr(query_resultsexp);
	auto result = query_result_holder->result.get();
	// somewhat dark magic below
	SEXP arrow_namespace_call = r.Protect(Rf_lang2(RStrings::get().getNamespace_sym, RStrings::get().arrow_str));
	SEXP arrow_namespace = r.Protect(RApi::REvalRerror(arrow_namespace_call, R_GlobalEnv));
	bool stream = LOGICAL_POINTER(streamsexp)[0] != 0;
	int num_of_vectors = NUMERIC_POINTER(vector_per_chunksexp)[0];
	bool return_table = LOGICAL_POINTER(return_tablesexp)[0] != 0;
	if (TYPEOF(streamsexp) != LGLSXP || LENGTH(streamsexp) != 1) {
		Rf_error("stream parameter needs to be single-value logical");
	}
	if (TYPEOF(return_tablesexp) != LGLSXP || LENGTH(return_tablesexp) != 1) {
		Rf_error("return_table parameter needs to be single-value logical");
	}
	if (TYPEOF(vector_per_chunksexp) != REALSXP || LENGTH(vector_per_chunksexp) != 1) {
		Rf_error("vector_per_chunks parameter needs to be single-value numeric");
	}
	// export schema setup
	ArrowSchema arrow_schema;
	auto schema_ptr_sexp = r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_schema))));
	auto schema_import_from_c = r.Protect(Rf_lang2(RStrings::get().ImportSchema_sym, schema_ptr_sexp));

	// export data setup
	ArrowArray arrow_data;
	auto data_ptr_sexp = r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_data))));
	auto batch_import_from_c =
	    r.Protect(Rf_lang3(RStrings::get().ImportRecordBatch_sym, data_ptr_sexp, schema_ptr_sexp));
	// create data batches
	AppendableRList batches_list;
	if (stream) {
		for (idx_t i = 0; i < (size_t)num_of_vectors; i++) {
			if (!FetchArrowChunk(result, batches_list, arrow_data, arrow_schema, batch_import_from_c,
			                     arrow_namespace)) {
				break;
			}
		}
	} else {
		while (FetchArrowChunk(result, batches_list, arrow_data, arrow_schema, batch_import_from_c, arrow_namespace)) {
		}
	}

	SET_LENGTH(batches_list.the_list, batches_list.size);

	result->ToArrowSchema(&arrow_schema);
	SEXP schema_arrow_obj = r.Protect(RApi::REvalRerror(schema_import_from_c, arrow_namespace));

	// create arrow::Table
	if (return_table) {
		auto from_record_batches = r.Protect(
		    Rf_lang3(RStrings::get().Table__from_record_batches_sym, batches_list.the_list, schema_arrow_obj));
		return RApi::REvalRerror(from_record_batches, arrow_namespace);
	}
	return batches_list.the_list;
}

// Turn a DuckDB result set into an RecordBatchReader
SEXP RApi::DuckDBRecordBatchR(SEXP query_resultsexp, SEXP approx_batch_sizeexp) {
	RProtector r;
	RQueryResult *query_result_holder = (RQueryResult *)R_ExternalPtrAddr(query_resultsexp);
	int approx_batch_size = NUMERIC_POINTER(approx_batch_sizeexp)[0];
	if (TYPEOF(approx_batch_sizeexp) != REALSXP || LENGTH(approx_batch_sizeexp) != 1) {
		Rf_error("vector_per_chunks parameter needs to be single-value numeric");
	}
	// somewhat dark magic below
	SEXP arrow_namespace_call = r.Protect(Rf_lang2(RStrings::get().getNamespace_sym, RStrings::get().arrow_str));
	SEXP arrow_namespace = r.Protect(RApi::REvalRerror(arrow_namespace_call, R_GlobalEnv));

	ResultArrowArrayStreamWrapper *result_stream =
	    new ResultArrowArrayStreamWrapper(move(query_result_holder->result), approx_batch_size);
	auto stream_ptr_sexp =
	    r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&result_stream->stream))));
	auto record_batch_reader = r.Protect(Rf_lang2(RStrings::get().ImportRecordBatchReader_sym, stream_ptr_sexp));
	return RApi::REvalRerror(record_batch_reader, arrow_namespace);
}

static SEXP DuckDBFinalizeQueryR(SEXP query_resultsexp) {
	RQueryResult *query_result_holder = (RQueryResult *)R_ExternalPtrAddr(query_resultsexp);
	if (query_resultsexp) {
		R_ClearExternalPtr(query_resultsexp);
		delete query_result_holder;
	}
	return R_NilValue;
}

SEXP RApi::Execute(SEXP stmtsexp, SEXP arrowsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_execute_R: Need external pointer for first parameter");
	}
	if (TYPEOF(arrowsexp) != LGLSXP) {
		Rf_error("duckdb_execute_R: Need logical for second parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (!stmtholder || !stmtholder->stmt) {
		Rf_error("duckdb_execute_R: Invalid statement");
	}

	bool arrow_fetch = LOGICAL_POINTER(arrowsexp)[0] != 0;
	auto generic_result = stmtholder->stmt->Execute(stmtholder->parameters, arrow_fetch);
	if (!generic_result->success) {
		Rf_error("duckdb_execute_R: Failed to run query\nError: %s", generic_result->error.c_str());
	}

	if (arrow_fetch) {
		RProtector r;
		auto query_result = new RQueryResult();
		query_result->result = move(generic_result);
		SEXP query_resultexp = r.Protect(R_MakeExternalPtr(query_result, R_NilValue, R_NilValue));
		R_RegisterCFinalizer(query_resultexp, (void (*)(SEXP))DuckDBFinalizeQueryR);
		return query_resultexp;
	} else {
		D_ASSERT(generic_result->type == QueryResultType::MATERIALIZED_RESULT);
		MaterializedQueryResult *result = (MaterializedQueryResult *)generic_result.get();
		return duckdb_execute_R_impl(result);
	}
}
