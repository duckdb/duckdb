#include "rapi.hpp"
#include "typesr.hpp"
#include "reltoaltrep.hpp"

using namespace duckdb;

R_altrep_class_t RelToAltrep::rownames_class;
R_altrep_class_t RelToAltrep::logical_class;
R_altrep_class_t RelToAltrep::int_class;
R_altrep_class_t RelToAltrep::real_class;
R_altrep_class_t RelToAltrep::string_class;

void RelToAltrep::Initialize(DllInfo *dll) {
	// this is a string so setting row names will not lead to materialization
	rownames_class = R_make_altinteger_class("reltoaltrep_rownames_class", "duckdb", dll);
	logical_class = R_make_altlogical_class("reltoaltrep_logical_class", "duckdb", dll);
	int_class = R_make_altinteger_class("reltoaltrep_int_class", "duckdb", dll);
	real_class = R_make_altreal_class("reltoaltrep_real_class", "duckdb", dll);
	string_class = R_make_altstring_class("reltoaltrep_string_class", "duckdb", dll);

	R_set_altrep_Inspect_method(rownames_class, RownamesInspect);
	R_set_altrep_Inspect_method(logical_class, RelInspect);
	R_set_altrep_Inspect_method(int_class, RelInspect);
	R_set_altrep_Inspect_method(real_class, RelInspect);
	R_set_altrep_Inspect_method(string_class, RelInspect);

	R_set_altrep_Length_method(rownames_class, RownamesLength);
	R_set_altrep_Length_method(logical_class, VectorLength);
	R_set_altrep_Length_method(int_class, VectorLength);
	R_set_altrep_Length_method(real_class, VectorLength);
	R_set_altrep_Length_method(string_class, VectorLength);

	R_set_altvec_Dataptr_method(rownames_class, RownamesDataptr);
	R_set_altvec_Dataptr_method(logical_class, VectorDataptr);
	R_set_altvec_Dataptr_method(int_class, VectorDataptr);
	R_set_altvec_Dataptr_method(real_class, VectorDataptr);
	R_set_altvec_Dataptr_method(string_class, VectorDataptr);

	R_set_altstring_Elt_method(string_class, VectorStringElt);
}

template <class T>
static T *GetFromExternalPtr(SEXP x) {
	if (!x) {
		cpp11::stop("need a SEXP pointer");
	}
	auto wrapper = (T *)R_ExternalPtrAddr(R_altrep_data1(x));
	if (!wrapper) {
		cpp11::stop("This looks like it has been freed");
	}
	return wrapper;
}

struct AltrepRelationWrapper {

	static AltrepRelationWrapper *Get(SEXP x) {
		return GetFromExternalPtr<AltrepRelationWrapper>(x);
	}

	AltrepRelationWrapper(shared_ptr<Relation> rel_p) : rel(rel_p) {
	}

	MaterializedQueryResult *GetQueryResult() {
		if (!res) {
			auto option = Rf_GetOption(RStrings::get().materialize_sym, R_BaseEnv);
			if (option != R_NilValue && !Rf_isNull(option) && LOGICAL_ELT(option, 0) == true) {
				Rprintf("materializing:\n%s\n", rel->ToString().c_str());
			}
			res = rel->Execute();
			if (res->HasError()) {
				cpp11::stop(res->GetError().c_str());
			}
			D_ASSERT(res->type == QueryResultType::MATERIALIZED_RESULT);
		}
		D_ASSERT(res);
		return (MaterializedQueryResult *)res.get();
	}

	shared_ptr<Relation> rel;
	unique_ptr<QueryResult> res;
};

struct AltrepRownamesWrapper {

	AltrepRownamesWrapper(shared_ptr<AltrepRelationWrapper> rel_p) : rel(rel_p) {
		rowlen_data[0] = NA_INTEGER;
	}

	static AltrepRownamesWrapper *Get(SEXP x) {
		return GetFromExternalPtr<AltrepRownamesWrapper>(x);
	}

	int32_t rowlen_data[2];
	shared_ptr<AltrepRelationWrapper> rel;
};

struct AltrepVectorWrapper {
	AltrepVectorWrapper(shared_ptr<AltrepRelationWrapper> rel_p, idx_t column_index_p)
	    : rel(rel_p), column_index(column_index_p) {
	}

	static AltrepVectorWrapper *Get(SEXP x) {
		return GetFromExternalPtr<AltrepVectorWrapper>(x);
	}

	void *Dataptr() {
		if (transformed_vector.data() == R_NilValue) {
			auto res = rel->GetQueryResult();
			RProtector r_varvalue;
			transformed_vector = duckdb_r_allocate(res->types[column_index], r_varvalue, res->RowCount());
			idx_t dest_offset = 0;
			for (auto &chunk : res->Collection().Chunks()) {
				SEXP dest = transformed_vector.data();
				duckdb_r_transform(chunk.data[column_index], dest, dest_offset, chunk.size(), false);
				dest_offset += chunk.size();
			}
		}
		return DATAPTR(transformed_vector);
	}

	SEXP Vector() {
		Dataptr();
		return transformed_vector;
	}

	shared_ptr<AltrepRelationWrapper> rel;
	idx_t column_index;
	cpp11::sexp transformed_vector;
};

Rboolean RelToAltrep::RownamesInspect(SEXP x, int pre, int deep, int pvec,
                                      void (*inspect_subtree)(SEXP, int, int, int)) {
	AltrepRownamesWrapper::Get(x); // make sure this is alive
	Rprintf("DUCKDB_ALTREP_REL_ROWNAMES\n");
	return TRUE;
}

Rboolean RelToAltrep::RelInspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int)) {
	auto wrapper = AltrepVectorWrapper::Get(x); // make sure this is alive
	auto &col = wrapper->rel->rel->Columns()[wrapper->column_index];
	Rprintf("DUCKDB_ALTREP_REL_VECTOR %s (%s)\n", col.Name().c_str(), col.Type().ToString().c_str());
	return TRUE;
}

// this allows us to set row names on a data frame with an int argument without calling INTPTR on it
static void install_new_attrib(SEXP vec, SEXP name, SEXP val) {
	SEXP attrib_vec = ATTRIB(vec);
	SEXP attrib_cell = Rf_cons(val, R_NilValue);
	SET_TAG(attrib_cell, name);
	SETCDR(attrib_vec, attrib_cell);
}

R_xlen_t RelToAltrep::RownamesLength(SEXP x) {
	// row.names vector has length 2 in the "compact" case which we're using
	// see https://stat.ethz.ch/R-manual/R-devel/library/base/html/row.names.html
	return 2;
}

void *RelToAltrep::RownamesDataptr(SEXP x, Rboolean writeable) {
	auto rownames_wrapper = AltrepRownamesWrapper::Get(x);
	auto row_count = rownames_wrapper->rel->GetQueryResult()->RowCount();
	if (row_count > (idx_t)NumericLimits<int32_t>::Maximum()) {
		cpp11::stop("Integer overflow for row.names attribute");
	}
	rownames_wrapper->rowlen_data[1] = -row_count;
	return rownames_wrapper->rowlen_data;
}

R_xlen_t RelToAltrep::VectorLength(SEXP x) {
	return AltrepVectorWrapper::Get(x)->rel->GetQueryResult()->RowCount();
}

void *RelToAltrep::VectorDataptr(SEXP x, Rboolean writeable) {
	return AltrepVectorWrapper::Get(x)->Dataptr();
}

SEXP RelToAltrep::VectorStringElt(SEXP x, R_xlen_t i) {
	return STRING_ELT(AltrepVectorWrapper::Get(x)->Vector(), i);
}

static R_altrep_class_t LogicalTypeToAltrepType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return RelToAltrep::logical_class;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::ENUM:
		return RelToAltrep::int_class;
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
	case LogicalTypeId::INTERVAL:
		return RelToAltrep::real_class;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::UUID:
		return RelToAltrep::string_class;
	default:
		cpp11::stop("rel_to_altrep: Unknown column type for altrep: %s", type.ToString().c_str());
	}
}

[[cpp11::register]] SEXP rapi_rel_to_altrep(duckdb::rel_extptr_t rel) {
	D_ASSERT(rel && rel->rel);
	auto drel = rel->rel;
	auto ncols = drel->Columns().size();

	cpp11::writable::list data_frame(NEW_LIST(ncols));
	data_frame.attr(R_ClassSymbol) = RStrings::get().dataframe_str;
	auto relation_wrapper = make_shared<AltrepRelationWrapper>(drel);
	RProtector r_protector;

	cpp11::external_pointer<AltrepRownamesWrapper> ptr(new AltrepRownamesWrapper(relation_wrapper));
	auto row_names_sexp = R_new_altrep(RelToAltrep::rownames_class, ptr, rel);
	install_new_attrib(data_frame, R_RowNamesSymbol, row_names_sexp);
	vector<string> names;
	for (auto &col : drel->Columns()) {
		names.push_back(col.Name());
	}

	SET_NAMES(data_frame, StringsToSexp(names));
	for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
		auto &column_type = drel->Columns()[col_idx].Type();
		cpp11::external_pointer<AltrepVectorWrapper> ptr(new AltrepVectorWrapper(relation_wrapper, col_idx));
		auto vector_sexp = r_protector.Protect(R_new_altrep(LogicalTypeToAltrepType(column_type), ptr, R_NilValue));
		duckdb_r_decorate(column_type, vector_sexp, false);
		SET_VECTOR_ELT(data_frame, col_idx, vector_sexp);
	}
	return data_frame;
}

[[cpp11::register]] SEXP rapi_rel_from_altrep_df(SEXP df) {
	if (!Rf_inherits(df, "data.frame")) {
		cpp11::stop("Not a data.frame");
	}

	SEXP row_names = R_NilValue;
	for (SEXP attrib = ATTRIB(df); attrib != R_NilValue; attrib = CDR(attrib)) {
		if (TAG(attrib) == R_RowNamesSymbol) {
			row_names = CAR(attrib);
		}
	}

	if (row_names == R_NilValue || !ALTREP(row_names)) {
		cpp11::stop("Not a 'special' data.frame");
	}
	auto res = R_altrep_data2(row_names);
	if (res == R_NilValue) {
		cpp11::stop("NULL in data2?");
	}
	return res;
}

[[cpp11::register]] bool rapi_df_is_materialized(SEXP df) {
	D_ASSERT(df);
	auto first_col = VECTOR_ELT(df, 0);
	if (!ALTREP(first_col)) {
		cpp11::stop("Not a lazy data frame");
	}
	auto altrep_data = R_altrep_data1(first_col);
	if (!altrep_data) {
		cpp11::stop("Not a lazy data frame");
	}
	auto wrapper = (AltrepVectorWrapper *)R_ExternalPtrAddr(altrep_data);
	if (!wrapper) {
		cpp11::stop("Invalid lazy data frame");
	}
	return wrapper->rel->res.get() != nullptr;
}

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void RelToAltrep_Initialize(DllInfo* dll) {
	// clang-format on
	RelToAltrep::Initialize(dll);
}
