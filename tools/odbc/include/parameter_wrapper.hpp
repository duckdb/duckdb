#ifndef PARAMETER_WRAPPER_HPP
#define PARAMETER_WRAPPER_HPP

#include "duckdb.hpp"
#include "duckdb/common/windows.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <vector>

namespace duckdb {

struct ParameterDescriptor;

class ParameterWrapper {
public:
	std::vector<ParameterDescriptor> param_descriptors;
	SQLULEN paramset_size;
	SQLULEN *param_processed_ptr;
	SQLUSMALLINT *param_status_ptr;

private:
	// a pool of allocated parameters during SQLPutData for character data
	vector<unique_ptr<char[]>> pool_allocated_ptr;
	vector<std::string> *error_messages;
	idx_t paramset_idx;
	idx_t cur_paramset_idx;
	idx_t cur_param_idx;

public:
	explicit ParameterWrapper(vector<std::string> *msgs)
	    : paramset_size(1), param_processed_ptr(nullptr), param_status_ptr(nullptr), error_messages(msgs),
	      paramset_idx(0), cur_paramset_idx(0), cur_param_idx(0) {
	}
	~ParameterWrapper();
	void Clear();
	void Reset();
	void SetParamProcessedPtr(SQLPOINTER value_ptr);
	SQLRETURN GetNextParam(SQLPOINTER *param);
	SQLRETURN GetValues(std::vector<Value> &values);
	SQLRETURN PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
	bool HasParamSetToProcess();

private:
	SQLRETURN PutCharData(ParameterDescriptor &param_desc, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillParamCharDataBuffer(ParameterDescriptor &param_desc, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillCurParamCharSet(ParameterDescriptor &param_desc, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
};

//! https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptor-handles?view=sql-server-ver15
struct AppParameterDescriptor {
public:
	SQLSMALLINT value_type;
	SQLPOINTER param_value_ptr;
	SQLLEN buffer_len;
	SQLLEN *str_len_or_ind_ptr;
};

struct ImplParameterDescriptor {
public:
	ImplParameterDescriptor() : allocated(false) {
	}

	SQLSMALLINT param_type;
	SQLULEN col_size;
	SQLSMALLINT dec_digits;
	// flag to check whether a parameter buffer was allocated
	bool allocated;
};

struct ParameterDescriptor {
public:
	AppParameterDescriptor apd;
	ImplParameterDescriptor ipd;
	SQLUSMALLINT idx;
	SQLSMALLINT io_type;
	std::vector<Value> values;

private:
	SQLRETURN ValidateNumeric(int precision, int scale);
	void SetValue(Value &value, idx_t val_idx);

public:
	SQLRETURN SetValue(idx_t val_idx);
};

} // namespace duckdb

#endif // PARAMETER_WRAPPER_HPP