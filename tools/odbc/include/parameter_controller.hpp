#ifndef PARAMETER_CONTROLLER_HPP
#define PARAMETER_CONTROLLER_HPP

#include "duckdb_odbc.hpp"

namespace duckdb {
class ParameterController {
public:
	ParameterController(OdbcHandleStmt *stmt_ptr);
	~ParameterController() {
	}
    OdbcHandleDesc *GetIPD();
    OdbcHandleDesc *GetAPD();
	void Clear();
	void Reset();
	void ResetParams(SQLSMALLINT count);

	SQLRETURN GetParamValues(std::vector<Value> &values);
	void SetParamProcessedPtr(SQLPOINTER value_ptr);
	SQLRETURN GetNextParam(SQLPOINTER *param);
	SQLRETURN PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
	bool HasParamSetToProcess();

public:
	unique_ptr<OdbcHandleDesc> apd;
	unique_ptr<OdbcHandleDesc> ipd;

private:
	SQLRETURN SetValue(idx_t rec_idx);
	void SetValue(Value &value, idx_t val_idx);
	Value GetNextValue();
	SQLRETURN SetParamIndex();
	SQLRETURN PutCharData(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                      SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillParamCharDataBuffer(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                                  SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillCurParamCharSet(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                              SQLLEN str_len_or_ind_ptr);
	SQLRETURN ValidateNumeric(int precision, int scale);

private:
	OdbcHandleStmt *stmt;

	//! a pool of allocated parameters during SQLPutData for character data
	vector<unique_ptr<char[]>> pool_allocated_ptr;
	//! Index of the
	idx_t paramset_idx;
	idx_t cur_paramset_idx;
	idx_t cur_param_idx;
	// duckdb Values for the parameters
	std::vector<Value> values;
};
} // namespace duckdb
#endif
