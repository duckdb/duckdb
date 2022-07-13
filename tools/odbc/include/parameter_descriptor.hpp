#ifndef PARAMETER_DESCRIPTOR_HPP
#define PARAMETER_DESCRIPTOR_HPP

#include "duckdb_odbc.hpp"

namespace duckdb {
class ParameterDescriptor {
public:
	explicit ParameterDescriptor(OdbcHandleStmt *stmt_ptr);
	~ParameterDescriptor() {
	}
	OdbcHandleDesc *GetIPD();
	OdbcHandleDesc *GetAPD();
	void Clear();
	void SetCurrentAPD(OdbcHandleDesc *new_apd);
	void Reset();
	void ResetParams(SQLSMALLINT count);
	void ResetCurrentAPD();

	SQLRETURN GetParamValues(std::vector<Value> &values);
	void SetParamProcessedPtr(SQLULEN *value_ptr);
	SQLULEN *GetParamProcessedPtr();
	void SetArrayStatusPtr(SQLUSMALLINT *value_ptr);
	SQLUSMALLINT *SetArrayStatusPtr();
	void SetBindOffesetPtr(SQLLEN *value_ptr);
	SQLLEN *GetBindOffesetPtr();

	SQLRETURN GetNextParam(SQLPOINTER *param);
	SQLRETURN PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);
	bool HasParamSetToProcess();

public:
	// implicitly allocated descriptors
	unique_ptr<OdbcHandleDesc> apd;
	unique_ptr<OdbcHandleDesc> ipd;

private:
	SQLRETURN SetValue(idx_t rec_idx);
	void SetValue(Value &value, idx_t val_idx);
	Value GetNextValue(idx_t val_idx);
	SQLRETURN SetParamIndex();
	SQLRETURN PutCharData(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                      SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillParamCharDataBuffer(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                                  SQLLEN str_len_or_ind_ptr);
	SQLRETURN FillCurParamCharSet(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
	                              SQLLEN str_len_or_ind_ptr);
	SQLRETURN ValidateNumeric(int precision, int scale);

	SQLPOINTER GetSQLDescDataPtr(DescRecord &apd_record);
	void SetSQLDescDataPtr(DescRecord &apd_record, SQLPOINTER data_ptr);

	SQLLEN *GetSQLDescIndicatorPtr(DescRecord &apd_record, idx_t set_idx = 0);
	void SetSQLDescIndicatorPtr(DescRecord &apd_record, SQLLEN *ind_ptr);
	void SetSQLDescIndicatorPtr(DescRecord &apd_record, SQLLEN value);

	SQLLEN *GetSQLDescOctetLengthPtr(DescRecord &apd_record, idx_t set_idx = 0);
	void SetSQLDescOctetLengthPtr(DescRecord &apd_record, SQLLEN *ind_ptr);

private:
	OdbcHandleStmt *stmt;
	// pointer to the current APD descriptor
	OdbcHandleDesc *cur_apd;

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
