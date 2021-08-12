#ifndef PARAMETER_WRAPPER_HPP
#define PARAMETER_WRAPPER_HPP

#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <vector>

using duckdb::Value;
using duckdb::idx_t;

namespace duckdb {

struct ParameterDescriptor;

class ParameterWrapper {
public:
    std::vector<ParameterDescriptor> param_descriptors;
   	SQLULEN paramset_size;
    SQLULEN *param_processed_ptr;
    SQLUSMALLINT *param_status_ptr;

public:
    ParameterWrapper(): paramset_size(1) {}
    ~ParameterWrapper();
    void GetValues(std::vector<Value> &values, idx_t paramset_idx);
    void SetValue(Value &value, idx_t param_idx);
};

//! https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptor-handles?view=sql-server-ver15
struct AppParameterDescriptor {
public:
    SQLSMALLINT value_type;
    SQLPOINTER  param_value_ptr;
    SQLLEN      buffer_len;
    SQLLEN      *str_len_or_ind_ptr;
};

struct ImplParameterDescriptor {
public:
    SQLSMALLINT param_type;
    SQLULEN     col_size;
    SQLSMALLINT dec_digits;
};

struct ParameterDescriptor {
public:
    AppParameterDescriptor  app_param_desc;
    ImplParameterDescriptor impl_param_desc;
    SQLUSMALLINT            idx;  
    SQLSMALLINT             io_type;
    std::vector<Value>      values;
};

} // namespace duckdb

#endif // PARAMETER_WRAPPER_HPP