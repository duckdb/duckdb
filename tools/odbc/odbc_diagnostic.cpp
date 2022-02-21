#include "odbc_diagnostic.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcDiagnostic;

DiagRecord &OdbcDiagnostic::GetDiagRecord(SQLINTEGER rec_idx) {
    D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
    return diag_records[rec_idx];
}