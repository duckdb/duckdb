#include "row_descriptor.hpp"

using duckdb::OdbcHandleDesc;
using duckdb::RowDescriptor;

RowDescriptor::RowDescriptor(OdbcHandleStmt *stmt_ptr) : stmt(stmt_ptr) {
	ard = make_uniq<OdbcHandleDesc>(stmt->dbc);
	ird = make_uniq<OdbcHandleDesc>(stmt->dbc);

	cur_ard = ard.get();
}

OdbcHandleDesc *RowDescriptor::GetIRD() {
	return ird.get();
}

OdbcHandleDesc *RowDescriptor::GetARD() {
	return cur_ard;
}

void RowDescriptor::Clear() {
	ird->records.clear();
	cur_ard->records.clear();
	Reset();
}

void RowDescriptor::SetCurrentARD(OdbcHandleDesc *new_ard) {
	cur_ard = new_ard;
	cur_ard->header.sql_desc_alloc_type = SQL_DESC_ALLOC_USER;
}

void RowDescriptor::Reset() {
	ird->header.sql_desc_count = 0;
	cur_ard->header.sql_desc_count = 0;
}

void RowDescriptor::ResetCurrentARD() {
	cur_ard = ard.get();
}
