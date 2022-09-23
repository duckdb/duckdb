#ifndef ROW_DESCRIPTOR_HPP
#define ROW_DESCRIPTOR_HPP

#include "duckdb_odbc.hpp"

namespace duckdb {
class RowDescriptor {
public:
	explicit RowDescriptor(OdbcHandleStmt *stmt_ptr);
	~RowDescriptor() {
	}
	OdbcHandleDesc *GetIRD();
	OdbcHandleDesc *GetARD();
	void Clear();
	void SetCurrentARD(OdbcHandleDesc *new_ard);
	void Reset();
	void ResetCurrentARD();

public:
	// implicitly allocated descriptors
	unique_ptr<OdbcHandleDesc> ard;
	unique_ptr<OdbcHandleDesc> ird;

private:
	OdbcHandleStmt *stmt;
	// pointer to the current ARD descriptor
	OdbcHandleDesc *cur_ard;
};
} // namespace duckdb

#endif
