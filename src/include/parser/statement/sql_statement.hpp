
#pragma once

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {
class SelectStatement;

class SQLStatement : public Printable {
  public:
	SQLStatement(StatementType type) : stmt_type(type){};
	virtual ~SQLStatement() {}

	StatementType GetType() const { return stmt_type; }

  private:
	StatementType stmt_type;
};
}
