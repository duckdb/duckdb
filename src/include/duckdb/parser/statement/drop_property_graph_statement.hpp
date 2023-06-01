#pragma once

#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DropPropertyGraphStatement : public SQLStatement {
public:
	DropPropertyGraphStatement();

	unique_ptr<DropPropertyGraphInfo> info;

protected:
	DropPropertyGraphStatement(const DropPropertyGraphStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
