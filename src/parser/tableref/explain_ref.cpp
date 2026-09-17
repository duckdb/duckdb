#include "duckdb/parser/tableref/explain_ref.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

ExplainRef::ExplainRef() : TableRef(TableReferenceType::EXPLAIN) {
}

string ExplainRef::ToString() const {
	auto select = make_uniq<SelectStatement>();
	select->node = query->Copy();
	ExplainStatement explain(std::move(select), ExplainType::EXPLAIN_STANDARD, ProfilerPrintFormat(format));
	return explain.ToString();
}

bool ExplainRef::Equals(const TableRef &other) const {
	if (!TableRef::Equals(other)) {
		return false;
	}
	auto &ref = other.Cast<ExplainRef>();
	return format == ref.format && query->Equals(ref.query.get());
}

unique_ptr<TableRef> ExplainRef::Copy() {
	auto result = make_uniq<ExplainRef>();
	result->query = query->Copy();
	result->format = format;
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
