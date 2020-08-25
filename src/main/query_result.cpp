#include "duckdb/main/query_result.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/arrow.hpp"

namespace duckdb {
using namespace std;

QueryResult::QueryResult(QueryResultType type, StatementType statement_type)
    : type(type), statement_type(statement_type), success(true) {
}

QueryResult::QueryResult(QueryResultType type, StatementType statement_type, vector<LogicalType> types,
                         vector<string> names)
    : type(type), statement_type(statement_type), types(move(types)), names(move(names)), success(true) {
	assert(types.size() == names.size());
}

QueryResult::QueryResult(QueryResultType type, string error) : type(type), success(false), error(error) {
}

bool QueryResult::Equals(QueryResult &other) {
	// first compare the success state of the results
	if (success != other.success) {
		return false;
	}
	if (!success) {
		return error == other.error;
	}
	// compare names
	if (names != other.names) {
		return false;
	}
	// compare types
	if (types != other.types) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = Fetch();
		auto rchunk = other.Fetch();
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		if (lchunk->size() != rchunk->size()) {
			return false;
		}
		assert(lchunk->column_count() == rchunk->column_count());
		for (idx_t col = 0; col < rchunk->column_count(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
}

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : names) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : types) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}

static void release_duckdb_arrow_schema(ArrowSchema *schema) {
	// TODO
}

void QueryResult::ToArrowSchema(ArrowSchema *out_schema) {
	assert(out_schema);

	auto schema_children = (ArrowSchema *)malloc(column_count() * sizeof(ArrowSchema));

	out_schema->format = "+s"; // struct apparently
	out_schema->n_children = column_count();
	out_schema->children = (ArrowSchema **)malloc(sizeof(ArrowSchema *) * column_count());

	out_schema->release = release_duckdb_arrow_schema;
	out_schema->flags = 0;
	out_schema->metadata = nullptr;
	out_schema->name = "duckdb_query_result";
	out_schema->dictionary = nullptr;

	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		auto &child = schema_children[col_idx];
		child.name = names[col_idx].c_str();
		child.n_children = 0;
		child.children = nullptr;
		child.flags = 0;
		child.metadata = nullptr;
		child.release = release_duckdb_arrow_schema;
		child.dictionary = nullptr;

		switch (types[col_idx].id()) {
		case LogicalTypeId::BOOLEAN:
			child.format = "b";
			break;
		case LogicalTypeId::TINYINT:
			child.format = "c";
			break;
		case LogicalTypeId::SMALLINT:
			child.format = "s";
			break;
		case LogicalTypeId::INTEGER:
			child.format = "i";
			break;
		case LogicalTypeId::BIGINT:
			child.format = "l";
			break;
		case LogicalTypeId::FLOAT:
			child.format = "f";
			break;
		case LogicalTypeId::HUGEINT:
			child.format = "d:38,0";
			break;
		case LogicalTypeId::DOUBLE:
			child.format = "g";
			break;
		case LogicalTypeId::VARCHAR:
			child.format = "u";
			break;
		default:
			throw NotImplementedException("Unsupported Arrow type " + types[col_idx].ToString());
		}
		out_schema->children[col_idx] = &child;
	}
}

} // namespace duckdb
