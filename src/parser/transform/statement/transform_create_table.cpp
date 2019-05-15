#include "parser/statement/create_table_statement.hpp"
#include "parser/transformer.hpp"
#include "parser/constraint.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CreateTableStatement> Transformer::TransformCreateTable(Node *node) {
	auto stmt = reinterpret_cast<CreateStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateTableStatement>();
	auto &info = *result->info.get();

	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	assert(stmt->relation);

	if (stmt->relation->schemaname) {
		info.schema = stmt->relation->schemaname;
	}
	info.table = stmt->relation->relname;
	info.if_not_exists = stmt->if_not_exists;
	info.temporary = stmt->relation->relpersistence == PostgresRelPersistence::RELPERSISTENCE_TEMP;

	assert(stmt->tableElts);

	for (auto c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		auto node = reinterpret_cast<Node *>(c->data.ptr_value);
		switch (node->type) {
		case T_ColumnDef: {
			auto cdef = (ColumnDef *)c->data.ptr_value;
			SQLType target_type = TransformTypeName(cdef->typeName);
			auto centry = ColumnDefinition(cdef->colname, target_type);

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, info.columns.size());
					if (constraint) {
						info.constraints.push_back(move(constraint));
					}
				}
			}
			info.columns.push_back(move(centry));
			break;
		}
		case T_Constraint: {
			info.constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}
	return result;
}
