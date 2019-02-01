#include "parser/statement/create_table_statement.hpp"
#include "parser/transformer.hpp"

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
	info.temporary = stmt->relation->relpersistence == 't'; // yeah totally a char

	assert(stmt->tableElts);

	for (auto c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		auto node = reinterpret_cast<Node *>(c->data.ptr_value);
		switch (node->type) {
		case T_ColumnDef: {
			auto cdef = (ColumnDef *)c->data.ptr_value;
			char *name = (reinterpret_cast<value *>(cdef->typeName->names->tail->data.ptr_value)->val.str);
			auto centry = ColumnDefinition(cdef->colname, TransformStringToTypeId(name));

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, info.columns.size());
					if (constraint) {
						info.constraints.push_back(move(constraint));
					}
				}
			}
			info.columns.push_back(centry);
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
