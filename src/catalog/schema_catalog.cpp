
#include "catalog/schema_catalog.hpp"
#include "catalog/catalog.hpp"

#include "common/exception.hpp"

#include "parser/constraints/list.hpp"

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : AbstractCatalogEntry(CatalogType::SCHEMA, catalog, name) {}

void SchemaCatalogEntry::CreateTable(
    Transaction &transaction, const string &table_name,
    const std::vector<ColumnDefinition> &columns,
    std::vector<std::unique_ptr<Constraint>> &constraints) {

	auto table = new TableCatalogEntry(catalog, this, table_name, columns);
	// resolve the constraints
	bool has_primary_key = false;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::DUMMY) {
			// have to resolve columns
			auto c = (ParsedConstraint *)constraint.get();
			vector<TypeId> types;
			vector<size_t> keys;
			if (c->index != (size_t)-1) {
				// column referenced by key is given by index
				types.push_back(columns[c->index].type);
				keys.push_back(c->index);
			} else {
				// have to resolve names
				for (auto &keyname : c->columns) {
					auto entry = table->name_map.find(keyname);
					if (entry == table->name_map.end()) {
						throw ParserException(
						    "column \"%s\" named in key does not exist",
						    keyname.c_str());
					}
					if (find(keys.begin(), keys.end(), entry->second) !=
					    keys.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname.c_str());
					}
					types.push_back(table->columns[entry->second].type);
					keys.push_back(entry->second);
				}
			}

			switch (c->ctype) {
			case ConstraintType::PRIMARY_KEY:
				if (has_primary_key) {
					throw ParserException(
					    "table \"%s\" has more than one primary key",
					    table_name.c_str());
				}
				has_primary_key = true;
				table->constraints.push_back(
				    make_unique<PrimaryKeyConstraint>(types, keys));
				break;
			case ConstraintType::UNIQUE:
				table->constraints.push_back(
				    make_unique<UniqueConstraint>(types, keys));
				break;
			default:
				throw NotImplementedException(
				    "Unimplemented type for ParsedConstraint");
			}
		} else {
			table->constraints.push_back(move(constraint));
		}
	}
	auto table_entry = unique_ptr<AbstractCatalogEntry>(table);
	if (!tables.CreateEntry(transaction, table_name, move(table_entry))) {
		throw CatalogException("Table with name %s already exists!",
		                       table_name.c_str());
	}
}

void SchemaCatalogEntry::DropTable(Transaction &transaction,
                                   const string &table_name) {
	GetTable(transaction, table_name);
	if (!tables.DropEntry(transaction, table_name)) {
		// TODO: do we care if its already marked as deleted?
	}
}

bool SchemaCatalogEntry::TableExists(Transaction &transaction,
                                     const string &table_name) {
	return tables.EntryExists(transaction, table_name);
}

TableCatalogEntry *SchemaCatalogEntry::GetTable(Transaction &transaction,
                                                const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		throw CatalogException("Table with name %s does not exist!",
		                       table_name.c_str());
	}
	return (TableCatalogEntry *)entry;
}
