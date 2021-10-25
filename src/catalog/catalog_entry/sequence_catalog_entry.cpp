#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/alter_sequence_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

SequenceCatalogEntry::SequenceCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateSequenceInfo *info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info->name), usage_count(info->usage_count),
      counter(info->start_value), increment(info->increment), start_value(info->start_value),
      min_value(info->min_value), max_value(info->max_value), cycle(info->cycle) {
	this->temporary = info->temporary;
}

void SequenceCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	// serializer.Write<int64_t>(counter);
	serializer.Write<uint64_t>(usage_count);
	serializer.Write<int64_t>(increment);
	serializer.Write<int64_t>(min_value);
	serializer.Write<int64_t>(max_value);
	serializer.Write<int64_t>(counter);
	serializer.Write<bool>(cycle);
}

unique_ptr<CreateSequenceInfo> SequenceCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSequenceInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	// info->counter = source.Read<int64_t>();
	info->usage_count = source.Read<uint64_t>();
	info->increment = source.Read<int64_t>();
	info->min_value = source.Read<int64_t>();
	info->max_value = source.Read<int64_t>();
	info->start_value = source.Read<int64_t>();
	info->cycle = source.Read<bool>();
	return info;
}

string SequenceCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SEQUENCE ";
	ss << name;
	ss << " INCREMENT BY " << increment;
	ss << " MINVALUE " << min_value;
	ss << " MAXVALUE " << max_value;
	ss << " START " << counter;
	ss << " " << (cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}
unique_ptr<CatalogEntry> SequenceCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	D_ASSERT(!internal);
	if (info->type != AlterType::ALTER_SEQUENCE) {
		throw CatalogException("Can only modify sequence with ALTER SEQUENCE statement");
	}
	auto sequence_info = (AlterSequenceInfo *)info;
	switch (sequence_info->alter_sequence_type) {
        case AlterSequenceType::CHANGE_OWNERSHIP: {
            return ChangeOwnership(context, (ChangeOwnershipInfo *) info);
        }
        default: {
            throw InternalException("Unrecognized alter sequence type!");
        }
    }
}

unique_ptr<CatalogEntry> SequenceCatalogEntry::ChangeOwnership(ClientContext &context, ChangeOwnershipInfo *info) {
    auto table_catalog_entry = catalog->GetEntry(context, CatalogType::TABLE_ENTRY, info->table_schema,
                                                info->table_name);
    auto dm = catalog->dependency_manager.get();

    // if the sequence is already owned by other table, throw error
    auto sequence_owned_by = dm->GetOwnedBy(context, this);
    for(auto entry : sequence_owned_by){
        if(entry->type == CatalogType::TABLE_ENTRY){
            if(entry == table_catalog_entry){
                // If this sequence is already owned by this table, then don't alter anything
                return nullptr;
            }
            else {
                throw CatalogException("This sequence is already owned by table: " + entry->name);
            }
        }
    }

    // if the sequence is used by other tables, then throw an error
    auto sequence_owns = dm->GetOwns(context, this);
    vector<string> used_by;
    bool used_by_other_tables = false;
    for(auto entry : sequence_owns){
        if(entry.entry->type == CatalogType::TABLE_ENTRY){
            if(entry.entry != table_catalog_entry){
                used_by_other_tables = true;
                used_by.push_back(entry.entry->name);
            }
        }
    }
    if(used_by_other_tables) {
        std::stringstream ss;
        ss << "This sequence is used by multiple tables( ";
        for(auto &t : used_by){
            ss << t << " ";
        }
        ss << ") and cannot be owned";
        throw CatalogException(ss.str());
    }

    auto create_sequence_info = make_unique<CreateSequenceInfo>();
    create_sequence_info->name = name;
    create_sequence_info->schema = schema->name;
    create_sequence_info->usage_count = usage_count;
    create_sequence_info->increment = increment;
    create_sequence_info->min_value = min_value;
    create_sequence_info->max_value = max_value;
    create_sequence_info->start_value = start_value;
    create_sequence_info->cycle = cycle;
    auto new_sequence_entry = make_unique<SequenceCatalogEntry>(catalog, schema,  create_sequence_info.get());
    new_sequence_entry->counter = counter;

    // Assert the new sequence has entries added to the dependency manager
    D_ASSERT(dm->GetOwns(context, new_sequence_entry.get()).empty());
    D_ASSERT(dm->GetOwnedBy(context, new_sequence_entry.get()).empty());

    // Move dependencies from old sequence to new sequence
    for(auto entry : sequence_owned_by) {
        // We expect that the sequence only depends on the schema
        D_ASSERT(entry->type == CatalogType::SCHEMA_ENTRY);
        dm->RemoveDependencyFromObject(context, entry, this);
        dm->AddDependencyToObject(context, entry, new_sequence_entry.get());
    }
    for(auto dependency : dm->GetOwns(context, this)) {
        // We expect that the sequence only owns tables
        D_ASSERT(dependency.entry->type == CatalogType::TABLE_ENTRY);
        dm->RemoveDependencyFromObject(context, this, dependency.entry);
    }

    // Add the dependency to the table
    dm->AddDependencyToObject(context, table_catalog_entry, new_sequence_entry.get());

    // Assert old sequence has no more dependencies
    D_ASSERT(dm->GetOwns(context, this).empty());
    D_ASSERT(dm->GetOwnedBy(context, this).empty());

    return new_sequence_entry;
}
} // namespace duckdb
