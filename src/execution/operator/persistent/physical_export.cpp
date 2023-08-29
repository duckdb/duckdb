#include "duckdb/execution/operator/persistent/physical_export.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

void ReorderTableEntries(catalog_entry_vector_t &tables);

using std::stringstream;

static void WriteCatalogEntries(stringstream &ss, catalog_entry_vector_t &entries) {
	for (auto &entry : entries) {
		if (entry.get().internal) {
			continue;
		}
		ss << entry.get().ToSQL() << std::endl;
	}
	ss << std::endl;
}

static void WriteStringStreamToFile(FileSystem &fs, stringstream &ss, const string &path) {
	auto ss_string = ss.str();
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
	                          FileLockType::WRITE_LOCK);
	fs.Write(*handle, (void *)ss_string.c_str(), ss_string.size());
	handle.reset();
}

static void WriteValueAsSQL(stringstream &ss, Value &val) {
	if (val.type().IsNumeric()) {
		ss << val.ToString();
	} else {
		ss << "'" << val.ToString() << "'";
	}
}

static void WriteCopyStatement(FileSystem &fs, stringstream &ss, CopyInfo &info, ExportedTableData &exported_table,
                               CopyFunction const &function) {
	ss << "COPY ";

	if (exported_table.schema_name != DEFAULT_SCHEMA) {
		ss << KeywordHelper::WriteOptionallyQuoted(exported_table.schema_name) << ".";
	}

	ss << StringUtil::Format("%s FROM %s (", SQLIdentifier(exported_table.table_name),
	                         SQLString(exported_table.file_path));

	// write the copy options
	ss << "FORMAT '" << info.format << "'";
	if (info.format == "csv") {
		// insert default csv options, if not specified
		if (info.options.find("header") == info.options.end()) {
			info.options["header"].push_back(Value::INTEGER(0));
		}
		if (info.options.find("delimiter") == info.options.end() && info.options.find("sep") == info.options.end() &&
		    info.options.find("delim") == info.options.end()) {
			info.options["delimiter"].push_back(Value(","));
		}
		if (info.options.find("quote") == info.options.end()) {
			info.options["quote"].push_back(Value("\""));
		}
	}
	for (auto &copy_option : info.options) {
		if (copy_option.first == "force_quote") {
			continue;
		}
		ss << ", " << copy_option.first << " ";
		if (copy_option.second.size() == 1) {
			WriteValueAsSQL(ss, copy_option.second[0]);
		} else {
			// FIXME handle multiple options
			throw NotImplementedException("FIXME: serialize list of options");
		}
	}
	ss << ");" << std::endl;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ExportSourceState : public GlobalSourceState {
public:
	ExportSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalExport::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ExportSourceState>();
}

static void AddEntries(catalog_entry_vector_t &all_entries, catalog_entry_vector_t &to_add) {
	all_entries.reserve(all_entries.size() + to_add.size());
	for (auto &entry : to_add) {
		all_entries.push_back(entry);
	}
	to_add.clear();
}

catalog_entry_vector_t GetNaiveExportOrder(ClientContext &context, const string &catalog) {
	catalog_entry_vector_t schemas;
	catalog_entry_vector_t custom_types;
	catalog_entry_vector_t sequences;
	catalog_entry_vector_t tables;
	catalog_entry_vector_t views;
	catalog_entry_vector_t indexes;
	catalog_entry_vector_t macros;

	// gather all catalog types to export
	auto schema_list = Catalog::GetSchemas(context, catalog);
	for (auto &schema_p : schema_list) {
		auto &schema = schema_p.get();
		if (!schema.internal) {
			schemas.push_back(schema);
		}
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_ENTRY) {
				auto &table_entry = entry.Cast<TableCatalogEntry>();
				tables.push_back(table_entry);
			} else if (entry.type == CatalogType::VIEW_ENTRY) {
				views.push_back(entry);
			} else {
				throw InternalException("Encountered an unrecognized entry type in GetNaiveExportOrder");
			}
		});
		schema.Scan(context, CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry &entry) { sequences.push_back(entry); });
		schema.Scan(context, CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) { custom_types.push_back(entry); });
		schema.Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) { indexes.push_back(entry); });
		schema.Scan(context, CatalogType::MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
		schema.Scan(context, CatalogType::TABLE_MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
	}

	ReorderTableEntries(tables);

	// order macro's by timestamp so nested macro's are imported nicely
	sort(macros.begin(), macros.end(), [](const reference<CatalogEntry> &lhs, const reference<CatalogEntry> &rhs) {
		return lhs.get().oid < rhs.get().oid;
	});

	catalog_entry_vector_t catalog_entries;
	AddEntries(catalog_entries, schemas);
	AddEntries(catalog_entries, custom_types);
	AddEntries(catalog_entries, sequences);
	AddEntries(catalog_entries, tables);
	AddEntries(catalog_entries, views);
	AddEntries(catalog_entries, indexes);
	AddEntries(catalog_entries, macros);
	return catalog_entries;
}

SourceResultType PhysicalExport::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<ExportSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);

	auto &catalog = Catalog::GetCatalog(ccontext, info->catalog);

	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX
	catalog_entry_vector_t catalog_entries;
	if (catalog.IsDuckCatalog()) {
		auto &duck_catalog = catalog.Cast<DuckCatalog>();
		auto &dependency_manager = duck_catalog.GetDependencyManager();
		catalog_entries = dependency_manager.GetExportOrder();
	} else {
		catalog_entries = GetNaiveExportOrder(context.client, info->catalog);
	}

	// write the schema.sql file
	stringstream ss;
	WriteCatalogEntries(ss, catalog_entries);
	WriteStringStreamToFile(fs, ss, fs.JoinPath(info->file_path, "schema.sql"));

	// write the load.sql file
	// for every table, we write COPY INTO statement with the specified options
	stringstream load_ss;
	for (idx_t i = 0; i < exported_tables.data.size(); i++) {
		auto exported_table_info = exported_tables.data[i].table_data;
		WriteCopyStatement(fs, load_ss, *info, exported_table_info, function);
	}
	WriteStringStreamToFile(fs, load_ss, fs.JoinPath(info->file_path, "load.sql"));
	state.finished = true;

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalExport::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// nop
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalExport::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// EXPORT has an optional child
	// we only need to schedule child pipelines if there is a child
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
	if (children.empty()) {
		return;
	}
	PhysicalOperator::BuildPipelines(current, meta_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalExport::GetSources() const {
	return {*this};
}

} // namespace duckdb
