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

#include <algorithm>
#include <sstream>

namespace duckdb {

using std::stringstream;

void ReorderTableEntries(catalog_entry_vector_t &tables);

static void WriteCatalogEntries(stringstream &ss, catalog_entry_vector_t &entries) {
	for (auto &entry : entries) {
		if (entry.get().internal) {
			continue;
		}
		auto create_info = entry.get().GetInfo();
		try {
			// Strip the catalog from the info
			create_info->catalog.clear();
			auto to_string = create_info->ToString();
			ss << to_string;
		} catch (const NotImplementedException &) {
			ss << entry.get().ToSQL();
		}
		ss << '\n';
	}
	ss << '\n';
}

static void WriteStringStreamToFile(FileSystem &fs, stringstream &ss, const string &path) {
	auto ss_string = ss.str();
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                                    FileLockType::WRITE_LOCK);
	fs.Write(*handle, (void *)ss_string.c_str(), NumericCast<int64_t>(ss_string.size()));
	handle.reset();
}

static void WriteCopyStatement(FileSystem &fs, stringstream &ss, CopyInfo &info, ExportedTableData &exported_table,
                               CopyFunction const &function) {
	ss << "COPY ";

	//! NOTE: The catalog is explicitly not set here
	if (exported_table.schema_name != DEFAULT_SCHEMA && !exported_table.schema_name.empty()) {
		ss << KeywordHelper::WriteOptionallyQuoted(exported_table.schema_name) << ".";
	}

	auto file_path = StringUtil::Replace(exported_table.file_path, "\\", "/");
	ss << StringUtil::Format("%s FROM %s (", SQLIdentifier(exported_table.table_name), SQLString(file_path));
	// write the copy options
	ss << "FORMAT '" << info.format << "'";
	if (info.format == "csv") {
		// insert default csv options, if not specified
		if (info.options.find("header") == info.options.end()) {
			info.options["header"].push_back(Value::INTEGER(1));
		}
		if (info.options.find("delimiter") == info.options.end() && info.options.find("sep") == info.options.end() &&
		    info.options.find("delim") == info.options.end()) {
			info.options["delimiter"].push_back(Value(","));
		}
		if (info.options.find("quote") == info.options.end()) {
			info.options["quote"].push_back(Value("\""));
		}
		info.options.erase("force_not_null");
		for (auto &not_null_column : exported_table.not_null_columns) {
			info.options["force_not_null"].push_back(not_null_column);
		}
	}
	for (auto &copy_option : info.options) {
		if (copy_option.first == "force_quote") {
			continue;
		}
		if (copy_option.second.empty()) {
			// empty options are interpreted as TRUE
			copy_option.second.push_back(true);
		}
		ss << ", " << copy_option.first << " ";
		if (copy_option.second.size() == 1) {
			ss << copy_option.second[0].ToSQLString();
		} else {
			// For Lists
			ss << "(";
			for (idx_t i = 0; i < copy_option.second.size(); i++) {
				ss << copy_option.second[i].ToSQLString();
				if (i != copy_option.second.size() - 1) {
					ss << ", ";
				}
			}
			ss << ")";
		}
	}
	ss << ");" << '\n';
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

void PhysicalExport::ExtractEntries(ClientContext &context, vector<reference<SchemaCatalogEntry>> &schema_list,
                                    ExportEntries &result) {
	for (auto &schema_p : schema_list) {
		auto &schema = schema_p.get();
		if (!schema.internal) {
			result.schemas.push_back(schema);
		}
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type != CatalogType::TABLE_ENTRY) {
				result.views.push_back(entry);
			}
			if (entry.type == CatalogType::TABLE_ENTRY) {
				result.tables.push_back(entry);
			}
		});
		schema.Scan(context, CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			result.sequences.push_back(entry);
		});
		schema.Scan(context, CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			result.custom_types.push_back(entry);
		});
		schema.Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			result.indexes.push_back(entry);
		});
		schema.Scan(context, CatalogType::MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::MACRO_ENTRY) {
				result.macros.push_back(entry);
			}
		});
		schema.Scan(context, CatalogType::TABLE_MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				result.macros.push_back(entry);
			}
		});
	}
}

static void AddEntries(catalog_entry_vector_t &all_entries, catalog_entry_vector_t &to_add) {
	for (auto &entry : to_add) {
		all_entries.push_back(entry);
	}
	to_add.clear();
}

catalog_entry_vector_t PhysicalExport::GetNaiveExportOrder(ClientContext &context, Catalog &catalog) {
	// gather all catalog types to export
	ExportEntries entries;
	auto schema_list = catalog.GetSchemas(context);
	PhysicalExport::ExtractEntries(context, schema_list, entries);

	ReorderTableEntries(entries.tables);

	// order macro's by timestamp so nested macro's are imported nicely
	sort(entries.macros.begin(), entries.macros.end(),
	     [](const reference<CatalogEntry> &lhs, const reference<CatalogEntry> &rhs) {
		     return lhs.get().oid < rhs.get().oid;
	     });

	catalog_entry_vector_t catalog_entries;
	idx_t size = 0;
	size += entries.schemas.size();
	size += entries.custom_types.size();
	size += entries.sequences.size();
	size += entries.tables.size();
	size += entries.views.size();
	size += entries.indexes.size();
	size += entries.macros.size();
	catalog_entries.reserve(size);
	AddEntries(catalog_entries, entries.schemas);
	AddEntries(catalog_entries, entries.sequences);
	AddEntries(catalog_entries, entries.custom_types);
	AddEntries(catalog_entries, entries.tables);
	AddEntries(catalog_entries, entries.macros);
	AddEntries(catalog_entries, entries.views);
	AddEntries(catalog_entries, entries.indexes);
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

	// gather all catalog types to export
	ExportEntries entries;

	auto schema_list = Catalog::GetSchemas(ccontext, info->catalog);
	ExtractEntries(context.client, schema_list, entries);

	// consider the order of tables because of foreign key constraint
	entries.tables.clear();
	for (idx_t i = 0; i < exported_tables.data.size(); i++) {
		entries.tables.push_back(exported_tables.data[i].entry);
	}

	// order macro's by timestamp so nested macro's are imported nicely
	sort(entries.macros.begin(), entries.macros.end(),
	     [](const reference<CatalogEntry> &lhs, const reference<CatalogEntry> &rhs) {
		     return lhs.get().oid < rhs.get().oid;
	     });

	// write the schema.sql file
	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX

	stringstream ss;
	WriteCatalogEntries(ss, entries.schemas);
	WriteCatalogEntries(ss, entries.custom_types);
	WriteCatalogEntries(ss, entries.sequences);
	WriteCatalogEntries(ss, entries.tables);
	WriteCatalogEntries(ss, entries.views);
	WriteCatalogEntries(ss, entries.indexes);
	WriteCatalogEntries(ss, entries.macros);

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
