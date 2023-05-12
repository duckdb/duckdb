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

static void WriteCatalogEntries(stringstream &ss, vector<reference<CatalogEntry>> &entries) {
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

SourceResultType PhysicalExport::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<ExportSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);

	// gather all catalog types to export
	vector<reference<CatalogEntry>> schemas;
	vector<reference<CatalogEntry>> custom_types;
	vector<reference<CatalogEntry>> sequences;
	vector<reference<CatalogEntry>> tables;
	vector<reference<CatalogEntry>> views;
	vector<reference<CatalogEntry>> indexes;
	vector<reference<CatalogEntry>> macros;

	auto schema_list = Catalog::GetSchemas(ccontext, info->catalog);
	for (auto &schema_p : schema_list) {
		auto &schema = schema_p.get();
		if (!schema.internal) {
			schemas.push_back(schema);
		}
		schema.Scan(context.client, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type != CatalogType::TABLE_ENTRY) {
				views.push_back(entry);
			}
		});
		schema.Scan(context.client, CatalogType::SEQUENCE_ENTRY,
		            [&](CatalogEntry &entry) { sequences.push_back(entry); });
		schema.Scan(context.client, CatalogType::TYPE_ENTRY,
		            [&](CatalogEntry &entry) { custom_types.push_back(entry); });
		schema.Scan(context.client, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) { indexes.push_back(entry); });
		schema.Scan(context.client, CatalogType::MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
		schema.Scan(context.client, CatalogType::TABLE_MACRO_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
	}

	// consider the order of tables because of foreign key constraint
	for (idx_t i = 0; i < exported_tables.data.size(); i++) {
		tables.push_back(exported_tables.data[i].entry);
	}

	// order macro's by timestamp so nested macro's are imported nicely
	sort(macros.begin(), macros.end(), [](const reference<CatalogEntry> &lhs, const reference<CatalogEntry> &rhs) {
		return lhs.get().oid < rhs.get().oid;
	});

	// write the schema.sql file
	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX

	stringstream ss;
	WriteCatalogEntries(ss, schemas);
	WriteCatalogEntries(ss, custom_types);
	WriteCatalogEntries(ss, sequences);
	WriteCatalogEntries(ss, tables);
	WriteCatalogEntries(ss, views);
	WriteCatalogEntries(ss, indexes);
	WriteCatalogEntries(ss, macros);

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
