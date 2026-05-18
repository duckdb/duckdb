#include "duckdb/execution/operator/persistent/physical_export.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

void ReorderTableEntries(catalog_entry_vector_t &tables);

using duckdb::stringstream;

PhysicalExport::PhysicalExport(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                               unique_ptr<CopyInfo> info, idx_t estimated_cardinality,
                               unique_ptr<BoundExportData> exported_tables)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXPORT, std::move(types), estimated_cardinality),
      function(std::move(function)), info(std::move(info)), exported_tables(std::move(exported_tables)) {
}

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
		ss << ";\n";
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

static void WriteCopyStatement(FileSystem &fs, stringstream &ss, CopyInfo &info, ExportedTableData &exported_table) {
	ss << "COPY ";

	//! NOTE: The catalog is explicitly not set here
	if (exported_table.schema_name != DEFAULT_SCHEMA && !exported_table.schema_name.empty()) {
		ss << SQLIdentifier(exported_table.schema_name) << ".";
	}

	auto file_path = StringUtil::Replace(exported_table.file_path, "\\", "/");
	ss << StringUtil::Format("%s FROM %s (", SQLIdentifier(exported_table.table_name), SQLString(file_path));
	// write the copy options
	ss << "FORMAT '" << info.format << "'";
	if (info.format == "csv") {
		info.options.erase("force_not_null");
		for (auto &not_null_column : exported_table.not_null_columns) {
			info.options["force_not_null"].push_back(not_null_column);
		}
	}
	for (auto &copy_option : info.options) {
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
		auto &catalog = schema.ParentCatalog();
		if (catalog.IsSystemCatalog() || catalog.IsTemporaryCatalog()) {
			continue;
		}
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

// Reorder views so that a view is written after every other view it references.
// View-to-view dependencies are not tracked by the DependencyManager unless the
// enable_view_dependencies setting is on (default off). Without this reorder,
// EXPORT DATABASE writes views in catalog scan order, which can produce a schema.sql
// that fails on IMPORT when a referring view appears before the view it depends on.
static void ReorderViewEntries(catalog_entry_vector_t &views) {
	if (views.size() <= 1) {
		return;
	}

	// (schema, name) -> index in views. Compared case-insensitively to match SQL identifier rules.
	// '\x1F' (ASCII Unit Separator) joins the two parts so identifiers containing '.' don't collide.
	auto make_key = [](const string &schema, const string &name) {
		return schema + '\x1F' + name;
	};
	case_insensitive_map_t<idx_t> view_index;
	for (idx_t i = 0; i < views.size(); ++i) {
		auto &entry = views[i].get();
		view_index[make_key(entry.ParentSchema().name, entry.name)] = i;
	}

	// deps[i] = indices of views that view i references (and must be written before view i).
	vector<vector<idx_t>> deps(views.size());
	for (idx_t i = 0; i < views.size(); ++i) {
		auto &entry = views[i].get();
		if (entry.type != CatalogType::VIEW_ENTRY) {
			continue;
		}
		auto &view_entry = entry.Cast<ViewCatalogEntry>();
		if (!view_entry.query || !view_entry.query->node) {
			continue;
		}
		const auto &default_schema = view_entry.ParentSchema().name;
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *view_entry.query->node, [](unique_ptr<ParsedExpression> &) {},
		    [&](TableRef &ref) {
			    if (ref.type != TableReferenceType::BASE_TABLE) {
				    return;
			    }
			    auto &base_ref = ref.Cast<BaseTableRef>();
			    const auto &ref_schema = base_ref.schema_name.empty() ? default_schema : base_ref.schema_name;
			    auto it = view_index.find(make_key(ref_schema, base_ref.table_name));
			    if (it == view_index.end() || it->second == i) {
				    return;
			    }
			    deps[i].push_back(it->second);
		    });
	}

	// Iterative DFS topo sort. On a cycle (shouldn't happen for views), break it
	// by treating the back-edge as absent so we still emit every view exactly once.
	catalog_entry_vector_t reordered;
	reordered.reserve(views.size());
	enum class VisitState : uint8_t { UNVISITED, ON_STACK, DONE };
	vector<VisitState> state(views.size(), VisitState::UNVISITED);
	vector<pair<idx_t, idx_t>> stack; // (view index, next dep cursor)
	for (idx_t root = 0; root < views.size(); ++root) {
		if (state[root] != VisitState::UNVISITED) {
			continue;
		}
		stack.emplace_back(root, 0);
		state[root] = VisitState::ON_STACK;
		while (!stack.empty()) {
			auto &frame = stack.back();
			auto i = frame.first;
			if (frame.second < deps[i].size()) {
				idx_t next = deps[i][frame.second++];
				if (state[next] == VisitState::UNVISITED) {
					stack.emplace_back(next, 0);
					state[next] = VisitState::ON_STACK;
				}
				// If ON_STACK, we found a cycle; skip the back-edge.
				// If DONE, already emitted.
				continue;
			}
			state[i] = VisitState::DONE;
			reordered.push_back(views[i]);
			stack.pop_back();
		}
	}

	views = std::move(reordered);
}

catalog_entry_vector_t PhysicalExport::GetNaiveExportOrder(ClientContext &context, Catalog &catalog) {
	// gather all catalog types to export
	ExportEntries entries;
	auto schema_list = catalog.GetSchemas(context);
	PhysicalExport::ExtractEntries(context, schema_list, entries);

	ReorderTableEntries(entries.tables);
	ReorderViewEntries(entries.views);

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

SourceResultType PhysicalExport::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<ExportSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);

	auto &catalog = Catalog::GetCatalog(ccontext, info->catalog);

	catalog_entry_vector_t catalog_entries;
	catalog_entries = GetNaiveExportOrder(context.client, catalog);
	auto dependency_manager = catalog.GetDependencyManager();
	if (dependency_manager) {
		dependency_manager->ReorderEntries(catalog_entries, ccontext);
	}

	// write the schema.sql file
	stringstream ss;
	WriteCatalogEntries(ss, catalog_entries);
	WriteStringStreamToFile(fs, ss, fs.JoinPath(info->file_path, "schema.sql"));

	// write the load.sql file
	// for every table, we write COPY INTO statement with the specified options
	stringstream load_ss;
	for (idx_t i = 0; i < exported_tables->data.size(); i++) {
		auto exported_table_info = exported_tables->data[i].table_data;
		WriteCopyStatement(fs, load_ss, *info, exported_table_info);
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
