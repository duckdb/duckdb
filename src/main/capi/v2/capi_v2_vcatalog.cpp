#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/capi_v2/capi_v2_function_internal.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb::capiv2 {

//! What every database attached with a vcatalog shares: its name, callbacks and user data.
struct CV2VCatalogDefinition {
	string name;
	duckdb_v2_vcatalog_attach_callback_fn attach_cb = nullptr;
	duckdb_v2_vcatalog_transaction_begin_callback_fn begin_cb = nullptr;
	duckdb_v2_vcatalog_transaction_commit_callback_fn commit_cb = nullptr;
	duckdb_v2_vcatalog_transaction_rollback_callback_fn rollback_cb = nullptr;
	duckdb_v2_vcatalog_schema_find_callback_fn schema_find_cb = nullptr;
	duckdb_v2_vcatalog_schema_list_callback_fn schema_list_cb = nullptr;
	duckdb_v2_vcatalog_table_find_callback_fn table_find_cb = nullptr;
	duckdb_v2_vcatalog_table_list_callback_fn table_list_cb = nullptr;
	duckdb_v2_vcatalog_schema_create_callback_fn schema_create_cb = nullptr;
	duckdb_v2_vcatalog_schema_drop_callback_fn schema_drop_cb = nullptr;
	duckdb_v2_vcatalog_table_create_callback_fn table_create_cb = nullptr;
	duckdb_v2_vcatalog_table_drop_callback_fn table_drop_cb = nullptr;
	duckdb_v2_vcatalog_view_create_callback_fn view_create_cb = nullptr;
	duckdb_v2_vcatalog_view_drop_callback_fn view_drop_cb = nullptr;
	shared_ptr<CV2UserData> user_data;

	void *GetUserData() const {
		return user_data ? user_data->GetData() : nullptr;
	}
};

//! Invokes a callback with a live error slot, rethrowing the error it reports.
template <class FN>
static void InvokeCatalogCallback(FN &&callback) {
	CV2ErrorInfo err;
	auto err_ptr = Convert(&err);
	callback(&err_ptr);
	if (err.HasError()) {
		err.ThrowAsException();
	}
}

//! Invokes a catalog callback with the context it runs for.
template <class FN>
static void InvokeCatalogCallback(ClientContext &context, FN &&callback) {
	CV2CallbackContext callback_context(context);
	InvokeCatalogCallback([&](duckdb_v2_error_info_handle *err) { callback(Convert(&callback_context), err); });
}

static void *GetState(const unique_ptr<CV2UserData> &state) {
	return state ? state->GetData() : nullptr;
}

static void *GetState(const shared_ptr<CV2UserData> &state) {
	return state ? state->GetData() : nullptr;
}

//----------------------------------------------------------------------------------------------------------------------
// Schema
//----------------------------------------------------------------------------------------------------------------------
//! A schema of a vcatalog database. Built and owned by the implementation, which hands it to DuckDB from its find and
//! list callbacks.
class CV2VCatalogSchema final : public SchemaCatalogEntry {
public:
	CV2VCatalogSchema(Catalog &catalog, CreateSchemaInfo &info, shared_ptr<CV2UserData> state_p)
	    : SchemaCatalogEntry(catalog, info), state(std::move(state_p)) {
	}

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	//! Without a context there is no transaction to list in
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
	}

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override {
		ThrowNotSupported("Creating indexes");
	}

	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override {
		ThrowNotSupported("Creating functions");
	}

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;

	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;

	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override {
		ThrowNotSupported("Creating sequences");
	}

	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override {
		ThrowNotSupported("Creating types");
	}

	void DropEntry(ClientContext &context, DropInfo &info) override;

	void Alter(CatalogTransaction transaction, AlterInfo &info) override {
		ThrowNotSupported("Altering entries");
	}

public:
	//! Shared with the builder, and with every other schema it built
	const shared_ptr<CV2UserData> state;

private:
	[[noreturn]] void ThrowNotSupported(const char *what) const {
		throw NotImplementedException("%s is not supported in %s databases", what, catalog.GetCatalogType());
	}
};

static auto Convert(duckdb_v2_vcatalog_schema_handle schema) -> CV2VCatalogSchema * {
	return reinterpret_cast<CV2VCatalogSchema *>(schema);
}
static auto Convert(CV2VCatalogSchema *schema) -> duckdb_v2_vcatalog_schema_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_handle>(schema);
}

//----------------------------------------------------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------------------------------------------------
//! How a table is scanned: with a function, bound by a callback of the table in place of the function's own
struct CV2VCatalogTableScan {
	shared_ptr<TableFunction> function;
	duckdb_v2_vcatalog_table_scan_bind_callback_fn bind_cb = nullptr;
};

//! A table of a vcatalog schema. Built and owned by the implementation, like its schema.
class CV2VCatalogTable final : public TableCatalogEntry {
public:
	CV2VCatalogTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                 shared_ptr<CV2UserData> state_p, CV2VCatalogTableScan scan_p)
	    : TableCatalogEntry(catalog, schema, info), state(std::move(state_p)), scan(std::move(scan_p)),
	      columns(std::move(info.columns)) {
	}

	const ColumnList &GetColumns() const override {
		return columns;
	}

	const ColumnDefinition &GetColumnChecked(idx_t index) const {
		if (index >= columns.LogicalColumnCount()) {
			throw InvalidInputException("Column index out of bounds.");
		}
		return columns.GetColumn(LogicalIndex(index));
	}

	bool IsColumnNotNull(idx_t index) const {
		GetColumnChecked(index);
		for (auto &constraint : GetConstraints()) {
			if (constraint->type == ConstraintType::NOT_NULL &&
			    constraint->Cast<NotNullConstraint>().index.index == index) {
				return true;
			}
		}
		return false;
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	//! The scan function produces the columns of the table, and nothing else
	virtual_column_map_t GetVirtualColumns() const override {
		return virtual_column_map_t();
	}

	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		return TableStorageInfo();
	}

	//! Throws unless the table can be handed to DuckDB as a table of `schema`.
	void Verify(const CV2VCatalogSchema &schema) const {
		if (&ParentSchema() != &schema) {
			throw InvalidInputException("The table %s was built in a different schema.", name);
		}
	}

public:
	//! Shared with the builder, and with every other table it built
	const shared_ptr<CV2UserData> state;
	const CV2VCatalogTableScan scan;

private:
	const ColumnList columns;
};

static auto Convert(duckdb_v2_vcatalog_table_handle table) -> CV2VCatalogTable * {
	return reinterpret_cast<CV2VCatalogTable *>(table);
}
static auto Convert(CV2VCatalogTable *table) -> duckdb_v2_vcatalog_table_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_handle>(table);
}

//----------------------------------------------------------------------------------------------------------------------
// View
//----------------------------------------------------------------------------------------------------------------------
//! A view of a vcatalog schema. Built and owned by the implementation, like its schema.
class CV2VCatalogView final : public ViewCatalogEntry {
public:
	CV2VCatalogView(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, shared_ptr<CV2UserData> state_p)
	    : ViewCatalogEntry(catalog, schema, info), state(std::move(state_p)) {
	}

	//! Throws unless the view can be handed to DuckDB as a view of `schema`.
	void Verify(const CV2VCatalogSchema &schema) const {
		if (&ParentSchema() != &schema) {
			throw InvalidInputException("The view %s was built in a different schema.", name);
		}
	}

public:
	//! Shared with the builder, and with every other view it built
	const shared_ptr<CV2UserData> state;
};

static auto Convert(duckdb_v2_vcatalog_view_handle view) -> CV2VCatalogView * {
	return reinterpret_cast<CV2VCatalogView *>(view);
}
static auto Convert(CV2VCatalogView *view) -> duckdb_v2_vcatalog_view_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_view_handle>(view);
}

class CV2VCatalogTransaction;

//----------------------------------------------------------------------------------------------------------------------
// Catalog
//----------------------------------------------------------------------------------------------------------------------
class CV2VirtualCatalog final : public Catalog {
public:
	CV2VirtualCatalog(AttachedDatabase &db, CV2VCatalogDefinition definition_p, string path_p)
	    : Catalog(db), definition(std::move(definition_p)), path(std::move(path_p)) {
	}

	void Initialize(bool load_builtin) override {
	}

	string GetCatalogType() override {
		return definition.name;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		return LookupSchemaPath(transaction, schema_lookup, if_not_found,
		                        [&](const EntryLookupInfo &lookup) -> optional_ptr<CatalogEntry> {
			                        return ResolveSchema(transaction, lookup.GetEntryName());
		                        });
	}

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<CatalogEntry> ResolveTable(CatalogTransaction transaction, CV2VCatalogSchema &schema,
	                                        const string &name);
	void ListTables(ClientContext &context, CV2VCatalogSchema &schema,
	                const std::function<void(CatalogEntry &)> &callback);

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, CV2VCatalogSchema &schema,
	                                       CreateTableInfo &info);
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CV2VCatalogSchema &schema,
	                                      CreateViewInfo &info);
	void DropTableOrView(ClientContext &context, CV2VCatalogSchema &schema, DropInfo &info);

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override {
		throw NotImplementedException("CREATE TABLE AS is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override {
		throw NotImplementedException("INSERT is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("DELETE is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("UPDATE is not supported in %s databases", definition.name);
	}

	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		return DatabaseSize();
	}

	bool InMemory() override {
		return false;
	}

	string GetDBPath() override {
		return path;
	}

	void *GetCatalogState() const {
		return GetState(catalog_state);
	}

	void SetCatalogState(unique_ptr<CV2UserData> state) {
		catalog_state = std::move(state);
	}

	optional<Identifier> GetDefaultSchema() const override {
		return default_schema;
	}

	void SetDefaultSchema(Identifier name) {
		default_schema = std::move(name);
	}

public:
	const CV2VCatalogDefinition definition;
	const string path;

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	//! Drops an entry the find callbacks handed out, through the drop callback of its kind.
	void DropEntry(CatalogTransaction transaction, CatalogEntry &entry, bool cascade);
	//! Resolves a create finding `existing` under its name: throws unless the statement provides for it, and returns
	//! whether to go on creating, having dropped the existing entry, rather than to keep it.
	bool ReplaceExisting(CatalogTransaction transaction, CatalogEntry &existing, CatalogType type,
	                     OnCreateConflict on_conflict);

	optional_ptr<CatalogEntry> ResolveSchema(CatalogTransaction transaction, const string &name);
	//! The transaction of this database a catalog transaction runs in.
	CV2VCatalogTransaction &GetTransaction(CatalogTransaction transaction);

private:
	//! Destroyed after every transaction state: the attached database destroys its transaction manager first
	unique_ptr<CV2UserData> catalog_state;
	//! Only set while the database is being attached
	Identifier default_schema = Identifier(DEFAULT_SCHEMA);
};

static auto Convert(duckdb_v2_vcatalog_database_handle database) -> CV2VirtualCatalog * {
	return reinterpret_cast<CV2VirtualCatalog *>(database);
}
static auto Convert(CV2VirtualCatalog *database) -> duckdb_v2_vcatalog_database_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_database_handle>(database);
}

//----------------------------------------------------------------------------------------------------------------------
// Entry Builders
//----------------------------------------------------------------------------------------------------------------------
//! Builds schemas of a vcatalog database, which share its state.
class CV2VCatalogSchemaBuilder {
public:
	CV2VCatalogSchemaBuilder(CV2VirtualCatalog &catalog, const Identifier &name) : catalog(catalog) {
		info.SetSchema(name);
	}

	unique_ptr<CV2VCatalogSchema> Build() {
		return make_uniq<CV2VCatalogSchema>(catalog, info, state);
	}

public:
	shared_ptr<CV2UserData> state;

private:
	CV2VirtualCatalog &catalog;
	CreateSchemaInfo info;
};

static auto Convert(duckdb_v2_vcatalog_schema_builder_handle builder) -> CV2VCatalogSchemaBuilder * {
	return reinterpret_cast<CV2VCatalogSchemaBuilder *>(builder);
}
static auto Convert(CV2VCatalogSchemaBuilder *builder) -> duckdb_v2_vcatalog_schema_builder_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_builder_handle>(builder);
}

//! Builds tables of a vcatalog schema, which share its state.
class CV2VCatalogTableBuilder {
public:
	CV2VCatalogTableBuilder(CV2VCatalogSchema &schema, const Identifier &name) : schema(schema), info(schema, name) {
	}

	void AddColumn(ColumnDefinition column) {
		if (info.columns.ColumnExists(column.Name())) {
			throw InvalidInputException("The table %s already has a column named %s.", info.GetTableName(),
			                            column.Name());
		}
		info.columns.AddColumn(std::move(column));
	}

	void SetColumnNotNull(idx_t index) {
		if (index >= info.columns.LogicalColumnCount()) {
			throw InvalidInputException("Column index out of bounds.");
		}
		for (auto &constraint : info.constraints) {
			if (constraint->type == ConstraintType::NOT_NULL &&
			    constraint->Cast<NotNullConstraint>().index.index == index) {
				return;
			}
		}
		info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(index)));
	}

	unique_ptr<CV2VCatalogTable> Build() const {
		if (info.columns.empty()) {
			throw InvalidInputException("The table %s has no columns.", info.GetTableName());
		}
		// The table takes the columns of the info it is created from
		auto table_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(info.Copy());
		return make_uniq<CV2VCatalogTable>(schema.ParentCatalog(), schema, *table_info, state, scan);
	}

	void SetScanFunction(duckdb_v2_table_function_handle function) {
		auto &catalog = schema.ParentCatalog();
		scan.function = make_shared_ptr<TableFunction>(
		    CV2BuildTableScanFunction(function, catalog.GetDatabase(), Identifier(catalog.GetCatalogType() + "_scan")));
	}

public:
	shared_ptr<CV2UserData> state;
	CV2VCatalogTableScan scan;

private:
	CV2VCatalogSchema &schema;
	CreateTableInfo info;
};

static auto Convert(duckdb_v2_vcatalog_table_builder_handle builder) -> CV2VCatalogTableBuilder * {
	return reinterpret_cast<CV2VCatalogTableBuilder *>(builder);
}
static auto Convert(CV2VCatalogTableBuilder *builder) -> duckdb_v2_vcatalog_table_builder_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_builder_handle>(builder);
}

//! Builds views of a vcatalog schema, which share its state.
class CV2VCatalogViewBuilder {
public:
	CV2VCatalogViewBuilder(CV2VCatalogSchema &schema, const Identifier &name)
	    : schema(schema), info(make_uniq<CreateViewInfo>(schema, name)) {
	}

	//! Takes the name, the column aliases and the query of the view from a CREATE VIEW statement.
	CV2VCatalogViewBuilder(CV2VCatalogSchema &schema, const string &sql) : schema(schema) {
		// A builder has no connection to parse for, so syntax added by extensions is not available
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery(sql);
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT ||
		    parser.statements[0]->Cast<CreateStatement>().info->type != CatalogType::VIEW_ENTRY) {
			throw InvalidInputException("The SQL of a view must be a single CREATE VIEW statement.");
		}
		info =
		    unique_ptr_cast<CreateInfo, CreateViewInfo>(std::move(parser.statements[0]->Cast<CreateStatement>().info));
		// Whatever the statement says, the view is one of this schema
		info->SetQualifiedName(schema.GetQualifiedName(info->GetViewName()));
		info->temporary = false;
	}

	void SetQuery(const string &sql) {
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery(sql);
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			throw InvalidInputException("The query of a view must be a single SELECT statement.");
		}
		info->query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		info->sql = sql;
	}

	void AddColumnAlias(Identifier alias) {
		info->aliases.push_back(std::move(alias));
	}

	unique_ptr<CV2VCatalogView> Build() const {
		if (!info->query) {
			throw InvalidInputException("The view %s has no query.", info->GetViewName());
		}
		// The view takes the query of the info it is created from
		auto view_info = unique_ptr_cast<CreateInfo, CreateViewInfo>(info->Copy());
		return make_uniq<CV2VCatalogView>(schema.ParentCatalog(), schema, *view_info, state);
	}

public:
	shared_ptr<CV2UserData> state;

private:
	CV2VCatalogSchema &schema;
	unique_ptr<CreateViewInfo> info;
};

static auto Convert(duckdb_v2_vcatalog_view_builder_handle builder) -> CV2VCatalogViewBuilder * {
	return reinterpret_cast<CV2VCatalogViewBuilder *>(builder);
}
static auto Convert(CV2VCatalogViewBuilder *builder) -> duckdb_v2_vcatalog_view_builder_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_view_builder_handle>(builder);
}

//----------------------------------------------------------------------------------------------------------------------
// Attach Info
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogAttachInfo {
public:
	CV2VCatalogAttachInfo(CV2VirtualCatalog &catalog, const string &name, const string &path, bool read_only,
	                      const unordered_map<string, Value> &options)
	    : catalog(catalog), name(name), path(path), read_only(read_only), options(options) {
	}

	//! Finds the option named `option_name`, marking it as recognized.
	optional_ptr<const Value> FindOption(const string &option_name) {
		for (auto &entry : options) {
			if (StringUtil::CIEquals(entry.first, option_name)) {
				recognized_options.insert(entry.first);
				return &entry.second;
			}
		}
		return nullptr;
	}

	void CheckOptionsRecognized() const {
		for (auto &entry : options) {
			if (recognized_options.find(entry.first) == recognized_options.end()) {
				throw BinderException("Unrecognized option for attach \"%s\"", entry.first);
			}
		}
	}

public:
	CV2VirtualCatalog &catalog;
	const string &name;
	const string &path;
	const bool read_only;

private:
	const unordered_map<string, Value> &options;
	unordered_set<string> recognized_options;
};

static auto Convert(duckdb_v2_vcatalog_attach_info_handle info) -> CV2VCatalogAttachInfo * {
	return reinterpret_cast<CV2VCatalogAttachInfo *>(info);
}
static auto Convert(CV2VCatalogAttachInfo *info) -> duckdb_v2_vcatalog_attach_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_attach_info_handle>(info);
}

//----------------------------------------------------------------------------------------------------------------------
// Transaction
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogTransaction final : public Transaction {
public:
	CV2VCatalogTransaction(TransactionManager &manager, Context &context) : Transaction(manager, context) {
	}

	unique_ptr<CV2UserData> transaction_state;
};

//! Backs the begin info handle.
class CV2VCatalogBeginInfo {
public:
	CV2VCatalogBeginInfo(CV2VCatalogTransaction &transaction, bool read_only)
	    : transaction(transaction), read_only(read_only) {
	}

	CV2VCatalogTransaction &transaction;
	const bool read_only;
};

static auto Convert(duckdb_v2_vcatalog_transaction_begin_info_handle info) -> CV2VCatalogBeginInfo * {
	return reinterpret_cast<CV2VCatalogBeginInfo *>(info);
}
static auto Convert(CV2VCatalogBeginInfo *info) -> duckdb_v2_vcatalog_transaction_begin_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_transaction_begin_info_handle>(info);
}

//! Backs the commit and rollback info handles.
class CV2VCatalogEndInfo {
public:
	explicit CV2VCatalogEndInfo(CV2VCatalogTransaction &transaction) : transaction(transaction) {
	}

	CV2VCatalogTransaction &transaction;
};

static auto Convert(duckdb_v2_vcatalog_transaction_commit_info_handle info) -> CV2VCatalogEndInfo * {
	return reinterpret_cast<CV2VCatalogEndInfo *>(info);
}
static auto Convert(duckdb_v2_vcatalog_transaction_rollback_info_handle info) -> CV2VCatalogEndInfo * {
	return reinterpret_cast<CV2VCatalogEndInfo *>(info);
}

//----------------------------------------------------------------------------------------------------------------------
// Finding and Listing Schemas
//----------------------------------------------------------------------------------------------------------------------
static void *GetTransactionState(CV2VCatalogTransaction &transaction) {
	return GetState(transaction.transaction_state);
}

//! Throws unless `schema` was created in `catalog`.
static void CheckSchemaOf(CV2VirtualCatalog &catalog, CV2VCatalogSchema &schema) {
	if (&schema.ParentCatalog() != &catalog) {
		throw InvalidInputException("The schema %s was built in a different database.", schema.name);
	}
}

//! Backs the schema find info handle.
class CV2VCatalogSchemaFindInfo {
public:
	CV2VCatalogSchemaFindInfo(CV2VirtualCatalog &catalog, CV2VCatalogTransaction &transaction, const string &name)
	    : catalog(catalog), transaction(transaction), name(name) {
	}

	CV2VirtualCatalog &catalog;
	CV2VCatalogTransaction &transaction;
	const string &name;
	optional_ptr<CV2VCatalogSchema> result;
};

static auto Convert(duckdb_v2_vcatalog_schema_find_info_handle info) -> CV2VCatalogSchemaFindInfo * {
	return reinterpret_cast<CV2VCatalogSchemaFindInfo *>(info);
}
static auto Convert(CV2VCatalogSchemaFindInfo *info) -> duckdb_v2_vcatalog_schema_find_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_find_info_handle>(info);
}

//! Backs the schema list info handle.
class CV2VCatalogSchemaListInfo {
public:
	CV2VCatalogSchemaListInfo(CV2VirtualCatalog &catalog, CV2VCatalogTransaction &transaction)
	    : catalog(catalog), transaction(transaction) {
	}

	void AddSchema(CV2VCatalogSchema &schema) {
		if (added.insert(schema).second) {
			schemas.push_back(schema);
		}
	}

	CV2VirtualCatalog &catalog;
	CV2VCatalogTransaction &transaction;
	vector<reference<CV2VCatalogSchema>> schemas;

private:
	reference_set_t<CV2VCatalogSchema> added;
};

static auto Convert(duckdb_v2_vcatalog_schema_list_info_handle info) -> CV2VCatalogSchemaListInfo * {
	return reinterpret_cast<CV2VCatalogSchemaListInfo *>(info);
}
static auto Convert(CV2VCatalogSchemaListInfo *info) -> duckdb_v2_vcatalog_schema_list_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_list_info_handle>(info);
}

CV2VCatalogTransaction &CV2VirtualCatalog::GetTransaction(CatalogTransaction transaction) {
	if (transaction.transaction && &transaction.transaction->manager == &GetAttached().GetTransactionManager()) {
		return transaction.transaction->Cast<CV2VCatalogTransaction>();
	}
	// Only DuckDB's own catalogs are used without a connection
	return Transaction::Get(transaction.GetContext(), *this).Cast<CV2VCatalogTransaction>();
}

optional_ptr<CatalogEntry> CV2VirtualCatalog::ResolveSchema(CatalogTransaction transaction, const string &name) {
	auto find_cb = definition.schema_find_cb;
	if (!find_cb) {
		return nullptr;
	}
	CV2VCatalogSchemaFindInfo info(*this, GetTransaction(transaction), name);
	InvokeCatalogCallback(transaction.GetContext(),
	                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		                      find_cb(Convert(&info), Convert(this), context, err);
	                      });
	return info.result.get();
}

void CV2VirtualCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto list_cb = definition.schema_list_cb;
	if (!list_cb) {
		return;
	}
	CV2VCatalogSchemaListInfo info(*this, Transaction::Get(context, *this).Cast<CV2VCatalogTransaction>());
	InvokeCatalogCallback(context, [&](duckdb_v2_context_handle callback_context, duckdb_v2_error_info_handle *err) {
		list_cb(Convert(&info), Convert(this), callback_context, err);
	});
	// Visited once the callback returned, so the engine's callback cannot run inside the implementation's
	for (auto &schema : info.schemas) {
		callback(schema.get());
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Finding and Listing Tables
//----------------------------------------------------------------------------------------------------------------------
//! Backs the table find info handle.
class CV2VCatalogTableFindInfo {
public:
	CV2VCatalogTableFindInfo(CV2VCatalogTransaction &transaction, CV2VCatalogSchema &schema, const string &name)
	    : transaction(transaction), schema(schema), name(name) {
	}

	CV2VCatalogTransaction &transaction;
	CV2VCatalogSchema &schema;
	const string &name;
	//! The table or view found
	optional_ptr<CatalogEntry> result;
};

static auto Convert(duckdb_v2_vcatalog_table_find_info_handle info) -> CV2VCatalogTableFindInfo * {
	return reinterpret_cast<CV2VCatalogTableFindInfo *>(info);
}
static auto Convert(CV2VCatalogTableFindInfo *info) -> duckdb_v2_vcatalog_table_find_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_find_info_handle>(info);
}

//! Backs the table list info handle.
class CV2VCatalogTableListInfo {
public:
	CV2VCatalogTableListInfo(CV2VCatalogTransaction &transaction, CV2VCatalogSchema &schema)
	    : transaction(transaction), schema(schema) {
	}

	void AddEntry(CatalogEntry &entry) {
		if (added.insert(entry).second) {
			entries.push_back(entry);
		}
	}

	CV2VCatalogTransaction &transaction;
	CV2VCatalogSchema &schema;
	//! The tables and views listed
	vector<reference<CatalogEntry>> entries;

private:
	reference_set_t<CatalogEntry> added;
};

static auto Convert(duckdb_v2_vcatalog_table_list_info_handle info) -> CV2VCatalogTableListInfo * {
	return reinterpret_cast<CV2VCatalogTableListInfo *>(info);
}
static auto Convert(CV2VCatalogTableListInfo *info) -> duckdb_v2_vcatalog_table_list_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_list_info_handle>(info);
}

optional_ptr<CatalogEntry> CV2VirtualCatalog::ResolveTable(CatalogTransaction transaction, CV2VCatalogSchema &schema,
                                                           const string &name) {
	auto find_cb = definition.table_find_cb;
	if (!find_cb) {
		return nullptr;
	}
	CV2VCatalogTableFindInfo info(GetTransaction(transaction), schema, name);
	InvokeCatalogCallback(transaction.GetContext(),
	                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		                      find_cb(Convert(&info), Convert(this), context, err);
	                      });
	return info.result.get();
}

void CV2VirtualCatalog::ListTables(ClientContext &context, CV2VCatalogSchema &schema,
                                   const std::function<void(CatalogEntry &)> &callback) {
	auto list_cb = definition.table_list_cb;
	if (!list_cb) {
		return;
	}
	CV2VCatalogTableListInfo info(Transaction::Get(context, *this).Cast<CV2VCatalogTransaction>(), schema);
	InvokeCatalogCallback(context, [&](duckdb_v2_context_handle callback_context, duckdb_v2_error_info_handle *err) {
		list_cb(Convert(&info), Convert(this), callback_context, err);
	});
	for (auto &entry : info.entries) {
		callback(entry.get());
	}
}

//! Tables and views share a namespace, and the callbacks finding and listing them
static bool IsTableOrView(CatalogType type) {
	return type == CatalogType::TABLE_ENTRY || type == CatalogType::VIEW_ENTRY;
}

optional_ptr<CatalogEntry> CV2VCatalogSchema::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
	if (!IsTableOrView(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	return ParentCatalog().Cast<CV2VirtualCatalog>().ResolveTable(transaction, *this, lookup_info.GetEntryName());
}

void CV2VCatalogSchema::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	if (!IsTableOrView(type)) {
		return;
	}
	ParentCatalog().Cast<CV2VirtualCatalog>().ListTables(context, *this, callback);
}

//----------------------------------------------------------------------------------------------------------------------
// Creating and Dropping Entries
//----------------------------------------------------------------------------------------------------------------------
//! Backs the create info handle every create callback receives: what creating a schema, a table and a view have in
//! common.
class CV2VCatalogEntryCreateInfo {
public:
	CV2VCatalogEntryCreateInfo(CV2VCatalogTransaction &transaction, const Identifier &name)
	    : transaction(transaction), name(name) {
	}

	CV2VCatalogTransaction &transaction;
	const Identifier &name;
};

static auto Convert(duckdb_v2_vcatalog_entry_create_info_handle info) -> CV2VCatalogEntryCreateInfo * {
	return reinterpret_cast<CV2VCatalogEntryCreateInfo *>(info);
}
static auto Convert(CV2VCatalogEntryCreateInfo *info) -> duckdb_v2_vcatalog_entry_create_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_entry_create_info_handle>(info);
}

//! Backs the schema create info handle.
class CV2VCatalogSchemaCreateInfo {
public:
	explicit CV2VCatalogSchemaCreateInfo(CV2VirtualCatalog &catalog) : catalog(catalog) {
	}

	CV2VirtualCatalog &catalog;
	optional_ptr<CV2VCatalogSchema> result;
};

static auto Convert(duckdb_v2_vcatalog_schema_create_info_handle info) -> CV2VCatalogSchemaCreateInfo * {
	return reinterpret_cast<CV2VCatalogSchemaCreateInfo *>(info);
}
static auto Convert(CV2VCatalogSchemaCreateInfo *info) -> duckdb_v2_vcatalog_schema_create_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_create_info_handle>(info);
}

//! Backs the table create info handle.
class CV2VCatalogTableCreateInfo {
public:
	CV2VCatalogTableCreateInfo(CV2VCatalogSchema &schema, const CreateTableInfo &info)
	    : schema(schema), columns(info.columns), not_null(columns.LogicalColumnCount(), false) {
		for (auto &column : columns.Logical()) {
			defaults.push_back(column.HasDefaultValue() ? column.DefaultValue().ToString() : string());
		}
		for (auto &constraint : info.constraints) {
			switch (constraint->type) {
			case ConstraintType::NOT_NULL:
				not_null[constraint->Cast<NotNullConstraint>().index.index] = true;
				break;
			case ConstraintType::UNIQUE: {
				// Written with the names of its columns, whichever way the statement declared it
				auto &unique = constraint->Cast<UniqueConstraint>();
				vector<Identifier> names;
				for (auto &index : unique.GetLogicalIndexes(columns)) {
					names.push_back(columns.GetColumn(index).Name());
				}
				constraints.push_back(UniqueConstraint(std::move(names), unique.IsPrimaryKey()).ToString());
				break;
			}
			default:
				constraints.push_back(constraint->ToString());
				break;
			}
		}
	}

	const ColumnDefinition &GetColumn(idx_t index) const {
		if (index >= columns.LogicalColumnCount()) {
			throw InvalidInputException("Column index out of bounds.");
		}
		return columns.GetColumn(LogicalIndex(index));
	}

	CV2VCatalogSchema &schema;
	const ColumnList &columns;
	//! Whether a column is declared NOT NULL
	vector<bool> not_null;
	//! The default of a column as SQL, empty without one
	vector<string> defaults;
	//! The other constraints of the table as SQL
	vector<string> constraints;
	optional_ptr<CV2VCatalogTable> result;
};

static auto Convert(duckdb_v2_vcatalog_table_create_info_handle info) -> CV2VCatalogTableCreateInfo * {
	return reinterpret_cast<CV2VCatalogTableCreateInfo *>(info);
}
static auto Convert(CV2VCatalogTableCreateInfo *info) -> duckdb_v2_vcatalog_table_create_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_create_info_handle>(info);
}

//! Backs the view create info handle.
class CV2VCatalogViewCreateInfo {
public:
	CV2VCatalogViewCreateInfo(CV2VCatalogSchema &schema, const CreateViewInfo &info)
	    : schema(schema), query(info.query->ToString()), aliases(info.aliases) {
	}

	CV2VCatalogSchema &schema;
	const string query;
	const vector<Identifier> &aliases;
	optional_ptr<CV2VCatalogView> result;
};

static auto Convert(duckdb_v2_vcatalog_view_create_info_handle info) -> CV2VCatalogViewCreateInfo * {
	return reinterpret_cast<CV2VCatalogViewCreateInfo *>(info);
}
static auto Convert(CV2VCatalogViewCreateInfo *info) -> duckdb_v2_vcatalog_view_create_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_view_create_info_handle>(info);
}

//! Backs the drop info handle every drop callback receives: what dropping a schema, a table and a view have in common.
class CV2VCatalogEntryDropInfo {
public:
	CV2VCatalogEntryDropInfo(CV2VCatalogTransaction &transaction, bool cascade)
	    : transaction(transaction), cascade(cascade) {
	}

	CV2VCatalogTransaction &transaction;
	const bool cascade;
};

static auto Convert(duckdb_v2_vcatalog_entry_drop_info_handle info) -> CV2VCatalogEntryDropInfo * {
	return reinterpret_cast<CV2VCatalogEntryDropInfo *>(info);
}
static auto Convert(CV2VCatalogEntryDropInfo *info) -> duckdb_v2_vcatalog_entry_drop_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_entry_drop_info_handle>(info);
}

//! Backs the schema, table and view drop info handles.
template <class ENTRY>
class CV2VCatalogDropInfo {
public:
	explicit CV2VCatalogDropInfo(ENTRY &entry) : entry(entry) {
	}

	ENTRY &entry;
};

static auto Convert(duckdb_v2_vcatalog_schema_drop_info_handle info) -> CV2VCatalogDropInfo<CV2VCatalogSchema> * {
	return reinterpret_cast<CV2VCatalogDropInfo<CV2VCatalogSchema> *>(info);
}
static auto Convert(CV2VCatalogDropInfo<CV2VCatalogSchema> *info) -> duckdb_v2_vcatalog_schema_drop_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_schema_drop_info_handle>(info);
}
static auto Convert(duckdb_v2_vcatalog_table_drop_info_handle info) -> CV2VCatalogDropInfo<CV2VCatalogTable> * {
	return reinterpret_cast<CV2VCatalogDropInfo<CV2VCatalogTable> *>(info);
}
static auto Convert(CV2VCatalogDropInfo<CV2VCatalogTable> *info) -> duckdb_v2_vcatalog_table_drop_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_drop_info_handle>(info);
}
static auto Convert(duckdb_v2_vcatalog_view_drop_info_handle info) -> CV2VCatalogDropInfo<CV2VCatalogView> * {
	return reinterpret_cast<CV2VCatalogDropInfo<CV2VCatalogView> *>(info);
}
static auto Convert(CV2VCatalogDropInfo<CV2VCatalogView> *info) -> duckdb_v2_vcatalog_view_drop_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_view_drop_info_handle>(info);
}

void CV2VirtualCatalog::DropEntry(CatalogTransaction transaction, CatalogEntry &entry, bool cascade) {
	CV2VCatalogEntryDropInfo info(GetTransaction(transaction), cascade);
	// Invokes the drop callback of the kind of the entry, if the vcatalog has one
	auto drop = [&](auto drop_cb, auto &kind_entry, const char *what) {
		if (!drop_cb) {
			throw NotImplementedException("Dropping %s is not supported in %s databases", what, definition.name);
		}
		CV2VCatalogDropInfo<std::remove_reference_t<decltype(kind_entry)>> kind_info(kind_entry);
		InvokeCatalogCallback(transaction.GetContext(),
		                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
			                      drop_cb(Convert(&info), Convert(&kind_info), Convert(this), context, err);
		                      });
	};
	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY:
		drop(definition.schema_drop_cb, entry.Cast<CV2VCatalogSchema>(), "schemas");
		break;
	case CatalogType::TABLE_ENTRY:
		drop(definition.table_drop_cb, entry.Cast<CV2VCatalogTable>(), "tables");
		break;
	case CatalogType::VIEW_ENTRY:
		drop(definition.view_drop_cb, entry.Cast<CV2VCatalogView>(), "views");
		break;
	default:
		throw InternalException("Unsupported entry type in CV2VirtualCatalog::DropEntry");
	}
}

bool CV2VirtualCatalog::ReplaceExisting(CatalogTransaction transaction, CatalogEntry &existing, CatalogType type,
                                        OnCreateConflict on_conflict) {
	switch (on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return false;
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		if (existing.type != type) {
			throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", existing.name,
			                       CatalogTypeToString(existing.type), CatalogTypeToString(type));
		}
		DropEntry(transaction, existing, false);
		return true;
	default:
		throw CatalogException::EntryAlreadyExists(type, existing.name);
	}
}

optional_ptr<CatalogEntry> CV2VirtualCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto create_cb = definition.schema_create_cb;
	if (!create_cb) {
		throw NotImplementedException("Creating schemas is not supported in %s databases", definition.name);
	}
	if (info.IsNested()) {
		throw NotImplementedException("Nested schemas are not supported in %s databases", definition.name);
	}
	auto &name = info.SchemaName();
	auto existing = ResolveSchema(transaction, name.GetIdentifierName());
	if (existing && !ReplaceExisting(transaction, *existing, CatalogType::SCHEMA_ENTRY, info.on_conflict)) {
		return nullptr;
	}
	CV2VCatalogEntryCreateInfo create_info(GetTransaction(transaction), name);
	CV2VCatalogSchemaCreateInfo schema_info(*this);
	InvokeCatalogCallback(transaction.GetContext(),
	                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		                      create_cb(Convert(&create_info), Convert(&schema_info), Convert(this), context, err);
	                      });
	if (!schema_info.result) {
		throw InvalidInputException("The schema create callback did not report the schema %s.", name);
	}
	return schema_info.result.get();
}

void CV2VirtualCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto transaction = GetCatalogTransaction(context);
	EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, info.GetQualifiedName());
	auto schema = LookupSchema(transaction, lookup, info.if_not_found);
	if (!schema) {
		return;
	}
	DropEntry(transaction, *schema, info.cascade);
}

optional_ptr<CatalogEntry> CV2VirtualCatalog::CreateTable(CatalogTransaction transaction, CV2VCatalogSchema &schema,
                                                          CreateTableInfo &info) {
	auto create_cb = definition.table_create_cb;
	if (!create_cb) {
		throw NotImplementedException("Creating tables is not supported in %s databases", definition.name);
	}
	for (auto &column : info.columns.Logical()) {
		if (column.Generated()) {
			throw NotImplementedException("Generated columns are not supported in %s databases", definition.name);
		}
	}
	auto &name = info.GetTableName();
	auto existing = ResolveTable(transaction, schema, name.GetIdentifierName());
	if (existing && !ReplaceExisting(transaction, *existing, CatalogType::TABLE_ENTRY, info.on_conflict)) {
		return nullptr;
	}
	CV2VCatalogEntryCreateInfo create_info(GetTransaction(transaction), name);
	CV2VCatalogTableCreateInfo table_info(schema, info);
	InvokeCatalogCallback(transaction.GetContext(),
	                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		                      create_cb(Convert(&create_info), Convert(&table_info), Convert(this), context, err);
	                      });
	if (!table_info.result) {
		throw InvalidInputException("The table create callback did not report the table %s.", name);
	}
	return table_info.result.get();
}

optional_ptr<CatalogEntry> CV2VirtualCatalog::CreateView(CatalogTransaction transaction, CV2VCatalogSchema &schema,
                                                         CreateViewInfo &info) {
	auto create_cb = definition.view_create_cb;
	if (!create_cb) {
		throw NotImplementedException("Creating views is not supported in %s databases", definition.name);
	}
	auto &name = info.GetViewName();
	auto existing = ResolveTable(transaction, schema, name.GetIdentifierName());
	if (existing && !ReplaceExisting(transaction, *existing, CatalogType::VIEW_ENTRY, info.on_conflict)) {
		return nullptr;
	}
	CV2VCatalogEntryCreateInfo create_info(GetTransaction(transaction), name);
	CV2VCatalogViewCreateInfo view_info(schema, info);
	InvokeCatalogCallback(transaction.GetContext(),
	                      [&](duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
		                      create_cb(Convert(&create_info), Convert(&view_info), Convert(this), context, err);
	                      });
	if (!view_info.result) {
		throw InvalidInputException("The view create callback did not report the view %s.", name);
	}
	return view_info.result.get();
}

void CV2VirtualCatalog::DropTableOrView(ClientContext &context, CV2VCatalogSchema &schema, DropInfo &info) {
	auto transaction = GetCatalogTransaction(context);
	auto &name = info.GetQualifiedName().Name();
	auto entry = ResolveTable(transaction, schema, name.GetIdentifierName());
	if (!entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException::MissingEntry(info.type, name, string());
	}
	if (entry->type != info.type) {
		throw CatalogException("Existing object %s is of type %s, trying to drop type %s", name,
		                       CatalogTypeToString(entry->type), CatalogTypeToString(info.type));
	}
	DropEntry(transaction, *entry, info.cascade);
}

optional_ptr<CatalogEntry> CV2VCatalogSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	return ParentCatalog().Cast<CV2VirtualCatalog>().CreateTable(transaction, *this, info.Base());
}

optional_ptr<CatalogEntry> CV2VCatalogSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	return ParentCatalog().Cast<CV2VirtualCatalog>().CreateView(transaction, *this, info);
}

void CV2VCatalogSchema::DropEntry(ClientContext &context, DropInfo &info) {
	if (!IsTableOrView(info.type)) {
		ThrowNotSupported("Dropping entries other than tables and views");
	}
	ParentCatalog().Cast<CV2VirtualCatalog>().DropTableOrView(context, *this, info);
}

//----------------------------------------------------------------------------------------------------------------------
// Scanning Tables
//----------------------------------------------------------------------------------------------------------------------
//! Backs the table scan bind info handle.
class CV2VCatalogTableScanBindInfo {
public:
	CV2VCatalogTableScanBindInfo(CV2VCatalogTransaction &transaction, CV2VCatalogTable &table)
	    : transaction(transaction), table(table) {
	}

	CV2VCatalogTransaction &transaction;
	CV2VCatalogTable &table;
	CV2TableScanBindResult result;
};

static auto Convert(duckdb_v2_vcatalog_table_scan_bind_info_handle info) -> CV2VCatalogTableScanBindInfo * {
	return reinterpret_cast<CV2VCatalogTableScanBindInfo *>(info);
}
static auto Convert(CV2VCatalogTableScanBindInfo *info) -> duckdb_v2_vcatalog_table_scan_bind_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_table_scan_bind_info_handle>(info);
}

TableFunction CV2VCatalogTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	if (!scan.function) {
		throw NotImplementedException("The table %s cannot be read: it has no scan function.", name);
	}
	auto &database = ParentCatalog().Cast<CV2VirtualCatalog>();
	CV2VCatalogTableScanBindInfo info(Transaction::Get(context, database).Cast<CV2VCatalogTransaction>(), *this);
	if (scan.bind_cb) {
		InvokeCatalogCallback(context,
		                      [&](duckdb_v2_context_handle callback_context, duckdb_v2_error_info_handle *err) {
			                      scan.bind_cb(Convert(&info), Convert(&database), callback_context, err);
		                      });
	}
	auto function = *scan.function;
	bind_data = CV2BindTableScan(function, *this, std::move(info.result));
	return function;
}

class CV2VCatalogTransactionManager final : public TransactionManager {
public:
	CV2VCatalogTransactionManager(AttachedDatabase &db, CV2VirtualCatalog &catalog)
	    : TransactionManager(db), catalog(catalog) {
	}

	//! Runs under the lock of the client's MetaTransaction.
	Transaction &StartTransaction(Context &context, MetaTransaction &meta_transaction) override {
		auto transaction = make_uniq<CV2VCatalogTransaction>(*this, context);
		auto begin_cb = catalog.definition.begin_cb;
		if (begin_cb) {
			auto read_only = db.IsReadOnly() || meta_transaction.IsReadOnly();
			CV2VCatalogBeginInfo info(*transaction, read_only);
			InvokeCatalogCallback(
			    [&](duckdb_v2_error_info_handle *err) { begin_cb(Convert(&info), Convert(&catalog), err); });
		}
		auto &result = *transaction;
		lock_guard<mutex> guard(transaction_lock);
		transactions[result] = std::move(transaction);
		return result;
	}

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override {
		auto &vcatalog_transaction = transaction.Cast<CV2VCatalogTransaction>();
		ErrorData error;
		auto commit_cb = catalog.definition.commit_cb;
		if (commit_cb) {
			try {
				CV2VCatalogEndInfo info(vcatalog_transaction);
				InvokeCatalogCallback([&](duckdb_v2_error_info_handle *err) {
					commit_cb(reinterpret_cast<duckdb_v2_vcatalog_transaction_commit_info_handle>(&info),
					          Convert(&catalog), err);
				});
			} catch (std::exception &ex) {
				error = ErrorData(ex);
			}
		}
		if (error.HasError()) {
			// DuckDB does not roll back a transaction whose commit failed; the commit error is the one reported
			TryRollback(vcatalog_transaction);
		}
		RemoveTransaction(transaction);
		return error;
	}

	void RollbackTransaction(ClientContext &context, Transaction &transaction) override {
		auto error = TryRollback(transaction.Cast<CV2VCatalogTransaction>());
		RemoveTransaction(transaction);
		if (error.HasError()) {
			error.Throw();
		}
	}

	void Checkpoint(ClientContext &context, bool force) override {
	}

private:
	ErrorData TryRollback(CV2VCatalogTransaction &transaction) {
		auto rollback_cb = catalog.definition.rollback_cb;
		if (!rollback_cb) {
			return ErrorData();
		}
		try {
			CV2VCatalogEndInfo info(transaction);
			InvokeCatalogCallback([&](duckdb_v2_error_info_handle *err) {
				rollback_cb(reinterpret_cast<duckdb_v2_vcatalog_transaction_rollback_info_handle>(&info),
				            Convert(&catalog), err);
			});
		} catch (std::exception &ex) {
			return ErrorData(ex);
		}
		return ErrorData();
	}

	//! Destroys the transaction, and with it its state, after releasing the lock: the state's destructor may block.
	void RemoveTransaction(Transaction &transaction) {
		unique_ptr<CV2VCatalogTransaction> removed;
		lock_guard<mutex> guard(transaction_lock);
		auto entry = transactions.find(transaction);
		if (entry == transactions.end()) {
			return;
		}
		removed = std::move(entry->second);
		transactions.erase(entry);
	}

private:
	CV2VirtualCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<CV2VCatalogTransaction>> transactions;
};

//----------------------------------------------------------------------------------------------------------------------
// Storage Extension
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogStorageInfo final : public StorageExtensionInfo {
public:
	explicit CV2VCatalogStorageInfo(CV2VCatalogDefinition definition_p) : definition(std::move(definition_p)) {
	}

	const CV2VCatalogDefinition definition;
};

static unique_ptr<Catalog> AttachVCatalog(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	auto &definition = static_cast<CV2VCatalogStorageInfo &>(*storage_info).definition;
	// Created first, so the callback receives it; a failed ATTACH destroys it, and with it the catalog state
	auto catalog = make_uniq<CV2VirtualCatalog>(db, definition, info.path);
	CV2VCatalogAttachInfo attach_info(*catalog, name, info.path, options.access_mode == AccessMode::READ_ONLY,
	                                  options.options);
	CV2CallbackContext callback_context(context);
	InvokeCatalogCallback([&](duckdb_v2_error_info_handle *err) {
		definition.attach_cb(Convert(&attach_info), Convert(catalog.get()), Convert(&callback_context), err);
	});
	attach_info.CheckOptionsRecognized();
	return std::move(catalog);
}

static unique_ptr<TransactionManager> CreateVCatalogTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<CV2VCatalogTransactionManager>(db, catalog.Cast<CV2VirtualCatalog>());
}

//----------------------------------------------------------------------------------------------------------------------
// Builder
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalog {
public:
	explicit CV2VCatalog(DatabaseInstance &db) : db(db) {
	}

	//! Validates the configuration and registers the vcatalog on `target`.
	void Register(DatabaseInstance &target) {
		CheckRegistrationTarget(db, target, "vcatalog");
		if (definition.name.empty()) {
			throw InvalidInputException("A name must be set for the vcatalog.");
		}
		if (!definition.attach_cb) {
			throw InvalidInputException("An attach callback must be set for the vcatalog.");
		}
		// DuckDB's own storage is selected by this name, so a vcatalog registered under it could never be attached
		if (StringUtil::CIEquals(definition.name, "duckdb")) {
			throw InvalidInputException("\"%s\" is the name of DuckDB's own database type.", definition.name);
		}
		auto &config = DBConfig::GetConfig(target);
		if (StorageExtension::Find(config, definition.name)) {
			throw InvalidInputException("A database type named \"%s\" is already registered.", definition.name);
		}
		auto extension = make_shared_ptr<StorageExtension>();
		extension->attach = AttachVCatalog;
		extension->create_transaction_manager = CreateVCatalogTransactionManager;
		extension->storage_info = make_shared_ptr<CV2VCatalogStorageInfo>(definition);
		StorageExtension::Register(config, definition.name, std::move(extension));
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	CV2VCatalogDefinition definition;
};

static auto Convert(duckdb_v2_vcatalog_handle vcatalog) -> CV2VCatalog * {
	return reinterpret_cast<CV2VCatalog *>(vcatalog);
}
static auto Convert(CV2VCatalog *vcatalog) -> duckdb_v2_vcatalog_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_handle>(vcatalog);
}

static void SetState(unique_ptr<CV2UserData> &state, const duckdb_v2_opaque &data) {
	state = make_uniq<CV2UserData>(data.ptr, data.destroy, data.equals);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_vcatalog_create(duckdb_v2_factory_handle factory, duckdb_v2_vcatalog_handle *vcatalog,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(vcatalog);
	*vcatalog = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2VCatalog>(Convert(factory)->GetDatabase());
		*vcatalog = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_destroy(duckdb_v2_vcatalog_handle *vcatalog) {
	return WithErrorHandler(nullptr, [&]() {
		if (!vcatalog) {
			return;
		}
		if (*vcatalog) {
			delete Convert(*vcatalog);
			*vcatalog = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_name(duckdb_v2_vcatalog_handle vcatalog, const char *name, idx_t name_length,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(
	    err, [&]() { Convert(vcatalog)->definition.name = duckdb::string(ConvertIdentifierName(name, name_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_user_data(duckdb_v2_vcatalog_handle vcatalog, duckdb_v2_opaque *data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(vcatalog)->definition.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_attach_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                       duckdb_v2_vcatalog_attach_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.attach_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_begin_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                  duckdb_v2_vcatalog_transaction_begin_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.begin_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_commit_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                   duckdb_v2_vcatalog_transaction_commit_callback_fn callback,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.commit_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_rollback_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                     duckdb_v2_vcatalog_transaction_rollback_callback_fn callback,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.rollback_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_name(duckdb_v2_vcatalog_attach_info_handle info, const char **name,
                                                   idx_t *name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &attach_name = Convert(info)->name;
		*name = attach_name.data();
		*name_length = attach_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_path(duckdb_v2_vcatalog_attach_info_handle info, const char **path,
                                                   idx_t *path_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	DUCKDB_CHECK_ARG(path_length);
	return WithErrorHandler(err, [&]() {
		auto &attach_path = Convert(info)->path;
		*path = attach_path.data();
		*path_length = attach_path.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_is_read_only(duckdb_v2_vcatalog_attach_info_handle info, bool *read_only,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(read_only);
	return WithErrorHandler(err, [&]() { *read_only = Convert(info)->read_only; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_option(duckdb_v2_vcatalog_attach_info_handle info, const char *name,
                                                     idx_t name_length, duckdb_v2_value_handle *value, bool *present,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(value);
	DUCKDB_CHECK_ARG(present);
	return WithErrorHandler(err, [&]() {
		auto option = Convert(info)->FindOption(duckdb::string(Convert(name, name_length)));
		*present = option != nullptr;
		if (option) {
			*value = Convert(new duckdb::Value(*option));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_set_catalog_state(duckdb_v2_vcatalog_attach_info_handle info,
                                                            duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		duckdb::unique_ptr<CV2UserData> state;
		SetState(state, *data);
		Convert(info)->catalog.SetCatalogState(std::move(state));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_set_default_schema(duckdb_v2_vcatalog_attach_info_handle info,
                                                             const char *name, idx_t name_length,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(err, [&]() {
		if (name_length == 0) {
			throw duckdb::InvalidInputException("The name of the default schema cannot be empty.");
		}
		Convert(info)->catalog.SetDefaultSchema(duckdb::Identifier(ConvertIdentifierName(name, name_length)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_transaction_begin_is_read_only(duckdb_v2_vcatalog_transaction_begin_info_handle info,
                                                                  bool *read_only, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(read_only);
	return WithErrorHandler(err, [&]() { *read_only = Convert(info)->read_only; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_begin_set_transaction_state(duckdb_v2_vcatalog_transaction_begin_info_handle info,
                                                           duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { SetState(Convert(info)->transaction.transaction_state, *data); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_commit_get_transaction_state(duckdb_v2_vcatalog_transaction_commit_info_handle info,
                                                            void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(info)->transaction.transaction_state); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_rollback_get_transaction_state(duckdb_v2_vcatalog_transaction_rollback_info_handle info,
                                                              void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(info)->transaction.transaction_state); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_schema_find_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                            duckdb_v2_vcatalog_schema_find_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.schema_find_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_schema_list_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                            duckdb_v2_vcatalog_schema_list_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.schema_list_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_database_get_user_data(duckdb_v2_vcatalog_database_handle database, void **data,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(database);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(database)->definition.GetUserData(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_database_get_catalog_state(duckdb_v2_vcatalog_database_handle database, void **data,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(database);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(database)->GetCatalogState(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_builder_create(duckdb_v2_vcatalog_database_handle database, const char *name,
                                                         idx_t name_length,
                                                         duckdb_v2_vcatalog_schema_builder_handle *builder,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(database);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(builder);
	*builder = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2VCatalogSchemaBuilder>(
		    *Convert(database), duckdb::Identifier(ConvertIdentifierName(name, name_length)));
		*builder = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_builder_destroy(duckdb_v2_vcatalog_schema_builder_handle *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (!builder) {
			return;
		}
		if (*builder) {
			delete Convert(*builder);
			*builder = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_builder_set_state(duckdb_v2_vcatalog_schema_builder_handle builder,
                                                            duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(builder)->state = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_builder_build(duckdb_v2_vcatalog_schema_builder_handle builder,
                                                        duckdb_v2_vcatalog_schema_handle *schema,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(schema);
	*schema = nullptr;
	return WithErrorHandler(err, [&]() { *schema = Convert(Convert(builder)->Build().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_destroy(duckdb_v2_vcatalog_schema_handle *schema) {
	return WithErrorHandler(nullptr, [&]() {
		if (!schema) {
			return;
		}
		if (*schema) {
			delete Convert(*schema);
			*schema = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_get_name(duckdb_v2_vcatalog_schema_handle schema, const char **name,
                                                   idx_t *name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &schema_name = Convert(schema)->name.GetIdentifierName();
		*name = schema_name.data();
		*name_length = schema_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_get_database(duckdb_v2_vcatalog_schema_handle schema,
                                                       duckdb_v2_vcatalog_database_handle *database,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_ARG(database);
	return WithErrorHandler(
	    err, [&]() { *database = Convert(&Convert(schema)->ParentCatalog().Cast<CV2VirtualCatalog>()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_get_state(duckdb_v2_vcatalog_schema_handle schema, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(schema)->state); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_schema_find_get_transaction_state(duckdb_v2_vcatalog_schema_find_info_handle info, void **data,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_find_get_name(duckdb_v2_vcatalog_schema_find_info_handle info,
                                                        const char **name, idx_t *name_length,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &lookup_name = Convert(info)->name;
		*name = lookup_name.data();
		*name_length = lookup_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_find_set_schema(duckdb_v2_vcatalog_schema_find_info_handle info,
                                                          duckdb_v2_vcatalog_schema_handle schema,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		CheckSchemaOf(request.catalog, *Convert(schema));
		request.result = Convert(schema);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_list_get_transaction_state(duckdb_v2_vcatalog_schema_list_info_handle info,
                                                                     void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_list_add_schema(duckdb_v2_vcatalog_schema_list_info_handle info,
                                                          duckdb_v2_vcatalog_schema_handle schema,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() {
		auto &listing = *Convert(info);
		CheckSchemaOf(listing.catalog, *Convert(schema));
		listing.AddSchema(*Convert(schema));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_table_find_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                           duckdb_v2_vcatalog_table_find_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.table_find_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_table_list_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                           duckdb_v2_vcatalog_table_list_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.table_list_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_create(duckdb_v2_vcatalog_schema_handle schema, const char *name,
                                                        idx_t name_length,
                                                        duckdb_v2_vcatalog_table_builder_handle *builder,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(builder);
	*builder = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2VCatalogTableBuilder>(
		    *Convert(schema), duckdb::Identifier(ConvertIdentifierName(name, name_length)));
		*builder = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_destroy(duckdb_v2_vcatalog_table_builder_handle *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (!builder) {
			return;
		}
		if (*builder) {
			delete Convert(*builder);
			*builder = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_add_column(duckdb_v2_vcatalog_table_builder_handle builder,
                                                            const char *name, idx_t name_length,
                                                            duckdb_v2_logical_type_handle type,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() {
		auto column_type_ref = Convert(type);
		const auto &column_type = *column_type_ref;
		if (!column_type.IsComplete()) {
			throw duckdb::InvalidInputException("The type of a column must be a fully defined concrete type.");
		}
		Convert(builder)->AddColumn(
		    duckdb::ColumnDefinition(duckdb::Identifier(ConvertIdentifierName(name, name_length)), column_type));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_set_column_not_null(duckdb_v2_vcatalog_table_builder_handle builder,
                                                                     idx_t index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	return WithErrorHandler(err, [&]() { Convert(builder)->SetColumnNotNull(index); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_set_state(duckdb_v2_vcatalog_table_builder_handle builder,
                                                           duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(builder)->state = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_set_scan_function(duckdb_v2_vcatalog_table_builder_handle builder,
                                                                   duckdb_v2_table_function_handle function,
                                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(builder)->SetScanFunction(function); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_table_builder_set_scan_bind_callback(duckdb_v2_vcatalog_table_builder_handle builder,
                                                        duckdb_v2_vcatalog_table_scan_bind_callback_fn callback,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	return WithErrorHandler(err, [&]() { Convert(builder)->scan.bind_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_builder_build(duckdb_v2_vcatalog_table_builder_handle builder,
                                                       duckdb_v2_vcatalog_table_handle *table,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(table);
	*table = nullptr;
	return WithErrorHandler(err, [&]() { *table = Convert(Convert(builder)->Build().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_destroy(duckdb_v2_vcatalog_table_handle *table) {
	return WithErrorHandler(nullptr, [&]() {
		if (!table) {
			return;
		}
		if (*table) {
			delete Convert(*table);
			*table = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_name(duckdb_v2_vcatalog_table_handle table, const char **name,
                                                  idx_t *name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &table_name = Convert(table)->name.GetIdentifierName();
		*name = table_name.data();
		*name_length = table_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_schema(duckdb_v2_vcatalog_table_handle table,
                                                    duckdb_v2_vcatalog_schema_handle *schema,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err,
	                        [&]() { *schema = Convert(&Convert(table)->ParentSchema().Cast<CV2VCatalogSchema>()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_state(duckdb_v2_vcatalog_table_handle table, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(table)->state); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_column_count(duckdb_v2_vcatalog_table_handle table, idx_t *count,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(table)->GetColumns().LogicalColumnCount(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_column_name(duckdb_v2_vcatalog_table_handle table, idx_t index,
                                                         const char **name, idx_t *name_length,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err,
	                        [&]() { SetStringOut(name, name_length, Convert(table)->GetColumnChecked(index).Name()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_get_column_type(duckdb_v2_vcatalog_table_handle table, idx_t index,
                                                         duckdb_v2_logical_type_handle *type,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() { *type = Convert(Convert(table)->GetColumnChecked(index).Type()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_is_column_not_null(duckdb_v2_vcatalog_table_handle table, idx_t index,
                                                            bool *not_null, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(table);
	DUCKDB_CHECK_ARG(not_null);
	return WithErrorHandler(err, [&]() { *not_null = Convert(table)->IsColumnNotNull(index); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_table_find_get_transaction_state(duckdb_v2_vcatalog_table_find_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_find_get_schema(duckdb_v2_vcatalog_table_find_info_handle info,
                                                         duckdb_v2_vcatalog_schema_handle *schema,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() { *schema = Convert(&Convert(info)->schema); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_find_get_name(duckdb_v2_vcatalog_table_find_info_handle info,
                                                       const char **name, idx_t *name_length,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &lookup_name = Convert(info)->name;
		*name = lookup_name.data();
		*name_length = lookup_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_find_set_table(duckdb_v2_vcatalog_table_find_info_handle info,
                                                        duckdb_v2_vcatalog_table_handle table,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(table);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		Convert(table)->Verify(request.schema);
		request.result = Convert(table);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_list_get_transaction_state(duckdb_v2_vcatalog_table_list_info_handle info,
                                                                    void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_list_get_schema(duckdb_v2_vcatalog_table_list_info_handle info,
                                                         duckdb_v2_vcatalog_schema_handle *schema,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() { *schema = Convert(&Convert(info)->schema); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_list_add_table(duckdb_v2_vcatalog_table_list_info_handle info,
                                                        duckdb_v2_vcatalog_table_handle table,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(table);
	return WithErrorHandler(err, [&]() {
		auto &listing = *Convert(info);
		Convert(table)->Verify(listing.schema);
		listing.AddEntry(*Convert(table));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_create(duckdb_v2_vcatalog_schema_handle schema, const char *name,
                                                       idx_t name_length,
                                                       duckdb_v2_vcatalog_view_builder_handle *builder,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(builder);
	*builder = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2VCatalogViewBuilder>(
		    *Convert(schema), duckdb::Identifier(ConvertIdentifierName(name, name_length)));
		*builder = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_create_from_sql(duckdb_v2_vcatalog_schema_handle schema,
                                                                const char *sql, idx_t sql_length,
                                                                duckdb_v2_vcatalog_view_builder_handle *builder,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_STR_ARG(sql, sql_length);
	DUCKDB_CHECK_ARG(builder);
	*builder = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result =
		    duckdb::make_uniq<CV2VCatalogViewBuilder>(*Convert(schema), duckdb::string(Convert(sql, sql_length)));
		*builder = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_destroy(duckdb_v2_vcatalog_view_builder_handle *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (!builder) {
			return;
		}
		if (*builder) {
			delete Convert(*builder);
			*builder = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_set_query(duckdb_v2_vcatalog_view_builder_handle builder,
                                                          const char *sql, idx_t sql_length,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_STR_ARG(sql, sql_length);
	return WithErrorHandler(err, [&]() { Convert(builder)->SetQuery(duckdb::string(Convert(sql, sql_length))); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_add_column_alias(duckdb_v2_vcatalog_view_builder_handle builder,
                                                                 const char *name, idx_t name_length,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(
	    err, [&]() { Convert(builder)->AddColumnAlias(duckdb::Identifier(ConvertIdentifierName(name, name_length))); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_set_state(duckdb_v2_vcatalog_view_builder_handle builder,
                                                          duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(builder)->state = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_builder_build(duckdb_v2_vcatalog_view_builder_handle builder,
                                                      duckdb_v2_vcatalog_view_handle *view,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(builder);
	DUCKDB_CHECK_ARG(view);
	*view = nullptr;
	return WithErrorHandler(err, [&]() { *view = Convert(Convert(builder)->Build().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_destroy(duckdb_v2_vcatalog_view_handle *view) {
	return WithErrorHandler(nullptr, [&]() {
		if (!view) {
			return;
		}
		if (*view) {
			delete Convert(*view);
			*view = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_get_name(duckdb_v2_vcatalog_view_handle view, const char **name,
                                                 idx_t *name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(view);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &view_name = Convert(view)->name.GetIdentifierName();
		*name = view_name.data();
		*name_length = view_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_get_schema(duckdb_v2_vcatalog_view_handle view,
                                                   duckdb_v2_vcatalog_schema_handle *schema,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(view);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err,
	                        [&]() { *schema = Convert(&Convert(view)->ParentSchema().Cast<CV2VCatalogSchema>()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_get_state(duckdb_v2_vcatalog_view_handle view, void **data,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(view);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(view)->state); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_find_set_view(duckdb_v2_vcatalog_table_find_info_handle info,
                                                       duckdb_v2_vcatalog_view_handle view,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(view);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		Convert(view)->Verify(request.schema);
		request.result = Convert(view);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_list_add_view(duckdb_v2_vcatalog_table_list_info_handle info,
                                                       duckdb_v2_vcatalog_view_handle view,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(view);
	return WithErrorHandler(err, [&]() {
		auto &listing = *Convert(info);
		Convert(view)->Verify(listing.schema);
		listing.AddEntry(*Convert(view));
	});
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_table_scan_bind_get_transaction_state(duckdb_v2_vcatalog_table_scan_bind_info_handle info,
                                                         void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_scan_bind_get_table(duckdb_v2_vcatalog_table_scan_bind_info_handle info,
                                                             duckdb_v2_vcatalog_table_handle *table,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(table);
	return WithErrorHandler(err, [&]() { *table = Convert(&Convert(info)->table); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_scan_bind_set_bind_data(duckdb_v2_vcatalog_table_scan_bind_info_handle info,
                                                                 duckdb_v2_opaque *data,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(info)->result.bind_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_scan_bind_set_cardinality(duckdb_v2_vcatalog_table_scan_bind_info_handle info,
                                                                   idx_t cardinality, bool is_exact,
                                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &result = Convert(info)->result;
		result.cardinality = cardinality;
		result.cardinality_is_exact = is_exact;
		result.cardinality_set = true;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_table_scan_bind_set_order_preservation(duckdb_v2_vcatalog_table_scan_bind_info_handle info,
                                                          DUCKDB_V2_ORDER_PRESERVATION order,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err,
	                        [&]() { Convert(info)->result.order_preservation = CV2ConvertOrderPreservation(order); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_schema_create_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                              duckdb_v2_vcatalog_schema_create_callback_fn callback,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.schema_create_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_schema_drop_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                            duckdb_v2_vcatalog_schema_drop_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.schema_drop_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_table_create_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                             duckdb_v2_vcatalog_table_create_callback_fn callback,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.table_create_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_table_drop_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                           duckdb_v2_vcatalog_table_drop_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.table_drop_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_view_create_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                            duckdb_v2_vcatalog_view_create_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.view_create_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_view_drop_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                          duckdb_v2_vcatalog_view_drop_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.view_drop_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_entry_create_get_name(duckdb_v2_vcatalog_entry_create_info_handle info,
                                                         const char **name, idx_t *name_length,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() { SetStringOut(name, name_length, Convert(info)->name); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_entry_create_get_transaction_state(duckdb_v2_vcatalog_entry_create_info_handle info, void **data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_create_set_schema(duckdb_v2_vcatalog_schema_create_info_handle info,
                                                            duckdb_v2_vcatalog_schema_handle schema,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		CheckSchemaOf(request.catalog, *Convert(schema));
		request.result = Convert(schema);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_schema(duckdb_v2_vcatalog_table_create_info_handle info,
                                                           duckdb_v2_vcatalog_schema_handle *schema,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() { *schema = Convert(&Convert(info)->schema); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_column_count(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                 idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->columns.LogicalColumnCount(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_column_name(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                idx_t index, const char **name, idx_t *name_length,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() { SetStringOut(name, name_length, Convert(info)->GetColumn(index).Name()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_column_type(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                idx_t index, duckdb_v2_logical_type_handle *type,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() { *type = Convert(Convert(info)->GetColumn(index).Type()); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_is_column_not_null(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                   idx_t index, bool *not_null,
                                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(not_null);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		request.GetColumn(index);
		*not_null = request.not_null[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_column_default(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                   idx_t index, const char **sql, idx_t *sql_length,
                                                                   bool *present, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(sql);
	DUCKDB_CHECK_ARG(sql_length);
	DUCKDB_CHECK_ARG(present);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		*present = request.GetColumn(index).HasDefaultValue();
		if (*present) {
			SetStringOut(sql, sql_length, request.defaults[index]);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_constraint_count(duckdb_v2_vcatalog_table_create_info_handle info,
                                                                     idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->constraints.size(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_get_constraint(duckdb_v2_vcatalog_table_create_info_handle info,
                                                               idx_t index, const char **sql, idx_t *sql_length,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(sql);
	DUCKDB_CHECK_ARG(sql_length);
	return WithErrorHandler(err, [&]() {
		auto &constraints = Convert(info)->constraints;
		if (index >= constraints.size()) {
			throw duckdb::InvalidInputException("Constraint index out of bounds.");
		}
		SetStringOut(sql, sql_length, constraints[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_create_set_table(duckdb_v2_vcatalog_table_create_info_handle info,
                                                          duckdb_v2_vcatalog_table_handle table,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(table);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		Convert(table)->Verify(request.schema);
		request.result = Convert(table);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_create_get_schema(duckdb_v2_vcatalog_view_create_info_handle info,
                                                          duckdb_v2_vcatalog_schema_handle *schema,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() { *schema = Convert(&Convert(info)->schema); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_create_get_query(duckdb_v2_vcatalog_view_create_info_handle info,
                                                         const char **sql, idx_t *sql_length,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(sql);
	DUCKDB_CHECK_ARG(sql_length);
	return WithErrorHandler(err, [&]() { SetStringOut(sql, sql_length, Convert(info)->query); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_create_get_column_alias_count(duckdb_v2_vcatalog_view_create_info_handle info,
                                                                      idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->aliases.size(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_create_get_column_alias(duckdb_v2_vcatalog_view_create_info_handle info,
                                                                idx_t index, const char **name, idx_t *name_length,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &aliases = Convert(info)->aliases;
		if (index >= aliases.size()) {
			throw duckdb::InvalidInputException("Column alias index out of bounds.");
		}
		SetStringOut(name, name_length, aliases[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_create_set_view(duckdb_v2_vcatalog_view_create_info_handle info,
                                                        duckdb_v2_vcatalog_view_handle view,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(view);
	return WithErrorHandler(err, [&]() {
		auto &request = *Convert(info);
		Convert(view)->Verify(request.schema);
		request.result = Convert(view);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_entry_drop_is_cascade(duckdb_v2_vcatalog_entry_drop_info_handle info, bool *cascade,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(cascade);
	return WithErrorHandler(err, [&]() { *cascade = Convert(info)->cascade; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_entry_drop_get_transaction_state(duckdb_v2_vcatalog_entry_drop_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetTransactionState(Convert(info)->transaction); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_schema_drop_get_schema(duckdb_v2_vcatalog_schema_drop_info_handle info,
                                                          duckdb_v2_vcatalog_schema_handle *schema,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(schema);
	return WithErrorHandler(err, [&]() { *schema = Convert(&Convert(info)->entry); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_table_drop_get_table(duckdb_v2_vcatalog_table_drop_info_handle info,
                                                        duckdb_v2_vcatalog_table_handle *table,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(table);
	return WithErrorHandler(err, [&]() { *table = Convert(&Convert(info)->entry); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_view_drop_get_view(duckdb_v2_vcatalog_view_drop_info_handle info,
                                                      duckdb_v2_vcatalog_view_handle *view,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(view);
	return WithErrorHandler(err, [&]() { *view = Convert(&Convert(info)->entry); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_register_vcatalog(duckdb_v2_connection_handle conn,
                                                       duckdb_v2_vcatalog_handle vcatalog,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->Register(*Convert(conn)->context->db); });
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_vcatalog(duckdb_v2_extension_handle extension,
                                                      duckdb_v2_vcatalog_handle vcatalog,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(
	    err, [&]() { Convert(vcatalog)->Register(GetExtensionLoader(extension).GetDatabaseInstance()); });
}
