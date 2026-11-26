#include "test_capi_v2.hpp"

#include <cctype>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 vcatalog tests: register a database type of our own, ATTACH databases of
// that type, observe the attach, transaction and catalog callbacks, scan the
// tables of the databases, and create and drop their schemas, tables and views.
//
// Callbacks avoid Catch assertions: they run inside DuckDB. They record what
// they see, and the test asserts on it afterwards.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

constexpr std::string_view TYPE_NAME = "vtest";

// What the callbacks saw, and how they should misbehave. Carried in the vcatalog's user data; declared before the
// fixture, so it outlives the instance and the states destroyed with it.
struct Recorder {
	idx_t attaches = 0;
	std::string name;
	std::string path;
	bool attach_read_only = false;
	std::string secret;
	idx_t begins = 0;
	bool begin_read_only = false;
	idx_t commits = 0;
	idx_t rollbacks = 0;
	//! Whether every callback saw the user data and the states set for its database and transaction
	bool states_seen = true;
	idx_t catalog_states_destroyed = 0;
	idx_t transaction_states_destroyed = 0;
	//! The schemas every database has
	std::vector<std::string> schema_names = {"main", "other"};
	idx_t schema_finds = 0;
	std::string last_schema_find;
	idx_t schema_lists = 0;
	idx_t schemas_destroyed = 0;
	//! Whether every catalog callback received a context
	bool contexts_seen = true;
	//! A schema of the first database whose schemas were created, while it is attached
	duckdb_v2_vcatalog_schema_handle first_schema = nullptr;

	bool fail_attach = false;
	bool fail_begin = false;
	bool fail_commit = false;
	bool fail_rollback = false;
	bool fail_schema_find = false;
	//! Report `first_schema` for every schema the find callback is asked for
	bool resolve_to_first_schema = false;
	idx_t table_finds = 0;
	std::string last_table_find;
	idx_t table_lists = 0;
	idx_t tables_destroyed = 0;
	//! Whether adding a second column of the same name failed
	bool duplicate_column_rejected = false;
	//! Whether building a table without columns failed
	bool empty_table_rejected = false;
	//! Whether a column was declared NOT NULL, twice, and a column that does not exist was not
	bool not_null_setter_checked = false;
	//! The default schema the ATTACH asked for with the `default_schema` option
	std::string default_schema;
	//! Whether an empty name was refused as the default schema
	bool empty_default_schema_rejected = false;
	//! Whether every builder built a second entry of its own, sharing its state
	bool builders_reusable = true;
	//! States of schemas and tables destroyed with the last entry sharing them
	idx_t entry_states_destroyed = 0;
	//! States destroyed with a builder that built nothing
	idx_t unbuilt_states_destroyed = 0;
	//! Report the tables of "main" for every schema the table find callback is asked about
	bool resolve_in_any_schema = false;
	//! Build the tables without a scan function
	bool no_scan_functions = false;
	idx_t scan_binds = 0;
	idx_t scan_bind_data_destroyed = 0;
	bool fail_scan_bind = false;
	//! Whether a scan bind could not set an order guarantee outside of the enum
	bool invalid_order_rejected = false;
	//! Whether a table function without an exec callback, and one with a parameter, were refused as scan functions
	bool invalid_scan_functions_rejected = false;
	idx_t views_destroyed = 0;
	idx_t schema_creates = 0;
	idx_t schema_drops = 0;
	idx_t table_creates = 0;
	idx_t table_drops = 0;
	idx_t view_creates = 0;
	idx_t view_drops = 0;
	//! The name the last create callback was given
	std::string last_create;
	//! The columns and constraints the last table create callback was given
	std::string last_table_create;
	//! The columns the table built by the last table create reports about itself
	std::string last_table_columns;
	//! Whether that table refused to report a column it does not have
	bool table_column_bounds_checked = false;
	//! The column aliases and query the last view create callback was given
	std::string last_view_create;
	bool last_drop_cascade = false;
	//! Fail every create and drop callback
	bool fail_ddl = false;
	//! Return from every create callback without creating anything
	bool skip_create_report = false;
	//! Whether a view could not be built without a query, or from SQL that is not what a view is made of
	bool invalid_views_rejected = false;
};

struct CreatedSchema {
	std::string name;
	duckdb_v2_vcatalog_schema_handle schema;
};

// A table or view a CREATE statement added to a schema.
template <class HANDLE>
struct CreatedEntry {
	std::string name;
	duckdb_v2_vcatalog_schema_handle schema;
	HANDLE entry;
};

struct CatalogState {
	Recorder *recorder;
	//! The schemas of the database, in the order of the recorder's names. Created on first use, destroyed on DETACH.
	std::vector<duckdb_v2_vcatalog_schema_handle> schemas;
	//! The tables of "main", in the order of MainTables(). Created on first use, destroyed on DETACH.
	std::vector<duckdb_v2_vcatalog_table_handle> tables;
	//! The views of "main", in the order of MainViews(). Created with the tables, destroyed on DETACH.
	std::vector<duckdb_v2_vcatalog_view_handle> views;
	//! What CREATE statements added to the database. Destroyed on DETACH.
	std::vector<CreatedSchema> created_schemas;
	std::vector<CreatedEntry<duckdb_v2_vcatalog_table_handle>> created_tables;
	std::vector<CreatedEntry<duckdb_v2_vcatalog_view_handle>> created_views;
	//! What DROP statements removed, kept until DETACH: the transaction dropping an entry may still be using it
	std::vector<duckdb_v2_vcatalog_schema_handle> dropped_schemas;
	std::vector<duckdb_v2_vcatalog_table_handle> dropped_tables;
	std::vector<duckdb_v2_vcatalog_view_handle> dropped_views;
};

void DestroyCatalogState(void *ptr) {
	auto state = static_cast<CatalogState *>(ptr);
	auto &recorder = *state->recorder;
	recorder.catalog_states_destroyed++;
	// Tables and views before the schemas they were created in
	for (auto &table : state->tables) {
		duckdb_v2_vcatalog_table_destroy(&table);
		recorder.tables_destroyed++;
	}
	for (auto &view : state->views) {
		duckdb_v2_vcatalog_view_destroy(&view);
		recorder.views_destroyed++;
	}
	for (auto &created : state->created_tables) {
		duckdb_v2_vcatalog_table_destroy(&created.entry);
	}
	for (auto &created : state->created_views) {
		duckdb_v2_vcatalog_view_destroy(&created.entry);
	}
	for (auto &table : state->dropped_tables) {
		duckdb_v2_vcatalog_table_destroy(&table);
	}
	for (auto &view : state->dropped_views) {
		duckdb_v2_vcatalog_view_destroy(&view);
	}
	for (auto &created : state->created_schemas) {
		duckdb_v2_vcatalog_schema_destroy(&created.schema);
	}
	for (auto &schema : state->dropped_schemas) {
		duckdb_v2_vcatalog_schema_destroy(&schema);
	}
	for (auto &schema : state->schemas) {
		if (recorder.first_schema == schema) {
			recorder.first_schema = nullptr;
		}
		duckdb_v2_vcatalog_schema_destroy(&schema);
		recorder.schemas_destroyed++;
	}
	delete state;
}

struct TransactionState {
	Recorder *recorder;
	CatalogState *catalog;
};

void DestroyTransactionState(void *ptr) {
	auto state = static_cast<TransactionState *>(ptr);
	state->recorder->transaction_states_destroyed++;
	delete state;
}

void Fail(duckdb_v2_error_info_handle *err, std::string_view text) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_set_text(*err, text.data(), text.size());
}

void Attach(duckdb_v2_vcatalog_attach_info_handle info, duckdb_v2_vcatalog_database_handle database,
            duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.attaches++;
	const char *text = nullptr;
	idx_t text_length = 0;
	if (duckdb_v2_vcatalog_attach_get_name(info, &text, &text_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.name.assign(text, text_length);
	if (duckdb_v2_vcatalog_attach_get_path(info, &text, &text_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.path.assign(text, text_length);
	if (duckdb_v2_vcatalog_attach_is_read_only(info, &recorder.attach_read_only, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	std::string_view option_name = "SECRET";
	duckdb_v2_value_handle secret = nullptr;
	bool present = false;
	if (duckdb_v2_vcatalog_attach_get_option(info, option_name.data(), option_name.size(), &secret, &present, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.secret.clear();
	if (present) {
		auto rc = duckdb_v2_value_get_varchar(secret, &text, &text_length, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			recorder.secret.assign(text, text_length);
		}
		duckdb_v2_value_destroy(&secret);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	option_name = "DEFAULT_SCHEMA";
	duckdb_v2_value_handle default_schema = nullptr;
	if (duckdb_v2_vcatalog_attach_get_option(info, option_name.data(), option_name.size(), &default_schema, &present,
	                                         err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.default_schema.clear();
	recorder.empty_default_schema_rejected =
	    duckdb_v2_vcatalog_attach_set_default_schema(info, nullptr, 0, nullptr) != DUCKDB_V2_ERROR_NONE;
	if (present) {
		auto rc = duckdb_v2_value_get_varchar(default_schema, &text, &text_length, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			recorder.default_schema.assign(text, text_length);
			rc = duckdb_v2_vcatalog_attach_set_default_schema(info, text, text_length, err);
		}
		duckdb_v2_value_destroy(&default_schema);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// The database has no catalog state until the callback sets one
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (catalog_state) {
		recorder.states_seen = false;
	}
	// Set before failing, so a failed ATTACH still has a state to destroy
	duckdb_v2_opaque state = {new CatalogState {&recorder}, DestroyCatalogState, nullptr};
	if (duckdb_v2_vcatalog_attach_set_catalog_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyCatalogState(state.ptr);
		return;
	}
	if (duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (catalog_state != state.ptr) {
		recorder.states_seen = false;
	}
	if (recorder.fail_attach) {
		Fail(err, "attach refused");
	}
}

void Begin(duckdb_v2_vcatalog_transaction_begin_info_handle info, duckdb_v2_vcatalog_database_handle database,
           duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.begins++;
	auto catalog = static_cast<CatalogState *>(catalog_state);
	if (!catalog || catalog->recorder != &recorder) {
		recorder.states_seen = false;
	}
	if (duckdb_v2_vcatalog_transaction_begin_is_read_only(info, &recorder.begin_read_only, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// Set before failing, so a failed begin still has a state to destroy
	duckdb_v2_opaque state = {new TransactionState {&recorder, catalog}, DestroyTransactionState, nullptr};
	if (duckdb_v2_vcatalog_transaction_begin_set_transaction_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyTransactionState(state.ptr);
		return;
	}
	if (recorder.fail_begin) {
		Fail(err, "begin refused");
	}
}

// Checks that a commit or rollback sees what its database and transaction set.
void CheckStates(Recorder &recorder, void *catalog_state, void *transaction_state) {
	auto transaction = static_cast<TransactionState *>(transaction_state);
	if (!catalog_state || !transaction || transaction->catalog != catalog_state || transaction->recorder != &recorder) {
		recorder.states_seen = false;
	}
}

void Commit(duckdb_v2_vcatalog_transaction_commit_info_handle info, duckdb_v2_vcatalog_database_handle database,
            duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_commit_get_transaction_state(info, &transaction_state, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.commits++;
	CheckStates(recorder, catalog_state, transaction_state);
	if (recorder.fail_commit) {
		Fail(err, "commit refused");
	}
}

void Rollback(duckdb_v2_vcatalog_transaction_rollback_info_handle info, duckdb_v2_vcatalog_database_handle database,
              duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_rollback_get_transaction_state(info, &transaction_state, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.rollbacks++;
	CheckStates(recorder, catalog_state, transaction_state);
	if (recorder.fail_rollback) {
		Fail(err, "rollback refused");
	}
}

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
			return false;
		}
	}
	return true;
}

// The recorder and catalog state of the database a catalog callback runs on.
bool GetStates(duckdb_v2_vcatalog_database_handle database, Recorder *&recorder, CatalogState *&state,
               duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	recorder = static_cast<Recorder *>(user_data);
	state = static_cast<CatalogState *>(catalog_state);
	if (state->recorder != recorder) {
		recorder->states_seen = false;
	}
	return true;
}

// The state of the schemas and tables: the catalog state, which outlives them.
void DestroyEntryState(void *ptr) {
	static_cast<CatalogState *>(ptr)->recorder->entry_states_destroyed++;
}

void DestroyUnbuiltState(void *ptr) {
	static_cast<Recorder *>(ptr)->unbuilt_states_destroyed++;
}

// Builds a schema of the database, carrying the catalog state as its own state.
duckdb_v2_vcatalog_schema_handle BuildSchema(duckdb_v2_vcatalog_database_handle database, CatalogState &state,
                                             std::string_view name, duckdb_v2_error_info_handle *err) {
	duckdb_v2_vcatalog_schema_builder_handle builder = nullptr;
	if (duckdb_v2_vcatalog_schema_builder_create(database, name.data(), name.size(), &builder, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	duckdb_v2_opaque schema_state = {&state, DestroyEntryState, nullptr};
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	if (duckdb_v2_vcatalog_schema_builder_set_state(builder, &schema_state, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_schema_builder_build(builder, &schema, err) == DUCKDB_V2_ERROR_NONE) {
		// The builder builds another schema, sharing the state
		duckdb_v2_vcatalog_schema_handle second = nullptr;
		void *second_state = nullptr;
		if (duckdb_v2_vcatalog_schema_builder_build(builder, &second, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    second == schema ||
		    duckdb_v2_vcatalog_schema_get_state(second, &second_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    second_state != &state) {
			state.recorder->builders_reusable = false;
		}
		duckdb_v2_vcatalog_schema_destroy(&second);
	}
	duckdb_v2_vcatalog_schema_builder_destroy(&builder);
	return schema;
}

// Builds the schemas of the database on first use.
bool EnsureSchemas(duckdb_v2_vcatalog_database_handle database, CatalogState &state, duckdb_v2_error_info_handle *err) {
	if (!state.schemas.empty()) {
		return true;
	}
	for (auto &name : state.recorder->schema_names) {
		auto schema = BuildSchema(database, state, name, err);
		if (!schema) {
			return false;
		}
		state.schemas.push_back(schema);
	}
	if (!state.recorder->first_schema) {
		state.recorder->first_schema = state.schemas[0];
	}
	// A builder destroyed without building destroys the state it was given
	std::string_view unbuilt_name = "unbuilt";
	duckdb_v2_vcatalog_schema_builder_handle unbuilt = nullptr;
	if (duckdb_v2_vcatalog_schema_builder_create(database, unbuilt_name.data(), unbuilt_name.size(), &unbuilt, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	duckdb_v2_opaque unbuilt_state = {state.recorder, DestroyUnbuiltState, nullptr};
	auto rc = duckdb_v2_vcatalog_schema_builder_set_state(unbuilt, &unbuilt_state, err);
	duckdb_v2_vcatalog_schema_builder_destroy(&unbuilt);
	return rc == DUCKDB_V2_ERROR_NONE;
}

// Checks that a schema reports the database, state and name it was built with.
void CheckSchema(duckdb_v2_vcatalog_schema_handle schema, duckdb_v2_vcatalog_database_handle database,
                 CatalogState &state, std::string_view name) {
	duckdb_v2_vcatalog_database_handle schema_database = nullptr;
	void *schema_state = nullptr;
	const char *schema_name = nullptr;
	idx_t schema_name_length = 0;
	if (duckdb_v2_vcatalog_schema_get_database(schema, &schema_database, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_get_state(schema, &schema_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_get_name(schema, &schema_name, &schema_name_length, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    schema_database != database || schema_state != &state ||
	    std::string_view(schema_name, schema_name_length) != name) {
		state.recorder->states_seen = false;
	}
}

void SchemaFind(duckdb_v2_vcatalog_schema_find_info_handle info, duckdb_v2_vcatalog_database_handle database,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	const char *name = nullptr;
	idx_t name_length = 0;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_schema_find_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_find_get_name(info, &name, &name_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->schema_finds++;
	recorder->last_schema_find.assign(name, name_length);
	if (!context) {
		recorder->contexts_seen = false;
	}
	// The begin callback sets a transaction state for every transaction
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (recorder->fail_schema_find) {
		Fail(err, "schema find refused");
		return;
	}
	if (recorder->resolve_to_first_schema && recorder->first_schema) {
		duckdb_v2_vcatalog_schema_find_set_schema(info, recorder->first_schema, err);
		return;
	}
	if (!EnsureSchemas(database, *state, err)) {
		return;
	}
	for (idx_t i = 0; i < state->schemas.size(); i++) {
		if (EqualsIgnoreCase(recorder->schema_names[i], recorder->last_schema_find)) {
			CheckSchema(state->schemas[i], database, *state, recorder->schema_names[i]);
			duckdb_v2_vcatalog_schema_find_set_schema(info, state->schemas[i], err);
			return;
		}
	}
	for (auto &created : state->created_schemas) {
		if (EqualsIgnoreCase(created.name, recorder->last_schema_find)) {
			duckdb_v2_vcatalog_schema_find_set_schema(info, created.schema, err);
			return;
		}
	}
}

void SchemaList(duckdb_v2_vcatalog_schema_list_info_handle info, duckdb_v2_vcatalog_database_handle database,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_schema_list_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->schema_lists++;
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (!EnsureSchemas(database, *state, err)) {
		return;
	}
	for (idx_t i = 0; i < state->schemas.size(); i++) {
		CheckSchema(state->schemas[i], database, *state, recorder->schema_names[i]);
		if (duckdb_v2_vcatalog_schema_list_add_schema(info, state->schemas[i], err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	for (auto &created : state->created_schemas) {
		if (duckdb_v2_vcatalog_schema_list_add_schema(info, created.schema, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// A schema added twice is listed once
	duckdb_v2_vcatalog_schema_list_add_schema(info, state->schemas[0], err);
}

struct ColumnSpec {
	std::string_view name;
	DUCKDB_V2_LOGICAL_TYPE_ID type;
};

struct TableSpec {
	std::string_view name;
	std::vector<ColumnSpec> columns;
};

// The tables of schema "main"; "other" has none.
const std::vector<TableSpec> &MainTables() {
	static const std::vector<TableSpec> tables = {
	    {"t1", {{"id", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER}, {"name", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR}}},
	    {"t2", {{"x", DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE}}}};
	return tables;
}

// The number of rows every table of "main" has: row i holds i in an INTEGER column, i * 1.5 in a DOUBLE column and
// "row<i>" in a VARCHAR column.
constexpr idx_t TABLE_ROWS = 3;

// What a scan of a table is bound with.
struct ScanBindData {
	Recorder *recorder;
	//! The position of the scanned table in MainTables()
	idx_t table_index;
};

void DestroyScanBindData(void *ptr) {
	auto data = static_cast<ScanBindData *>(ptr);
	data->recorder->scan_bind_data_destroyed++;
	delete data;
}

void ScanBind(duckdb_v2_vcatalog_table_scan_bind_info_handle info, duckdb_v2_vcatalog_database_handle database,
              duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	duckdb_v2_vcatalog_table_handle table = nullptr;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_table_scan_bind_get_transaction_state(info, &transaction_state, err) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_scan_bind_get_table(info, &table, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->scan_binds++;
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	idx_t table_index = 0;
	while (table_index < state->tables.size() && state->tables[table_index] != table) {
		table_index++;
	}
	if (table_index == state->tables.size()) {
		// Not a table the find and list callbacks hand out
		recorder->states_seen = false;
		return;
	}
	// Set before failing, so a failed bind still has bind data to destroy
	duckdb_v2_opaque bind_data = {new ScanBindData {recorder, table_index}, DestroyScanBindData, nullptr};
	if (duckdb_v2_vcatalog_table_scan_bind_set_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyScanBindData(bind_data.ptr);
		return;
	}
	recorder->invalid_order_rejected =
	    duckdb_v2_vcatalog_table_scan_bind_set_order_preservation(info, static_cast<DUCKDB_V2_ORDER_PRESERVATION>(99),
	                                                              nullptr) != DUCKDB_V2_ERROR_NONE;
	if (duckdb_v2_vcatalog_table_scan_bind_set_cardinality(info, TABLE_ROWS, true, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_scan_bind_set_order_preservation(info, DUCKDB_V2_ORDER_PRESERVATION_INSERTION_ORDER,
	                                                              err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (recorder->fail_scan_bind) {
		Fail(err, "scan bind refused");
	}
}

struct ScanGlobal {
	idx_t position = 0;
};

void DestroyScanGlobal(void *ptr) {
	delete static_cast<ScanGlobal *>(ptr);
}

void ScanInitGlobal(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                    duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new ScanGlobal(), DestroyScanGlobal, nullptr};
	if (duckdb_v2_table_function_init_global_set_global_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyScanGlobal(state.ptr);
	}
}

// Writes the value row `row` holds in a column of type `type` into `vec` at `index`.
DUCKDB_V2_ERROR WriteCell(duckdb_v2_vector_handle vec, idx_t index, DUCKDB_V2_LOGICAL_TYPE_ID type, idx_t row,
                          duckdb_v2_error_info_handle *err) {
	if (type == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR) {
		auto text = "row" + std::to_string(row);
		return V2VectorAssignString(vec, index, text.data(), text.size(), err);
	}
	void *raw = nullptr;
	auto rc = duckdb_v2_vector_get_data_mutable(vec, &raw, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	if (type == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER) {
		static_cast<int32_t *>(raw)[index] = static_cast<int32_t>(row);
	} else {
		static_cast<double *>(raw)[index] = static_cast<double>(row) * 1.5;
	}
	return DUCKDB_V2_ERROR_NONE;
}

void ScanExec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
              duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	void *global_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	idx_t column_count = 0;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_column_count(info, &column_count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &columns = MainTables()[static_cast<ScanBindData *>(bind_ptr)->table_index].columns;
	auto &global = *static_cast<ScanGlobal *>(global_ptr);
	// Capped by the vector size, so the scan also serves builds with a tiny STANDARD_VECTOR_SIZE
	idx_t rows = TABLE_ROWS - global.position;
	if (rows > STANDARD_VECTOR_SIZE) {
		rows = STANDARD_VECTOR_SIZE;
	}
	for (idx_t i = 0; i < column_count; i++) {
		// With projection pushdown, the chunk holds the columns of the table the statement uses
		idx_t column = 0;
		duckdb_v2_vector_handle vec = nullptr;
		if (duckdb_v2_table_function_exec_get_column_index(info, i, &column, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(chunk, i, &vec, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		for (idx_t row = 0; row < rows; row++) {
			if (WriteCell(vec, row, columns[column].type, global.position + row, err) != DUCKDB_V2_ERROR_NONE) {
				return;
			}
		}
		if (i == 0 && duckdb_v2_vector_set_size(vec, rows, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	global.position += rows;
}

// The function the tables of "main" are scanned with.
duckdb_v2_table_function_handle MakeScanFunction(duckdb_v2_factory_handle factory, duckdb_v2_error_info_handle *err) {
	duckdb_v2_table_function_handle function = nullptr;
	if (duckdb_v2_table_function_create(factory, &function, err) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	if (duckdb_v2_table_function_set_init_global_callback(function, ScanInitGlobal, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_exec_callback(function, ScanExec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_projection_pushdown(function, true, err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_table_function_destroy(&function);
	}
	return function;
}

DUCKDB_V2_ERROR AddColumn(duckdb_v2_vcatalog_table_builder_handle builder, duckdb_v2_factory_handle factory,
                          const ColumnSpec &column, duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle type = nullptr;
	auto rc = duckdb_v2_factory_create_type_from_id(factory, column.type, nullptr, nullptr, nullptr, 0, &type, err);
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_builder_add_column(builder, column.name.data(), column.name.size(), type, err);
	}
	duckdb_v2_logical_type_destroy(&type);
	return rc;
}

// Builds a table of "main", carrying the catalog state as its own state, and scanned with `scan_function` if given.
duckdb_v2_vcatalog_table_handle BuildTable(duckdb_v2_factory_handle factory, CatalogState &state, const TableSpec &spec,
                                           duckdb_v2_table_function_handle scan_function,
                                           duckdb_v2_error_info_handle *err) {
	duckdb_v2_vcatalog_table_builder_handle builder = nullptr;
	if (duckdb_v2_vcatalog_table_builder_create(state.schemas[0], spec.name.data(), spec.name.size(), &builder, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	auto rc = DUCKDB_V2_ERROR_NONE;
	for (auto &column : spec.columns) {
		if (rc == DUCKDB_V2_ERROR_NONE) {
			rc = AddColumn(builder, factory, column, err);
		}
	}
	if (rc == DUCKDB_V2_ERROR_NONE && scan_function) {
		rc = duckdb_v2_vcatalog_table_builder_set_scan_function(builder, scan_function, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_builder_set_scan_bind_callback(builder, ScanBind, err);
	}
	duckdb_v2_opaque table_state = {&state, DestroyEntryState, nullptr};
	duckdb_v2_vcatalog_table_handle table = nullptr;
	if (rc == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_table_builder_set_state(builder, &table_state, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_table_builder_build(builder, &table, err) == DUCKDB_V2_ERROR_NONE) {
		// The builder builds another table, sharing the state; a column added in between leaves the first as it was
		duckdb_v2_vcatalog_table_handle second = nullptr;
		void *second_state = nullptr;
		if (AddColumn(builder, factory, {"late", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER}, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vcatalog_table_builder_build(builder, &second, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    second == table ||
		    duckdb_v2_vcatalog_table_get_state(second, &second_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    second_state != &state) {
			state.recorder->builders_reusable = false;
		}
		duckdb_v2_vcatalog_table_destroy(&second);
	}
	duckdb_v2_vcatalog_table_builder_destroy(&builder);
	return table;
}

// Whether `builder` refuses a table function without an exec callback, and one with a parameter, as its scan function.
bool RejectsInvalidScanFunctions(duckdb_v2_vcatalog_table_builder_handle builder, duckdb_v2_factory_handle factory) {
	duckdb_v2_table_function_handle function = nullptr;
	duckdb_v2_function_signature_handle signature = nullptr;
	duckdb_v2_logical_type_handle type = nullptr;
	std::string_view parameter_name = "n";
	auto without_exec_rejected = false;
	auto with_parameter_rejected = false;
	if (duckdb_v2_table_function_create(factory, &function, nullptr) == DUCKDB_V2_ERROR_NONE) {
		without_exec_rejected =
		    duckdb_v2_vcatalog_table_builder_set_scan_function(builder, function, nullptr) != DUCKDB_V2_ERROR_NONE;
	}
	if (without_exec_rejected &&
	    duckdb_v2_table_function_set_exec_callback(function, ScanExec, nullptr) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_table_function_get_signature(function, &signature, nullptr) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, nullptr, 0,
	                                          &type, nullptr) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_function_signature_add_parameter(signature, parameter_name.data(), parameter_name.size(), type,
	                                               nullptr, DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
	                                               nullptr) == DUCKDB_V2_ERROR_NONE) {
		with_parameter_rejected =
		    duckdb_v2_vcatalog_table_builder_set_scan_function(builder, function, nullptr) != DUCKDB_V2_ERROR_NONE;
	}
	duckdb_v2_logical_type_destroy(&type);
	duckdb_v2_table_function_destroy(&function);
	return without_exec_rejected && with_parameter_rejected;
}

struct ViewSpec {
	std::string_view name;
	//! A query, or a CREATE VIEW statement if `from_create_view` is set
	std::string_view sql;
	bool from_create_view;
};

// The views of schema "main". Their queries find the tables of "main" by their bare names.
const std::vector<ViewSpec> &MainViews() {
	static const std::vector<ViewSpec> views = {{"v1", "SELECT id, name FROM t1 WHERE id > 0", false},
	                                            {"v2", "CREATE VIEW elsewhere.v2 (y) AS SELECT x * 2 FROM t2", true}};
	return views;
}

// Builds a view of "main", carrying the catalog state as its own state.
duckdb_v2_vcatalog_view_handle BuildView(CatalogState &state, const ViewSpec &spec, duckdb_v2_error_info_handle *err) {
	duckdb_v2_vcatalog_view_builder_handle builder = nullptr;
	auto rc = DUCKDB_V2_ERROR_NONE;
	if (spec.from_create_view) {
		rc = duckdb_v2_vcatalog_view_builder_create_from_sql(state.schemas[0], spec.sql.data(), spec.sql.size(),
		                                                     &builder, err);
	} else {
		rc =
		    duckdb_v2_vcatalog_view_builder_create(state.schemas[0], spec.name.data(), spec.name.size(), &builder, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			rc = duckdb_v2_vcatalog_view_builder_set_query(builder, spec.sql.data(), spec.sql.size(), err);
		}
	}
	duckdb_v2_opaque view_state = {&state, DestroyEntryState, nullptr};
	duckdb_v2_vcatalog_view_handle view = nullptr;
	if (rc == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_view_builder_set_state(builder, &view_state, err) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_vcatalog_view_builder_build(builder, &view, err);
	}
	duckdb_v2_vcatalog_view_builder_destroy(&builder);
	return view;
}

// Whether views cannot be built without a query, or from SQL that is not what a view is made of.
bool RejectsInvalidViews(CatalogState &state) {
	std::string_view name = "invalid";
	std::string_view not_a_query = "DROP TABLE t1";
	std::string_view not_sql = "SELECT FROM WHERE";
	std::string_view not_a_view = "CREATE TABLE t (i INTEGER)";
	std::string_view alias = "a";
	std::string_view query = "SELECT 42";
	duckdb_v2_vcatalog_view_builder_handle builder = nullptr;
	duckdb_v2_vcatalog_view_handle view = nullptr;
	auto rejected =
	    duckdb_v2_vcatalog_view_builder_create_from_sql(state.schemas[0], not_a_view.data(), not_a_view.size(),
	                                                    &builder, nullptr) != DUCKDB_V2_ERROR_NONE &&
	    !builder;
	if (duckdb_v2_vcatalog_view_builder_create(state.schemas[0], name.data(), name.size(), &builder, nullptr) !=
	    DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	rejected =
	    rejected && duckdb_v2_vcatalog_view_builder_build(builder, &view, nullptr) != DUCKDB_V2_ERROR_NONE && !view;
	rejected = rejected && duckdb_v2_vcatalog_view_builder_set_query(builder, not_a_query.data(), not_a_query.size(),
	                                                                 nullptr) != DUCKDB_V2_ERROR_NONE;
	rejected = rejected && duckdb_v2_vcatalog_view_builder_set_query(builder, not_sql.data(), not_sql.size(),
	                                                                 nullptr) != DUCKDB_V2_ERROR_NONE;
	// The builder is still usable after the rejected queries
	rejected = rejected &&
	           duckdb_v2_vcatalog_view_builder_add_column_alias(builder, alias.data(), alias.size(), nullptr) ==
	               DUCKDB_V2_ERROR_NONE &&
	           duckdb_v2_vcatalog_view_builder_set_query(builder, query.data(), query.size(), nullptr) ==
	               DUCKDB_V2_ERROR_NONE &&
	           duckdb_v2_vcatalog_view_builder_build(builder, &view, nullptr) == DUCKDB_V2_ERROR_NONE;
	duckdb_v2_vcatalog_view_builder_destroy(&builder);
	duckdb_v2_vcatalog_view_destroy(&view);
	return rejected;
}

bool BuildTables(duckdb_v2_factory_handle factory, duckdb_v2_table_function_handle scan_function, CatalogState &state,
                 duckdb_v2_error_info_handle *err) {
	for (auto &spec : MainTables()) {
		auto table = BuildTable(factory, state, spec, scan_function, err);
		if (!table) {
			return false;
		}
		state.tables.push_back(table);
	}
	for (auto &spec : MainViews()) {
		auto view = BuildView(state, spec, err);
		if (!view) {
			return false;
		}
		state.views.push_back(view);
	}
	state.recorder->invalid_views_rejected = RejectsInvalidViews(state);
	// A builder builds no table without columns, and rejects a second column of the same name
	std::string_view empty_name = "empty";
	duckdb_v2_vcatalog_table_builder_handle builder = nullptr;
	if (duckdb_v2_vcatalog_table_builder_create(state.schemas[0], empty_name.data(), empty_name.size(), &builder,
	                                            err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	auto &column = MainTables()[0].columns[0];
	duckdb_v2_vcatalog_table_handle empty_table = nullptr;
	state.recorder->empty_table_rejected =
	    duckdb_v2_vcatalog_table_builder_build(builder, &empty_table, nullptr) != DUCKDB_V2_ERROR_NONE && !empty_table;
	auto rc = AddColumn(builder, factory, column, err);
	state.recorder->duplicate_column_rejected = AddColumn(builder, factory, column, nullptr) != DUCKDB_V2_ERROR_NONE;
	// A column is declared NOT NULL by its index: once is enough, and only a column the builder has
	state.recorder->not_null_setter_checked =
	    duckdb_v2_vcatalog_table_builder_set_column_not_null(builder, 1, nullptr) != DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_table_builder_set_column_not_null(builder, 0, nullptr) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_vcatalog_table_builder_set_column_not_null(builder, 0, nullptr) == DUCKDB_V2_ERROR_NONE;
	state.recorder->invalid_scan_functions_rejected = RejectsInvalidScanFunctions(builder, factory);
	duckdb_v2_vcatalog_table_builder_destroy(&builder);
	duckdb_v2_vcatalog_table_destroy(&empty_table);
	return rc == DUCKDB_V2_ERROR_NONE;
}

// Builds the tables and views of "main" on first use.
bool EnsureTables(duckdb_v2_context_handle context, CatalogState &state, duckdb_v2_error_info_handle *err) {
	if (!state.tables.empty()) {
		return true;
	}
	duckdb_v2_factory_handle factory = nullptr;
	if (duckdb_v2_context_get_factory(context, &factory, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	auto scan_function = MakeScanFunction(factory, err);
	if (!scan_function) {
		return false;
	}
	// The builders copy the function, so it need not outlive them
	auto built = BuildTables(factory, state.recorder->no_scan_functions ? nullptr : scan_function, state, err);
	duckdb_v2_table_function_destroy(&scan_function);
	return built;
}

// Checks that a table reports the schema, state and name it was built with.
void CheckTable(duckdb_v2_vcatalog_table_handle table, CatalogState &state, std::string_view name) {
	duckdb_v2_vcatalog_schema_handle table_schema = nullptr;
	void *table_state = nullptr;
	const char *table_name = nullptr;
	idx_t table_name_length = 0;
	if (duckdb_v2_vcatalog_table_get_schema(table, &table_schema, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_get_state(table, &table_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_get_name(table, &table_name, &table_name_length, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    table_schema != state.schemas[0] || table_state != &state ||
	    std::string_view(table_name, table_name_length) != name) {
		state.recorder->states_seen = false;
	}
}

// Checks that a view reports the schema, state and name it was built with.
void CheckView(duckdb_v2_vcatalog_view_handle view, CatalogState &state, std::string_view name) {
	duckdb_v2_vcatalog_schema_handle view_schema = nullptr;
	void *view_state = nullptr;
	const char *view_name = nullptr;
	idx_t view_name_length = 0;
	if (duckdb_v2_vcatalog_view_get_schema(view, &view_schema, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_view_get_state(view, &view_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_view_get_name(view, &view_name, &view_name_length, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    view_schema != state.schemas[0] || view_state != &state ||
	    std::string_view(view_name, view_name_length) != name) {
		state.recorder->states_seen = false;
	}
}

// Reports the table or view a CREATE statement added to `schema` under `name`, if there is one.
bool FindCreated(CatalogState &state, duckdb_v2_vcatalog_schema_handle schema, std::string_view name,
                 duckdb_v2_vcatalog_table_find_info_handle info, duckdb_v2_error_info_handle *err) {
	for (auto &created : state.created_tables) {
		if (created.schema == schema && EqualsIgnoreCase(created.name, name)) {
			duckdb_v2_vcatalog_table_find_set_table(info, created.entry, err);
			return true;
		}
	}
	for (auto &created : state.created_views) {
		if (created.schema == schema && EqualsIgnoreCase(created.name, name)) {
			duckdb_v2_vcatalog_table_find_set_view(info, created.entry, err);
			return true;
		}
	}
	return false;
}

void TableFind(duckdb_v2_vcatalog_table_find_info_handle info, duckdb_v2_vcatalog_database_handle database,
               duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	const char *name = nullptr;
	idx_t name_length = 0;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_table_find_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_find_get_schema(info, &schema, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_find_get_name(info, &name, &name_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->table_finds++;
	recorder->last_table_find.assign(name, name_length);
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (FindCreated(*state, schema, recorder->last_table_find, info, err)) {
		return;
	}
	// The schema was handed out by the schema find callback, which built them
	if (state->schemas.empty() || (schema != state->schemas[0] && !recorder->resolve_in_any_schema) ||
	    !EnsureTables(context, *state, err)) {
		return;
	}
	auto &specs = MainTables();
	for (idx_t i = 0; i < specs.size(); i++) {
		if (!EqualsIgnoreCase(specs[i].name, recorder->last_table_find)) {
			continue;
		}
		CheckTable(state->tables[i], *state, specs[i].name);
		duckdb_v2_vcatalog_table_find_set_table(info, state->tables[i], err);
		return;
	}
	// Views share the namespace of the tables
	auto &view_specs = MainViews();
	for (idx_t i = 0; i < view_specs.size(); i++) {
		if (!EqualsIgnoreCase(view_specs[i].name, recorder->last_table_find)) {
			continue;
		}
		CheckView(state->views[i], *state, view_specs[i].name);
		duckdb_v2_vcatalog_table_find_set_view(info, state->views[i], err);
		return;
	}
}

void TableList(duckdb_v2_vcatalog_table_list_info_handle info, duckdb_v2_vcatalog_database_handle database,
               duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_table_list_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_list_get_schema(info, &schema, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->table_lists++;
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	for (auto &created : state->created_tables) {
		if (created.schema == schema &&
		    duckdb_v2_vcatalog_table_list_add_table(info, created.entry, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	for (auto &created : state->created_views) {
		if (created.schema == schema &&
		    duckdb_v2_vcatalog_table_list_add_view(info, created.entry, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	if (state->schemas.empty() || schema != state->schemas[0] || !EnsureTables(context, *state, err)) {
		return;
	}
	auto &specs = MainTables();
	for (idx_t i = 0; i < specs.size(); i++) {
		CheckTable(state->tables[i], *state, specs[i].name);
		if (duckdb_v2_vcatalog_table_list_add_table(info, state->tables[i], err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	for (auto &view : state->views) {
		if (duckdb_v2_vcatalog_table_list_add_view(info, view, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// A table or view added twice is listed once
	if (duckdb_v2_vcatalog_table_list_add_table(info, state->tables[0], err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vcatalog_table_list_add_view(info, state->views[0], err);
}

// What every create callback starts with: the name to create, checked states, and the misbehaviour asked for. Returns
// whether to go on creating.
bool BeginCreate(duckdb_v2_vcatalog_entry_create_info_handle info, duckdb_v2_vcatalog_database_handle database,
                 duckdb_v2_context_handle context, Recorder *&recorder, CatalogState *&state, std::string &name,
                 duckdb_v2_error_info_handle *err) {
	void *transaction_state = nullptr;
	const char *text = nullptr;
	idx_t text_length = 0;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_entry_create_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_entry_create_get_name(info, &text, &text_length, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	name.assign(text, text_length);
	recorder->last_create = name;
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (recorder->fail_ddl) {
		Fail(err, "create refused");
		return false;
	}
	return !recorder->skip_create_report;
}

// What every drop callback starts with. Returns whether to go on dropping.
bool BeginDrop(duckdb_v2_vcatalog_entry_drop_info_handle info, duckdb_v2_vcatalog_database_handle database,
               duckdb_v2_context_handle context, Recorder *&recorder, CatalogState *&state,
               duckdb_v2_error_info_handle *err) {
	void *transaction_state = nullptr;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_entry_drop_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_entry_drop_is_cascade(info, &recorder->last_drop_cascade, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (recorder->fail_ddl) {
		Fail(err, "drop refused");
		return false;
	}
	return true;
}

void SchemaCreate(duckdb_v2_vcatalog_entry_create_info_handle info,
                  duckdb_v2_vcatalog_schema_create_info_handle schema_info, duckdb_v2_vcatalog_database_handle database,
                  duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	std::string name;
	if (!BeginCreate(info, database, context, recorder, state, name, err)) {
		return;
	}
	auto schema = BuildSchema(database, *state, name, err);
	if (!schema) {
		return;
	}
	recorder->schema_creates++;
	state->created_schemas.push_back({name, schema});
	duckdb_v2_vcatalog_schema_create_set_schema(schema_info, schema, err);
}

void SchemaDrop(duckdb_v2_vcatalog_entry_drop_info_handle info, duckdb_v2_vcatalog_schema_drop_info_handle schema_info,
                duckdb_v2_vcatalog_database_handle database, duckdb_v2_context_handle context,
                duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	if (!BeginDrop(info, database, context, recorder, state, err) ||
	    duckdb_v2_vcatalog_schema_drop_get_schema(schema_info, &schema, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (auto entry = state->created_schemas.begin(); entry != state->created_schemas.end(); entry++) {
		if (entry->schema == schema) {
			recorder->schema_drops++;
			state->dropped_schemas.push_back(schema);
			state->created_schemas.erase(entry);
			return;
		}
	}
	Fail(err, "the schema is built in");
}

// The text of a type, without the assertions of Render(): this runs inside callbacks.
std::string TypeText(duckdb_v2_logical_type_handle type) {
	idx_t length = 0;
	if (duckdb_v2_logical_type_to_text(type, nullptr, 0, &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	std::vector<char> buffer(length + 1);
	if (duckdb_v2_logical_type_to_text(type, buffer.data(), buffer.size(), &length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	return std::string(buffer.data(), length);
}

// Adds the column at `index` of a table to create to `builder`, and what the callback is told about it to
// `description`.
DUCKDB_V2_ERROR AddCreatedColumn(duckdb_v2_vcatalog_table_create_info_handle table_info, idx_t index,
                                 duckdb_v2_vcatalog_table_builder_handle builder, std::string &description,
                                 duckdb_v2_error_info_handle *err) {
	const char *name = nullptr;
	idx_t name_length = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	bool not_null = false;
	const char *default_sql = nullptr;
	idx_t default_sql_length = 0;
	bool has_default = false;
	auto rc = duckdb_v2_vcatalog_table_create_get_column_name(table_info, index, &name, &name_length, err);
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_create_get_column_type(table_info, index, &type, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_create_is_column_not_null(table_info, index, &not_null, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_create_get_column_default(table_info, index, &default_sql, &default_sql_length,
		                                                        &has_default, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_builder_add_column(builder, name, name_length, type, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE && not_null) {
		rc = duckdb_v2_vcatalog_table_builder_set_column_not_null(builder, index, err);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		description += (index > 0 ? ", " : "") + std::string(name, name_length) + " " + TypeText(type);
		if (not_null) {
			description += " NOT NULL";
		}
		if (has_default) {
			description += " DEFAULT " + std::string(default_sql, default_sql_length);
		}
	}
	duckdb_v2_logical_type_destroy(&type);
	return rc;
}

// The columns a table reports about itself, in the form AddCreatedColumn describes them, without the defaults.
std::string DescribeColumns(duckdb_v2_vcatalog_table_handle table) {
	idx_t count = 0;
	if (duckdb_v2_vcatalog_table_get_column_count(table, &count, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return std::string();
	}
	std::string description;
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_length = 0;
		duckdb_v2_logical_type_handle type = nullptr;
		bool not_null = false;
		if (duckdb_v2_vcatalog_table_get_column_name(table, i, &name, &name_length, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vcatalog_table_get_column_type(table, i, &type, nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vcatalog_table_is_column_not_null(table, i, &not_null, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return std::string();
		}
		description += (i > 0 ? ", " : "") + std::string(name, name_length) + " " + TypeText(type);
		if (not_null) {
			description += " NOT NULL";
		}
		duckdb_v2_logical_type_destroy(&type);
	}
	return description;
}

// Whether a table refuses to report a column past its last.
bool ChecksColumnBounds(duckdb_v2_vcatalog_table_handle table) {
	idx_t count = 0;
	const char *name = nullptr;
	idx_t name_length = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	bool not_null = false;
	return duckdb_v2_vcatalog_table_get_column_count(table, &count, nullptr) == DUCKDB_V2_ERROR_NONE &&
	       duckdb_v2_vcatalog_table_get_column_name(table, count, &name, &name_length, nullptr) !=
	           DUCKDB_V2_ERROR_NONE &&
	       duckdb_v2_vcatalog_table_get_column_type(table, count, &type, nullptr) != DUCKDB_V2_ERROR_NONE && !type &&
	       duckdb_v2_vcatalog_table_is_column_not_null(table, count, &not_null, nullptr) != DUCKDB_V2_ERROR_NONE;
}

void TableCreate(duckdb_v2_vcatalog_entry_create_info_handle info,
                 duckdb_v2_vcatalog_table_create_info_handle table_info, duckdb_v2_vcatalog_database_handle database,
                 duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	std::string name;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	idx_t column_count = 0;
	idx_t constraint_count = 0;
	duckdb_v2_vcatalog_table_builder_handle builder = nullptr;
	if (!BeginCreate(info, database, context, recorder, state, name, err) ||
	    duckdb_v2_vcatalog_table_create_get_schema(table_info, &schema, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_create_get_column_count(table_info, &column_count, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_create_get_constraint_count(table_info, &constraint_count, err) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_table_builder_create(schema, name.data(), name.size(), &builder, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	std::string description;
	auto rc = DUCKDB_V2_ERROR_NONE;
	for (idx_t i = 0; i < column_count && rc == DUCKDB_V2_ERROR_NONE; i++) {
		rc = AddCreatedColumn(table_info, i, builder, description, err);
	}
	for (idx_t i = 0; i < constraint_count && rc == DUCKDB_V2_ERROR_NONE; i++) {
		const char *sql = nullptr;
		idx_t sql_length = 0;
		rc = duckdb_v2_vcatalog_table_create_get_constraint(table_info, i, &sql, &sql_length, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			description += " | " + std::string(sql, sql_length);
		}
	}
	duckdb_v2_vcatalog_table_handle table = nullptr;
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_table_builder_build(builder, &table, err);
	}
	duckdb_v2_vcatalog_table_builder_destroy(&builder);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->table_creates++;
	recorder->last_table_create = description;
	recorder->last_table_columns = DescribeColumns(table);
	recorder->table_column_bounds_checked = ChecksColumnBounds(table);
	state->created_tables.push_back({name, schema, table});
	duckdb_v2_vcatalog_table_create_set_table(table_info, table, err);
}

void TableDrop(duckdb_v2_vcatalog_entry_drop_info_handle info, duckdb_v2_vcatalog_table_drop_info_handle table_info,
               duckdb_v2_vcatalog_database_handle database, duckdb_v2_context_handle context,
               duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	duckdb_v2_vcatalog_table_handle table = nullptr;
	if (!BeginDrop(info, database, context, recorder, state, err) ||
	    duckdb_v2_vcatalog_table_drop_get_table(table_info, &table, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (auto entry = state->created_tables.begin(); entry != state->created_tables.end(); entry++) {
		if (entry->entry == table) {
			recorder->table_drops++;
			state->dropped_tables.push_back(table);
			state->created_tables.erase(entry);
			return;
		}
	}
	Fail(err, "the table is built in");
}

void ViewCreate(duckdb_v2_vcatalog_entry_create_info_handle info, duckdb_v2_vcatalog_view_create_info_handle view_info,
                duckdb_v2_vcatalog_database_handle database, duckdb_v2_context_handle context,
                duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	std::string name;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	const char *query = nullptr;
	idx_t query_length = 0;
	idx_t alias_count = 0;
	duckdb_v2_vcatalog_view_builder_handle builder = nullptr;
	if (!BeginCreate(info, database, context, recorder, state, name, err) ||
	    duckdb_v2_vcatalog_view_create_get_schema(view_info, &schema, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_view_create_get_query(view_info, &query, &query_length, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_view_create_get_column_alias_count(view_info, &alias_count, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_view_builder_create(schema, name.data(), name.size(), &builder, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	std::string description = "(";
	auto rc = duckdb_v2_vcatalog_view_builder_set_query(builder, query, query_length, err);
	for (idx_t i = 0; i < alias_count && rc == DUCKDB_V2_ERROR_NONE; i++) {
		const char *alias = nullptr;
		idx_t alias_length = 0;
		rc = duckdb_v2_vcatalog_view_create_get_column_alias(view_info, i, &alias, &alias_length, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			rc = duckdb_v2_vcatalog_view_builder_add_column_alias(builder, alias, alias_length, err);
			description += (i > 0 ? ", " : "") + std::string(alias, alias_length);
		}
	}
	duckdb_v2_vcatalog_view_handle view = nullptr;
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_vcatalog_view_builder_build(builder, &view, err);
	}
	duckdb_v2_vcatalog_view_builder_destroy(&builder);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->view_creates++;
	recorder->last_view_create = description + ") AS " + std::string(query, query_length);
	state->created_views.push_back({name, schema, view});
	duckdb_v2_vcatalog_view_create_set_view(view_info, view, err);
}

void ViewDrop(duckdb_v2_vcatalog_entry_drop_info_handle info, duckdb_v2_vcatalog_view_drop_info_handle view_info,
              duckdb_v2_vcatalog_database_handle database, duckdb_v2_context_handle context,
              duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	duckdb_v2_vcatalog_view_handle view = nullptr;
	if (!BeginDrop(info, database, context, recorder, state, err) ||
	    duckdb_v2_vcatalog_view_drop_get_view(view_info, &view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (auto entry = state->created_views.begin(); entry != state->created_views.end(); entry++) {
		if (entry->entry == view) {
			recorder->view_drops++;
			state->dropped_views.push_back(view);
			state->created_views.erase(entry);
			return;
		}
	}
	Fail(err, "the view is built in");
}

// A vcatalog with every callback set, recording into `recorder`.
duckdb_v2_vcatalog_handle MakeVCatalog(duckdb_v2_factory_handle factory, std::string_view name, Recorder *recorder) {
	duckdb_v2_vcatalog_handle vcatalog = nullptr;
	REQUIRE(duckdb_v2_vcatalog_create(factory, &vcatalog, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (!name.empty()) {
		REQUIRE(duckdb_v2_vcatalog_set_name(vcatalog, name.data(), name.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	REQUIRE(duckdb_v2_vcatalog_set_attach_callback(vcatalog, Attach, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_begin_callback(vcatalog, Begin, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_commit_callback(vcatalog, Commit, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_rollback_callback(vcatalog, Rollback, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_find_callback(vcatalog, SchemaFind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_list_callback(vcatalog, SchemaList, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_find_callback(vcatalog, TableFind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_list_callback(vcatalog, TableList, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_create_callback(vcatalog, SchemaCreate, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_drop_callback(vcatalog, SchemaDrop, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_create_callback(vcatalog, TableCreate, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_drop_callback(vcatalog, TableDrop, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_view_create_callback(vcatalog, ViewCreate, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_view_drop_callback(vcatalog, ViewDrop, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (recorder) {
		duckdb_v2_opaque data = {recorder, nullptr, nullptr};
		REQUIRE(duckdb_v2_vcatalog_set_user_data(vcatalog, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	return vcatalog;
}

DUCKDB_V2_ERROR RegisterVCatalog(duckdb_v2_connection_handle conn, duckdb_v2_vcatalog_handle vcatalog) {
	auto rc = duckdb_v2_connection_register_vcatalog(conn, vcatalog, nullptr);
	duckdb_v2_vcatalog_destroy(&vcatalog);
	return rc;
}

// Runs a statement expected to fail, returning the text of its error, or an empty string if it succeeded.
std::string ErrorText(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_error_info_handle err = nullptr;
	duckdb_v2_result_handle result = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	auto status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	while (rc == DUCKDB_V2_ERROR_NONE && status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_step(result, &chunk, &status, &err);
		duckdb_v2_data_chunk_destroy(&chunk);
		if (rc == DUCKDB_V2_ERROR_NONE && status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			rc = duckdb_v2_result_wait(result, &err);
		}
	}
	duckdb_v2_result_destroy(&result);
	std::string message;
	if (rc != DUCKDB_V2_ERROR_NONE && err) {
		const char *text = nullptr;
		idx_t text_length = 0;
		if (duckdb_v2_error_info_get_text(err, &text, &text_length) == DUCKDB_V2_ERROR_NONE) {
			message.assign(text, text_length);
		}
	}
	duckdb_v2_error_info_destroy(&err);
	return message;
}

bool Contains(const std::string &haystack, std::string_view needle) {
	return haystack.find(needle) != std::string::npos;
}

idx_t CountRows(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto count = DrainRowCount(result);
	duckdb_v2_result_destroy(&result);
	return count;
}

// A fixture with the vcatalog registered under TYPE_NAME, and every transaction touching every attached database.
struct VCatalogFixture {
	Recorder recorder;
	EnvFixture fx;

	VCatalogFixture() {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "SET immediate_transaction_mode = true") == DUCKDB_V2_ERROR_NONE);
	}

	void ResetCounts() {
		recorder.begins = 0;
		recorder.commits = 0;
		recorder.rollbacks = 0;
		recorder.transaction_states_destroyed = 0;
	}
};

} // namespace

TEST_CASE("V2 vcatalog: ATTACH invokes the attach callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;

	REQUIRE(RunSql(conn, "ATTACH 'some/path' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.attaches == 1);
	REQUIRE(recorder.name == "v");
	REQUIRE(recorder.path == "some/path");
	REQUIRE_FALSE(recorder.attach_read_only);
	REQUIRE(recorder.secret.empty());
	REQUIRE(recorder.states_seen);

	SECTION("the type can be given as a path prefix") {
		REQUIRE(RunSql(conn, "ATTACH 'vtest:other/path' AS w") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.attaches == 2);
		REQUIRE(recorder.name == "w");
		REQUIRE(recorder.path == "other/path");
	}
	SECTION("read-only attaches are reported") {
		REQUIRE(RunSql(conn, "ATTACH 'p' AS r (TYPE vtest, READ_ONLY)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.attach_read_only);
	}
	SECTION("options are retrieved case-insensitively") {
		REQUIRE(RunSql(conn, "ATTACH 'p' AS o (TYPE vtest, secret 'hunter2')") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.secret == "hunter2");
	}
	SECTION("DETACH destroys the catalog state") {
		REQUIRE(recorder.catalog_states_destroyed == 0);
		REQUIRE(RunSql(conn, "DETACH v") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.catalog_states_destroyed == 1);
	}
}

TEST_CASE("V2 vcatalog: a failing ATTACH destroys the catalog state", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;

	SECTION("the attach callback reports an error") {
		recorder.fail_attach = true;
		REQUIRE(Contains(ErrorText(conn, "ATTACH 'p' AS v (TYPE vtest)"), "attach refused"));
	}
	SECTION("an option the attach callback did not retrieve") {
		REQUIRE(Contains(ErrorText(conn, "ATTACH 'p' AS v (TYPE vtest, bogus 1)"), "Unrecognized option"));
	}
	REQUIRE(recorder.attaches == 1);
	REQUIRE(recorder.catalog_states_destroyed == 1);
	// The name is free again
	recorder.fail_attach = false;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: transactions begin, commit and roll back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	SECTION("COMMIT") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE_FALSE(recorder.begin_read_only);
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.commits == 1);
		REQUIRE(recorder.rollbacks == 0);
	}
	SECTION("ROLLBACK") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE(recorder.commits == 0);
		REQUIRE(recorder.rollbacks == 1);
	}
	SECTION("BEGIN TRANSACTION READ ONLY") {
		REQUIRE(RunSql(conn, "BEGIN TRANSACTION READ ONLY") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begin_read_only);
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("a lookup in auto-commit mode begins a transaction") {
		REQUIRE(RunSql(conn, "SET immediate_transaction_mode = false") == DUCKDB_V2_ERROR_NONE);
		vfx.ResetCounts();
		// No schema exists yet, so the statement fails and its transaction rolls back
		REQUIRE(RunSql(conn, "SELECT * FROM v.main.t") != DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE(recorder.rollbacks == 1);
	}
	SECTION("a statement failing in an explicit transaction rolls back only when the transaction ends") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "SELECT error('boom')") != DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.rollbacks == 0);
		// COMMIT of the invalidated transaction silently rolls it back
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.commits == 0);
		REQUIRE(recorder.rollbacks == 1);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.transaction_states_destroyed == recorder.begins);
}

TEST_CASE("V2 vcatalog: transactions on read-only databases are read-only", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest, READ_ONLY)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.begin_read_only);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: a failed commit rolls the transaction back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_commit = true;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Contains(ErrorText(conn, "COMMIT"), "commit refused"));
	REQUIRE(recorder.commits == 1);
	REQUIRE(recorder.rollbacks == 1);
	REQUIRE(recorder.transaction_states_destroyed == 1);
	REQUIRE(recorder.states_seen);

	// The transaction has ended, and the connection is usable
	recorder.fail_commit = false;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.commits == 2);
}

TEST_CASE("V2 vcatalog: a failed rollback still ends the transaction", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_rollback = true;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Contains(ErrorText(conn, "ROLLBACK"), "rollback refused"));
	REQUIRE(recorder.rollbacks == 1);
	REQUIRE(recorder.transaction_states_destroyed == 1);

	recorder.fail_rollback = false;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.commits == 1);
}

TEST_CASE("V2 vcatalog: a failed begin neither commits nor rolls back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "SET immediate_transaction_mode = false") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_begin = true;
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.main.t"), "begin refused"));
	REQUIRE(recorder.begins == 1);
	REQUIRE(recorder.commits == 0);
	REQUIRE(recorder.rollbacks == 0);
	REQUIRE(recorder.transaction_states_destroyed == 1);
}

TEST_CASE("V2 vcatalog: registration validates the vcatalog", "[capi_v2][vcatalog]") {
	Recorder recorder;
	EnvFixture fx;

	SECTION("a name is required") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, std::string_view(), &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("an attach callback is required") {
		auto vcatalog = MakeVCatalog(fx.factory, TYPE_NAME, &recorder);
		REQUIRE(duckdb_v2_vcatalog_set_attach_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, vcatalog) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("a name can only be registered once") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, "VTEST", &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("DuckDB's own database type cannot be taken") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, "duckdb", &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("registration is not transactional") {
		REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("null handles are rejected, and destroy is null-safe") {
		REQUIRE(duckdb_v2_connection_register_vcatalog(fx.conn, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vcatalog_create(fx.factory, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vcatalog_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vcatalog_handle vcatalog = nullptr;
		REQUIRE(duckdb_v2_vcatalog_destroy(&vcatalog) == DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 vcatalog: the user data lives as long as the instance", "[capi_v2][vcatalog]") {
	bool destroyed = false;
	{
		EnvFixture fx;
		auto vcatalog = MakeVCatalog(fx.factory, TYPE_NAME, nullptr);
		duckdb_v2_opaque data = {&destroyed, [](void *ptr) { *static_cast<bool *>(ptr) = true; }, nullptr};
		REQUIRE(duckdb_v2_vcatalog_set_user_data(vcatalog, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, vcatalog) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(destroyed);
	}
	REQUIRE(destroyed);
}

TEST_CASE("V2 vcatalog: schemas are found through the find callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("a schema the find callback reports") {
		REQUIRE(RunSql(conn, "USE v.other") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_find == "other");
	}
	SECTION("the default schema") {
		REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_find == "main");
	}
	SECTION("the find callback decides how names match") {
		REQUIRE(RunSql(conn, "USE v.OTHER") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_find == "OTHER");
	}
	SECTION("a name the find callback reports nothing for") {
		REQUIRE(Contains(ErrorText(conn, "USE v.nope"), "nope"));
	}
	SECTION("an error the find callback reports fails the statement") {
		recorder.fail_schema_find = true;
		REQUIRE(Contains(ErrorText(conn, "USE v.other"), "schema find refused"));
	}
	REQUIRE(recorder.schema_finds > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: schemas are listed through the list callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	// "main" is added twice, and listed once
	REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v'") == 2);
	REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v' AND schema_name = "
	                        "'other'") == 1);
	REQUIRE(recorder.schema_lists > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: the implementation owns its schemas", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE memory") == DUCKDB_V2_ERROR_NONE);
	// Kept in the catalog state across transactions, and destroyed with it
	REQUIRE(recorder.schemas_destroyed == 0);
	REQUIRE(recorder.entry_states_destroyed == 0);
	REQUIRE(RunSql(conn, "DETACH v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.schemas_destroyed == 2);
	// The state a schema shares with its builder is destroyed with the last of them
	REQUIRE(recorder.entry_states_destroyed == 2);
}

TEST_CASE("V2 vcatalog: a schema builder builds schemas sharing its state", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.builders_reusable);
	// The state of a builder that built nothing is destroyed with the builder
	REQUIRE(recorder.unbuilt_states_destroyed == 1);
	// A state outlives its builder, and the other schemas sharing it
	REQUIRE(recorder.entry_states_destroyed == 0);
}

TEST_CASE("V2 vcatalog: the find callback cannot report a schema of another database", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "ATTACH 'q' AS w (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE memory") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.first_schema);

	recorder.resolve_to_first_schema = true;
	auto error = ErrorText(conn, "USE w");
	CAPTURE(error, recorder.last_schema_find);
	REQUIRE(Contains(error, "different database"));
}

TEST_CASE("V2 vcatalog: schema arguments are checked", "[capi_v2][vcatalog]") {
	REQUIRE(duckdb_v2_vcatalog_schema_builder_create(nullptr, "s", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_builder_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_schema_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_vcatalog_schema_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_builder_set_state(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_builder_build(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_vcatalog_schema_destroy(&schema) == DUCKDB_V2_ERROR_NONE);
	void *state = nullptr;
	REQUIRE(duckdb_v2_vcatalog_schema_get_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: tables are found through the find callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("a table's columns") {
		REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
		REQUIRE(recorder.last_table_find == "t1");
		REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.T2')") == 1);
		REQUIRE(recorder.last_table_find == "T2");
	}
	SECTION("a name the find callback reports nothing for") {
		REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.main.nope"), "nope"));
		REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.other.t1"), "t1"));
	}
	REQUIRE(recorder.table_finds > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: tables are listed through the list callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	// "t1" is added twice, and listed once
	REQUIRE(CountRows(conn, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'v'") == 2);
	REQUIRE(CountRows(conn, "SELECT column_name FROM duckdb_columns() WHERE database_name = 'v' AND table_name = "
	                        "'t1'") == 2);
	// The two tables and the two views
	REQUIRE(CountRows(conn, "SELECT * FROM (SHOW ALL TABLES) WHERE database = 'v'") == 4);
	REQUIRE(recorder.table_lists > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: a table builder builds tables with columns", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	REQUIRE(recorder.empty_table_rejected);
	REQUIRE(recorder.duplicate_column_rejected);
	REQUIRE(recorder.not_null_setter_checked);
	REQUIRE(recorder.builders_reusable);
	// A column added to the builder after a table was built is not added to that table
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t2')") == 1);
}

TEST_CASE("V2 vcatalog: the find callback cannot report a table of another schema", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	// Build the tables of "main" first
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	recorder.resolve_in_any_schema = true;
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM pragma_table_info('v.other.t1')"), "different schema"));
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.other.v1"), "different schema"));
}

TEST_CASE("V2 vcatalog: tables are scanned with their scan function", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("every row and column") {
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.t1") == 3);
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.t1 WHERE id = 1 AND name = 'row1'") == 1);
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.t2 WHERE x = 3.0") == 1);
	}
	SECTION("some of the columns") {
		REQUIRE(CountRows(conn, "SELECT name FROM v.main.t1 WHERE name = 'row2'") == 1);
	}
	SECTION("none of the columns") {
		REQUIRE(CountRows(conn, "SELECT c FROM (SELECT count(*) AS c FROM v.main.t1) WHERE c = 3") == 1);
	}
	SECTION("a table scanned twice is bound twice") {
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.t1 AS a JOIN v.main.t1 AS b USING (id)") == 3);
		REQUIRE(recorder.scan_binds >= 2);
	}
	SECTION("DESCRIBE") {
		REQUIRE(CountRows(conn, "DESCRIBE v.main.t1") == 2);
	}
	REQUIRE(recorder.scan_binds > 0);
	REQUIRE(recorder.invalid_order_rejected);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
	// The bind data of a scan is destroyed with its statement
	REQUIRE(recorder.scan_bind_data_destroyed == recorder.scan_binds);
}

TEST_CASE("V2 vcatalog: an error the scan bind callback reports fails the statement", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	recorder.fail_scan_bind = true;
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.main.t1"), "scan bind refused"));
	// The bind data set before the error is destroyed
	REQUIRE(recorder.scan_binds > 0);
	REQUIRE(recorder.scan_bind_data_destroyed == recorder.scan_binds);
}

TEST_CASE("V2 vcatalog: a table without a scan function cannot be read", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	recorder.no_scan_functions = true;
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	auto error = ErrorText(conn, "SELECT * FROM v.main.t1");
	CAPTURE(error);
	REQUIRE(Contains(error, "no scan function"));
	REQUIRE(recorder.scan_binds == 0);
}

TEST_CASE("V2 vcatalog: a scan function needs an exec callback and takes no parameters", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	REQUIRE(recorder.invalid_scan_functions_rejected);
}

TEST_CASE("V2 vcatalog: views are found through the table find callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("a view runs its query") {
		// "t1" is found next to the view, although the connection is not using "v"
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.v1") == 2);
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.V1 WHERE id = 2 AND name = 'row2'") == 1);
		REQUIRE(recorder.scan_binds > 0);
	}
	SECTION("a view built from a CREATE VIEW statement") {
		// The statement names the view and its column; the schema it names is ignored
		REQUIRE(CountRows(conn, "SELECT y FROM v.main.v2 WHERE y = 3.0") == 1);
	}
	SECTION("the columns of a view") {
		REQUIRE(CountRows(conn, "SELECT * FROM (DESCRIBE v.main.v1)") == 2);
		REQUIRE(CountRows(conn, "SELECT * FROM (DESCRIBE v.main.v2) WHERE column_name = 'y'") == 1);
	}
	SECTION("a view and a table of the same statement") {
		REQUIRE(CountRows(conn, "SELECT * FROM v.main.v1 JOIN v.main.t1 USING (id)") == 2);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: views are listed through the table list callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	// "v1" is added twice, and listed once
	REQUIRE(CountRows(conn, "SELECT view_name FROM duckdb_views() WHERE database_name = 'v'") == 2);
	REQUIRE(CountRows(conn, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'v'") == 2);
	// The columns of a listed view are those of its query
	REQUIRE(CountRows(conn, "SELECT column_name FROM duckdb_columns() WHERE database_name = 'v' AND table_name = "
	                        "'v2' AND column_name = 'y'") == 1);
	REQUIRE(recorder.table_lists > 0);
	REQUIRE(recorder.states_seen);
}

TEST_CASE("V2 vcatalog: a view builder needs a query DuckDB can parse", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	REQUIRE(recorder.invalid_views_rejected);
}

TEST_CASE("V2 vcatalog: view arguments are checked", "[capi_v2][vcatalog]") {
	REQUIRE(duckdb_v2_vcatalog_view_builder_create(nullptr, "v", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_create_from_sql(nullptr, "v", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_view_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_vcatalog_view_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_set_query(nullptr, "v", 1, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_add_column_alias(nullptr, "v", 1, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_set_state(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_builder_build(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_view_handle view = nullptr;
	REQUIRE(duckdb_v2_vcatalog_view_destroy(&view) == DUCKDB_V2_ERROR_NONE);
	void *state = nullptr;
	REQUIRE(duckdb_v2_vcatalog_view_get_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_find_set_view(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_list_add_view(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: the implementation owns its tables and views", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.t1')") == 2);
	REQUIRE(recorder.tables_destroyed == 0);
	REQUIRE(recorder.views_destroyed == 0);
	REQUIRE(recorder.entry_states_destroyed == 0);
	REQUIRE(RunSql(conn, "DETACH v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.tables_destroyed == 2);
	REQUIRE(recorder.views_destroyed == 2);
	// The states of the two tables, the two views and the two schemas
	REQUIRE(recorder.entry_states_destroyed == 6);
}

TEST_CASE("V2 vcatalog: table arguments are checked", "[capi_v2][vcatalog]") {
	REQUIRE(duckdb_v2_vcatalog_table_builder_create(nullptr, "t", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_table_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_vcatalog_table_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_add_column(nullptr, "c", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_set_column_not_null(nullptr, 0, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_set_state(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_set_scan_function(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_set_scan_bind_callback(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_builder_build(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_table_handle table = nullptr;
	REQUIRE(duckdb_v2_vcatalog_table_destroy(&table) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	const char *name = nullptr;
	idx_t name_length = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	bool not_null = false;
	REQUIRE(duckdb_v2_vcatalog_table_get_column_count(nullptr, &count, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_get_column_name(nullptr, 0, &name, &name_length, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_get_column_type(nullptr, 0, &type, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_is_column_not_null(nullptr, 0, &not_null, nullptr) != DUCKDB_V2_ERROR_NONE);
	void *state = nullptr;
	REQUIRE(duckdb_v2_vcatalog_table_scan_bind_get_transaction_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_scan_bind_get_table(nullptr, &table, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_scan_bind_set_bind_data(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_scan_bind_set_cardinality(nullptr, 1, true, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_scan_bind_set_order_preservation(nullptr, DUCKDB_V2_ORDER_PRESERVATION_NO_ORDER,
	                                                                  nullptr) != DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: schemas are created and dropped through their callbacks", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	REQUIRE(RunSql(conn, "CREATE SCHEMA v.s1") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.schema_creates == 1);
	REQUIRE(recorder.last_create == "s1");
	REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v'") == 3);

	SECTION("a schema that exists is not created again") {
		auto error = ErrorText(conn, "CREATE SCHEMA v.s1");
		CAPTURE(error);
		REQUIRE(Contains(error, "already exists"));
		REQUIRE(RunSql(conn, "CREATE SCHEMA IF NOT EXISTS v.s1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.schema_creates == 1);
		REQUIRE(recorder.schema_drops == 0);
	}
	SECTION("OR REPLACE drops the schema that exists first") {
		REQUIRE(RunSql(conn, "CREATE OR REPLACE SCHEMA v.s1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.schema_drops == 1);
		REQUIRE(recorder.schema_creates == 2);
		REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v'") == 3);
	}
	SECTION("a schema is dropped") {
		REQUIRE(RunSql(conn, "DROP SCHEMA v.s1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.schema_drops == 1);
		REQUIRE_FALSE(recorder.last_drop_cascade);
		REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v'") == 2);
		// Nothing is left to drop
		REQUIRE_FALSE(ErrorText(conn, "DROP SCHEMA v.s1").empty());
		REQUIRE(RunSql(conn, "DROP SCHEMA IF EXISTS v.s1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.schema_drops == 1);
	}
	SECTION("CASCADE is passed on") {
		REQUIRE(RunSql(conn, "DROP SCHEMA v.s1 CASCADE") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_drop_cascade);
	}
	SECTION("tables are created in a created schema") {
		REQUIRE(RunSql(conn, "CREATE TABLE v.s1.c1 (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.s1.c1')") == 1);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: tables are created and dropped through their callbacks", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	REQUIRE(RunSql(conn, "CREATE TABLE v.main.c1 (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL DEFAULT 'x', score "
	                     "DOUBLE CHECK (score > 0), UNIQUE (name, score))") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.table_creates == 1);
	REQUIRE(recorder.last_create == "c1");
	CAPTURE(recorder.last_table_create);
	// The columns, with their NOT NULL and default
	REQUIRE(Contains(recorder.last_table_create, "id INTEGER"));
	REQUIRE(Contains(recorder.last_table_create, "name VARCHAR NOT NULL DEFAULT 'x'"));
	REQUIRE(Contains(recorder.last_table_create, "score DOUBLE"));
	// The other constraints, in the form of table constraints
	REQUIRE(Contains(recorder.last_table_create, "| PRIMARY KEY(id)"));
	REQUIRE(Contains(recorder.last_table_create, "| UNIQUE(\"name\", score)"));
	REQUIRE(Contains(recorder.last_table_create, "| CHECK("));
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.c1')") == 3);
	REQUIRE(CountRows(conn, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'v'") == 3);
	// The table the callback built reports its columns, and the ones it declared NOT NULL are not nullable: the
	// column DuckDB was told is NOT NULL, and the one of the primary key
	REQUIRE(recorder.last_table_columns == "id INTEGER NOT NULL, name VARCHAR NOT NULL, score DOUBLE");
	REQUIRE(recorder.table_column_bounds_checked);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.c1') WHERE \"notnull\"") == 2);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.c1') WHERE NOT \"notnull\" AND name = 'score'") ==
	        1);

	SECTION("a table that exists is not created again") {
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.c1 (i INTEGER)"), "already exists"));
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.v1 (i INTEGER)"), "already exists"));
		REQUIRE(RunSql(conn, "CREATE TABLE IF NOT EXISTS v.main.c1 (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.table_creates == 1);
		REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.c1')") == 3);
	}
	SECTION("OR REPLACE drops the table that exists first") {
		REQUIRE(RunSql(conn, "CREATE OR REPLACE TABLE v.main.c1 (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.table_drops == 1);
		REQUIRE(recorder.table_creates == 2);
		REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.main.c1')") == 1);
		// A view is not replaced by a table
		REQUIRE(Contains(ErrorText(conn, "CREATE OR REPLACE TABLE v.main.v1 (i INTEGER)"), "trying to replace"));
	}
	SECTION("a table is dropped") {
		REQUIRE(RunSql(conn, "DROP TABLE v.main.c1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.table_drops == 1);
		REQUIRE(CountRows(conn, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'v'") == 2);
		// Nothing is left to drop
		REQUIRE_FALSE(ErrorText(conn, "DROP TABLE v.main.c1").empty());
		REQUIRE(RunSql(conn, "DROP TABLE IF EXISTS v.main.c1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.table_drops == 1);
	}
	SECTION("a table is not dropped as a view") {
		REQUIRE(Contains(ErrorText(conn, "DROP VIEW v.main.c1"), "trying to drop"));
		REQUIRE(recorder.table_drops == 0);
		REQUIRE(recorder.view_drops == 0);
	}
	SECTION("what the callbacks cannot be given") {
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.g (i INTEGER, j AS (i + 1))"), "Generated columns"));
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.a AS SELECT 42 AS i"), "not supported"));
		REQUIRE(recorder.table_creates == 1);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: views are created and dropped through their callbacks", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	REQUIRE(RunSql(conn, "CREATE VIEW v.main.cv (a, b) AS SELECT id, name FROM v.main.t1 WHERE id < 2") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.view_creates == 1);
	REQUIRE(recorder.last_create == "cv");
	CAPTURE(recorder.last_view_create);
	REQUIRE(Contains(recorder.last_view_create, "(a, b) AS SELECT"));
	REQUIRE(Contains(recorder.last_view_create, "t1"));
	// The view built from the query the callback was given runs it
	REQUIRE(CountRows(conn, "SELECT a, b FROM v.main.cv WHERE b = 'row1'") == 1);
	REQUIRE(CountRows(conn, "SELECT view_name FROM duckdb_views() WHERE database_name = 'v'") == 3);

	SECTION("a view that exists is not created again") {
		REQUIRE(Contains(ErrorText(conn, "CREATE VIEW v.main.cv AS SELECT 42"), "already exists"));
		REQUIRE(RunSql(conn, "CREATE VIEW IF NOT EXISTS v.main.cv AS SELECT 42") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.view_creates == 1);
	}
	SECTION("OR REPLACE drops the view that exists first") {
		REQUIRE(RunSql(conn, "CREATE OR REPLACE VIEW v.main.cv AS SELECT 42 AS answer") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.view_drops == 1);
		REQUIRE(recorder.view_creates == 2);
		REQUIRE(CountRows(conn, "SELECT answer FROM v.main.cv WHERE answer = 42") == 1);
		// A table is not replaced by a view
		REQUIRE(Contains(ErrorText(conn, "CREATE OR REPLACE VIEW v.main.t1 AS SELECT 42"), "trying to replace"));
	}
	SECTION("a view is dropped") {
		REQUIRE(RunSql(conn, "DROP VIEW v.main.cv") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.view_drops == 1);
		REQUIRE(CountRows(conn, "SELECT view_name FROM duckdb_views() WHERE database_name = 'v'") == 2);
		REQUIRE(RunSql(conn, "DROP VIEW IF EXISTS v.main.cv") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.view_drops == 1);
	}
	SECTION("a view is not dropped as a table") {
		REQUIRE(Contains(ErrorText(conn, "DROP TABLE v.main.cv"), "trying to drop"));
		REQUIRE(recorder.view_drops == 0);
		REQUIRE(recorder.table_drops == 0);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: the attach callback sets the default schema", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(duckdb_v2_vcatalog_attach_set_default_schema(nullptr, "s", 1, nullptr) != DUCKDB_V2_ERROR_NONE);

	// Without one, a name without a schema refers to "main"
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.empty_default_schema_rejected);
	REQUIRE(RunSql(conn, "CREATE SCHEMA v.extra") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "CREATE TABLE v.extra.only_here (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('v.t1')") == 2);
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM pragma_table_info('v.only_here')"), "does not exist"));

	// With one, it refers to that schema, which need not exist yet when the database is attached
	REQUIRE(RunSql(conn, "ATTACH 'p' AS d (TYPE vtest, default_schema 'extra')") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.default_schema == "extra");
	REQUIRE(RunSql(conn, "CREATE SCHEMA d.extra") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "CREATE TABLE d.extra.only_here (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('d.only_here')") == 1);
	REQUIRE(CountRows(conn, "SELECT * FROM pragma_table_info('d.main.t1')") == 2);
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM pragma_table_info('d.t1')"), "does not exist"));
}

TEST_CASE("V2 vcatalog: a create callback reports what it created, or an error", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("a callback reporting nothing") {
		recorder.skip_create_report = true;
		REQUIRE(Contains(ErrorText(conn, "CREATE SCHEMA v.s1"), "did not report the schema"));
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.c1 (i INTEGER)"), "did not report the table"));
		REQUIRE(Contains(ErrorText(conn, "CREATE VIEW v.main.cv AS SELECT 42"), "did not report the view"));
	}
	SECTION("a callback reporting an error") {
		REQUIRE(RunSql(conn, "CREATE SCHEMA v.s1") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "CREATE TABLE v.main.c1 (i INTEGER)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "CREATE VIEW v.main.cv AS SELECT 42") == DUCKDB_V2_ERROR_NONE);
		recorder.fail_ddl = true;
		REQUIRE(Contains(ErrorText(conn, "CREATE SCHEMA v.s2"), "create refused"));
		REQUIRE(Contains(ErrorText(conn, "CREATE TABLE v.main.c2 (i INTEGER)"), "create refused"));
		REQUIRE(Contains(ErrorText(conn, "CREATE VIEW v.main.cv2 AS SELECT 42"), "create refused"));
		REQUIRE(Contains(ErrorText(conn, "DROP SCHEMA v.s1"), "drop refused"));
		REQUIRE(Contains(ErrorText(conn, "DROP TABLE v.main.c1"), "drop refused"));
		REQUIRE(Contains(ErrorText(conn, "DROP VIEW v.main.cv"), "drop refused"));
		// A drop that fails keeps OR REPLACE from creating
		REQUIRE(Contains(ErrorText(conn, "CREATE OR REPLACE TABLE v.main.c1 (i INTEGER)"), "drop refused"));
		REQUIRE(recorder.table_creates == 1);
	}
}

TEST_CASE("V2 vcatalog: CREATE and DROP need their callbacks", "[capi_v2][vcatalog]") {
	Recorder recorder;
	EnvFixture fx;
	auto vcatalog = MakeVCatalog(fx.factory, TYPE_NAME, &recorder);
	REQUIRE(duckdb_v2_vcatalog_set_schema_create_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_drop_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_create_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_table_drop_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_view_create_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_view_drop_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RegisterVCatalog(fx.conn, vcatalog) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	REQUIRE(Contains(ErrorText(fx.conn, "CREATE SCHEMA v.s1"), "Creating schemas is not supported"));
	REQUIRE(Contains(ErrorText(fx.conn, "CREATE TABLE v.main.c1 (i INTEGER)"), "Creating tables is not supported"));
	REQUIRE(Contains(ErrorText(fx.conn, "CREATE VIEW v.main.cv AS SELECT 42"), "Creating views is not supported"));
	REQUIRE(Contains(ErrorText(fx.conn, "DROP SCHEMA v.other"), "Dropping schemas is not supported"));
	REQUIRE(Contains(ErrorText(fx.conn, "DROP TABLE v.main.t1"), "Dropping tables is not supported"));
	REQUIRE(Contains(ErrorText(fx.conn, "DROP VIEW v.main.v1"), "Dropping views is not supported"));
}

TEST_CASE("V2 vcatalog: nothing is created or dropped in a read-only database", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS r (TYPE vtest, READ_ONLY)") == DUCKDB_V2_ERROR_NONE);

	for (auto sql : {"CREATE SCHEMA r.s1", "CREATE TABLE r.main.c1 (i INTEGER)", "CREATE VIEW r.main.cv AS SELECT 42",
	                 "DROP TABLE r.main.t1", "DROP SCHEMA r.other"}) {
		auto error = ErrorText(conn, sql);
		CAPTURE(sql, error);
		REQUIRE(Contains(error, "read-only"));
	}
	REQUIRE(recorder.schema_creates + recorder.table_creates + recorder.view_creates == 0);
	REQUIRE(recorder.last_create.empty());
}

TEST_CASE("V2 vcatalog: create and drop arguments are checked", "[capi_v2][vcatalog]") {
	const char *text = nullptr;
	idx_t length = 0;
	idx_t count = 0;
	void *state = nullptr;
	bool flag = false;
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	duckdb_v2_vcatalog_table_handle table = nullptr;
	duckdb_v2_vcatalog_view_handle view = nullptr;
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_vcatalog_set_schema_create_callback(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_view_drop_callback(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_entry_create_get_name(nullptr, &text, &length, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_entry_create_get_transaction_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_create_set_schema(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_schema(nullptr, &schema, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_column_count(nullptr, &count, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_column_name(nullptr, 0, &text, &length, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_column_type(nullptr, 0, &type, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_is_column_not_null(nullptr, 0, &flag, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_column_default(nullptr, 0, &text, &length, &flag, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_constraint_count(nullptr, &count, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_get_constraint(nullptr, 0, &text, &length, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_create_set_table(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_create_get_schema(nullptr, &schema, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_create_get_query(nullptr, &text, &length, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_create_get_column_alias_count(nullptr, &count, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_create_get_column_alias(nullptr, 0, &text, &length, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_create_set_view(nullptr, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_entry_drop_is_cascade(nullptr, &flag, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_entry_drop_get_transaction_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_drop_get_schema(nullptr, &schema, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_table_drop_get_table(nullptr, &table, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_view_drop_get_view(nullptr, &view, nullptr) != DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
