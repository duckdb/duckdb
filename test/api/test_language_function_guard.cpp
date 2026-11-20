#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/function/language_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_data/alter_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/logical_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// SQL cannot reach the invariants below: it cannot create an internal macro entry, the stock grammar never nests an
// unresolved type inside a container, CREATE FUNCTION ... LANGUAGE carries one member per statement, an undeclared
// RETURNS type with metadata needs a hand-built member, the validator's type views are a C++ interface over types
// with extension metadata only programmatic types carry, and ClientContext::RegisterFunction is a C++ registration
// path. The SQL-observable behavior stays in test/sql/function/language_function.

namespace {

static void ConstantCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement(ConstantExpression::FromValue(Value(input.source)));
}

static void DirectCallUnsupported(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("test language functions cannot be called directly");
}

//! Register a function language whose members return their source, the way an extension registers a function
static void RegisterTestLanguage(DatabaseInstance &db, const string &language,
                                 scalar_function_create_validate_t validate = nullptr) {
	ScalarFunction function(Identifier(language), vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        DirectCallUnsupported, nullptr, nullptr, nullptr, LogicalType::ANY);
	function.SetCallReplaceCallback(ConstantCallReplace);
	ScalarFunctionSet set {Identifier(language)};
	set.AddFunction(std::move(function));
	set.DeclareLanguage();
	set.SetCreateValidateCallback(validate);
	CreateScalarFunctionInfo info(std::move(set));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

} // namespace

static unique_ptr<MacroFunction> DateMember() {
	auto member = make_uniq<LanguageMacroFunction>("intlang", "date member");
	member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
	member->types.push_back(LogicalType::DATE);
	return std::move(member);
}

TEST_CASE("Member mutations respect internal entry protection", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterTestLanguage(*db.instance, "intlang");

	// register an internal two-member language entry in the system catalog, the way an extension
	// registering functions does
	CreateMacroInfo info(CatalogType::MACRO_ENTRY);
	info.SetFunctionName(Identifier("internal_fun"));
	info.internal = true;
	auto int_member = make_uniq<LanguageMacroFunction>("intlang", "int member");
	int_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
	int_member->types.push_back(LogicalType::INTEGER);
	info.macros.push_back(std::move(int_member));
	auto varchar_member = make_uniq<LanguageMacroFunction>("intlang", "varchar member");
	varchar_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
	varchar_member->types.push_back(LogicalType::VARCHAR);
	info.macros.push_back(std::move(varchar_member));
	auto &system_catalog = Catalog::GetSystemCatalog(*db.instance);
	auto data = CatalogTransaction::GetSystemTransaction(*db.instance);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	schema.CreateFunction(data, info);

	// member-granular drop from SQL is an alter, which the generic binder refuses for system catalog entries
	auto result = con.Query("DROP FUNCTION internal_fun(INTEGER)");
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->GetError(), "System Catalog"));

	// a CREATE from SQL resolves into the user catalog and creates a shadowing entry there, it
	// never merges into the system entry
	REQUIRE_NO_FAIL(con.Query("CREATE FUNCTION internal_fun(x DATE) LANGUAGE intlang AS 'date member'"));
	result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'internal_fun' AND "
	                   "database_name = 'memory'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DROP FUNCTION internal_fun"));

	// a direct CREATE on the schema never merges into the existing entry, even with internal authority
	CreateMacroInfo create_info(CatalogType::MACRO_ENTRY);
	create_info.SetFunctionName(Identifier("internal_fun"));
	create_info.internal = true;
	create_info.macros.push_back(DateMember());
	auto create_data = CatalogTransaction::GetSystemTransaction(*db.instance);
	REQUIRE_THROWS_WITH(schema.CreateFunction(create_data, create_info), Catch::Matchers::Contains("already exists"));

	// an overload add is an alter of the entry, and the internal protection applies to it
	AlterEntryData add_data(QualifiedName(Identifier(), Identifier::DefaultSchema(), Identifier("internal_fun")),
	                        OnEntryNotFound::THROW_EXCEPTION);
	AddMacroOverloadInfo add_info(add_data, DateMember(), OnCreateConflict::ERROR_ON_CONFLICT);
	auto add_transaction = CatalogTransaction::GetSystemTransaction(*db.instance);
	REQUIRE_THROWS_WITH(schema.Alter(add_transaction, add_info), Catch::Matchers::Contains("internal system entry"));

	// the entry is untouched and still callable
	result = con.Query("SELECT internal_fun(1), internal_fun('a'::VARCHAR)");
	REQUIRE(CHECK_COLUMN(result, 0, {"int member"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"varchar member"}));

	// the drop guard directly: a member drop without authority is rejected ...
	AlterEntryData drop_data(QualifiedName(Identifier(), Identifier::DefaultSchema(), Identifier("internal_fun")),
	                         OnEntryNotFound::THROW_EXCEPTION);
	RemoveMacroOverloadInfo drop_info(drop_data, CatalogType::MACRO_ENTRY, {LogicalType::INTEGER});
	con.BeginTransaction();
	bool threw = false;
	try {
		schema.Alter(schema.GetCatalogTransaction(*con.context), drop_info);
	} catch (std::exception &ex) {
		threw = StringUtil::Contains(ex.what(), "internal system entry");
	}
	REQUIRE(threw);
	con.Rollback();

	// an extension registering internal members one at a time adds them as overload alters that carry
	// the authority the guard checks, like function overloads
	AddMacroOverloadInfo readd_info(add_data, DateMember(), OnCreateConflict::ERROR_ON_CONFLICT);
	readd_info.allow_internal = true;
	auto readd_transaction = CatalogTransaction::GetSystemTransaction(*db.instance);
	schema.Alter(readd_transaction, readd_info);
	result = con.Query("SELECT internal_fun(DATE '2020-01-01'), internal_fun('a'::VARCHAR)");
	REQUIRE(CHECK_COLUMN(result, 0, {"date member"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"varchar member"}));
}

static LogicalType UnresolvedUserType(const string &name) {
	vector<unique_ptr<ParsedExpression>> no_children;
	return LogicalType::UNBOUND(make_uniq<TypeExpression>(Identifier(name), std::move(no_children)));
}

TEST_CASE("Nested unbound member types resolve at CREATE", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterTestLanguage(*db.instance, "nestlang");
	REQUIRE_NO_FAIL(con.Query("CREATE TYPE mood AS ENUM ('happy', 'sad')"));

	// a dialect transformer may emit a concrete container around an unresolved user type, unlike
	// the stock grammar, which wraps the whole unresolved type at the top level
	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	info->SetFunctionName(Identifier("nested_fun"));
	auto list_member = make_uniq<LanguageMacroFunction>("nestlang", "arg:0");
	list_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
	list_member->types.push_back(LogicalType::LIST(UnresolvedUserType("mood")));
	list_member->return_type = LogicalType::LIST(UnresolvedUserType("mood"));
	info->macros.push_back(std::move(list_member));
	auto struct_member = make_uniq<LanguageMacroFunction>("nestlang", "struct member");
	struct_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
	struct_member->types.push_back(LogicalType::STRUCT({{"m", UnresolvedUserType("mood")}}));
	info->macros.push_back(std::move(struct_member));
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	unique_ptr<SQLStatement> sql_statement = std::move(statement);
	auto result = con.Query(std::move(sql_statement));
	REQUIRE_NO_FAIL(*result);

	// the stored signatures are fully resolved: overload matching selects by the nested user type,
	// the RETURNS cast applies, and introspection shows resolved types
	auto call = con.Query("SELECT typeof(nested_fun(['happy'::mood])), nested_fun({'m': 'happy'::mood})");
	REQUIRE(CHECK_COLUMN(call, 0, {"ENUM('happy', 'sad')[]"}));
	REQUIRE(CHECK_COLUMN(call, 1, {"struct member"}));
	auto types = con.Query("SELECT bool_and(parameter_types[1] NOT LIKE '%mood%') "
	                       "FROM duckdb_functions() WHERE function_name = 'nested_fun'");
	REQUIRE(CHECK_COLUMN(types, 0, {true}));

	// bound sibling subtrees keep their aliases: two members whose aliased payloads differ must
	// stay distinct signatures when the unresolved sibling is bound
	auto alias_info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	alias_info->SetFunctionName(Identifier("aliased_sibling"));
	for (auto payload_alias : {"payload_a", "payload_b"}) {
		auto member = make_uniq<LanguageMacroFunction>("nestlang", payload_alias);
		member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("x")));
		child_list_t<LogicalType> fields;
		fields.emplace_back("payload", LogicalType::LIST(LogicalType::INTEGER).WithAlias(payload_alias));
		fields.emplace_back("m", UnresolvedUserType("mood"));
		member->types.push_back(LogicalType::STRUCT(std::move(fields)));
		alias_info->macros.push_back(std::move(member));
	}
	auto alias_statement = make_uniq<CreateStatement>();
	alias_statement->info = std::move(alias_info);
	unique_ptr<SQLStatement> alias_sql = std::move(alias_statement);
	auto alias_result = con.Query(std::move(alias_sql));
	REQUIRE_NO_FAIL(*alias_result);
	auto alias_types = con.Query("SELECT count(*), bool_and(parameter_types[1] LIKE '%payload_a%' OR "
	                             "parameter_types[1] LIKE '%payload_b%') "
	                             "FROM duckdb_functions() WHERE function_name = 'aliased_sibling'");
	REQUIRE(CHECK_COLUMN(alias_types, 0, {2}));
	REQUIRE(CHECK_COLUMN(alias_types, 1, {true}));
}

TEST_CASE("Batch member creation follows the overload identity", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterTestLanguage(*db.instance, "batchlang");

	// a dialect may submit several members in one CREATE: the set must satisfy the same conflict
	// identity as incremental creation, or EXPORT emits a set IMPORT cannot re-create
	auto make_member = [](const string &source, bool with_default) {
		auto member = make_uniq<LanguageMacroFunction>("batchlang", source);
		member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("a")));
		member->types.push_back(LogicalType::INTEGER);
		if (with_default) {
			auto default_expr = make_uniq<ConstantExpression>(Literal(LiteralKind::INTEGER, "5"));
			default_expr->SetAlias(Identifier("c"));
			member->default_parameters[Identifier("c")] = std::move(default_expr);
			member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("c")));
			member->types.push_back(LogicalType::INTEGER);
		}
		return member;
	};

	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	info->SetFunctionName(Identifier("batch_fun"));
	info->macros.push_back(make_member("fixed", false));
	info->macros.push_back(make_member("ranged", true));
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	unique_ptr<SQLStatement> sql_statement = std::move(statement);
	auto result = con.Query(std::move(sql_statement));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->GetError(), "conflicting overload members"));

	// the identity is compared on the resolved signatures: an unresolved spelling of the same
	// type must not slip past the pairwise check
	auto spelled_info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	spelled_info->SetFunctionName(Identifier("batch_fun"));
	auto spelled_member = make_uniq<LanguageMacroFunction>("batchlang", "fixed");
	spelled_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("a")));
	spelled_member->types.push_back(UnresolvedUserType("integer"));
	spelled_info->macros.push_back(std::move(spelled_member));
	spelled_info->macros.push_back(make_member("ranged", true));
	auto spelled_statement = make_uniq<CreateStatement>();
	spelled_statement->info = std::move(spelled_info);
	unique_ptr<SQLStatement> spelled_sql = std::move(spelled_statement);
	auto spelled_result = con.Query(std::move(spelled_sql));
	REQUIRE_FAIL(spelled_result);
	REQUIRE(StringUtil::Contains(spelled_result->GetError(), "conflicting overload members"));

	// a pairwise valid batch is accepted
	auto valid_info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	valid_info->SetFunctionName(Identifier("batch_fun"));
	valid_info->macros.push_back(make_member("fixed", false));
	auto varchar_member = make_uniq<LanguageMacroFunction>("batchlang", "varchar ranged");
	varchar_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("a")));
	varchar_member->types.push_back(LogicalType::VARCHAR);
	auto default_expr = make_uniq<ConstantExpression>(Literal(LiteralKind::INTEGER, "5"));
	default_expr->SetAlias(Identifier("c"));
	varchar_member->default_parameters[Identifier("c")] = std::move(default_expr);
	varchar_member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier("c")));
	varchar_member->types.push_back(LogicalType::INTEGER);
	valid_info->macros.push_back(std::move(varchar_member));
	auto valid_statement = make_uniq<CreateStatement>();
	valid_statement->info = std::move(valid_info);
	unique_ptr<SQLStatement> valid_sql = std::move(valid_statement);
	auto valid_result = con.Query(std::move(valid_sql));
	REQUIRE_NO_FAIL(*valid_result);
	auto count = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name = 'batch_fun'");
	REQUIRE(CHECK_COLUMN(count, 0, {2}));
}

//! A zero-parameter language member whose undeclared RETURNS type carries extension metadata
static unique_ptr<LanguageMacroFunction> UndeclaredReturnsMetadataMember(const string &language) {
	auto ext_info = make_uniq<ExtensionTypeInfo>();
	ext_info->modifiers.emplace_back(Value::INTEGER(1));
	auto member = make_uniq<LanguageMacroFunction>(language, "x");
	member->return_type = LogicalType(LogicalTypeId::UNKNOWN).WithExtensionInfo(std::move(ext_info));
	return member;
}

TEST_CASE("Metadata on an undeclared RETURNS type is rejected at the catalog boundary", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// an undeclared RETURNS type serializes as its bare default, so attached metadata would
	// silently vanish on reopen: the rejection sits at the catalog boundary every producer funnels
	// through, not only in the CREATE binder
	RegisterTestLanguage(*db.instance, "unkretlang");

	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	info->SetFunctionName(Identifier("unk_ret_fun"));
	info->macros.push_back(UndeclaredReturnsMetadataMember("unkretlang"));
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	auto result = con.Query(std::move(statement));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->GetError(), "metadata to an undeclared RETURNS type"));

	// the direct registration path an extension uses bypasses the CREATE binder and is rejected too
	CreateMacroInfo direct_info(CatalogType::MACRO_ENTRY);
	direct_info.SetFunctionName(Identifier("unk_ret_direct"));
	direct_info.macros.push_back(UndeclaredReturnsMetadataMember("unkretlang"));
	auto &system_catalog = Catalog::GetSystemCatalog(*db.instance);
	auto data = CatalogTransaction::GetSystemTransaction(*db.instance);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	REQUIRE_THROWS(schema.CreateFunction(data, direct_info));
}

//! Build a type the way only the raw C++ constructor can: a parameterless (INVALID) info carrying
//! extension metadata
static void CreateMembers(Connection &con, const string &name, vector<unique_ptr<LanguageMacroFunction>> members) {
	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	info->SetFunctionName(Identifier(name));
	for (auto &member : members) {
		info->macros.push_back(std::move(member));
	}
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	REQUIRE_NO_FAIL(con.Query(std::move(statement)));
}

// the view gives the validator no handle to a stored type: every accessor returns a value or another view
static_assert(!std::is_convertible<LanguageTypeView, LogicalType>::value, "a view must not convert to a type");
static_assert(std::is_same<decltype(std::declval<const LanguageTypeView &>().GetChild(0)), LanguageTypeView>::value,
              "a child is a view by value");
static_assert(std::is_same<decltype(std::declval<const LanguageTypeView &>().ToString()), string>::value,
              "the rendering is a copy");
static_assert(
    std::is_same<decltype(std::declval<const LanguageMemberView &>().parameter_types), vector<LanguageTypeView>>::value,
    "parameter types are views");

static string received_rendering;

//! Renders a member's types through the view
static string RenderView(const LanguageTypeView &view) {
	string result = view.ToString() + "|" + EnumUtil::ToString(view.GetId()) + "|" + view.GetAlias();
	for (idx_t i = 0; i < view.GetChildCount(); i++) {
		result += "|" + view.GetChildName(i) + ":" + view.GetChild(i).ToString();
	}
	return result;
}

static void RecordViews(ScalarFunctionCreateValidateInput &input) {
	received_rendering.clear();
	for (auto &type : input.members[0].parameter_types) {
		received_rendering += RenderView(type) + "\n";
	}
	received_rendering += RenderView(input.members[0].return_type);
}

TEST_CASE("A validator reads member types through read-only views", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterTestLanguage(*db.instance, "viewlang", RecordViews);
	// the view keeps the stored identity: a BLOB aliased as GEOMETRY stays one, where a storage round trip would
	// migrate it to the native geometry type
	auto metadata = make_uniq<ExtensionTypeInfo>();
	metadata->modifiers.emplace_back(Value::INTEGER(1));
	auto geometry_alias = LogicalType(LogicalTypeId::BLOB).WithAlias("GEOMETRY").WithExtensionInfo(std::move(metadata));
	auto member = make_uniq<LanguageMacroFunction>("viewlang", "x");
	for (auto name : {"g", "s", "m", "l", "u"}) {
		member->parameters.push_back(make_uniq<ColumnRefExpression>(Identifier(name)));
	}
	member->types.push_back(geometry_alias);
	member->types.push_back(LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
	member->types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT));
	member->types.push_back(LogicalType::LIST(LogicalType::DOUBLE));
	member->types.push_back(LogicalType::UNKNOWN);
	vector<unique_ptr<LanguageMacroFunction>> members;
	members.push_back(std::move(member));
	CreateMembers(con, "view_fun", std::move(members));
	REQUIRE(received_rendering == "GEOMETRY(1)|BLOB|GEOMETRY\n"
	                              "STRUCT(a INTEGER, b VARCHAR)|STRUCT||a:INTEGER|b:VARCHAR\n"
	                              "MAP(VARCHAR, BIGINT)|MAP||key:VARCHAR|value:BIGINT\n"
	                              "DOUBLE[]|LIST||:DOUBLE\n"
	                              "UNKNOWN|UNKNOWN|\n"
	                              "UNKNOWN|UNKNOWN|");

	// the stored type is unchanged, metadata included
	con.BeginTransaction();
	auto &entry =
	    Catalog::GetEntry(*con.context, CatalogType::MACRO_ENTRY,
	                      QualifiedName(Identifier("memory"), Identifier::DefaultSchema(), Identifier("view_fun")));
	auto &stored = entry.Cast<ScalarMacroCatalogEntry>().macros[0]->types[0];
	REQUIRE(stored.id() == LogicalTypeId::BLOB);
	REQUIRE(stored.GetAlias() == "GEOMETRY");
	REQUIRE(stored.GetTypeInfo().extension_info->modifiers.size() == 1);
	con.Rollback();
	received_rendering.clear();
}

static void NoDirectCall(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("no direct call");
}

static void RejectInvalid(ScalarFunctionCreateValidateInput &input) {
	if (input.members[0].source == "invalid") {
		throw BinderException("ctxlang: rejected");
	}
}

TEST_CASE("Registering overloads through the client context keeps the language declaration", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterTestLanguage(*db.instance, "ctxlang", RejectInvalid);
	REQUIRE_NO_FAIL(con.Query("CREATE FUNCTION ctx_fun() LANGUAGE ctxlang AS 'before'"));

	// an ordinary overload added to the language function through the client context
	CreateScalarFunctionInfo overload_info(
	    ScalarFunction("ctxlang", vector<LogicalType> {LogicalType::INTEGER}, LogicalType::INTEGER, NoDirectCall));
	con.context->RegisterFunction(overload_info);

	auto result = con.Query("SELECT ctx_fun()");
	REQUIRE(CHECK_COLUMN(result, 0, {"before"}));
	REQUIRE_FAIL(con.Query("CREATE FUNCTION ctx_rejected() LANGUAGE ctxlang AS 'invalid'"));

	// a declared language registered for another schema is rejected: member calls only look in the main schema
	ScalarFunction schema_function("ctx_schema_lang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               NoDirectCall, nullptr, nullptr, nullptr, LogicalType::ANY);
	ScalarFunctionSet schema_set {Identifier("ctx_schema_lang")};
	schema_set.AddFunction(std::move(schema_function));
	schema_set.DeclareLanguage();
	CreateScalarFunctionInfo schema_info(std::move(schema_set));
	schema_info.SetQualifiedName(QualifiedName(Identifier(), Identifier("pg_catalog"), Identifier("ctx_schema_lang")));
	REQUIRE_THROWS_WITH(con.context->RegisterFunction(schema_info),
	                    Catch::Matchers::Contains("must be registered in the main schema of the system catalog"));
	// ... and accepted in the main schema
	schema_info.SetQualifiedName(
	    QualifiedName(Identifier("system"), Identifier::DefaultSchema(), Identifier("ctx_schema_lang")));
	con.context->RegisterFunction(schema_info);
	REQUIRE_NO_FAIL(con.Query("CREATE FUNCTION ctx_schema_fun() LANGUAGE ctx_schema_lang AS 'body'"));

	// an undeclared set added to a language inherits its declaration: the check runs on the merged set, and the
	// existing function is looked up under the whole path, so the nested schema does not find system.main's language
	auto &system_catalog = Catalog::GetSystemCatalog(*db.instance);
	auto schema_data = CatalogTransaction::GetSystemTransaction(*db.instance);
	CreateSchemaInfo outer_schema;
	outer_schema.SetQualifiedName(QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier()}));
	outer_schema.internal = true;
	system_catalog.CreateSchema(schema_data, outer_schema);
	CreateSchemaInfo nested_schema;
	nested_schema.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier::DefaultSchema(), Identifier()}));
	nested_schema.internal = true;
	system_catalog.CreateSchema(schema_data, nested_schema);
	CreateScalarFunctionInfo inherit_info(
	    ScalarFunction("ctxlang", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::VARCHAR, NoDirectCall));
	inherit_info.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier("main"), Identifier("ctxlang")}));
	con.context->RegisterFunction(inherit_info);
	REQUIRE_FALSE(inherit_info.functions.IsLanguage());
	// ... while registering the same set at system.main inherits the declaration and is accepted
	CreateScalarFunctionInfo inherit_main_info(
	    ScalarFunction("ctxlang", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::VARCHAR, NoDirectCall));
	con.context->RegisterFunction(inherit_main_info);
	REQUIRE(inherit_main_info.functions.IsLanguage());
	result = con.Query("SELECT ctx_fun()");
	REQUIRE(CHECK_COLUMN(result, 0, {"before"}));
}

TEST_CASE("The catalog keeps a function language in the main schema on every registration path", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Catalog::CreateFunction directly, as the C API V2 registration does, applies the rules the client context used
	// to apply on its own: a declared language for a schema named main nested in another schema is rejected, and one
	// for system.main is created there
	auto &system_catalog = Catalog::GetSystemCatalog(*db.instance);
	auto data = CatalogTransaction::GetSystemTransaction(*db.instance);
	CreateSchemaInfo outer_schema;
	outer_schema.SetQualifiedName(QualifiedName::FromPath({Identifier("system"), Identifier("x"), Identifier()}));
	outer_schema.internal = true;
	system_catalog.CreateSchema(data, outer_schema);
	CreateSchemaInfo nested_schema;
	nested_schema.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("x"), Identifier::DefaultSchema(), Identifier()}));
	nested_schema.internal = true;
	system_catalog.CreateSchema(data, nested_schema);

	ScalarFunction function("nestedlang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        DirectCallUnsupported, nullptr, nullptr, nullptr, LogicalType::ANY);
	function.SetCallReplaceCallback(ConstantCallReplace);
	ScalarFunctionSet set {Identifier("nestedlang")};
	set.AddFunction(std::move(function));
	set.DeclareLanguage();
	CreateScalarFunctionInfo info(std::move(set));
	info.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("x"), Identifier("main"), Identifier("nestedlang")}));
	REQUIRE_THROWS_WITH(system_catalog.CreateFunction(data, info),
	                    Catch::Matchers::Contains("must be registered in the main schema of the system catalog"));
	info.SetQualifiedName(QualifiedName(Identifier("system"), Identifier::DefaultSchema(), Identifier("nestedlang")));
	system_catalog.CreateFunction(data, info);
	auto schemas = con.Query("SELECT schema_name FROM duckdb_functions() WHERE function_name = 'nestedlang'");
	REQUIRE(CHECK_COLUMN(schemas, 0, {"main"}));
	REQUIRE_NO_FAIL(con.Query("CREATE FUNCTION nested_member() LANGUAGE nestedlang AS 'body'"));
	auto result = con.Query("SELECT nested_member()");
	REQUIRE(CHECK_COLUMN(result, 0, {"body"}));

	// an overload registered for the main schema without the declaration joins the language and keeps it
	CreateScalarFunctionInfo overload(ScalarFunction("nestedlang", vector<LogicalType> {LogicalType::INTEGER},
	                                                 LogicalType::VARCHAR, DirectCallUnsupported));
	overload.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	system_catalog.CreateFunction(data, overload);
	REQUIRE(overload.functions.IsLanguage());
	REQUIRE_NO_FAIL(con.Query("CREATE FUNCTION nested_member_two() LANGUAGE nestedlang AS 'two'"));
}
