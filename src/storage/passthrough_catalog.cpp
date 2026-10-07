#include "duckdb/storage/passthrough_catalog.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//
PassthroughCatalog::PassthroughCatalog(AttachedDatabase &db, string type_name_p, QualifiedName query_function_p,
                                       string path_p, unordered_map<string, Value> options_p)
    : Catalog(db), type_name(std::move(type_name_p)), query_function(std::move(query_function_p)),
      path(std::move(path_p)), options(std::move(options_p)) {
}

PassthroughCatalog::~PassthroughCatalog() {
}

void PassthroughCatalog::Initialize(bool load_builtin) {
}

string PassthroughCatalog::GetCatalogType() {
	return type_name;
}

optional_ptr<CatalogEntry> PassthroughCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("Catalog \"%s\" is a passthrough catalog: it has no schemas of its own. CONNECT to "
	                              "it and run the statement there instead",
	                              GetName().GetIdentifierName());
}

void PassthroughCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
}

optional_ptr<SchemaCatalogEntry> PassthroughCatalog::LookupSchema(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &schema_lookup,
                                                                  OnEntryNotFound if_not_found) {
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw BinderException("Catalog \"%s\" is a passthrough catalog: it has no schemas of its own. CONNECT to it "
		                      "and run the statement there instead",
		                      GetName().GetIdentifierName());
	}
	return nullptr;
}

void PassthroughCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("Catalog \"%s\" is a passthrough catalog: it has no schemas of its own",
	                              GetName().GetIdentifierName());
}

[[noreturn]] static void ThrowNoTables(const PassthroughCatalog &catalog) {
	throw NotImplementedException("Catalog \"%s\" is a passthrough catalog without tables of its own. CONNECT to it "
	                              "and run the statement there instead",
	                              catalog.GetName().GetIdentifierName());
}

PhysicalOperator &PassthroughCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                        LogicalCreateTable &op, PhysicalOperator &plan) {
	ThrowNoTables(*this);
}

PhysicalOperator &PassthroughCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	ThrowNoTables(*this);
}

PhysicalOperator &PassthroughCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalDelete &op, PhysicalOperator &plan) {
	ThrowNoTables(*this);
}

PhysicalOperator &PassthroughCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalUpdate &op, PhysicalOperator &plan) {
	ThrowNoTables(*this);
}

DatabaseSize PassthroughCatalog::GetDatabaseSize(ClientContext &context) {
	return DatabaseSize();
}

bool PassthroughCatalog::InMemory() {
	return false;
}

string PassthroughCatalog::GetDBPath() {
	return path;
}

bool PassthroughCatalog::Supports(RemoteCapability capability) const {
	return capability == RemoteCapability::CONNECT;
}

unique_ptr<TableRef> PassthroughCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(ConstantExpression::FromValue(Value(path)));
	arguments.push_back(ConstantExpression::FromValue(Value(sql)));
	for (auto &option : options) {
		// a named argument is a constant whose alias is the parameter name
		auto argument = ConstantExpression::FromValue(option.second);
		argument->SetAlias(Identifier(StringUtil::Lower(option.first)));
		arguments.push_back(std::move(argument));
	}
	auto function_ref = make_uniq<TableFunctionRef>();
	function_ref->function = make_uniq<FunctionExpression>(query_function, std::move(arguments));
	return std::move(function_ref);
}

string PassthroughCatalog::GetConnectDisplay() {
	return type_name + ":" + path;
}

//===--------------------------------------------------------------------===//
// Transaction manager
//===--------------------------------------------------------------------===//
PassthroughTransactionManager::PassthroughTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

Transaction &PassthroughTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<Transaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> guard(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData PassthroughTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void PassthroughTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
}

void PassthroughTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

//===--------------------------------------------------------------------===//
// Storage extension
//===--------------------------------------------------------------------===//
static unique_ptr<Catalog> PassthroughAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                             AttachedDatabase &db, const string &name, AttachInfo &info,
                                             AttachOptions &options) {
	auto &passthrough_info = static_cast<PassthroughStorageInfo &>(*storage_info);
	return make_uniq<PassthroughCatalog>(db, passthrough_info.type_name, passthrough_info.query_function, info.path,
	                                     options.options);
}

static unique_ptr<TransactionManager> PassthroughCreateTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                          AttachedDatabase &db, Catalog &) {
	return make_uniq<PassthroughTransactionManager>(db);
}

PassthroughStorageExtension::PassthroughStorageExtension(string type_name, QualifiedName query_function) {
	attach = PassthroughAttach;
	create_transaction_manager = PassthroughCreateTransactionManager;
	storage_info = make_shared_ptr<PassthroughStorageInfo>(std::move(type_name), std::move(query_function));
}

} // namespace duckdb
