//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"

#include "pg_definitions.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/primnodes.hpp"

namespace duckdb {

class ColumnDefinition;
class StackChecker;
struct OrderByNode;
struct CopyInfo;
struct CommonTableExpressionInfo;
struct GroupingExpressionMap;
class OnConflictInfo;
class UpdateSetInfo;
struct ParserOptions;
struct PivotColumn;

//! The transformer class is responsible for transforming the internal Postgres
//! parser representation into the DuckDB representation
class Transformer {
	friend class StackChecker;

	struct CreatePivotEntry {
		string enum_name;
		unique_ptr<SelectNode> base;
		unique_ptr<ParsedExpression> column;
		unique_ptr<QueryNode> subquery;
	};

public:
	explicit Transformer(ParserOptions &options);
	explicit Transformer(Transformer &parent);
	~Transformer();

	//! Transforms a Postgres parse tree into a set of SQL Statements
	bool TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements);
	string NodetypeToString(duckdb_libpgquery::PGNodeTag type);

	idx_t ParamCount() const;

private:
	optional_ptr<Transformer> parent;
	//! Parser options
	ParserOptions &options;
	//! The current prepared statement parameter index
	idx_t prepared_statement_parameter_index = 0;
	//! Map from named parameter to parameter index;
	case_insensitive_map_t<idx_t> named_param_map;
	//! Holds window expressions defined by name. We need those when transforming the expressions referring to them.
	unordered_map<string, duckdb_libpgquery::PGWindowDef *> window_clauses;
	//! The set of pivot entries to create
	vector<unique_ptr<CreatePivotEntry>> pivot_entries;
	//! Sets of stored CTEs, if any
	vector<CommonTableExpressionMap *> stored_cte_map;
	//! Whether or not we are currently binding a window definition
	bool in_window_definition = false;

	void Clear();
	bool InWindowDefinition();

	Transformer &RootTransformer();
	const Transformer &RootTransformer() const;
	void SetParamCount(idx_t new_count);
	void SetNamedParam(const string &name, int32_t index);
	bool GetNamedParam(const string &name, int32_t &index);
	bool HasNamedParameters() const;

	void AddPivotEntry(string enum_name, unique_ptr<SelectNode> source, unique_ptr<ParsedExpression> column,
	                   unique_ptr<QueryNode> subquery);
	unique_ptr<SQLStatement> GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry);
	bool HasPivotEntries();
	idx_t PivotEntryCount();
	vector<unique_ptr<CreatePivotEntry>> &GetPivotEntries();
	void PivotEntryCheck(const string &type);
	void ExtractCTEsRecursive(CommonTableExpressionMap &cte_map);

private:
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatement(duckdb_libpgquery::PGNode *stmt);
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatementInternal(duckdb_libpgquery::PGNode *stmt);
	//===--------------------------------------------------------------------===//
	// Statement transformation
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres duckdb_libpgquery::T_PGSelectStmt node into a SelectStatement
	unique_ptr<SelectStatement> TransformSelect(duckdb_libpgquery::PGNode *node, bool isSelect = true);
	//! Transform a Postgres T_AlterStmt node into a AlterStatement
	unique_ptr<AlterStatement> TransformAlter(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGRenameStmt node into a RenameStatement
	unique_ptr<AlterStatement> TransformRename(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateTable(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateTableAs(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateSchema(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateSeqStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateSequence(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGViewStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateView(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGIndexStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateIndex(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateFunctionStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateFunction(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateTypeStmt node into CreateStatement
	unique_ptr<CreateStatement> TransformCreateType(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGCreateDatabaseStmt node into a CreateStatement
	unique_ptr<CreateStatement> TransformCreateDatabase(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGAlterSeqStmt node into CreateStatement
	unique_ptr<AlterStatement> TransformAlterSequence(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGDropStmt node into a Drop[Table,Schema]Statement
	unique_ptr<SQLStatement> TransformDrop(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGInsertStmt node into a InsertStatement
	unique_ptr<InsertStatement> TransformInsert(duckdb_libpgquery::PGNode *node);

	//! Transform a Postgres duckdb_libpgquery::T_PGOnConflictClause node into a OnConflictInfo
	unique_ptr<OnConflictInfo> TransformOnConflictClause(duckdb_libpgquery::PGOnConflictClause *node,
	                                                     const string &relname);
	//! Transform a ON CONFLICT shorthand into a OnConflictInfo
	unique_ptr<OnConflictInfo> DummyOnConflictClause(duckdb_libpgquery::PGOnConflictActionAlias type,
	                                                 const string &relname);
	//! Transform a Postgres duckdb_libpgquery::T_PGCopyStmt node into a CopyStatement
	unique_ptr<CopyStatement> TransformCopy(duckdb_libpgquery::PGNode *node);
	void TransformCopyOptions(CopyInfo &info, duckdb_libpgquery::PGList *options);
	//! Transform a Postgres duckdb_libpgquery::T_PGTransactionStmt node into a TransactionStatement
	unique_ptr<TransactionStatement> TransformTransaction(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
	unique_ptr<DeleteStatement> TransformDelete(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGUpdateStmt node into a UpdateStatement
	unique_ptr<UpdateStatement> TransformUpdate(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGPragmaStmt node into a PragmaStatement
	unique_ptr<SQLStatement> TransformPragma(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGExportStmt node into a ExportStatement
	unique_ptr<ExportStatement> TransformExport(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres duckdb_libpgquery::T_PGImportStmt node into a PragmaStatement
	unique_ptr<PragmaStatement> TransformImport(duckdb_libpgquery::PGNode *node);
	unique_ptr<ExplainStatement> TransformExplain(duckdb_libpgquery::PGNode *node);
	unique_ptr<SQLStatement> TransformVacuum(duckdb_libpgquery::PGNode *node);
	unique_ptr<SQLStatement> TransformShow(duckdb_libpgquery::PGNode *node);
	unique_ptr<ShowStatement> TransformShowSelect(duckdb_libpgquery::PGNode *node);
	unique_ptr<AttachStatement> TransformAttach(duckdb_libpgquery::PGNode *node);
	unique_ptr<DetachStatement> TransformDetach(duckdb_libpgquery::PGNode *node);
	unique_ptr<SetStatement> TransformUse(duckdb_libpgquery::PGNode *node);

	unique_ptr<PrepareStatement> TransformPrepare(duckdb_libpgquery::PGNode *node);
	unique_ptr<ExecuteStatement> TransformExecute(duckdb_libpgquery::PGNode *node);
	unique_ptr<CallStatement> TransformCall(duckdb_libpgquery::PGNode *node);
	unique_ptr<DropStatement> TransformDeallocate(duckdb_libpgquery::PGNode *node);
	unique_ptr<QueryNode> TransformPivotStatement(duckdb_libpgquery::PGSelectStmt *stmt);
	unique_ptr<SQLStatement> CreatePivotStatement(unique_ptr<SQLStatement> statement);
	PivotColumn TransformPivotColumn(duckdb_libpgquery::PGPivot *pivot);
	vector<PivotColumn> TransformPivotList(duckdb_libpgquery::PGList *list);

	//===--------------------------------------------------------------------===//
	// SetStatement Transform
	//===--------------------------------------------------------------------===//
	unique_ptr<SetStatement> TransformSet(duckdb_libpgquery::PGNode *node);
	unique_ptr<SetStatement> TransformSetVariable(duckdb_libpgquery::PGVariableSetStmt *stmt);
	unique_ptr<SetStatement> TransformResetVariable(duckdb_libpgquery::PGVariableSetStmt *stmt);

	unique_ptr<SQLStatement> TransformCheckpoint(duckdb_libpgquery::PGNode *node);
	unique_ptr<LoadStatement> TransformLoad(duckdb_libpgquery::PGNode *node);

	//===--------------------------------------------------------------------===//
	// Query Node Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres duckdb_libpgquery::T_PGSelectStmt node into a QueryNode
	unique_ptr<QueryNode> TransformSelectNode(duckdb_libpgquery::PGSelectStmt *node);
	unique_ptr<QueryNode> TransformSelectInternal(duckdb_libpgquery::PGSelectStmt *node);
	void TransformModifiers(duckdb_libpgquery::PGSelectStmt &stmt, QueryNode &node);

	//===--------------------------------------------------------------------===//
	// Expression Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<ParsedExpression> TransformBoolExpr(duckdb_libpgquery::PGBoolExpr *root);
	//! Transform a Postgres case expression into an Expression
	unique_ptr<ParsedExpression> TransformCase(duckdb_libpgquery::PGCaseExpr *root);
	//! Transform a Postgres type cast into an Expression
	unique_ptr<ParsedExpression> TransformTypeCast(duckdb_libpgquery::PGTypeCast *root);
	//! Transform a Postgres coalesce into an Expression
	unique_ptr<ParsedExpression> TransformCoalesce(duckdb_libpgquery::PGAExpr *root);
	//! Transform a Postgres column reference into an Expression
	unique_ptr<ParsedExpression> TransformColumnRef(duckdb_libpgquery::PGColumnRef *root);
	//! Transform a Postgres constant value into an Expression
	unique_ptr<ConstantExpression> TransformValue(duckdb_libpgquery::PGValue val);
	//! Transform a Postgres operator into an Expression
	unique_ptr<ParsedExpression> TransformAExpr(duckdb_libpgquery::PGAExpr *root);
	unique_ptr<ParsedExpression> TransformAExprInternal(duckdb_libpgquery::PGAExpr *root);
	//! Transform a Postgres abstract expression into an Expression
	unique_ptr<ParsedExpression> TransformExpression(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres function call into an Expression
	unique_ptr<ParsedExpression> TransformFuncCall(duckdb_libpgquery::PGFuncCall *root);
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<ParsedExpression> TransformInterval(duckdb_libpgquery::PGIntervalConstant *root);
	//! Transform a Postgres lambda node [e.g. (x, y) -> x + y] into a lambda expression
	unique_ptr<ParsedExpression> TransformLambda(duckdb_libpgquery::PGLambdaFunction *node);
	//! Transform a Postgres array access node (e.g. x[1] or x[1:3])
	unique_ptr<ParsedExpression> TransformArrayAccess(duckdb_libpgquery::PGAIndirection *node);
	//! Transform a positional reference (e.g. #1)
	unique_ptr<ParsedExpression> TransformPositionalReference(duckdb_libpgquery::PGPositionalReference *node);
	unique_ptr<ParsedExpression> TransformStarExpression(duckdb_libpgquery::PGNode *node);
	unique_ptr<ParsedExpression> TransformBooleanTest(duckdb_libpgquery::PGBooleanTest *node);

	//! Transform a Postgres constant value into an Expression
	unique_ptr<ParsedExpression> TransformConstant(duckdb_libpgquery::PGAConst *c);
	unique_ptr<ParsedExpression> TransformGroupingFunction(duckdb_libpgquery::PGGroupingFunc *n);
	unique_ptr<ParsedExpression> TransformResTarget(duckdb_libpgquery::PGResTarget *root);
	unique_ptr<ParsedExpression> TransformNullTest(duckdb_libpgquery::PGNullTest *root);
	unique_ptr<ParsedExpression> TransformParamRef(duckdb_libpgquery::PGParamRef *node);
	unique_ptr<ParsedExpression> TransformNamedArg(duckdb_libpgquery::PGNamedArgExpr *root);

	unique_ptr<ParsedExpression> TransformSQLValueFunction(duckdb_libpgquery::PGSQLValueFunction *node);

	unique_ptr<ParsedExpression> TransformSubquery(duckdb_libpgquery::PGSubLink *root);
	//===--------------------------------------------------------------------===//
	// Constraints transform
	//===--------------------------------------------------------------------===//
	unique_ptr<Constraint> TransformConstraint(duckdb_libpgquery::PGListCell *cell);

	unique_ptr<Constraint> TransformConstraint(duckdb_libpgquery::PGListCell *cell, ColumnDefinition &column,
	                                           idx_t index);

	//===--------------------------------------------------------------------===//
	// Update transform
	//===--------------------------------------------------------------------===//
	unique_ptr<UpdateSetInfo> TransformUpdateSetInfo(duckdb_libpgquery::PGList *target_list,
	                                                 duckdb_libpgquery::PGNode *where_clause);

	//===--------------------------------------------------------------------===//
	// Index transform
	//===--------------------------------------------------------------------===//
	vector<unique_ptr<ParsedExpression>> TransformIndexParameters(duckdb_libpgquery::PGList *list,
	                                                              const string &relation_name);

	//===--------------------------------------------------------------------===//
	// Collation transform
	//===--------------------------------------------------------------------===//
	unique_ptr<ParsedExpression> TransformCollateExpr(duckdb_libpgquery::PGCollateClause *collate);

	string TransformCollation(duckdb_libpgquery::PGCollateClause *collate);

	ColumnDefinition TransformColumnDefinition(duckdb_libpgquery::PGColumnDef *cdef);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	OnCreateConflict TransformOnConflict(duckdb_libpgquery::PGOnCreateConflict conflict);
	string TransformAlias(duckdb_libpgquery::PGAlias *root, vector<string> &column_name_alias);
	vector<string> TransformStringList(duckdb_libpgquery::PGList *list);
	void TransformCTE(duckdb_libpgquery::PGWithClause *de_with_clause, CommonTableExpressionMap &cte_map,
	                  vector<unique_ptr<CTENode>> *materialized_ctes);
	static unique_ptr<QueryNode> TransformMaterializedCTE(unique_ptr<QueryNode> root,
	                                                      vector<unique_ptr<CTENode>> &materialized_ctes);
	unique_ptr<SelectStatement> TransformRecursiveCTE(duckdb_libpgquery::PGCommonTableExpr *node,
	                                                  CommonTableExpressionInfo &info);

	unique_ptr<ParsedExpression> TransformUnaryOperator(const string &op, unique_ptr<ParsedExpression> child);
	unique_ptr<ParsedExpression> TransformBinaryOperator(string op, unique_ptr<ParsedExpression> left,
	                                                     unique_ptr<ParsedExpression> right);
	//===--------------------------------------------------------------------===//
	// TableRef transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres node into a TableRef
	unique_ptr<TableRef> TransformTableRefNode(duckdb_libpgquery::PGNode *node);
	//! Transform a Postgres FROM clause into a TableRef
	unique_ptr<TableRef> TransformFrom(duckdb_libpgquery::PGList *root);
	//! Transform a Postgres table reference into a TableRef
	unique_ptr<TableRef> TransformRangeVar(duckdb_libpgquery::PGRangeVar *root);
	//! Transform a Postgres table-producing function into a TableRef
	unique_ptr<TableRef> TransformRangeFunction(duckdb_libpgquery::PGRangeFunction *root);
	//! Transform a Postgres join node into a TableRef
	unique_ptr<TableRef> TransformJoin(duckdb_libpgquery::PGJoinExpr *root);
	//! Transform a Postgres pivot node into a TableRef
	unique_ptr<TableRef> TransformPivot(duckdb_libpgquery::PGPivotExpr *root);
	//! Transform a table producing subquery into a TableRef
	unique_ptr<TableRef> TransformRangeSubselect(duckdb_libpgquery::PGRangeSubselect *root);
	//! Transform a VALUES list into a set of expressions
	unique_ptr<TableRef> TransformValuesList(duckdb_libpgquery::PGList *list);

	//! Transform a range var into a (schema) qualified name
	QualifiedName TransformQualifiedName(duckdb_libpgquery::PGRangeVar *root);

	//! Transform a Postgres TypeName string into a LogicalType
	LogicalType TransformTypeName(duckdb_libpgquery::PGTypeName *name);

	//! Transform a Postgres GROUP BY expression into a list of Expression
	bool TransformGroupBy(duckdb_libpgquery::PGList *group, SelectNode &result);
	void TransformGroupByNode(duckdb_libpgquery::PGNode *n, GroupingExpressionMap &map, SelectNode &result,
	                          vector<GroupingSet> &result_sets);
	void AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map, GroupByNode &result,
	                          vector<idx_t> &result_set);
	void TransformGroupByExpression(duckdb_libpgquery::PGNode *n, GroupingExpressionMap &map, GroupByNode &result,
	                                vector<idx_t> &result_set);
	//! Transform a Postgres ORDER BY expression into an OrderByDescription
	bool TransformOrderBy(duckdb_libpgquery::PGList *order, vector<OrderByNode> &result);

	//! Transform a Postgres SELECT clause into a list of Expressions
	void TransformExpressionList(duckdb_libpgquery::PGList &list, vector<unique_ptr<ParsedExpression>> &result);

	//! Transform a Postgres PARTITION BY/ORDER BY specification into lists of expressions
	void TransformWindowDef(duckdb_libpgquery::PGWindowDef *window_spec, WindowExpression *expr,
	                        const char *window_name = nullptr);
	//! Transform a Postgres window frame specification into frame expressions
	void TransformWindowFrame(duckdb_libpgquery::PGWindowDef *window_spec, WindowExpression *expr);

	unique_ptr<SampleOptions> TransformSampleOptions(duckdb_libpgquery::PGNode *options);
	//! Returns true if an expression is only a star (i.e. "*", without any other decorators)
	bool ExpressionIsEmptyStar(ParsedExpression &expr);

	OnEntryNotFound TransformOnEntryNotFound(bool missing_ok);

private:
	//! Current stack depth
	idx_t stack_depth;

	void InitializeStackCheck();
	StackChecker StackCheck(idx_t extra_stack = 1);
};

class StackChecker {
public:
	StackChecker(Transformer &transformer, idx_t stack_usage);
	~StackChecker();
	StackChecker(StackChecker &&) noexcept;
	StackChecker(const StackChecker &) = delete;

private:
	Transformer &transformer;
	idx_t stack_usage;
};

vector<string> ReadPgListToString(duckdb_libpgquery::PGList *column_list);

} // namespace duckdb
