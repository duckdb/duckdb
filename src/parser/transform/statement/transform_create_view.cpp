#include "parser/statement/create_view_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

// typedef enum ViewCheckOption
//{
//	NO_CHECK_OPTION,
//	LOCAL_CHECK_OPTION,
//	CASCADED_CHECK_OPTION
//} ViewCheckOption;
//
// typedef struct ViewStmt
//{
//	NodeTag		type;
//	RangeVar   *view;			/* the view to be created */
//	List	   *aliases;		/* target column names */
//	Node	   *query;			/* the SELECT query */
//	bool		replace;		/* replace an existing view? */
//	List	   *options;		/* options from WITH clause */
//	ViewCheckOption withCheckOption;	/* WITH CHECK OPTION */
//} ViewStmt;

unique_ptr<CreateViewStatement> Transformer::TransformCreateView(Node *node) {
	assert(node);
	assert(node->type == T_ViewStmt);

	auto stmt = reinterpret_cast<ViewStmt *>(node);
	assert(stmt);
	assert(stmt->view);

	auto result = make_unique<CreateViewStatement>();
	auto &info = *result->info.get();

	if (stmt->view->schemaname) {
		info.schema = stmt->view->schemaname;
	}
	info.view_name = stmt->view->relname;
	info.replace = stmt->replace;

	info.query = TransformSelectNode((SelectStmt *)stmt->query);

	// todo aliases
	// todo options

	return result;
}
