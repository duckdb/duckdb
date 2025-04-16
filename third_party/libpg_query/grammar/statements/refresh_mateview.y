/*****************************************************************************
 *
 *		QUERY :
 *				REFRESH MATERIALIZED VIEW matviewname
 *
 *****************************************************************************/
RefreshMatViewStmt:
		REFRESH MATERIALIZED VIEW qualified_name
                {
                    PGRefreshMatViewStmt *stmt = makeNode(PGRefreshMatViewStmt);
                    stmt->matView = $4;
                    $$ = (PGNode *) stmt;
                }
		;
