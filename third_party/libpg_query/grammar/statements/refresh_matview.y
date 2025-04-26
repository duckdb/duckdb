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
                    stmt->removeType = PG_OBJECT_MATVIEW;
                    $$ = (PGNode *) stmt;
                }
		;
