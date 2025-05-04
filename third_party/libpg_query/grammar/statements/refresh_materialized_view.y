/*****************************************************************************
 *
 *		QUERY :
 *				REFRESH MATERIALIZED VIEW materialized_view_name
 *
 *****************************************************************************/
RefreshMaterializedViewStmt:
		REFRESH MATERIALIZED VIEW qualified_name
                {
                    PGRefreshMaterializedViewStmt *stmt = makeNode(PGRefreshMaterializedViewStmt);
                    stmt->materializedView = $4;
                    stmt->removeType = PG_OBJECT_MATVIEW;
                    $$ = (PGNode *) stmt;
                }
		;
