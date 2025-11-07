/*****************************************************************************
 *
 * Cluster Statement
 *
 *****************************************************************************/
ClusterStmt:
	CLUSTER qualified_name sort_clause
		{
			PGClusterStmt *n = makeNode(PGClusterStmt);
			n->targetTable = $2;
			n->sortClause = $3;
			$$ = (PGNode *)n;
		}
	;
