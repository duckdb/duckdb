/*****************************************************************************
 *
 * CREATE DATABASE / DROP DATABASE
 *
 * Mapped to DuckDB's ATTACH/DETACH with TYPE serenedb.
 *
 *****************************************************************************/
CreateDatabaseStmt:
			CREATE_P DATABASE ColLabel
			{
				PGAttachStmt *n = makeNode(PGAttachStmt);
				n->path = (char *)"";
				n->name = $3;
				n->options = list_make1(makeDefElem("type",
					(PGNode *)makeString("serenedb"), -1));
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
			| CREATE_P DATABASE IF_P NOT EXISTS ColLabel
			{
				PGAttachStmt *n = makeNode(PGAttachStmt);
				n->path = (char *)"";
				n->name = $6;
				n->options = list_make1(makeDefElem("type",
					(PGNode *)makeString("serenedb"), -1));
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
			| DROP DATABASE ColLabel
			{
				PGDetachStmt *n = makeNode(PGDetachStmt);
				n->missing_ok = false;
				n->is_drop = true;
				n->db_name = $3;
				$$ = (PGNode *)n;
			}
			| DROP DATABASE IF_P EXISTS ColLabel
			{
				PGDetachStmt *n = makeNode(PGDetachStmt);
				n->missing_ok = true;
				n->is_drop = true;
				n->db_name = $5;
				$$ = (PGNode *)n;
			}
			| DROP DATABASE ColLabel '(' FORCE ')'
			{
				PGDetachStmt *n = makeNode(PGDetachStmt);
				n->missing_ok = false;
				n->is_drop = true;
				n->db_name = $3;
				$$ = (PGNode *)n;
			}
			| DROP DATABASE IF_P EXISTS ColLabel '(' FORCE ')'
			{
				PGDetachStmt *n = makeNode(PGDetachStmt);
				n->missing_ok = true;
				n->is_drop = true;
				n->db_name = $5;
				$$ = (PGNode *)n;
			}
		;
