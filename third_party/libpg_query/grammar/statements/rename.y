/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/
RenameStmt: ALTER SCHEMA name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_SCHEMA;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_TABLE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER SEQUENCE qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_SEQUENCE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_SEQUENCE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER VIEW qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_VIEW;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_VIEW;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER INDEX qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_INDEX;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_COLUMN;
					n->relationType = PG_OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_COLUMN;
					n->relationType = PG_OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_TABCONSTRAINT;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME CONSTRAINT name TO name
				{
					PGRenameStmt *n = makeNode(PGRenameStmt);
					n->renameType = PG_OBJECT_TABCONSTRAINT;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		;


opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;
