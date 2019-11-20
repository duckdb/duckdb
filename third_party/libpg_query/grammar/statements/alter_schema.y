/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/
AlterObjectSchemaStmt:
			ALTER TABLE relation_expr SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_TABLE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_TABLE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER SEQUENCE qualified_name SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_SEQUENCE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_SEQUENCE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			| ALTER VIEW qualified_name SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_VIEW;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					PGAlterObjectSchemaStmt *n = makeNode(PGAlterObjectSchemaStmt);
					n->objectType = PG_OBJECT_VIEW;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		;
