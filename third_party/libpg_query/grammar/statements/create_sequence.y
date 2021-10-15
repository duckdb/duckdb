/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/
CreateSeqStmt:
			CREATE_P OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					PGCreateSeqStmt *n = makeNode(PGCreateSeqStmt);
					$4->relpersistence = $2;
					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OptTemp SEQUENCE IF_P NOT EXISTS qualified_name OptSeqOptList
				{
					PGCreateSeqStmt *n = makeNode(PGCreateSeqStmt);
					$7->relpersistence = $2;
					n->sequence = $7;
					n->options = $8;
					n->ownerId = InvalidOid;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;


OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
