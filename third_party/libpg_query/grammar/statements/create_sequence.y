/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/
CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->relpersistence = $2;
					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE OptTemp SEQUENCE IF_P NOT EXISTS qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$7->relpersistence = $2;
					n->sequence = $7;
					n->options = $8;
					n->ownerId = InvalidOid;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;


OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
