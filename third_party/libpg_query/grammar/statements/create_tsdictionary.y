/*****************************************************************************
 *
 * CREATE TEXT SEARCH DICTIONARY
 *
 * Produces a PGDefineStmt with kind = PG_OBJECT_TSDICTIONARY.
 *
 *****************************************************************************/
CreateTSDictionaryStmt:
			CREATE_P TEXT_P SEARCH DICTIONARY any_name definition
			{
				PGDefineStmt *n = makeNode(PGDefineStmt);
				n->kind = PG_OBJECT_TSDICTIONARY;
				n->defnames = $5;
				n->definition = $6;
				n->if_not_exists = false;
				$$ = (PGNode *)n;
			}
			| CREATE_P TEXT_P SEARCH DICTIONARY IF_P NOT EXISTS any_name definition
			{
				PGDefineStmt *n = makeNode(PGDefineStmt);
				n->kind = PG_OBJECT_TSDICTIONARY;
				n->defnames = $8;
				n->definition = $9;
				n->if_not_exists = true;
				$$ = (PGNode *)n;
			}
		;
