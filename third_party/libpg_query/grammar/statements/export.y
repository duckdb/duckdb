/*****************************************************************************
 *
 * EXPORT/IMPORT stmt
 *
 *****************************************************************************/
ExportStmt:
			EXPORT_P DATABASE Sconst copy_options
				{
					PGExportStmt *n = makeNode(PGExportStmt);
					n->filename = $3;
					n->options = NIL;
					if ($4) {
						n->options = list_concat(n->options, $4);
					}
					$$ = (PGNode *)n;
				}
		;

ImportStmt:
			IMPORT_P DATABASE Sconst
				{
					PGImportStmt *n = makeNode(PGImportStmt);
					n->filename = $3;
					$$ = (PGNode *)n;
				}
		;
