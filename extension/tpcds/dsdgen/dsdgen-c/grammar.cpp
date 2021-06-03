/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "config.hpp"
#include "porting.hpp"
#include "grammar.hpp"
#include "error_msg.hpp"
#include "StringBuffer.hpp"
//#include "expr.hpp"
#include "decimal.hpp"
#include "date.hpp"

int nLineNumber = 0;
extern char *CurrentFileName;
token_t *pTokens;
int ProcessOther(char *stmt, token_t *pTokens);

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
char *ProcessStr(char *stmt, token_t *tokens) {
	char *cp;

	if ((cp = SafeStrtok(NULL, "\"")) == NULL)
		ReportError(QERR_BAD_STRING, NULL, 1);

	return (cp);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ProcessComments(char *line) {
	char *cp;
	int i = 0;

	if (line == NULL)
		return (-1);

	if ((cp = strchr(line, COMMENT_CHAR)) != NULL) {
		if (*(cp + 1) == COMMENT_CHAR)
			*cp = '\0';
	}

	cp = line;
	while (*cp && (*cp == ' ' || *cp == '\t' || *cp == '\r')) {
		i += 1;
		cp += 1;
	}

	return (i);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
char *AddLine(char *line) {
	static int nCharAllocated = 0;
	static int nCharInUse = 0;
	static char *szResult;
	int nCharAvailable, nCharRequested, nCharAdditional;

	if (line == NULL) /*  initialization */
	{
		nCharInUse = 0;
		return (NULL);
	}

	nCharAvailable = nCharAllocated - nCharInUse - 1;
	nCharRequested = strlen(line);

	if (nCharRequested == 0) /*  asked to add a null line */
		return (szResult);
	nCharRequested += 1; /* add a space between pieces */

	if (nCharAvailable < nCharRequested) /*  need more room */
	{
		nCharAdditional = (nCharRequested > 250) ? nCharRequested : 250;
		szResult = (char *)realloc((void *)szResult, nCharAllocated + nCharAdditional);
		nCharAllocated += 250;
	}

	if (szResult != NULL) {
		if (nCharInUse == 0)
			strcpy(szResult, line);
		else
			strcat(szResult, line);
		strcat(szResult, " "); /* and add the space we reserved room for above */
		nCharInUse += nCharRequested;
	}

	return (szResult);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void SetTokens(token_t *pToken) {
	pTokens = pToken;

	return;
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int FindToken(char *word) {
	int nRetCode = 0;
	int i;

	/*  Note: linear search should be replaced if the word count gets large */
	for (i = 1; pTokens[i].index != -1; i++) {
		if (!strcasecmp(pTokens[i].word, word))
			nRetCode = pTokens[i].index;
	}

	return (nRetCode);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int ParseFile(char *szPath) {
	FILE *fp;
	char szLine[4096]; /*  is there a good portable constant for this? */
	char *stmt, *line_start, *cp;
	int i, nRetCode = 0;
	StringBuffer_t *pLineBuffer;

	/*  open the template, or return an error */

/*  Gosh, I love non-standard extensions to standard functions! */
#ifdef WIN32
	if ((fp = fopen(szPath, "rt")) == NULL)
#else
	if ((fp = fopen(szPath, "r")) == NULL)
#endif
		return (ReportErrorNoLine(QERR_NO_FILE, szPath, 0));

	/* shift current file indicator for error messages */
	if (CurrentFileName != NULL)
		free(CurrentFileName);
	CurrentFileName = strdup(szPath);
	pLineBuffer = InitBuffer(100, 20);
	nLineNumber = 0;
	SetErrorGlobals(szPath, &nLineNumber);

	while ((fgets(szLine, 4096, fp) != NULL) && (nRetCode >= 0)) {
		nLineNumber += 1;
		if ((cp = strchr(szLine, '\n')))
			*cp = '\0';
		else
			ReportError(QERR_LINE_TOO_LONG, NULL, 1);

		/*  build a complete statement  */
		i = ProcessComments(szLine);
		if (i < 0)
			return (i);
		line_start = (szLine + i);
		if (strlen(line_start) == 0)
			continue; /*  nothing to do with an empty line */

		AddBuffer(pLineBuffer, line_start);
		if ((cp = strchr(line_start, STMT_END)) == NULL) {
			AddBuffer(pLineBuffer, " ");
			continue;
		}
		if (*(cp - 1) == '\\')
			if ((cp = strchr(cp + 1, STMT_END)) == NULL) {
				AddBuffer(pLineBuffer, " ");
				continue;
			}

		/*
		 * NOTE: this assumes that the first word indentifies the statement type
		 */
		stmt = GetBuffer(pLineBuffer);
		cp = SafeStrtok(stmt, " \t");
		i = FindToken(cp);
		if (i != 0) {
			if (pTokens[i].handler != NULL)
				nRetCode = pTokens[i].handler(stmt, pTokens);
			else
				nRetCode = -17; /* QERR_SYNTAX; */
		} else                  /*  other text (i.e., SQL) possibly with subsitution targets) */
			nRetCode = ProcessOther(stmt, pTokens);

		ResetBuffer(pLineBuffer);
	}

	if (!feof(fp) && (nRetCode >= 0))
		ReportError(QERR_READ_FAILED, szPath, 0);
	if (nRetCode < 0)
		ReportError(nRetCode, szLine, 0);

	fclose(fp);
	/* jms -- need to reintroduce this
	   FreeBuffer(pLineBuffer);
	   */

	return (nRetCode);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
char *SafeStrtok(char *string, char *delims) {
	static char *szScratch = NULL;
	static int nScratchLen = 0;

	if (string != NULL) {
		if (szScratch == NULL) {
			szScratch = (char *)malloc(strlen(string) + 1);
			MALLOC_CHECK(szScratch);
			if (szScratch == NULL)
				ReportError(QERR_NO_MEMORY, "SafeStrtok", 1);
			else
				nScratchLen = strlen(string);
		} else {
			if (nScratchLen < (int)strlen(string)) {
				szScratch = (char *)realloc(szScratch, strlen(string) + 1);
				if (szScratch == NULL)
					ReportError(QERR_NO_MEMORY, "SafeStrtok", 1);
				else
					nScratchLen = strlen(string);
			}
		}
		strcpy(szScratch, string);
		return (strtok(szScratch, delims));
	}
	return (strtok(NULL, delims));
}
