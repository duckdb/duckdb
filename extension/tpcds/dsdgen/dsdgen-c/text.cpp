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
#include "config.h"
#include "porting.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string>
#include "decimal.h"
#include "date.h"
#include "genrand.h"
#include "dist.h"

/*
 * Routine: mk_sentence()
 * Purpose: create a sample sentence
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
static void mk_sentence(std::string &verbiage, int stream) {
	char *syntax, *cp, *word = NULL, temp[2];

	verbiage.clear();
	temp[1] = '\0';
	pick_distribution(&syntax, "sentences", 1, 1, stream);

	for (cp = syntax; *cp; cp++) {
		switch (*cp) {
		case 'N': /* pick a noun */
			pick_distribution(&word, "nouns", 1, 1, stream);
			break;
		case 'V': /* pick a verb */
			pick_distribution(&word, "verbs", 1, 1, stream);
			break;
		case 'J': /* pick a adjective */
			pick_distribution(&word, "adjectives", 1, 1, stream);
			break;
		case 'D': /* pick a adverb */
			pick_distribution(&word, "adverbs", 1, 1, stream);
			break;
		case 'X': /* pick a auxiliary verb */
			pick_distribution(&word, "auxiliaries", 1, 1, stream);
			break;
		case 'P': /* pick a preposition */
			pick_distribution(&word, "prepositions", 1, 1, stream);
			break;
		case 'A': /* pick an article */
			pick_distribution(&word, "articles", 1, 1, stream);
			break;
		case 'T': /* pick an terminator */
			pick_distribution(&word, "terminators", 1, 1, stream);
			break;
		default:
			temp[0] = *cp;
			break;
		}

		if (word == NULL) {
			verbiage.append(temp, 1);
		} else {
			verbiage.append(word);
		}
		word = NULL;
	}
}

/*
 * Routine: gen_text()
 * Purpose: entry point for this module. Generate a truncated sentence in a
 *			given length range
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
char *gen_text(char *dest, int min, int max, int stream) {
	int target_len, generated_length, capitalize = 1;
	std::string sentence;

	genrand_integer(&target_len, DIST_UNIFORM, min, max, 0, stream);
	if (dest)
		*dest = '\0';
	else {
		dest = (char *)malloc((max + 1) * sizeof(char));
		MALLOC_CHECK(dest);
	}

	while (target_len > 0) {
		mk_sentence(sentence, stream);
		if (capitalize)
			sentence[0] = static_cast<char>(toupper(sentence[0]));
		generated_length = static_cast<int>(sentence.size());
		capitalize = (sentence[generated_length - 1] == '.');
		if (target_len <= generated_length)
			sentence.resize(target_len);
		strcat(dest, sentence.c_str());
		target_len -= generated_length;
		if (target_len > 0) {
			strcat(dest, " ");
			target_len -= 1;
		}
	}

	return (dest);
}

#ifdef TEST
#define DECLARER
#include "r_driver.h"
#include "r_params.h"

typedef struct {
	char *name;
} tdef;
/* tdef tdefs[] = {NULL}; */

option_t options[] = {

    {"DISTRIBUTIONS", OPT_STR, 0, NULL, NULL, "tester_dist.idx"}, NULL};

char *params[2];

main() {
	char test_dest[201];
	int i;

	init_params();

	for (i = 0; i < 100; i++) {
		gen_text(test_dest, 100, 200, 1);
		printf("%s\n", test_dest);
		test_dest[0] = '\0';
	}

	return (0);
}
#endif
