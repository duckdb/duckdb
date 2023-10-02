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
/*
 * parameter handling functions
 */
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "config.h"
#include "porting.h"
#include "r_params.h"
#include "tdefs.h"
#include "release.h"
#include <cassert>

#define PARAM_MAX_LEN 80

#ifndef TEST
extern option_t options[];
extern char *params[];
#else
option_t options[] = {{"PROG", OPT_STR | OPT_HIDE, 0, NULL, NULL, "tester"},
                      {"PARAMS", OPT_STR, 1, "read parameters from file <s>", read_file, ""},
                      {"DISTRIBUTIONS", OPT_STR, 2, "read distributions from file <s>", NULL, "tester_dist.idx"},
                      {"OUTDIR", OPT_STR, 3, "generate files in directory <s>", NULL, "./"},
                      {"VERBOSE", OPT_FLG, 4, "enable verbose output", NULL, "N"},
                      {"HELP", OPT_FLG, 5, "display this message", usage, "N"},
                      {"scale", OPT_INT, 6, "set scale to <i>", NULL, "1"},
                      NULL};
char *params[9];
#endif

#define MAX_LINE_LEN 120
#ifdef _WIN32
#define OPTION_START '/'
#else
#define OPTION_START '-'
#endif

int read_file(const char *param_name, const char *option);
int fnd_param(const char *name);
void print_params(void);

/*
 * Routine:  load_params()
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
 * TODO:
 * 20010621 JMS shared memory not yet implemented
 */
void load_params() {
	/*
	    int i=0;
	    while (options[i].name != NULL)
	    {
	        load_param(i, GetSharedMemoryParam(options[i].index));
	        i++;
	    }
	    SetSharedMemoryStat(STAT_ROWCOUNT, get_int("STEP"), 0);
	*/
	return;
}

/*
 * Routine:  set_flag(int f)
 * Purpose:  set a toggle parameter
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
void set_flg(const char *flag) {
	int nParam;

	init_params();
	nParam = fnd_param(flag);
	if (nParam >= 0)
		strcpy(params[options[nParam].index], "Y");

	return;
}

/*
 * Routine: clr_flg(f)
 * Purpose: clear a toggle parameter
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
void clr_flg(const char *flag) {
	int nParam;

	init_params();
	nParam = fnd_param(flag);
	if (nParam >= 0)
		strcpy(params[options[nParam].index], "N");
	return;
}

/*
 * Routine: is_set(int f)
 * Purpose: return the state of a toggle parameter, or whether or not a string
 * or int parameter has been set Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int is_set(const char *flag) {
	int nParam, bIsSet = 0;

	init_params();
	nParam = fnd_param(flag);
	if (nParam >= 0) {
		if ((options[nParam].flags & TYPE_MASK) == OPT_FLG)
			bIsSet = (params[options[nParam].index][0] == 'Y') ? 1 : 0;
		else
			bIsSet = (options[nParam].flags & OPT_SET) || (strlen(options[nParam].dflt) > 0);
	}

	return (bIsSet); /* better a false negative than a false positive ? */
}

/*
 * Routine: set_int(int var, char *value)
 * Purpose: set an integer parameter
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
void set_int(const char *var, const char *val) {
	int nParam;

	init_params();
	nParam = fnd_param(var);
	if (nParam >= 0) {
		strcpy(params[options[nParam].index], val);
		options[nParam].flags |= OPT_SET;
	}
	return;
}

/*
 * Routine: get_int(char *var)
 * Purpose: return the value of an integer parameter
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
int get_int(const char *var) {
	int nParam;

	init_params();
	nParam = fnd_param(var);
	if (nParam >= 0)
		return (atoi(params[options[nParam].index]));
	else
		return (0);
}

double get_dbl(const char *var) {
	int nParam;

	init_params();
	nParam = fnd_param(var);
	if (nParam >= 0)
		return (atof(params[options[nParam].index]));
	else
		return (0);
}

/*
 * Routine: set_str(int var, char *value)
 * Purpose: set a character parameter
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
void set_str(const char *var, const char *val) {
	int nParam;

	init_params();
	nParam = fnd_param(var);
	if (nParam >= 0) {
		strcpy(params[options[nParam].index], val);
		options[nParam].flags |= OPT_SET;
	}

	return;
}

/*
 * Routine: get_str(char * var)
 * Purpose: return the value of a character parameter
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
char *get_str(const char *var) {
	int nParam;

	init_params();
	nParam = fnd_param(var);
	if (nParam >= 0)
		return (params[options[nParam].index]);
	else
		return (NULL);
}

/*
 * Routine: init_params(void)
 * Purpose: initialize a parameter set, setting default values
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
int init_params(void) {
	int i;

	if (InitConstants::init_params_init)
		return (0);

	for (i = 0; options[i].name != NULL; i++) {
		params[options[i].index] = (char *)malloc(PARAM_MAX_LEN * sizeof(char));
		MALLOC_CHECK(params[options[i].index]);
		strncpy(params[options[i].index], options[i].dflt, 80);
		if (*options[i].dflt)
			options[i].flags |= OPT_DFLT;
	}

	InitConstants::init_params_init = 1;

	return (0);
}

/*
 * Routine: print_options(struct OPTION_T *o, int file, int depth)
 * Purpose: print a summary of options
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
static void print_options(struct OPTION_T *o, int bShowOptional) {
	int i, w_adjust, bShow = 0, nCount = 0;

	for (i = 0; options[i].name != NULL; i++) {
		/*
		 * options come in two groups, general and "hidden". Decide which group
		 * to show in this pass, and ignore others
		 */
		bShow = 0;
		if (bShowOptional && (o[i].flags & OPT_ADV))
			bShow = 1;
		if (!bShowOptional && !(o[i].flags & OPT_ADV))
			bShow = 1;

		if (!bShow || (o[i].flags & OPT_HIDE))
			continue;

		nCount += 1;
		printf("%s = ", o[i].name);
		w_adjust = 15 - strlen(o[i].name);
		if (o[i].flags & OPT_INT)
			printf(" <n>   ");
		else if (o[i].flags & OPT_STR)
			printf(" <s>   ");
		else if (o[i].flags & OPT_SUB)
			printf(" <opt> ");
		else if (o[i].flags & OPT_FLG)
			printf(" [Y|N] ");
		else
			printf("       ");
		printf("%*s-- %s", w_adjust, " ", o[i].usage);
		if (o[i].flags & OPT_NOP)
			printf(" NOT IMPLEMENTED");
		printf("\n");
	}

	if (nCount == 0)
		printf("None defined.\n");

	return;
}
/*
 * Routine: save_file(char *path)
 * Purpose: print a summary of options
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
int save_file(const char *path) {
	int i, w_adjust;
	FILE *ofp;
	time_t timestamp;

	init_params();
	time(&timestamp);

	if ((ofp = fopen(path, "w")) == NULL)
		return (-1);

	fprintf(ofp, "--\n-- %s Benchmark Parameter File\n-- Created: %s", get_str("PROG"), ctime(&timestamp));
	fprintf(ofp, "--\n-- Each entry is of the form: '<parameter> = <value> -- "
	             "optional comment'\n");
	fprintf(ofp, "-- Refer to benchmark documentation for more details\n--\n");

	for (i = 0; options[i].name != NULL; i++) {
		if (options[i].flags & OPT_HIDE) /* hidden option */
			continue;
		if (strlen(params[options[i].index]) == 0)
			continue;

		fprintf(ofp, "%s = ", options[i].name);
		w_adjust = strlen(options[i].name) + 3;
		if (options[i].flags & OPT_STR) {
			fprintf(ofp, "\"%s\"", params[options[i].index]);
			w_adjust += 2;
		} else
			fprintf(ofp, "%s", params[options[i].index]);
		w_adjust += strlen(params[options[i].index]) + 3;
		w_adjust = 60 - w_adjust;
		fprintf(ofp, "%*s-- %s", w_adjust, " ", options[i].usage);
		if (options[i].flags & OPT_NOP)
			fprintf(ofp, " NOT IMPLEMENTED");
		fprintf(ofp, "\n");
	}

	fclose(ofp);

	return (0);
}

/*
 * Routine: usage(char *param_name, char *msg)
 * Purpose: display a usage message, with an optional error message
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
int usage(const char *param_name, const char *msg) {
	init_params();

	fprintf(stderr, "%s Population Generator (Version %d.%d.%d%s)\n", get_str("PROG"), VERSION, RELEASE, MODIFICATION,
	        PATCH);
	fprintf(stderr, "Copyright %s %s\n", COPYRIGHT, C_DATES);

	if (msg != NULL)
		printf("\nERROR: %s\n\n", msg);

	printf("\n\nUSAGE: %s [options]\n", get_str("PROG"));
	printf("\nNote: When defined in a parameter file (using -p), parmeters "
	       "should\n");
	printf("use the form below. Each option can also be set from the command\n");
	printf("line, using a form of '%cparam [optional argument]'\n", OPTION_START);
	printf("Unique anchored substrings of options are also recognized, and \n");
	printf("case is ignored, so '%csc' is equivalent to '%cSCALE'\n\n", OPTION_START, OPTION_START);
	printf("General Options\n===============\n");
	print_options(options, 0);
	printf("\n");
	printf("Advanced Options\n===============\n");
	print_options(options, 1);
	printf("\n");
	exit((msg == NULL) ? 0 : 1);
}

/*
 * Routine: set_option(int var, char *value)
 * Purpose: set a particular parameter; main entry point for the module
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
int set_option(const char *name, const char *param) {
	printf("ERROR: set_option not supported");
	assert(0);
	exit(1);
	//	int res = 1;
	//	option_t *o;
	//	char parse_int[15];
	//	char *cp;
	//
	//	init_params();
	//
	//	res = fnd_param(name);
	//	if (res == -1)
	//		return (res);
	//
	//	o = &options[res];
	//
	//	if (o->flags & OPT_NOP) {
	//		printf("ERROR: Cannot accept %s.\tNot Implemented!\n", o->name);
	//		return (0);
	//	}
	//
	//	/* option is already set from the command line or hard-coded */
	//	/* and doesn't allow multiple settings */
	//
	//	switch (o->flags & TYPE_MASK) {
	//	case OPT_FLG:
	//		if ((param && (*param == 'Y' || *param == 'Y' || *param == OPTION_START)) || (param == NULL)) {
	//			if (o->action)
	//				if (o->action((char *)o->name, NULL) < 0)
	//					usage((char *)o->name, "Cannot process option");
	//			set_flg(name);
	//		} else
	//			clr_flg(name);
	//		res = 1;
	//		break;
	//	case OPT_INT:
	//		if (o->action) {
	//			if ((res = o->action((char *)o->name, param)) < 0)
	//				usage(NULL, "Bad parameter argument");
	//			else
	//				sprintf(parse_int, "%d", res);
	//		}
	//		set_int(name, (o->action) ? parse_int : param);
	//		res = 2;
	//		break;
	//	case OPT_STR:
	//		if (*param == '"') {
	//			cp = strchr((param + 1), '"');
	//			if (cp == NULL) /* non-terminated string literal */
	//				usage(NULL, "Non-terminated string");
	//			*cp = '\0';
	//			param += 1;
	//		} else {
	//			cp = strpbrk(param, " \t\n");
	//			if (cp != NULL)
	//				*cp = '\0';
	//		}
	//		if (o->action && strlen(param))
	//			if (o->action((char *)o->name, param) < 0)
	//				usage((char *)o->name, "Cannot process option");
	//		set_str(name, param);
	//		res = 2;
	//		break;
	//	default:
	//		fprintf(stderr, "Invalid option/type (%d/%s)\n", o->flags & TYPE_MASK, o->name);
	//		exit(0);
	//		break;
	//	}
	//
	//	o->flags |= OPT_SET; /* marked as set */
	//
	//	return (res);
}

/*
 * Routine: process_options(int count, char **vector)
 * Purpose:  process a set of command line options
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20000309 need to return integer to allow processing of left-over args
 */
int process_options(int count, const char **vector) {
	int option_num = 1, res = 1;

	init_params();

	while (option_num < count) {
		if (*vector[option_num] == OPTION_START) {
			if (option_num == (count - 1))
				res = set_option(vector[option_num] + 1, NULL);
			else
				res = set_option(vector[option_num] + 1, vector[option_num + 1]);
		}

		if (res < 0) {
			printf("ERROR: option '%s' or its argument unknown.\n", (vector[option_num] + 1));
			usage(NULL, NULL);
			exit(1);
		} else
			option_num += res;
	}

#ifdef JMS
	if (is_set("VERBOSE"))
		print_params();
#endif

	return (option_num);
}

/*
 * Routine: read_file(char *param_name, char *fname)
 * Purpose: process a parameter file
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
int read_file(const char *param_name, const char *optarg) {
	FILE *fp;
	char *cp;
	char line[MAX_LINE_LEN];
	char name[100];
	int index;

	init_params();

	if ((fp = fopen(optarg, "r")) == NULL)
		return (-1);
	while (fgets(line, MAX_LINE_LEN, fp) != NULL) {
		if ((cp = strchr(line, '\n')) != NULL)
			*cp = '\0';
		if ((cp = strchr(line, '-')) != NULL)
			if (*(cp + 1) == '-')
				*cp = '\0';
		if ((cp = strtok(line, " \t=\n")) != NULL) {
			strcpy(name, cp);
			index = fnd_param(name);
			if (index == -1)
				continue; /* JMS: errors are silently ignored */
			cp += strlen(cp) + 1;
			while (*cp && strchr(" \t =", *cp))
				cp++;

			/* command line options over-ride those in a file */
			if (options[index].flags & OPT_SET)
				continue;

			if (*cp) {
				switch (options[index].flags & TYPE_MASK) {
				case OPT_INT:
					if ((cp = strtok(cp, " \t\n")) != NULL)
						set_option(name, cp);
					break;
				case OPT_STR:
				case OPT_FLG:
					set_option(name, cp);
					break;
				}
			}
		}
	}

	fclose(fp);

	return (0);
}

/*
 * Routine: print_params(void)
 * Purpose: print a parameter summary to display current settings
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
void print_params(void) {
	int i;

	init_params();

	for (i = 0; options[i].name != NULL; i++)
		if (options[i].name != NULL) {
			printf("%s = ", options[i].name);
			switch (options[i].flags & TYPE_MASK) {
			case OPT_INT:
				printf("%d\n", get_int((char *)options[i].name));
				break;
			case OPT_STR:
				printf("%s\n", get_str((char *)options[i].name));
				break;
			case OPT_FLG:
				printf("%c\n", is_set((char *)options[i].name) ? 'Y' : 'N');
				break;
			}
		}

	return;
}

/*
 * Routine: fnd_param(char *name, int *type, char *value)
 * Purpose: traverse the defined parameters, looking for a match
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: index of option
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int fnd_param(const char *name) {
	int i, res = -1;

	for (i = 0; options[i].name != NULL; i++) {
		if (strncasecmp(name, options[i].name, strlen(name)) == 0) {
			if (res == -1)
				res = i;
			else
				return (-1);
		}
	}

	return (res);
}

/*
 * Routine:  GetParamName(int nParam)
 * Purpose:  Translate between a parameter index and its name
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
char *GetParamName(int nParam) {
	init_params();

	return (char *)(options[nParam].name);
}

/*
 * Routine:  GetParamValue(int nParam)
 * Purpose:  Retrieve a parameters string value based on an index
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
char *GetParamValue(int nParam) {
	init_params();

	return (params[options[nParam].index]);
}

/*
 * Routine:  load_param(char *szValue, int nParam)
 * Purpose:  Set a parameter based on an index
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
int load_param(int nParam, const char *szValue) {
	init_params();

	if (options[nParam].flags & OPT_SET) /* already set from the command line */
		return (0);
	else
		strcpy(params[options[nParam].index], szValue);

	return (0);
}

/*
 * Routine:  IsIntParam(char *szValue, int nParam)
 * Purpose:  Boolean test for integer parameter
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
int IsIntParam(const char *szParam) {
	int nParam;

	if ((nParam = fnd_param(szParam)) == -1)
		return (nParam);

	return ((options[nParam].flags & OPT_INT) ? 1 : 0);
}

/*
 * Routine:  IsStrParam(char *szValue, int nParam)
 * Purpose:  Boolean test for string parameter
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
int IsStrParam(const char *szParam) {
	int nParam;

	if ((nParam = fnd_param(szParam)) == -1)
		return (nParam);

	return ((options[nParam].flags & OPT_STR) ? 1 : 0);
}

#ifdef TEST

main() {
	init_params();
	set_int("SCALE", "7");
	set_flg("VERBOSE");
	set_str("DISTRIBUTIONS", "'some file name'");
	print_params();
	set_int("s", "8");
	clr_flg("VERBOSE");
	printf("DIST is %s\n", get_str("DISTRIBUTIONS"));
	print_params();
	usage(NULL, NULL);
}
#endif /* TEST_PARAMS */
