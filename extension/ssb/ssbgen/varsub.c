/* Sccsid:     @(#)varsub.c	2.1.8.3 */
#include "include/config.h"
#include <stdio.h>
#ifndef _POSIX_SOURCE
// #include <malloc.h>
#endif /* POSIX_SOURCE */
#if (defined(_POSIX_) || !defined(WIN32))
#ifndef DOS
#include <unistd.h>
#endif
#endif /* WIN32 */
#include <string.h>
#include "include/config.h"
#include "include/dss.h"
#include "include/tpcd.h"
#ifdef ADHOC
#include "adhoc.h"
extern adhoc_t adhocs[];
#endif /* ADHOC */

#define MAX_PARAM 10 /* maximum number of parameter substitutions in a query */

extern long Seed[];
extern char **asc_date;
extern double flt_scale;
extern distribution q13a, q13b;
long *permute(long *set, int cnt, long stream);

long brands[25] = {11, 12, 13, 14, 15, 21, 22, 23, 24, 25, 31, 32, 33, 34, 35,
				   41, 42, 43, 44, 45, 51, 52, 53, 54, 55};
long sizes[50] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
				  21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
				  41, 42, 43, 44, 45, 46, 47, 48, 49, 50};
long ccode[25] = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34};
char *defaults[24][11] =
	{
		{"90", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 1 */
		{"15", "BRASS", "EUROPE",
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 2 */
		{"BUILDING", "1995-03-15", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 3 */
		{"1993-07-01", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 4 */
		{"ASIA", "1994-01-01", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 5 */
		{"1994-01-01", ".06", "24",
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 6 */
		{"FRANCE", "GERMANY", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 7 */
		{"BRAZIL", "AMERICA", "ECONOMY ANODIZED STEEL",
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 8 */
		{"green", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 9 */
		{"1993-10-01", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 10 */
		{"GERMANY", "0.0001", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 11 */
		{"MAIL", "SHIP", "1994-01-01",
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 12 */
		{"special", "requests", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 13 */
		{"1995-09-01", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 14 */
		{"1996-01-01", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}, /* 15 */
		{"Brand#45", "MEDIUM POLISHED", "49",
		 "14", "23", "45", "19", "3", "36", "9", NULL}, /* 16 */
		{"Brand#23", "MED BOX", NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},									 /* 17 */
		{"300", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},				 /* 18 */
		{"Brand#12", "Brand#23", "Brand#34", "1", "10", "20", NULL, NULL, NULL, NULL, NULL}, /* 19 */
		{"forest", "1994-01-01", "CANADA", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},	 /* 20 */
		{"SAUDI ARABIA", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},		 /* 21 */
		{"13", "31", "23", "29", "30", "18", "17", NULL, NULL, NULL, NULL},					 /* 22 */
		{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},					 /* UF1 */
		{NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},					 /* UF2 */
};
void varsub(int qnum, int vnum, int flags)
{
	static char param[11][128];
	static FILE *lfp = NULL;
	long *lptr;
	char *ptr;
	int i = 0,
		tmp_date;
	long tmp1,
		tmp2;

	if (vnum == 0)
	{
		if ((flags & DFLT) == 0)
		{
			switch (qnum)
			{
			case 1:
				sprintf(param[1], "%d", UnifInt((long)60, (long)120, (long)qnum));
				param[2][0] = '\0';
				break;
			case 2:
				sprintf(param[1], "%d",
						UnifInt((long)P_SIZE_MIN, (long)P_SIZE_MAX, qnum));
				pick_str(&p_types_set, qnum, param[3]);
				ptr = param[3] + strlen(param[3]);
				while (*(ptr - 1) != ' ')
					ptr--;
				strcpy(param[2], ptr);
				pick_str(&regions, qnum, param[3]);
				param[4][0] = '\0';
				break;
			case 3:
				pick_str(&c_mseg_set, qnum, param[1]);
				/*
				 * pick a random offset within the month of march and add the
				 * appropriate magic numbers to position the output functions
				 * at the start of March '95
				 */
				tmp_date = UnifInt((long)0, (long)30, (long)qnum);
				strcpy(param[2], *(asc_date + tmp_date + 1155));
				param[3][0] = '\0';
				break;
			case 4:
				tmp_date = UnifInt(1, 58, qnum);
				sprintf(param[1], "19%02d-%02d-01",
						93 + tmp_date / 12, tmp_date % 12 + 1);
				param[2][0] = '\0';
				break;
			case 5:
				pick_str(&regions, qnum, param[1]);
				tmp_date = UnifInt((long)93, (long)97, (long)qnum);
				sprintf(param[2], "19%d-01-01", tmp_date);
				param[3][0] = '\0';
				break;
			case 6:
				tmp_date = UnifInt(93, 97, qnum);
				sprintf(param[1], "19%d-01-01", tmp_date);
				sprintf(param[2], "0.0%d", UnifInt(2, 9, qnum));
				sprintf(param[3], "%d", UnifInt((long)24, (long)25, (long)qnum));
				param[4][0] = '\0';
				break;
			case 7:
				tmp_date = pick_str(&nations2, qnum, param[1]);
				while (pick_str(&nations2, qnum, param[2]) == tmp_date)
					;
				param[3][0] = '\0';
				break;
			case 8:
				tmp_date = pick_str(&nations2, qnum, param[1]);
				tmp_date = nations.list[tmp_date].weight;
				strcpy(param[2], regions.list[tmp_date].text);
				pick_str(&p_types_set, qnum, param[3]);
				param[4][0] = '\0';
				break;
			case 9:
				pick_str(&colors, qnum, param[1]);
				param[2][0] = '\0';
				break;
			case 10:
				tmp_date = UnifInt(1, 24, qnum);
				sprintf(param[1], "19%02d-%02d-01",
						93 + tmp_date / 12, tmp_date % 12 + 1);
				param[2][0] = '\0';
				break;
			case 11:
				pick_str(&nations2, qnum, param[1]);
				sprintf(param[2], "%11.10f", Q11_FRACTION / flt_scale);
				param[3][0] = '\0';
				break;
			case 12:
				tmp_date = pick_str(&l_smode_set, qnum, param[1]);
				while (tmp_date == pick_str(&l_smode_set, qnum, param[2]))
					;
				tmp_date = UnifInt(93, 97, qnum);
				sprintf(param[3], "19%d-01-01", tmp_date);
				param[4][0] = '\0';
				break;
			case 13:
				pick_str(&q13a, qnum, param[1]);
				pick_str(&q13b, qnum, param[2]);
				param[3][0] = '\0';
				break;
			case 14:
				tmp_date = UnifInt(1, 60, qnum);
				sprintf(param[1], "19%02d-%02d-01",
						93 + tmp_date / 12, tmp_date % 12 + 1);
				param[2][0] = '\0';
				break;
			case 15:
				tmp_date = UnifInt(1, 58, qnum);
				sprintf(param[1], "19%02d-%02d-01",
						93 + tmp_date / 12, tmp_date % 12 + 1);
				param[2][0] = '\0';
				break;
			case 16:
				tmp1 = UnifInt(1, 5, qnum);
				tmp2 = UnifInt(1, 5, qnum);
				sprintf(param[1], "Brand#%d%d", tmp1, tmp2);
				pick_str(&p_types_set, qnum, param[2]);
				ptr = param[2] + strlen(param[2]);
				while (*(--ptr) != ' ')
					;
				*ptr = '\0';
				lptr = &sizes[0];
				for (i = 3; i <= MAX_PARAM; i++)
				{
					sprintf(param[i], "%ld", *permute(lptr, 50, qnum) + 1);
					lptr = (long *)NULL;
				}
				break;
			case 17:
				tmp1 = UnifInt(1, 5, qnum);
				tmp2 = UnifInt(1, 5, qnum);
				sprintf(param[1], "Brand#%d%d", tmp1, tmp2);
				pick_str(&p_cntr_set, qnum, param[2]);
				param[3][0] = '\0';
				break;
			case 18:
				sprintf(param[1], "%ld", UnifInt(312, 315, qnum));
				param[2][0] = '\0';
				break;
			case 19:
				tmp1 = UnifInt(1, 5, qnum);
				tmp2 = UnifInt(1, 5, qnum);
				sprintf(param[1], "Brand#%d%d", tmp1, tmp2);
				tmp1 = UnifInt(1, 5, qnum);
				tmp2 = UnifInt(1, 5, qnum);
				sprintf(param[2], "Brand#%d%d", tmp1, tmp2);
				tmp1 = UnifInt(1, 5, qnum);
				tmp2 = UnifInt(1, 5, qnum);
				sprintf(param[3], "Brand#%d%d", tmp1, tmp2);
				sprintf(param[4], "%ld", UnifInt(1, 10, qnum));
				sprintf(param[5], "%ld", UnifInt(10, 20, qnum));
				sprintf(param[6], "%ld", UnifInt(20, 30, qnum));
				param[7][0] = '\0';
				break;
			case 20:
				pick_str(&colors, qnum, param[1]);
				tmp_date = UnifInt(93, 97, qnum);
				sprintf(param[2], "19%d-01-01", tmp_date);
				pick_str(&nations2, qnum, param[3]);
				param[4][0] = '\0';
				break;
			case 21:
				pick_str(&nations2, qnum, param[1]);
				param[2][0] = '\0';
				break;
			case 22:
				lptr = &ccode[0];
				for (i = 0; i <= 7; i++)
				{
					sprintf(param[i + 1], "%ld", 10 + *permute(lptr, 25, qnum));
					lptr = (long *)NULL;
				}
				param[8][0] = '\0';
				break;
			case 23:
			case 24:
				break;
			default:
				fprintf(stderr,
						"No variable definitions available for query %d\n",
						qnum);
				return;
			}
		}

		if (flags & LOG)
		{
			if (lfp == NULL)
			{
				lfp = fopen(lfile, "a");
				OPEN_CHECK(lfp, lfile);
			}
			fprintf(lfp, "%d", qnum);
			for (i = 1; i <= 10; i++)
				if (flags & DFLT)
				{
					if (defaults[qnum - 1][i - 1] == NULL)
						break;
					else
						fprintf(lfp, "\t%s", defaults[qnum - 1][i - 1]);
				}
				else
				{
					if (param[i][0] == '\0')
						break;
					else
						fprintf(lfp, "\t%s", param[i]);
				}
			fprintf(lfp, "\n");
		}
	}
	else
	{
		if (flags & DFLT)
		{
			/* to allow -d to work at all scale factors */
			if (qnum == 11 && vnum == 2)
				fprintf(ofp, "%11.10f", Q11_FRACTION / flt_scale);
			else if (defaults[qnum - 1][vnum - 1])
				fprintf(ofp, "%s", defaults[qnum - 1][vnum - 1]);
			else
				fprintf(stderr,
						"Bad default request (q: %d, p: %d)\n",
						qnum, vnum);
		}
		else
		{
			if (param[vnum] && vnum <= MAX_PARAM)
				fprintf(ofp, "%s", param[vnum]);
			else
				fprintf(stderr, "Bad parameter request (q: %d, p: %d)\n",
						qnum, vnum);
		}
	}
	return;
}
