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
#ifndef DCOMP_H
#define DCOMP_H

#include "config.h"
#include "porting.h"
#include "grammar.h"
#include "dist.h"

/*
 * query template grammar definition
 */
#define TKN_UNKNOWN 0
#define TKN_CREATE  1
#define TKN_WEIGHTS 2
#define TKN_TYPES   3
#define TKN_INCLUDE 4
#define TKN_SET     5
#define TKN_VARCHAR 6
#define TKN_INT     7
#define TKN_ADD     8
#define TKN_DATE    9
#define TKN_DECIMAL 10
#define TKN_NAMES   11
#define MAX_TOKEN   11

int ProcessDistribution(char *s, token_t *t);
int ProcessTypes(char *s, token_t *t);
int ProcessInclude(char *s, token_t *t);
int ProcessSet(char *s, token_t *t);
int ProcessAdd(char *s, token_t *t);

#ifdef DECLARER
token_t dcomp_tokens[MAX_TOKEN + 2] = {{TKN_UNKNOWN, "", NULL},
                                       {TKN_CREATE, "create", ProcessDistribution},
                                       {TKN_WEIGHTS, "weights", NULL},
                                       {TKN_TYPES, "types", NULL},
                                       {TKN_INCLUDE, "#include", ProcessInclude},
                                       {TKN_SET, "set", ProcessSet},
                                       {TKN_VARCHAR, "varchar", NULL},
                                       {TKN_INT, "int", NULL},
                                       {TKN_ADD, "add", ProcessAdd},
                                       {TKN_DATE, "date", NULL},
                                       {TKN_DECIMAL, "decimal", NULL},
                                       {TKN_NAMES, "names", NULL},
                                       {-1, "", NULL}};
#else
extern token_t tokens[];
#endif

#endif /* DCOMP_H */
