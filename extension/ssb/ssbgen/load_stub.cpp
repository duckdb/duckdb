/*****************************************************************
 *  Title:      load_stub.c
 *  Sccsid:     @(#)load_stub.c	2.1.8.1
 *  Description:
 *              stub routines for:
 *          inline load of dss benchmark
 *          header creation for dss benchmark
 *
 *****************************************************************
 */

#include <stdio.h>
extern "C" {
#include "include/config.h"
#include "include/dss.h"
#include "include/dsstypes.h"
}

int close_direct(void) {
	/* any post load cleanup goes here */
	return (0);
}

int prep_direct(void) {
	/* any preload prep goes here */
	return (0);
}

int hd_cust(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the customer table\n");

	return (0);
}

int ld_cust(customer_t *cp, int mode) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No load routine has been defined", "for the customer table");

	return (0);
}

int hd_part(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the part table\n");

	return (0);
}

int ld_part(part_t *pp, int mode) {
	static int count = 0;

	if (!count++)
		printf("No load routine has been defined for the part table\n");

	return (0);
}

int ld_psupp(part_t *pp, int mode) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No load routine has been defined for the", "psupp table\n");

	return (0);
}

int hd_supp(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the supplier table\n");

	return (0);
}

int ld_supp(supplier_t *sp, int mode) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No load routine has been defined", "for the supplier table\n");

	return (0);
}

int hd_order(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the order table\n");

	return (0);
}

int ld_order(order_t *p, int mode) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No load routine has been defined", "for the order table");

	return (0);
}

int ld_line(order_t *p, int mode) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No load routine has been defined", "for the line table");

	return (0);
}

int hd_psupp(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("%s %s\n", "No header has been defined for the", "part supplier table");

	return (0);
}

int hd_line(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the lineitem table\n");

	return (0);
}

int hd_nation(FILE *f) {
	static int count = 0;

	if (!count++)
		printf("No header has been defined for the nation table\n");

	return (0);
}

int ld_date(ssb_date_t *d, int mode) {
	/*do nothing for now*/
	return (0);
}
