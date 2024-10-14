#ifndef DRIVER_H
#define DRIVER_H
#include "dss.h"
#include "dsstypes.h"

typedef int (*part_appender)(part_t *, int);
typedef int (*customer_appender)(customer_t *, int);
typedef int (*order_appender)(order_t *, int);
typedef int (*supplier_appender)(supplier_t *, int);
typedef int (*date_appender)(date_t *, int);

typedef struct {
	part_appender pr_part;
	customer_appender pr_cust;
	order_appender pr_line;
	supplier_appender pr_supp;
	date_appender pr_date;
} ssb_appender;

int gen_main(double, ssb_appender *);
#endif