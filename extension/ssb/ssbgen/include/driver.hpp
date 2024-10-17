#ifndef DRIVER_H
#define DRIVER_H

#include <functional>

extern "C" {
#include "dss.h"
#include "dsstypes.h"
}

using part_appender = std::function<int(part_t *, int)>;
using customer_appender = std::function<int(customer_t *, int)>;
using order_appender = std::function<int(order_t *, int)>;
using supplier_appender = std::function<int(supplier_t *, int)>;
using date_appender = std::function<int(ssb_date_t *, int)>;

typedef struct {
	part_appender pr_part;
	customer_appender pr_cust;
	order_appender pr_line;
	supplier_appender pr_supp;
	date_appender pr_date;
} ssb_appender;

int gen_main(double, ssb_appender *);
#endif