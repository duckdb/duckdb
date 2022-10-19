#pragma once
#include "duckdb.hpp"
#include "liblwgeom/lwgeodetic.hpp"

namespace duckdb {

#define CIRC_NODE_SIZE 8

/**
 * Note that p1 and p2 are pointers into an independent POINTARRAY, do not free them.
 */
typedef struct circ_node {
	GEOGRAPHIC_POINT center;
	double radius;
	uint32_t num_nodes;
	struct circ_node **nodes;
	int edge_num;
	uint32_t geom_type;
	double d;
	POINT2D pt_outside;
	POINT2D *p1;
	POINT2D *p2;
} CIRC_NODE;

void circ_tree_free(CIRC_NODE *node);
CIRC_NODE *circ_tree_new(const POINTARRAY *pa);
CIRC_NODE *lwgeom_calculate_circ_tree(const LWGEOM *lwgeom);
double circ_tree_distance_tree(const CIRC_NODE *n1, const CIRC_NODE *n2, const SPHEROID *spheroid, double threshold);

} // namespace duckdb
