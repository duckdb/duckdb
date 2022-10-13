#include "liblwgeom/lwgeodetic_tree.hpp"

#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

namespace duckdb {

/***********************************************************************
 * Internal node sorting routine to make distance calculations faster?
 */

struct sort_node {
	CIRC_NODE *node;
	double d;
};

static int circ_nodes_sort_cmp(const void *a, const void *b) {
	struct sort_node *node_a = (struct sort_node *)(a);
	struct sort_node *node_b = (struct sort_node *)(b);
	if (node_a->d < node_b->d)
		return -1;
	else if (node_a->d > node_b->d)
		return 1;
	else
		return 0;
}

/**
 * Recurse from top of node tree and free all children.
 * does not free underlying point array.
 */
void circ_tree_free(CIRC_NODE *node) {
	uint32_t i;
	if (!node)
		return;

	if (node->nodes) {
		for (i = 0; i < node->num_nodes; i++)
			circ_tree_free(node->nodes[i]);
		lwfree(node->nodes);
	}
	lwfree(node);
}

/**
 * Return a point node (zero radius, referencing one point)
 */
static CIRC_NODE *circ_node_leaf_point_new(const POINTARRAY *pa) {
	CIRC_NODE *tree = (CIRC_NODE *)lwalloc(sizeof(CIRC_NODE));
	tree->p1 = tree->p2 = (POINT2D *)getPoint_internal(pa, 0);
	geographic_point_init(tree->p1->x, tree->p1->y, &(tree->center));
	tree->radius = 0.0;
	tree->nodes = NULL;
	tree->num_nodes = 0;
	tree->edge_num = 0;
	tree->geom_type = POINTTYPE;
	tree->pt_outside.x = 0.0;
	tree->pt_outside.y = 0.0;
	return tree;
}

/**
 * Create a new leaf node, storing pointers back to the end points for later.
 */
static CIRC_NODE *circ_node_leaf_new(const POINTARRAY *pa, int i) {
	POINT2D *p1, *p2;
	POINT3D q1, q2, c;
	GEOGRAPHIC_POINT g1, g2, gc;
	CIRC_NODE *node;
	double diameter;

	p1 = (POINT2D *)getPoint_internal(pa, i);
	p2 = (POINT2D *)getPoint_internal(pa, i + 1);
	geographic_point_init(p1->x, p1->y, &g1);
	geographic_point_init(p2->x, p2->y, &g2);

	diameter = sphere_distance(&g1, &g2);

	/* Zero length edge, doesn't get a node */
	if (FP_EQUALS(diameter, 0.0))
		return NULL;

	/* Allocate */
	node = (CIRC_NODE *)lwalloc(sizeof(CIRC_NODE));
	node->p1 = p1;
	node->p2 = p2;

	/* Convert ends to X/Y/Z, sum, and normalize to get mid-point */
	geog2cart(&g1, &q1);
	geog2cart(&g2, &q2);
	vector_sum(&q1, &q2, &c);
	normalize(&c);
	cart2geog(&c, &gc);
	node->center = gc;
	node->radius = diameter / 2.0;

	/* Leaf has no children */
	node->num_nodes = 0;
	node->nodes = NULL;
	node->edge_num = i;

	/* Zero out metadata */
	node->pt_outside.x = 0.0;
	node->pt_outside.y = 0.0;
	node->geom_type = 0;

	return node;
}

/**
 * Given the centers of two circles, and the offset distance we want to put the new center between them
 * (calculated as the distance implied by the radii of the inputs and the distance between the centers)
 * figure out where the new center point is, by getting the direction from c1 to c2 and projecting
 * from c1 in that direction by the offset distance.
 */
static int circ_center_spherical(const GEOGRAPHIC_POINT *c1, const GEOGRAPHIC_POINT *c2, double distance, double offset,
                                 GEOGRAPHIC_POINT *center) {
	/* Direction from c1 to c2 */
	double dir = sphere_direction(c1, c2, distance);

	/* Catch sphere_direction when it barfs */
	if (isnan(dir))
		return LW_FAILURE;

	/* Center of new circle is projection from start point, using offset distance*/
	return sphere_project(c1, offset, dir, center);
}

/**
 * Where the circ_center_spherical() function fails, we need a fall-back. The failures
 * happen in short arcs, where the spherical distance between two points is practically
 * the same as the straight-line distance, so our fallback will be to use the straight-line
 * between the two to calculate the new projected center. For proportions far from 0.5
 * this will be increasingly more incorrect.
 */
static int circ_center_cartesian(const GEOGRAPHIC_POINT *c1, const GEOGRAPHIC_POINT *c2, double distance, double offset,
                                 GEOGRAPHIC_POINT *center) {
	POINT3D p1, p2;
	POINT3D p1p2, pc;
	double proportion = offset / distance;

	geog2cart(c1, &p1);
	geog2cart(c2, &p2);

	/* Difference between p2 and p1 */
	p1p2.x = p2.x - p1.x;
	p1p2.y = p2.y - p1.y;
	p1p2.z = p2.z - p1.z;

	/* Scale difference to proportion */
	p1p2.x *= proportion;
	p1p2.y *= proportion;
	p1p2.z *= proportion;

	/* Add difference to p1 to get approximate center point */
	pc.x = p1.x + p1p2.x;
	pc.y = p1.y + p1p2.y;
	pc.z = p1.z + p1p2.z;
	normalize(&pc);

	/* Convert center point to geographics */
	cart2geog(&pc, center);

	return LW_SUCCESS;
}

/**
 * Create a new internal node, calculating the new measure range for the node,
 * and storing pointers to the child nodes.
 */
static CIRC_NODE *circ_node_internal_new(CIRC_NODE **c, uint32_t num_nodes) {
	CIRC_NODE *node = NULL;
	GEOGRAPHIC_POINT new_center, c1;
	double new_radius;
	double offset1, dist, D, r1, ri;
	uint32_t i, new_geom_type;

	/* Can't do anything w/ empty input */
	if (num_nodes < 1)
		return node;

	/* Initialize calculation with values of the first circle */
	new_center = c[0]->center;
	new_radius = c[0]->radius;
	new_geom_type = c[0]->geom_type;

	/* Merge each remaining circle into the new circle */
	for (i = 1; i < num_nodes; i++) {
		c1 = new_center;
		r1 = new_radius;

		dist = sphere_distance(&c1, &(c[i]->center));
		ri = c[i]->radius;

		/* Promote geometry types up the tree, getting more and more collected */
		/* Go until we find a value */
		if (!new_geom_type) {
			new_geom_type = c[i]->geom_type;
		}
		/* Promote singleton to a multi-type */
		// Need to do with postgis
		// else if ( ! lwtype_is_collection(new_geom_type) )
		// {
		// 	/* Anonymous collection if types differ */
		// 	if ( new_geom_type != c[i]->geom_type )
		// 	{
		// 		new_geom_type = COLLECTIONTYPE;
		// 	}
		// 	else
		// 	{
		// 		new_geom_type = lwtype_get_collectiontype(new_geom_type);
		// 	}
		// }
		// /* If we can't add next feature to this collection cleanly, promote again to anonymous collection */
		// else if ( new_geom_type != lwtype_get_collectiontype(c[i]->geom_type) )
		// {
		// 	new_geom_type = COLLECTIONTYPE;
		// }

		if (FP_EQUALS(dist, 0)) {
			new_radius = r1 + 2 * dist;
			new_center = c1;
		} else if (dist < fabs(r1 - ri)) {
			/* new contains next */
			if (r1 > ri) {
				new_center = c1;
				new_radius = r1;
			}
			/* next contains new */
			else {
				new_center = c[i]->center;
				new_radius = ri;
			}
		} else {
			/* New circle diameter */
			D = dist + r1 + ri;

			/* New radius */
			new_radius = D / 2.0;

			/* Distance from cn1 center to the new center */
			offset1 = ri + (D - (2.0 * r1 + 2.0 * ri)) / 2.0;

			/* Sometimes the sphere_direction function fails... this causes the center calculation */
			/* to fail too. In that case, we're going to fall back to a cartesian calculation, which */
			/* is less exact, so we also have to pad the radius by (hack alert) an arbitrary amount */
			/* which is hopefully always big enough to contain the input edges */
			if (circ_center_spherical(&c1, &(c[i]->center), dist, offset1, &new_center) == LW_FAILURE) {
				circ_center_cartesian(&c1, &(c[i]->center), dist, offset1, &new_center);
				new_radius *= 1.1;
			}
		}
	}

	node = (CIRC_NODE *)lwalloc(sizeof(CIRC_NODE));
	node->p1 = NULL;
	node->p2 = NULL;
	node->center = new_center;
	node->radius = new_radius;
	node->num_nodes = num_nodes;
	node->nodes = c;
	node->edge_num = -1;
	node->geom_type = new_geom_type;
	node->pt_outside.x = 0.0;
	node->pt_outside.y = 0.0;
	return node;
}

static CIRC_NODE *circ_nodes_merge(CIRC_NODE **nodes, int num_nodes) {
	CIRC_NODE **inodes = NULL;
	int num_children = num_nodes;
	int inode_num = 0;
	int num_parents = 0;
	int j;

	/* TODO, roll geom_type *up* as tree is built, changing to collection types as simple types are merged
	 * TODO, change the distance algorithm to drive down to simple types first, test pip on poly/other cases, then test
	 * edges
	 */

	while (num_children > 1) {
		for (j = 0; j < num_children; j++) {
			inode_num = (j % CIRC_NODE_SIZE);
			if (inode_num == 0)
				inodes = (CIRC_NODE **)lwalloc(sizeof(CIRC_NODE *) * CIRC_NODE_SIZE);

			inodes[inode_num] = nodes[j];

			if (inode_num == CIRC_NODE_SIZE - 1)
				nodes[num_parents++] = circ_node_internal_new(inodes, CIRC_NODE_SIZE);
		}

		/* Clean up any remaining nodes... */
		if (inode_num == 0) {
			/* Promote solo nodes without merging */
			nodes[num_parents++] = inodes[0];
			lwfree(inodes);
		} else if (inode_num < CIRC_NODE_SIZE - 1) {
			/* Merge spare nodes */
			nodes[num_parents++] = circ_node_internal_new(inodes, inode_num + 1);
		}

		num_children = num_parents;
		num_parents = 0;
	}

	/* Return a reference to the head of the tree */
	return nodes[0];
}

/**
 * Build a tree of nodes from a point array, one node per edge.
 */
CIRC_NODE *circ_tree_new(const POINTARRAY *pa) {
	int num_edges;
	int i, j;
	CIRC_NODE **nodes;
	CIRC_NODE *node;
	CIRC_NODE *tree;

	/* Can't do anything with no points */
	if (pa->npoints < 1)
		return NULL;

	/* Special handling for a single point */
	if (pa->npoints == 1)
		return circ_node_leaf_point_new(pa);

	/* First create a flat list of nodes, one per edge. */
	num_edges = pa->npoints - 1;
	nodes = (CIRC_NODE **)lwalloc(sizeof(CIRC_NODE *) * pa->npoints);
	j = 0;
	for (i = 0; i < num_edges; i++) {
		node = circ_node_leaf_new(pa, i);
		if (node) /* Not zero length? */
			nodes[j++] = node;
	}

	/* Special case: only zero-length edges. Make a point node. */
	if (j == 0) {
		lwfree(nodes);
		return circ_node_leaf_point_new(pa);
	}

	/* Merge the node list pairwise up into a tree */
	tree = circ_nodes_merge(nodes, j);

	/* Free the old list structure, leaving the tree in place */
	lwfree(nodes);

	return tree;
}

static CIRC_NODE *lwpoint_calculate_circ_tree(const LWPOINT *lwpoint) {
	CIRC_NODE *node;
	node = circ_tree_new(lwpoint->point);
	node->geom_type = lwgeom_get_type((LWGEOM *)lwpoint);
	return node;
}

CIRC_NODE *lwgeom_calculate_circ_tree(const LWGEOM *lwgeom) {
	if (lwgeom_is_empty(lwgeom))
		return NULL;

	switch (lwgeom->type) {
	case POINTTYPE:
		return lwpoint_calculate_circ_tree((LWPOINT *)lwgeom);

		// Need to do with postgis

	default:
		lwerror("Unable to calculate spherical index tree for type %s", lwtype_name(lwgeom->type));
		return NULL;
	}
}

static double circ_node_min_distance(const CIRC_NODE *n1, const CIRC_NODE *n2) {
	double d = sphere_distance(&(n1->center), &(n2->center));
	double r1 = n1->radius;
	double r2 = n2->radius;

	if (d < r1 + r2)
		return 0.0;

	return d - r1 - r2;
}

static double circ_node_max_distance(const CIRC_NODE *n1, const CIRC_NODE *n2) {
	return sphere_distance(&(n1->center), &(n2->center)) + n1->radius + n2->radius;
}

/**
 * Internal nodes have their point references set to NULL.
 */
static inline int circ_node_is_leaf(const CIRC_NODE *node) {
	return (node->num_nodes == 0);
}

static void circ_internal_nodes_sort(CIRC_NODE **nodes, uint32_t num_nodes, const CIRC_NODE *target_node) {
	uint32_t i;
	struct sort_node sort_nodes[CIRC_NODE_SIZE];

	/* Copy incoming nodes into sorting array and calculate */
	/* distance to the target node */
	for (i = 0; i < num_nodes; i++) {
		sort_nodes[i].node = nodes[i];
		sort_nodes[i].d = sphere_distance(&(nodes[i]->center), &(target_node->center));
	}

	/* Sort the nodes and copy the result back into the input array */
	qsort(sort_nodes, num_nodes, sizeof(struct sort_node), circ_nodes_sort_cmp);
	for (i = 0; i < num_nodes; i++) {
		nodes[i] = sort_nodes[i].node;
	}
	return;
}

/***********************************************************************/

static double circ_tree_distance_tree_internal(const CIRC_NODE *n1, const CIRC_NODE *n2, double threshold,
                                               double *min_dist, double *max_dist, GEOGRAPHIC_POINT *closest1,
                                               GEOGRAPHIC_POINT *closest2) {
	double max;
	double d, d_min;
	uint32_t i;

	// printf("-==-\n");
	// circ_tree_print(n1, 0);
	// printf("--\n");
	// circ_tree_print(n2, 0);

	/* Short circuit if we've already hit the minimum */
	if (*min_dist < threshold || *min_dist == 0.0)
		return *min_dist;

	/* If your minimum is greater than anyone's maximum, you can't hold the winner */
	if (circ_node_min_distance(n1, n2) > *max_dist) {
		return FLT_MAX;
	}

	/* If your maximum is a new low, we'll use that as our new global tolerance */
	max = circ_node_max_distance(n1, n2);
	if (max < *max_dist)
		*max_dist = max;

	/* Polygon on one side, primitive type on the other. Check for point-in-polygon */
	/* short circuit. */
	// Need to do with postgis

	// if ( n1->geom_type == POLYGONTYPE && n2->geom_type && ! lwtype_is_collection(n2->geom_type) )
	// {
	// 	POINT2D pt;
	// 	circ_tree_get_point(n2, &pt);
	// 	if ( circ_tree_contains_point(n1, &pt, &(n1->pt_outside), 0, NULL) )
	// 	{
	// 		*min_dist = 0.0;
	// 		geographic_point_init(pt.x, pt.y, closest1);
	// 		geographic_point_init(pt.x, pt.y, closest2);
	// 		return *min_dist;
	// 	}
	// }
	/* Polygon on one side, primitive type on the other. Check for point-in-polygon */
	/* short circuit. */
	// Need to do with postgis

	// if ( n2->geom_type == POLYGONTYPE && n1->geom_type && ! lwtype_is_collection(n1->geom_type) )
	// {
	// 	POINT2D pt;
	// 	circ_tree_get_point(n1, &pt);
	// 	if ( circ_tree_contains_point(n2, &pt, &(n2->pt_outside), 0, NULL) )
	// 	{
	// 		geographic_point_init(pt.x, pt.y, closest1);
	// 		geographic_point_init(pt.x, pt.y, closest2);
	// 		*min_dist = 0.0;
	// 		return *min_dist;
	// 	}
	// }

	/* Both leaf nodes, do a real distance calculation */
	if (circ_node_is_leaf(n1) && circ_node_is_leaf(n2)) {
		double d;
		GEOGRAPHIC_POINT close1, close2;
		/* One of the nodes is a point */
		if (n1->p1 == n1->p2 || n2->p1 == n2->p2) {
			GEOGRAPHIC_EDGE e;
			GEOGRAPHIC_POINT gp1, gp2;

			/* Both nodes are points! */
			if (n1->p1 == n1->p2 && n2->p1 == n2->p2) {
				geographic_point_init(n1->p1->x, n1->p1->y, &gp1);
				geographic_point_init(n2->p1->x, n2->p1->y, &gp2);
				close1 = gp1;
				close2 = gp2;
				d = sphere_distance(&gp1, &gp2);
			}
			/* Node 1 is a point */
			else if (n1->p1 == n1->p2) {
				geographic_point_init(n1->p1->x, n1->p1->y, &gp1);
				geographic_point_init(n2->p1->x, n2->p1->y, &(e.start));
				geographic_point_init(n2->p2->x, n2->p2->y, &(e.end));
				close1 = gp1;
				d = edge_distance_to_point(&e, &gp1, &close2);
			}
			/* Node 2 is a point */
			else {
				geographic_point_init(n2->p1->x, n2->p1->y, &gp1);
				geographic_point_init(n1->p1->x, n1->p1->y, &(e.start));
				geographic_point_init(n1->p2->x, n1->p2->y, &(e.end));
				close1 = gp1;
				d = edge_distance_to_point(&e, &gp1, &close2);
			}
		}
		/* Both nodes are edges */
		else {
			GEOGRAPHIC_EDGE e1, e2;
			GEOGRAPHIC_POINT g;
			POINT3D A1, A2, B1, B2;
			geographic_point_init(n1->p1->x, n1->p1->y, &(e1.start));
			geographic_point_init(n1->p2->x, n1->p2->y, &(e1.end));
			geographic_point_init(n2->p1->x, n2->p1->y, &(e2.start));
			geographic_point_init(n2->p2->x, n2->p2->y, &(e2.end));
			geog2cart(&(e1.start), &A1);
			geog2cart(&(e1.end), &A2);
			geog2cart(&(e2.start), &B1);
			geog2cart(&(e2.end), &B2);
			if (edge_intersects(&A1, &A2, &B1, &B2)) {
				d = 0.0;
				edge_intersection(&e1, &e2, &g);
				close1 = close2 = g;
			} else {
				d = edge_distance_to_edge(&e1, &e2, &close1, &close2);
			}
		}
		if (d < *min_dist) {
			*min_dist = d;
			*closest1 = close1;
			*closest2 = close2;
		}
		return d;
	} else {
		d_min = FLT_MAX;
		/* Drive the recursion into the COLLECTION types first so we end up with */
		/* pairings of primitive geometries that can be forced into the point-in-polygon */
		/* tests above. */
		// if ( n1->geom_type && lwtype_is_collection(n1->geom_type) )
		// {
		// 	circ_internal_nodes_sort(n1->nodes, n1->num_nodes, n2);
		// 	for ( i = 0; i < n1->num_nodes; i++ )
		// 	{
		// 		d = circ_tree_distance_tree_internal(n1->nodes[i], n2, threshold, min_dist, max_dist, closest1,
		// closest2); 		d_min = FP_MIN(d_min, d);
		// 	}
		// }
		// else if ( n2->geom_type && lwtype_is_collection(n2->geom_type) )
		// {
		// 	circ_internal_nodes_sort(n2->nodes, n2->num_nodes, n1);
		// 	for ( i = 0; i < n2->num_nodes; i++ )
		// 	{
		// 		d = circ_tree_distance_tree_internal(n1, n2->nodes[i], threshold, min_dist, max_dist, closest1,
		// closest2); 		d_min = FP_MIN(d_min, d);
		// 	}
		// }
		// else if ( ! circ_node_is_leaf(n1) )
		// Need to do with postgis

		if (!circ_node_is_leaf(n1)) {
			circ_internal_nodes_sort(n1->nodes, n1->num_nodes, n2);
			for (i = 0; i < n1->num_nodes; i++) {
				d = circ_tree_distance_tree_internal(n1->nodes[i], n2, threshold, min_dist, max_dist, closest1,
				                                     closest2);
				d_min = FP_MIN(d_min, d);
			}
		} else if (!circ_node_is_leaf(n2)) {
			circ_internal_nodes_sort(n2->nodes, n2->num_nodes, n1);
			for (i = 0; i < n2->num_nodes; i++) {
				d = circ_tree_distance_tree_internal(n1, n2->nodes[i], threshold, min_dist, max_dist, closest1,
				                                     closest2);
				d_min = FP_MIN(d_min, d);
			}
		} else {
			/* Never get here */
		}

		return d_min;
	}
}

double circ_tree_distance_tree(const CIRC_NODE *n1, const CIRC_NODE *n2, const SPHEROID *spheroid, double threshold) {
	double min_dist = FLT_MAX;
	double max_dist = FLT_MAX;
	GEOGRAPHIC_POINT closest1, closest2;
	/* Quietly decrease the threshold just a little to avoid cases where */
	/* the actual spheroid distance is larger than the sphere distance */
	/* causing the return value to be larger than the threshold value */
	double threshold_radians = 0.95 * threshold / spheroid->radius;

	circ_tree_distance_tree_internal(n1, n2, threshold_radians, &min_dist, &max_dist, &closest1, &closest2);

	/* Spherical case */
	if (spheroid->a == spheroid->b) {
		return spheroid->radius * sphere_distance(&closest1, &closest2);
	} else {
		return spheroid_distance(&closest1, &closest2, spheroid);
	}
}

} // namespace duckdb
