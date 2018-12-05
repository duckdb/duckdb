#include "dbgen_gunk.hpp"

#include "dss.h"
#include "dsstypes.h"

void load_dists(void) {
	read_dist(env_config(DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "colors", &colors);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "nations", &nations);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "regions", &regions);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "o_oprio", &o_priority_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "instruct", &l_instruct_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "category", &l_category_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

	/* load the distributions that contain text generation */
	read_dist(env_config(DIST_TAG, DIST_DFLT), "nouns", &nouns);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "verbs", &verbs);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "terminators", &terminators);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "articles", &articles);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "grammar", &grammar);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "np", &np);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "vp", &vp);
}

static void cleanup_dist(distribution *target) {
	if (!target) {
		return;
	}
	if (target->list) {
		for (int i = 0; i < target->count; i++) {
			if (target->list[i].text) {
				free(target->list[i].text);
			}
		}
		free(target->list);
	}
}

void cleanup_dists(void) {
	cleanup_dist(&p_cntr_set);
	cleanup_dist(&colors);
	cleanup_dist(&p_types_set);
	cleanup_dist(&nations);
	cleanup_dist(&regions);
	cleanup_dist(&o_priority_set);
	cleanup_dist(&l_instruct_set);
	cleanup_dist(&l_smode_set);
	cleanup_dist(&l_category_set);
	cleanup_dist(&l_rflag_set);
	cleanup_dist(&c_mseg_set);
	cleanup_dist(&nouns);
	cleanup_dist(&verbs);
	cleanup_dist(&adjectives);
	cleanup_dist(&adverbs);
	cleanup_dist(&auxillaries);
	cleanup_dist(&terminators);
	cleanup_dist(&articles);
	cleanup_dist(&prepositions);
	cleanup_dist(&grammar);
	cleanup_dist(&np);
	cleanup_dist(&vp);
}
