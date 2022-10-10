#include "json.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cstring>

using namespace json;

namespace duckdb {

static inline json_object *findMemberByName(json_object *poObj, const char *pszName) {
	json_object *poTmp;
	json_object_iter it;

	poTmp = poObj;

	if (!pszName || !poObj)
		return NULL;

	it.key = NULL;
	it.val = NULL;
	it.entry = NULL;

	if (json_object_get_object(poTmp)) {
		if (!json_object_get_object(poTmp)->head) {
			// lwerror("invalid GeoJSON representation");
			return NULL;
		}

		for (it.entry = json_object_get_object(poTmp)->head;
		     (it.entry ? (it.key = (char *)it.entry->k, it.val = (json_object *)it.entry->v, it.entry) : 0);
		     it.entry = it.entry->next) {
			if (strcasecmp((char *)it.key, pszName) == 0)
				return it.val;
		}
	}

	return NULL;
}

static inline json_object *parse_coordinates(json_object *geojson) {
	json_object *coordinates = findMemberByName(geojson, "coordinates");
	if (!coordinates) {
		// lwerror("Unable to find 'coordinates' in GeoJSON string");
		return NULL;
	}

	if (json_type_array != json_object_get_type(coordinates)) {
		// lwerror("The 'coordinates' in GeoJSON are not an array");
		return NULL;
	}
	return coordinates;
}

static inline int parse_geojson_coord(json_object *poObj, int *hasz, POINTARRAY *pa) {
	POINT4D pt = {0, 0, 0, 0};

	if (json_object_get_type(poObj) == json_type_array) {
		json_object *poObjCoord = NULL;
		const int nSize = json_object_array_length(poObj);
		if (nSize < 2) {
			// lwerror("Too few ordinates in GeoJSON");
			return LW_FAILURE;
		}

		/* Read X coordinate */
		poObjCoord = json_object_array_get_idx(poObj, 0);
		pt.x = json_object_get_double(poObjCoord);

		/* Read Y coordinate */
		poObjCoord = json_object_array_get_idx(poObj, 1);
		pt.y = json_object_get_double(poObjCoord);

		if (nSize > 2) /* should this be >= 3 ? */
		{
			/* Read Z coordinate */
			poObjCoord = json_object_array_get_idx(poObj, 2);
			pt.z = json_object_get_double(poObjCoord);
			*hasz = LW_TRUE;
		}
	} else {
		/* If it's not an array, just don't handle it */
		// lwerror("The 'coordinates' in GeoJSON are not sufficiently nested");
		return LW_FAILURE;
	}

	return ptarray_append_point(pa, &pt, LW_TRUE);
}

static inline LWGEOM *parse_geojson_point(json_object *geojson, int *hasz) {
	json_object *coords = parse_coordinates(geojson);
	if (!coords)
		return NULL;
	POINTARRAY *pa = ptarray_construct_empty(1, 0, 1);
	parse_geojson_coord(coords, hasz, pa);
	return (LWGEOM *)lwpoint_construct(0, NULL, pa);
}

static inline LWGEOM *parse_geojson(json_object *geojson, int *hasz) {
	json_object *type = NULL;
	const char *name;

	if (!geojson) {
		// lwerror("invalid GeoJSON representation");
		return NULL;
	}

	type = findMemberByName(geojson, "type");
	if (!type) {
		// lwerror("unknown GeoJSON type");
		return NULL;
	}

	name = json_object_get_string(type);

	if (strcasecmp(name, "Point") == 0)
		return parse_geojson_point(geojson, hasz);

	// Need to do with postgis

	// lwerror("invalid GeoJson representation");
	return NULL; /* Never reach */
}

LWGEOM *lwgeom_from_geojson(const char *geojson, char **srs) {
	/* Begin to Parse json */
	json_tokener *jstok = json_tokener_new();
	json_object *poObj = json_tokener_parse_ex(jstok, geojson, -1);
	if (jstok->err != json_tokener_success) {
		char err[256];
		snprintf(err, 256, "%s (at offset %d)", json_tokener_error_desc(jstok->err), jstok->char_offset);
		json_tokener_free(jstok);
		json_object_put(poObj);
		// lwerror(err);
		return NULL;
	}
	json_tokener_free(jstok);

	*srs = NULL;
	json_object *poObjSrs = findMemberByName(poObj, "crs");
	if (poObjSrs != NULL) {
		json_object *poObjSrsType = findMemberByName(poObjSrs, "type");
		if (poObjSrsType != NULL) {
			json_object *poObjSrsProps = findMemberByName(poObjSrs, "properties");
			if (poObjSrsProps) {
				json_object *poNameURL = findMemberByName(poObjSrsProps, "name");
				if (poNameURL) {
					const char *pszName = json_object_get_string(poNameURL);
					if (pszName) {
						*srs = (char *)lwalloc(strlen(pszName) + 1);
						strcpy(*srs, pszName);
					}
				}
			}
		}
	}

	int hasz = LW_FALSE;
	LWGEOM *lwgeom = parse_geojson(poObj, &hasz);
	json_object_put(poObj);
	if (!lwgeom)
		return NULL;

	if (!hasz) {
		LWGEOM *tmp = lwgeom_force_2d(lwgeom);
		lwgeom_free(lwgeom);
		lwgeom = tmp;
	}
	lwgeom_add_bbox(lwgeom);
	return lwgeom;
}

} // namespace duckdb
