// #include "json.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cstring>

// using namespace json;

namespace duckdb {

LWGEOM *lwgeom_from_geojson(const char *geojson, char **srs) {
	return nullptr;
	// /* Begin to Parse json */
	// json_tokener *jstok = json_tokener_new();
	// json_object *poObj = json_tokener_parse_ex(jstok, geojson, -1);
	// if (jstok->err != json_tokener_success) {
	// 	char err[256];
	// 	snprintf(err, 256, "%s (at offset %d)", json_tokener_error_desc(jstok->err), jstok->char_offset);
	// 	json_tokener_free(jstok);
	// 	json_object_put(poObj);
	// 	// lwerror(err);
	// 	return NULL;
	// }
	// json_tokener_free(jstok);

	// *srs = NULL;
	// json_object *poObjSrs = findMemberByName(poObj, "crs");
	// if (poObjSrs != NULL) {
	// 	json_object *poObjSrsType = findMemberByName(poObjSrs, "type");
	// 	if (poObjSrsType != NULL) {
	// 		json_object *poObjSrsProps = findMemberByName(poObjSrs, "properties");
	// 		if (poObjSrsProps) {
	// 			json_object *poNameURL = findMemberByName(poObjSrsProps, "name");
	// 			if (poNameURL) {
	// 				const char *pszName = json_object_get_string(poNameURL);
	// 				if (pszName) {
	// 					*srs = (char *)lwalloc(strlen(pszName) + 1);
	// 					strcpy(*srs, pszName);
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// int hasz = LW_FALSE;
	// LWGEOM *lwgeom = parse_geojson(poObj, &hasz);
	// json_object_put(poObj);
	// if (!lwgeom)
	// 	return NULL;

	// if (!hasz) {
	// 	LWGEOM *tmp = lwgeom_force_2d(lwgeom);
	// 	lwgeom_free(lwgeom);
	// 	lwgeom = tmp;
	// }
	// lwgeom_add_bbox(lwgeom);
	// return lwgeom;
}

} // namespace duckdb