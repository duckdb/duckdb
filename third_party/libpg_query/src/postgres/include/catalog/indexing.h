/*-------------------------------------------------------------------------
 *
 * indexing.h
 *	  This file provides some definitions to support indexing
 *	  on system catalogs
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/indexing.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXING_H
#define INDEXING_H

#include "access/htup.h"
#include "utils/relcache.h"

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *CatalogIndexState;

/*
 * indexing.c prototypes
 */
extern CatalogIndexState CatalogOpenIndexes(Relation heapRel);
extern void CatalogCloseIndexes(CatalogIndexState indstate);
extern void CatalogIndexInsert(CatalogIndexState indstate,
				   HeapTuple heapTuple);
extern void CatalogUpdateIndexes(Relation heapRel, HeapTuple heapTuple);


/*
 * These macros are just to keep the C compiler from spitting up on the
 * upcoming commands for genbki.pl.
 */
#define DECLARE_INDEX(name,oid,decl) extern int no_such_variable
#define DECLARE_UNIQUE_INDEX(name,oid,decl) extern int no_such_variable
#define BUILD_INDICES


/*
 * What follows are lines processed by genbki.pl to create the statements
 * the bootstrap parser will turn into DefineIndex calls.
 *
 * The keyword is DECLARE_INDEX or DECLARE_UNIQUE_INDEX.  The first two
 * arguments are the index name and OID, the rest is much like a standard
 * 'create index' SQL command.
 *
 * For each index, we also provide a #define for its OID.  References to
 * the index in the C code should always use these #defines, not the actual
 * index name (much less the numeric OID).
 */

DECLARE_UNIQUE_INDEX(pg_aggregate_fnoid_index, 2650, on pg_aggregate using btree(aggfnoid oid_ops));
#define AggregateFnoidIndexId  2650

DECLARE_UNIQUE_INDEX(pg_am_name_index, 2651, on pg_am using btree(amname name_ops));
#define AmNameIndexId  2651
DECLARE_UNIQUE_INDEX(pg_am_oid_index, 2652, on pg_am using btree(oid oid_ops));
#define AmOidIndexId  2652

DECLARE_UNIQUE_INDEX(pg_amop_fam_strat_index, 2653, on pg_amop using btree(amopfamily oid_ops, amoplefttype oid_ops, amoprighttype oid_ops, amopstrategy int2_ops));
#define AccessMethodStrategyIndexId  2653
DECLARE_UNIQUE_INDEX(pg_amop_opr_fam_index, 2654, on pg_amop using btree(amopopr oid_ops, amoppurpose char_ops, amopfamily oid_ops));
#define AccessMethodOperatorIndexId  2654
DECLARE_UNIQUE_INDEX(pg_amop_oid_index, 2756, on pg_amop using btree(oid oid_ops));
#define AccessMethodOperatorOidIndexId	2756

DECLARE_UNIQUE_INDEX(pg_amproc_fam_proc_index, 2655, on pg_amproc using btree(amprocfamily oid_ops, amproclefttype oid_ops, amprocrighttype oid_ops, amprocnum int2_ops));
#define AccessMethodProcedureIndexId  2655
DECLARE_UNIQUE_INDEX(pg_amproc_oid_index, 2757, on pg_amproc using btree(oid oid_ops));
#define AccessMethodProcedureOidIndexId  2757

DECLARE_UNIQUE_INDEX(pg_attrdef_adrelid_adnum_index, 2656, on pg_attrdef using btree(adrelid oid_ops, adnum int2_ops));
#define AttrDefaultIndexId	2656
DECLARE_UNIQUE_INDEX(pg_attrdef_oid_index, 2657, on pg_attrdef using btree(oid oid_ops));
#define AttrDefaultOidIndexId  2657

DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnam_index, 2658, on pg_attribute using btree(attrelid oid_ops, attname name_ops));
#define AttributeRelidNameIndexId  2658
DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnum_index, 2659, on pg_attribute using btree(attrelid oid_ops, attnum int2_ops));
#define AttributeRelidNumIndexId  2659

DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, on pg_authid using btree(rolname name_ops));
#define AuthIdRolnameIndexId	2676
DECLARE_UNIQUE_INDEX(pg_authid_oid_index, 2677, on pg_authid using btree(oid oid_ops));
#define AuthIdOidIndexId	2677

DECLARE_UNIQUE_INDEX(pg_auth_members_role_member_index, 2694, on pg_auth_members using btree(roleid oid_ops, member oid_ops));
#define AuthMemRoleMemIndexId	2694
DECLARE_UNIQUE_INDEX(pg_auth_members_member_role_index, 2695, on pg_auth_members using btree(member oid_ops, roleid oid_ops));
#define AuthMemMemRoleIndexId	2695

DECLARE_UNIQUE_INDEX(pg_cast_oid_index, 2660, on pg_cast using btree(oid oid_ops));
#define CastOidIndexId	2660
DECLARE_UNIQUE_INDEX(pg_cast_source_target_index, 2661, on pg_cast using btree(castsource oid_ops, casttarget oid_ops));
#define CastSourceTargetIndexId  2661

DECLARE_UNIQUE_INDEX(pg_class_oid_index, 2662, on pg_class using btree(oid oid_ops));
#define ClassOidIndexId  2662
DECLARE_UNIQUE_INDEX(pg_class_relname_nsp_index, 2663, on pg_class using btree(relname name_ops, relnamespace oid_ops));
#define ClassNameNspIndexId  2663
DECLARE_INDEX(pg_class_tblspc_relfilenode_index, 3455, on pg_class using btree(reltablespace oid_ops, relfilenode oid_ops));
#define ClassTblspcRelfilenodeIndexId  3455

DECLARE_UNIQUE_INDEX(pg_collation_name_enc_nsp_index, 3164, on pg_collation using btree(collname name_ops, collencoding int4_ops, collnamespace oid_ops));
#define CollationNameEncNspIndexId 3164
DECLARE_UNIQUE_INDEX(pg_collation_oid_index, 3085, on pg_collation using btree(oid oid_ops));
#define CollationOidIndexId  3085

DECLARE_INDEX(pg_constraint_conname_nsp_index, 2664, on pg_constraint using btree(conname name_ops, connamespace oid_ops));
#define ConstraintNameNspIndexId  2664
DECLARE_INDEX(pg_constraint_conrelid_index, 2665, on pg_constraint using btree(conrelid oid_ops));
#define ConstraintRelidIndexId	2665
DECLARE_INDEX(pg_constraint_contypid_index, 2666, on pg_constraint using btree(contypid oid_ops));
#define ConstraintTypidIndexId	2666
DECLARE_UNIQUE_INDEX(pg_constraint_oid_index, 2667, on pg_constraint using btree(oid oid_ops));
#define ConstraintOidIndexId  2667

DECLARE_UNIQUE_INDEX(pg_conversion_default_index, 2668, on pg_conversion using btree(connamespace oid_ops, conforencoding int4_ops, contoencoding int4_ops, oid oid_ops));
#define ConversionDefaultIndexId  2668
DECLARE_UNIQUE_INDEX(pg_conversion_name_nsp_index, 2669, on pg_conversion using btree(conname name_ops, connamespace oid_ops));
#define ConversionNameNspIndexId  2669
DECLARE_UNIQUE_INDEX(pg_conversion_oid_index, 2670, on pg_conversion using btree(oid oid_ops));
#define ConversionOidIndexId  2670

DECLARE_UNIQUE_INDEX(pg_database_datname_index, 2671, on pg_database using btree(datname name_ops));
#define DatabaseNameIndexId  2671
DECLARE_UNIQUE_INDEX(pg_database_oid_index, 2672, on pg_database using btree(oid oid_ops));
#define DatabaseOidIndexId	2672

DECLARE_INDEX(pg_depend_depender_index, 2673, on pg_depend using btree(classid oid_ops, objid oid_ops, objsubid int4_ops));
#define DependDependerIndexId  2673
DECLARE_INDEX(pg_depend_reference_index, 2674, on pg_depend using btree(refclassid oid_ops, refobjid oid_ops, refobjsubid int4_ops));
#define DependReferenceIndexId	2674

DECLARE_UNIQUE_INDEX(pg_description_o_c_o_index, 2675, on pg_description using btree(objoid oid_ops, classoid oid_ops, objsubid int4_ops));
#define DescriptionObjIndexId  2675
DECLARE_UNIQUE_INDEX(pg_shdescription_o_c_index, 2397, on pg_shdescription using btree(objoid oid_ops, classoid oid_ops));
#define SharedDescriptionObjIndexId 2397

DECLARE_UNIQUE_INDEX(pg_enum_oid_index, 3502, on pg_enum using btree(oid oid_ops));
#define EnumOidIndexId	3502
DECLARE_UNIQUE_INDEX(pg_enum_typid_label_index, 3503, on pg_enum using btree(enumtypid oid_ops, enumlabel name_ops));
#define EnumTypIdLabelIndexId 3503
DECLARE_UNIQUE_INDEX(pg_enum_typid_sortorder_index, 3534, on pg_enum using btree(enumtypid oid_ops, enumsortorder float4_ops));
#define EnumTypIdSortOrderIndexId 3534

DECLARE_INDEX(pg_index_indrelid_index, 2678, on pg_index using btree(indrelid oid_ops));
#define IndexIndrelidIndexId  2678
DECLARE_UNIQUE_INDEX(pg_index_indexrelid_index, 2679, on pg_index using btree(indexrelid oid_ops));
#define IndexRelidIndexId  2679

DECLARE_UNIQUE_INDEX(pg_inherits_relid_seqno_index, 2680, on pg_inherits using btree(inhrelid oid_ops, inhseqno int4_ops));
#define InheritsRelidSeqnoIndexId  2680
DECLARE_INDEX(pg_inherits_parent_index, 2187, on pg_inherits using btree(inhparent oid_ops));
#define InheritsParentIndexId  2187

DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, on pg_language using btree(lanname name_ops));
#define LanguageNameIndexId  2681
DECLARE_UNIQUE_INDEX(pg_language_oid_index, 2682, on pg_language using btree(oid oid_ops));
#define LanguageOidIndexId	2682

DECLARE_UNIQUE_INDEX(pg_largeobject_loid_pn_index, 2683, on pg_largeobject using btree(loid oid_ops, pageno int4_ops));
#define LargeObjectLOidPNIndexId  2683

DECLARE_UNIQUE_INDEX(pg_largeobject_metadata_oid_index, 2996, on pg_largeobject_metadata using btree(oid oid_ops));
#define LargeObjectMetadataOidIndexId	2996

DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, on pg_namespace using btree(nspname name_ops));
#define NamespaceNameIndexId  2684
DECLARE_UNIQUE_INDEX(pg_namespace_oid_index, 2685, on pg_namespace using btree(oid oid_ops));
#define NamespaceOidIndexId  2685

DECLARE_UNIQUE_INDEX(pg_opclass_am_name_nsp_index, 2686, on pg_opclass using btree(opcmethod oid_ops, opcname name_ops, opcnamespace oid_ops));
#define OpclassAmNameNspIndexId  2686
DECLARE_UNIQUE_INDEX(pg_opclass_oid_index, 2687, on pg_opclass using btree(oid oid_ops));
#define OpclassOidIndexId  2687

DECLARE_UNIQUE_INDEX(pg_operator_oid_index, 2688, on pg_operator using btree(oid oid_ops));
#define OperatorOidIndexId	2688
DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, on pg_operator using btree(oprname name_ops, oprleft oid_ops, oprright oid_ops, oprnamespace oid_ops));
#define OperatorNameNspIndexId	2689

DECLARE_UNIQUE_INDEX(pg_opfamily_am_name_nsp_index, 2754, on pg_opfamily using btree(opfmethod oid_ops, opfname name_ops, opfnamespace oid_ops));
#define OpfamilyAmNameNspIndexId  2754
DECLARE_UNIQUE_INDEX(pg_opfamily_oid_index, 2755, on pg_opfamily using btree(oid oid_ops));
#define OpfamilyOidIndexId	2755

DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137

DECLARE_UNIQUE_INDEX(pg_proc_oid_index, 2690, on pg_proc using btree(oid oid_ops));
#define ProcedureOidIndexId  2690
DECLARE_UNIQUE_INDEX(pg_proc_proname_args_nsp_index, 2691, on pg_proc using btree(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops));
#define ProcedureNameArgsNspIndexId  2691

DECLARE_UNIQUE_INDEX(pg_rewrite_oid_index, 2692, on pg_rewrite using btree(oid oid_ops));
#define RewriteOidIndexId  2692
DECLARE_UNIQUE_INDEX(pg_rewrite_rel_rulename_index, 2693, on pg_rewrite using btree(ev_class oid_ops, rulename name_ops));
#define RewriteRelRulenameIndexId  2693

DECLARE_INDEX(pg_shdepend_depender_index, 1232, on pg_shdepend using btree(dbid oid_ops, classid oid_ops, objid oid_ops, objsubid int4_ops));
#define SharedDependDependerIndexId		1232
DECLARE_INDEX(pg_shdepend_reference_index, 1233, on pg_shdepend using btree(refclassid oid_ops, refobjid oid_ops));
#define SharedDependReferenceIndexId	1233

DECLARE_UNIQUE_INDEX(pg_statistic_relid_att_inh_index, 2696, on pg_statistic using btree(starelid oid_ops, staattnum int2_ops, stainherit bool_ops));
#define StatisticRelidAttnumInhIndexId	2696

DECLARE_UNIQUE_INDEX(pg_tablespace_oid_index, 2697, on pg_tablespace using btree(oid oid_ops));
#define TablespaceOidIndexId  2697
DECLARE_UNIQUE_INDEX(pg_tablespace_spcname_index, 2698, on pg_tablespace using btree(spcname name_ops));
#define TablespaceNameIndexId  2698

DECLARE_UNIQUE_INDEX(pg_transform_oid_index, 3574, on pg_transform using btree(oid oid_ops));
#define TransformOidIndexId 3574
DECLARE_UNIQUE_INDEX(pg_transform_type_lang_index, 3575, on pg_transform using btree(trftype oid_ops, trflang oid_ops));
#define TransformTypeLangIndexId  3575

DECLARE_INDEX(pg_trigger_tgconstraint_index, 2699, on pg_trigger using btree(tgconstraint oid_ops));
#define TriggerConstraintIndexId  2699
DECLARE_UNIQUE_INDEX(pg_trigger_tgrelid_tgname_index, 2701, on pg_trigger using btree(tgrelid oid_ops, tgname name_ops));
#define TriggerRelidNameIndexId  2701
DECLARE_UNIQUE_INDEX(pg_trigger_oid_index, 2702, on pg_trigger using btree(oid oid_ops));
#define TriggerOidIndexId  2702

DECLARE_UNIQUE_INDEX(pg_event_trigger_evtname_index, 3467, on pg_event_trigger using btree(evtname name_ops));
#define EventTriggerNameIndexId  3467
DECLARE_UNIQUE_INDEX(pg_event_trigger_oid_index, 3468, on pg_event_trigger using btree(oid oid_ops));
#define EventTriggerOidIndexId	3468

DECLARE_UNIQUE_INDEX(pg_ts_config_cfgname_index, 3608, on pg_ts_config using btree(cfgname name_ops, cfgnamespace oid_ops));
#define TSConfigNameNspIndexId	3608
DECLARE_UNIQUE_INDEX(pg_ts_config_oid_index, 3712, on pg_ts_config using btree(oid oid_ops));
#define TSConfigOidIndexId	3712

DECLARE_UNIQUE_INDEX(pg_ts_config_map_index, 3609, on pg_ts_config_map using btree(mapcfg oid_ops, maptokentype int4_ops, mapseqno int4_ops));
#define TSConfigMapIndexId	3609

DECLARE_UNIQUE_INDEX(pg_ts_dict_dictname_index, 3604, on pg_ts_dict using btree(dictname name_ops, dictnamespace oid_ops));
#define TSDictionaryNameNspIndexId	3604
DECLARE_UNIQUE_INDEX(pg_ts_dict_oid_index, 3605, on pg_ts_dict using btree(oid oid_ops));
#define TSDictionaryOidIndexId	3605

DECLARE_UNIQUE_INDEX(pg_ts_parser_prsname_index, 3606, on pg_ts_parser using btree(prsname name_ops, prsnamespace oid_ops));
#define TSParserNameNspIndexId	3606
DECLARE_UNIQUE_INDEX(pg_ts_parser_oid_index, 3607, on pg_ts_parser using btree(oid oid_ops));
#define TSParserOidIndexId	3607

DECLARE_UNIQUE_INDEX(pg_ts_template_tmplname_index, 3766, on pg_ts_template using btree(tmplname name_ops, tmplnamespace oid_ops));
#define TSTemplateNameNspIndexId	3766
DECLARE_UNIQUE_INDEX(pg_ts_template_oid_index, 3767, on pg_ts_template using btree(oid oid_ops));
#define TSTemplateOidIndexId	3767

DECLARE_UNIQUE_INDEX(pg_type_oid_index, 2703, on pg_type using btree(oid oid_ops));
#define TypeOidIndexId	2703
DECLARE_UNIQUE_INDEX(pg_type_typname_nsp_index, 2704, on pg_type using btree(typname name_ops, typnamespace oid_ops));
#define TypeNameNspIndexId	2704

DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_oid_index, 112, on pg_foreign_data_wrapper using btree(oid oid_ops));
#define ForeignDataWrapperOidIndexId	112
DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_name_index, 548, on pg_foreign_data_wrapper using btree(fdwname name_ops));
#define ForeignDataWrapperNameIndexId	548

DECLARE_UNIQUE_INDEX(pg_foreign_server_oid_index, 113, on pg_foreign_server using btree(oid oid_ops));
#define ForeignServerOidIndexId 113
DECLARE_UNIQUE_INDEX(pg_foreign_server_name_index, 549, on pg_foreign_server using btree(srvname name_ops));
#define ForeignServerNameIndexId	549

DECLARE_UNIQUE_INDEX(pg_user_mapping_oid_index, 174, on pg_user_mapping using btree(oid oid_ops));
#define UserMappingOidIndexId	174
DECLARE_UNIQUE_INDEX(pg_user_mapping_user_server_index, 175, on pg_user_mapping using btree(umuser oid_ops, umserver oid_ops));
#define UserMappingUserServerIndexId	175

DECLARE_UNIQUE_INDEX(pg_foreign_table_relid_index, 3119, on pg_foreign_table using btree(ftrelid oid_ops));
#define ForeignTableRelidIndexId 3119

DECLARE_UNIQUE_INDEX(pg_default_acl_role_nsp_obj_index, 827, on pg_default_acl using btree(defaclrole oid_ops, defaclnamespace oid_ops, defaclobjtype char_ops));
#define DefaultAclRoleNspObjIndexId 827
DECLARE_UNIQUE_INDEX(pg_default_acl_oid_index, 828, on pg_default_acl using btree(oid oid_ops));
#define DefaultAclOidIndexId	828

DECLARE_UNIQUE_INDEX(pg_db_role_setting_databaseid_rol_index, 2965, on pg_db_role_setting using btree(setdatabase oid_ops, setrole oid_ops));
#define DbRoleSettingDatidRolidIndexId	2965

DECLARE_UNIQUE_INDEX(pg_seclabel_object_index, 3597, on pg_seclabel using btree(objoid oid_ops, classoid oid_ops, objsubid int4_ops, provider text_pattern_ops));
#define SecLabelObjectIndexId				3597

DECLARE_UNIQUE_INDEX(pg_shseclabel_object_index, 3593, on pg_shseclabel using btree(objoid oid_ops, classoid oid_ops, provider text_pattern_ops));
#define SharedSecLabelObjectIndexId			3593

DECLARE_UNIQUE_INDEX(pg_extension_oid_index, 3080, on pg_extension using btree(oid oid_ops));
#define ExtensionOidIndexId 3080
DECLARE_UNIQUE_INDEX(pg_extension_name_index, 3081, on pg_extension using btree(extname name_ops));
#define ExtensionNameIndexId 3081

DECLARE_UNIQUE_INDEX(pg_range_rngtypid_index, 3542, on pg_range using btree(rngtypid oid_ops));
#define RangeTypidIndexId					3542

DECLARE_UNIQUE_INDEX(pg_policy_oid_index, 3257, on pg_policy using btree(oid oid_ops));
#define PolicyOidIndexId				3257

DECLARE_UNIQUE_INDEX(pg_policy_polrelid_polname_index, 3258, on pg_policy using btree(polrelid oid_ops, polname name_ops));
#define PolicyPolrelidPolnameIndexId				3258

DECLARE_UNIQUE_INDEX(pg_replication_origin_roiident_index, 6001, on pg_replication_origin using btree(roident oid_ops));
#define ReplicationOriginIdentIndex 6001

DECLARE_UNIQUE_INDEX(pg_replication_origin_roname_index, 6002, on pg_replication_origin using btree(roname text_pattern_ops));
#define ReplicationOriginNameIndex 6002

/* last step of initialization script: build the indexes declared above */
BUILD_INDICES

#endif   /* INDEXING_H */
