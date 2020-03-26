#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test IN statement", "[sql]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL);"));

	result = con.Query("SELECT * FROM integers WHERE i IN (1, 2) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	result = con.Query("SELECT * FROM integers WHERE i IN (1, 2, 3, 4, 5, 6, 7, 8) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query("SELECT i, i IN (1, 2, 3, 4, 5, 6, 7, 8) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));

	result = con.Query("SELECT i, i NOT IN (1, 2, 3, 4, 5, 6, 7, 8) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, false}));

	result = con.Query("SELECT i, i IN (1, 2, NULL, 4, 5, 6, 7, 8) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, Value()}));

	result = con.Query("SELECT i, i IN (i + 1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, false}));

	result = con.Query("SELECT i, i IN (i + 1, 42, i) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));

	result = con.Query("SELECT i, 1 IN (i - 1, i, i + 1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, false}));

	result = con.Query("SELECT i, 1 NOT IN (i - 1, i, i + 1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));

	result = con.Query("SELECT i, i IN (11, 12, 13, 14, 15, 16, 17, 18, 1, i) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));

	result = con.Query("SELECT i, i NOT IN (11, 12, 13, 14, 15, 16, 17, 18, 1, i) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, false}));

	result = con.Query("SELECT i, 1 IN (11, 12, 13, 14, 15, 16, 17, 18, 1, i) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, true, true, true}));

	result = con.Query("SELECT i, 1 NOT IN (11, 12, 13, 14, 15, 16, 17, 18, 1, i) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));

	// multiple subqueries in IN-clause
	result = con.Query(
	    "SELECT i, i IN ((SELECT MAX(i) FROM integers), (SELECT MIN(i) FROM integers)) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, true}));

	// multiple correlated subqueries in IN-clause
	result = con.Query("SELECT i, (SELECT MAX(i) FROM integers WHERE i <> i1.i), (SELECT MIN(i) FROM integers WHERE i "
	                   "<= i1.i) FROM integers i1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 3, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 1, 1}));

	// multiple correlated subqueries in IN-clause
	result = con.Query("SELECT i, i IN ((SELECT MAX(i) FROM integers WHERE i <> i1.i), (SELECT MIN(i) FROM integers "
	                   "WHERE i <= i1.i)) FROM integers i1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, false}));
}

TEST_CASE("Test large IN statement with varchar", "[sql][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('HXR'), ('NUT'), ('ZZZ'), (NULL);"));

	string in_list =
	    "'QOY', 'AAZ', 'AOR', 'VJR', 'ROL', 'KOS', 'JBC', 'EMZ', 'LQT', 'HYX', "
	    "'IYA', 'YAA', 'XMR', 'HGJ', 'LDR', 'MVT', 'XEQ', 'NPX', 'ZKV', 'HET', 'GZI', 'PLI', 'ABK', 'LOJ', 'KYB', "
	    "'UKG', 'PNY', 'SBV', 'VSJ', 'FAR', 'AXH', 'XWO', 'NKA', 'CPE', 'GSC', 'TWK', 'SXK', 'SUU', 'KPX', 'NXU', "
	    "'ZMR', 'BZG', 'CBO', 'SQA', 'VNE', 'NUT', 'LKF', 'IWL', 'CQE', 'UED', 'BHQ', 'RQR', 'FVG', 'EZZ', 'SRJ', "
	    "'UPA', 'JUV', 'AXP', 'JGL', 'HKC', 'CYM', 'AAK', 'WUM', 'NWK', 'DCQ', 'YXT', 'DQZ', 'WXB', 'YVG', 'YXL', "
	    "'FWG', 'PLN', 'YPK', 'UPA', 'GIC', 'IPS', 'WQB', 'NSR', 'WRT', 'WOP', 'JAX', 'XWZ', 'TIM', 'EAO', 'ANC', "
	    "'NAT', 'KFL', 'XJA', 'HUG', 'SVP', 'HVX', 'APA', 'RJI', 'QKP', 'MZB', 'MME', 'GJW', 'TBX', 'TRT', 'IRL', "
	    "'BUR', 'OWP', 'DAK', 'IGZ', 'OJL', 'BON', 'KLG', 'SSI', 'UOD', 'SCW', 'LGT', 'KTL', 'DPA', 'WTC', 'NSW', "
	    "'TEQ', 'HNU', 'GEI', 'OFK', 'UQB', 'QWI', 'ACS', 'MMR', 'EHZ', 'PXR', 'SPK', 'POO', 'CMF', 'UEE', 'QUK', "
	    "'BZR', 'ZBH', 'KJV', 'RIV', 'HOX', 'GZU', 'PQO', 'NLU', 'BRV', 'XND', 'APA', 'QXV', 'QEW', 'AJR', 'OYC', "
	    "'YTA', 'FMS', 'GSK', 'MCC', 'QVQ', 'FYD', 'QQJ', 'VBM', 'EYV', 'ELQ', 'KHI', 'HTU', 'KDQ', 'YRI', 'CEB', "
	    "'WGP', 'HIY', 'XSM', 'FTD', 'EHU', 'KFE', 'FCN', 'YRC', 'WFC', 'YRC', 'PZH', 'ZAG', 'AAM', 'UBM', 'JIH', "
	    "'CRQ', 'BOE', 'KLI', 'SAB', 'TYY', 'AGX', 'FEH', 'ZPX', 'PRU', 'GXA', 'HDC', 'UJG', 'OCO', 'BLZ', 'ZFH', "
	    "'LRN', 'GYF', 'FBR', 'OVH', 'XTL', 'FQZ', 'MKP', 'UKA', 'RMN', 'IWX', 'RYF', 'WTB', 'KWU', 'MKN', 'JFM', "
	    "'VUU', 'UTU', 'CLW', 'HKQ', 'SJA', 'ADT', 'RTN', 'ZPJ', 'NUX', 'HMY', 'RDA', 'YQQ', 'COL', 'JRB', 'PYV', "
	    "'YAU', 'AYM', 'STV', 'BDC', 'PIZ', 'CAK', 'VWS', 'GNU', 'SZA', 'PMP', 'WOV', 'LPL', 'PJY', 'FWV', 'CLH', "
	    "'IVC', 'YHN', 'QWB', 'SCE', 'PFN', 'WDI', 'PIN', 'QSN', 'WDE', 'JOZ', 'DPZ', 'WFX', 'IQS', 'TBZ', 'FXQ', "
	    "'PLM', 'WQH', 'BEJ', 'KFM', 'KLH', 'ZVT', 'MBV', 'JQT', 'RIR', 'FDB', 'AOV', 'GZV', 'CTN', 'YCY', 'XSB', "
	    "'UUI', 'EWM', 'MQR', 'SKE', 'ACO', 'AGP', 'RWD', 'XGR', 'PCO', 'OSY', 'QHG', 'YEW', 'DHQ', 'FNH', 'QWX', "
	    "'RSL', 'GKL', 'VEP', 'SME', 'TKH', 'JOQ', 'XZY', 'ZBF', 'UJP', 'UKD', 'QIZ', 'VQF', 'QUW', 'XGX', 'TPG', "
	    "'LYD', 'WBX', 'GRI', 'YDC', 'PJU', 'EFU', 'MZV', 'GBN', 'EJE', 'JYH', 'XTC', 'ZNI', 'JJE', 'XJD', 'SJU', "
	    "'SNP', 'ZLL', 'ZRL', 'CUC', 'EOQ', 'IZU', 'MZH', 'UMQ', 'ULS', 'ETH', 'AUT', 'NPL', 'IYB', 'OXQ', 'CXH', "
	    "'QRI', 'RHM', 'PNO', 'QGF', 'THK', 'KYU', 'OIE', 'OGO', 'UDS', 'OIS', 'DXJ', 'ETV', 'UWI', 'VLX', 'SOG', "
	    "'KNW', 'FOR', 'CCT', 'PHH', 'YNI', 'BOW', 'AJR', 'LJE', 'PUB', 'NPF', 'ELB', 'SIE', 'NAC', 'IZD', 'BQE', "
	    "'SUH', 'YQE', 'AVC', 'CIV', 'SOK', 'VRB', 'YUG', 'JFO', 'FCB', 'LEY', 'IPX', 'PEZ', 'BFV', 'BCV', 'RWZ', "
	    "'BHX', 'OJV', 'QPZ', 'QQX', 'ECG', 'MCD', 'UEI', 'IAE', 'VKD', 'QUJ', 'WYL', 'UOI', 'XDP', 'JTW', 'LFJ', "
	    "'AHH', 'XRL', 'QQM', 'AGA', 'RLE', 'MFI', 'UYI', 'HEM', 'KKJ', 'VLN', 'PYN', 'ZZA', 'WGI', 'PTH', 'BMR', "
	    "'MZK', 'IPY', 'SLA', 'JOS', 'ZLT', 'ZDF', 'XWY', 'OHJ', 'RPL', 'BPE', 'NVZ', 'YIF', 'HWM', 'BGM', 'VUO', "
	    "'YIW', 'UBS', 'EAW', 'AYN', 'UCE', 'UIB', 'DNV', 'YAQ', 'TBE', 'GMT', 'EOW', 'RWL', 'TYU', 'UTX', 'SRJ', "
	    "'FZZ', 'RJU', 'QJB', 'MFY', 'PID', 'RWM', 'PWH', 'PJS', 'BNK', 'IUO', 'POW', 'MBL', 'XMH', 'YWN', 'FTC', "
	    "'XME', 'MUS', 'XYD', 'BVI', 'FGU', 'BCJ', 'KRH', 'XPM', 'HDV', 'SQS', 'PFE', 'SIU', 'PZL', 'APT', 'CZK', "
	    "'HHL', 'EVI', 'GOE', 'RWG', 'PZF', 'DPS', 'GXN', 'WVC', 'ZDE', 'BDQ', 'WYP', 'PLB', 'SZZ', 'HPM', 'BUL', "
	    "'VRY', 'ZNG', 'XUS', 'SLU', 'SLQ', 'UOH', 'YHR', 'FGE', 'RJY', 'CMO', 'NGZ', 'PCG', 'PFN', 'EPG', 'ILE', "
	    "'RXF', 'NLD', 'PYW', 'WTG', 'HKL', 'IGB', 'YNF', 'TAL', 'MBR', 'YEK', 'VKO', 'ZGO', 'UKH', 'GJN', 'QCD', "
	    "'OQD', 'HEU', 'GWT', 'THS', 'LAS', 'JLU', 'TNV', 'UQB', 'YDO', 'ZFI', 'XBV', 'VYI', 'GAC', 'GDU', 'DLL', "
	    "'JXX', 'GUP', 'FUF', 'KQI', 'TLC', 'DAQ', 'OMN', 'FYT', 'GIZ', 'NON', 'UGU', 'CSI', 'NEO', 'SVA', 'ZBM', "
	    "'GGU', 'PFF', 'QFB', 'JZH', 'OBB', 'VHM', 'PAJ', 'BKD', 'JEI', 'ZQG', 'GBX', 'UCR', 'TXS', 'PAK', 'HYP', "
	    "'CAI', 'WJY', 'VLH', 'QLY', 'XKI', 'SDS', 'QEF', 'KKF', 'XBQ', 'FTE', 'CEX', 'PJM', 'RTA', 'TXQ', 'BSK', "
	    "'OXU', 'ELZ', 'WII', 'THG', 'RDY', 'LFS', 'JNY', 'ORJ', 'FXD', 'OTG', 'BUG', 'EDF', 'DDM', 'HTZ', 'IAA', "
	    "'RWR', 'XBL', 'PXV', 'FVH', 'OSB', 'UQW', 'CUP', 'OIF', 'VQZ', 'KCT', 'VWJ', 'IRP', 'UAZ', 'CCF', 'MCY', "
	    "'GGP', 'HDT', 'SZT', 'JSR', 'HDR', 'MQR', 'OCP', 'XQD', 'QYT', 'NSA', 'LPC', 'MGS', 'DAZ', 'SDQ', 'BMS', "
	    "'EJL', 'ADY', 'JFP', 'YQD', 'CIP', 'CST', 'ATU', 'JTD', 'HDI', 'JMF', 'LJO', 'SEC', 'ZCN', 'EMG', 'RLJ', "
	    "'AMV', 'BVT', 'IZV', 'DND', 'VCJ', 'HNY', 'DSI', 'DWM', 'EDI', 'LQO', 'NDN', 'GGA', 'EXH', 'XFP', 'NLU', "
	    "'DNC', 'OTA', 'KQD', 'JRJ', 'JXV', 'UAK', 'TNB', 'ZXJ', 'YTU', 'MMS', 'NCV', 'VIR', 'EUI', 'KND', 'PTL', "
	    "'LKF', 'FUT', 'VUX', 'PWI', 'ETZ', 'IHN', 'GYS', 'KAQ', 'UYN', 'HOQ', 'END', 'WHD', 'NMT', 'HEW', 'OBN', "
	    "'RDZ', 'SKP', 'LHN', 'HQS', 'YTY', 'GXW', 'EKC', 'BCD', 'BRQ', 'CSX', 'OCI', 'OZP', 'TBN', 'HHG', 'VFZ', "
	    "'WJY', 'VBT', 'LNT', 'YDL', 'KOC', 'IJF', 'HSB', 'USO', 'HOK', 'MEK', 'WXT', 'DRT', 'SNE', 'WAI', 'EMP', "
	    "'IGC', 'QPJ', 'GCF', 'CJW', 'ONH', 'XQO', 'KFY', 'CZQ', 'AUC', 'BQD', 'KBH', 'GFS', 'BVT', 'QLX', 'TYO', "
	    "'WWE', 'TXE', 'MQM', 'WNC', 'WKC', 'ZLW', 'RDJ', 'WCZ', 'UOV', 'IBT', 'HOQ', 'VIA', 'DPE', 'AJX', 'AYA', "
	    "'DEM', 'YDD', 'SOU', 'KZU', 'ATH', 'BPS', 'GTM', 'ALW', 'ODR', 'DBZ', 'KYG', 'CES', 'NWC', 'QER', 'FFA', "
	    "'UZS', 'GPL', 'MTD', 'TPI', 'GMR', 'WXV', 'YYY', 'FAX', 'MNL', 'JUS', 'SVJ', 'ZNX', 'QDQ', 'FIH', 'EXR', "
	    "'XSM', 'OQX', 'GUW', 'KEG', 'DHJ', 'GXH', 'ROZ', 'TCT', 'UNU', 'XGS', 'ODG', 'GZH', 'TZV', 'LFP', 'CDF', "
	    "'LJZ', 'RLL', 'QBY', 'CEQ', 'JWH', 'GYJ', 'LSJ', 'CQV', 'HJT', 'PVB', 'DLH', 'BFI', 'RIV', 'UCG', 'KOG', "
	    "'TXS', 'JDF', 'DKD', 'FOE', 'UIO', 'GDY', 'OIG', 'FWH', 'YSY', 'HGV', 'GIS', 'CMB', 'SDT', 'ROH', 'NMU', "
	    "'YHA', 'WDJ', 'VDV', 'LDE', 'PUR', 'UNS', 'NDY', 'YCZ', 'SAQ', 'OJW', 'OIP', 'IWC', 'WPT', 'PJI', 'PMP', "
	    "'CEB', 'WRZ', 'LSI', 'QUA', 'HMA', 'VWZ', 'KWX', 'RTA', 'IXL', 'XZH', 'UOT', 'IWF', 'LXN', 'VXY', 'DMU', "
	    "'EKC', 'JRF', 'EMA', 'LNG', 'JWT', 'NLH', 'EPH', 'FFJ', 'RTR', 'XJE', 'OAW', 'HSP', 'AFE', 'EPI', 'RSV', "
	    "'OIZ', 'BEC', 'ILJ', 'UPU', 'JZC', 'HGP', 'HLT', 'GSY', 'XGP', 'NYL', 'VHY', 'CQN', 'WEE', 'GFJ', 'LGE', "
	    "'IBM', 'NCI', 'OPR', 'TIO', 'DPL', 'TFD', 'XKJ', 'DWM', 'DPV', 'RIE', 'UKK', 'OBM', 'NLJ', 'VUM', 'IRE', "
	    "'DNR', 'KVP', 'PZX', 'FBD', 'GDI', 'YAW', 'MKG', 'EAX', 'CBM', 'TCH', 'LGX', 'UCY', 'EWE', 'ZUN', 'DPM', "
	    "'BUH', 'NQJ', 'RFX', 'XUI', 'BHB', 'BMM', 'MGZ', 'PTR', 'RUM', 'FVC', 'BRO', 'ZVP', 'YMV', 'PNX', 'WRO', "
	    "'KKR', 'WCE', 'UFT', 'QWG', 'UNY', 'IZE', 'ZFX', 'KBC', 'DPS', 'QYY', 'ZNO', 'DHR', 'GWP', 'UTY', 'CSO', "
	    "'SQH', 'TBU', 'CYF', 'ROH', 'GYF', 'BWT', 'RKM', 'WLV', 'FSB', 'EWV', 'DHJ', 'RPB', 'NVJ', 'LUF', 'TTV', "
	    "'WPT', 'APQ', 'IXF', 'HAQ', 'OVS', 'JTE', 'LOQ', 'PXF', 'MAZ', 'AHV', 'TZO', 'LHO', 'ATL', 'MII', 'LTR', "
	    "'KPQ', 'ZXP', 'JPI', 'GLF', 'VKG', 'CAM', 'GVX', 'JUO', 'YWZ', 'NNQ', 'RGV', 'CDS', 'CYA', 'QPX', 'HHG', "
	    "'DDF', 'YXE', 'ZKL', 'XDJ', 'HLC', 'ORZ', 'UEX', 'FKH', 'RWT', 'RKN', 'USW', 'UBC', 'QGT', 'MGN', 'WXC', "
	    "'ENU', 'RLW', 'OFP', 'XGN', 'PSX', 'JTD', 'LFK', 'PBI', 'BWD', 'JVG', 'HFG', 'WLM', 'XNC', 'HWH', 'FGZ', "
	    "'JEK', 'HRL', 'NBL', 'THW', 'WKI', 'IME', 'UIY', 'WAA', 'XQO', 'UUD', 'IRS', 'FBF', 'JZV', 'CCR', 'YLF', "
	    "'RXP', 'UIP', 'OGN', 'IID', 'ENW', 'XPL', 'PFE', 'MEX', 'GPH', 'RBW', 'JML', 'ZVR', 'YWL', 'QPJ', 'LYY', "
	    "'NKS', 'VIJ', 'SAZ', 'LNC', 'NGU', 'RRD', 'PJK', 'FOH', 'ORW', 'YLQ', 'GFG', 'NOW', 'IDV', 'WHE', 'RTH', "
	    "'NVL', 'BAQ', 'HAK', 'SKH', 'LTZ', 'HDF', 'SIB', 'PHY', 'PWZ', 'CWY', 'JNE', 'CXW', 'EPN', 'LAO', 'WEI', "
	    "'AGK', 'XJQ', 'ILR', 'CVO', 'MEX', 'BGD', 'AEK', 'DHK', 'KAC', 'DQL', 'WDA', 'GVU', 'RUN', 'XQU', 'EXU', "
	    "'WER', 'UBS', 'XOK', 'WML', 'XQM', 'XBW', 'VTT', 'SMF', 'DRX', 'XGZ', 'KXD', 'IVV', 'OIJ', 'KAW', 'SEW', "
	    "'URD', 'QAW', 'RFL', 'ZGP', 'VVR', 'ZLA', 'LXV', 'OWU', 'YGF', 'BJO', 'OTZ', 'RSD', 'GTT', 'CEP', 'HVH', "
	    "'ZNI', 'BHV', 'IKT', 'GCF', 'XYF', 'FOL', 'NLV', 'NFL', 'TOO', 'GYY', 'POS', 'JGL', 'BUE', 'DYK', 'VVV', "
	    "'RMJ', 'LBS', 'WSL', 'FHZ', 'RWB', 'DLC', 'XPL', 'YLW', 'OYK', 'HQN', 'MRA', 'QAJ', 'MIX', 'ICY', 'WZS', "
	    "'IJG', 'VYF', 'XON', 'AZB', 'RQC', 'AYK', 'CUV', 'JST', 'BNP', 'UCF', 'DIG', 'ANC', 'XVQ', 'CEC', 'TND', "
	    "'SHS', 'YTJ', 'XFX', 'TBI', 'JRE', 'WBD', 'ZVA', 'TSG', 'DYY', 'XUB', 'JDH', 'VSO', 'NMU', 'QWP', 'SKI', "
	    "'XMG', 'RVK', 'XVO', 'OAV', 'NYW', 'NRR', 'QRY', 'EBG', 'OYJ', 'ZDN', 'LLU', 'ACT', 'MYP', 'KUK', 'TDO', "
	    "'CUB', 'QHL', 'QRO', 'FTD', 'KBL', 'YEY', 'NQD', 'OAH', 'ACU', 'EDM', 'QYP', 'OUO', 'PDK', 'VSP', 'PKW', "
	    "'JZF', 'GAL', 'COS', 'RFL', 'DIM', 'MPZ', 'IZX', 'KBU', 'LVK', 'DTP', 'PWC', 'EUQ', 'ESR', 'PIL', 'PTW', "
	    "'DNF', 'VLW', 'NBA', 'DFW', 'RDK', 'PYF', 'NIA', 'LLQ', 'WUE', 'SLJ', 'TEF', 'GNC', 'FIC', 'EQD', 'ERL', "
	    "'IKZ', 'UKG', 'TNT', 'XCP', 'NRV', 'GVU', 'XFZ', 'VEL', 'GCR', 'XAE', 'GTN', 'VRZ', 'WSY', 'RKD', 'WIA', "
	    "'ZPB', 'EWE', 'AVT', 'EZD', 'YCN', 'TJY', 'PCS', 'PXA', 'EAR', 'CIE', 'PPO', 'PFP', 'PFJ', 'SZK', 'IBH', "
	    "'LQB', 'EDI', 'ELJ', 'OJS', 'QBC', 'GTV', 'GXH', 'MFG', 'QQO', 'PIK', 'LDF', 'KIX', 'CSV', 'MAP', 'ALA', "
	    "'KZH', 'QPA', 'BCG', 'DWQ', 'MUM', 'UJT', 'ZIK', 'XPU', 'CQL', 'DZH', 'ZGR', 'UCU', 'LAT', 'NXO', 'QYM', "
	    "'PNV', 'UTV', 'SGK', 'EXJ', 'NTQ', 'LPQ', 'RVT', 'GHZ', 'FSE', 'PGQ', 'WJL', 'JQI', 'IWL', 'SWW', 'DEQ', "
	    "'VLF', 'NUD', 'ZNG', 'DCR', 'LCI', 'NZK', 'QAK', 'GHL', 'UCZ', 'ESU', 'JQG', 'NJL', 'PPD', 'QGA', 'GSW', "
	    "'YDG', 'QNJ', 'EYF', 'NNZ', 'EAG', 'RFE', 'CBE', 'ENO', 'HDT', 'WFV', 'CFE', 'MGY', 'PAE', 'WUG', 'LPU', "
	    "'HZD', 'GFM', 'JHE', 'TPQ', 'DAO', 'KBZ', 'GBA', 'NFH', 'DDS', 'HYA', 'VWS', 'ZLG', 'XKO', 'HFE', 'OJS', "
	    "'ZNK', 'OAT', 'YFE', 'NYJ', 'RDP', 'NTN', 'BEW', 'IGK', 'AYV', 'QXZ', 'NJB', 'QYE', 'GGE', 'DVI', 'UFG', "
	    "'AWY', 'JAN', 'GPW', 'GYE', 'BKS', 'REI', 'XYZ', 'VTY', 'NCD', 'EMH', 'YZE', 'LKN', 'JFF', 'IHZ', 'KUE', "
	    "'VNJ', 'SAW', 'NWS', 'HTH', 'ZXC', 'IAW', 'EPV', 'ONR', 'NFN', 'KMQ', 'CCE', 'ASU', 'SYF', 'PIQ', 'EHB', "
	    "'IWX', 'TAV', 'PJV', 'KET', 'BCR', 'UTS', 'OCI', 'PBE', 'XDJ', 'XSN', 'CUS', 'XBF', 'ZQF', 'RTW', 'CJI', "
	    "'XPI', 'KGD', 'ZNS', 'JCM', 'SRD', 'ADD', 'NJY', 'TQU', 'RNK', 'IQR', 'FYF', 'VDQ', 'IUO', 'SXI', 'DRM', "
	    "'XSX', 'FBH', 'YQE', 'BTM', 'GZJ', 'JJR', 'ONH', 'NLJ', 'XVR', 'XCP', 'VLW', 'ISS', 'FDK', 'FAS', 'HDE', "
	    "'ETY', 'KIA', 'BJX', 'ZZM', 'EIB', 'HWD', 'HDY', 'QGB', 'BQH', 'PFT', 'MBI', 'NND', 'DRP', 'IWC', 'TFF', "
	    "'PDU', 'YTB', 'WHR', 'QNY', 'IUB', 'QZK', 'NXL', 'JOB', 'FEF', 'DZC', 'DAL', 'IJK', 'HBO', 'ZRR', 'JJH', "
	    "'MXI', 'YSP', 'BRV', 'JRA', 'JQE', 'XWQ', 'GVY', 'CEN', 'NYY', 'JCW', 'BIN', 'QGA', 'SCX', 'LZV', 'SPA', "
	    "'CRQ', 'RUJ', 'PYJ', 'BCG', 'QFR', 'QED', 'RLR', 'JXV', 'NIQ', 'WHL', 'UQJ', 'EPN', 'HEF', 'IUR', 'MCH', "
	    "'THC', 'WTN', 'OCP', 'DSM', 'LXW', 'ZQJ', 'FMU', 'VNW', 'IXM', 'BYF', 'NXY', 'CJO', 'IIY', 'CGW', 'PNE', "
	    "'WFH', 'SHG', 'SBO', 'XFA', 'KLO', 'YMJ', 'INZ', 'PMK', 'RYE', 'KHB', 'YXB', 'TZK', 'LES', 'ZZN', 'FXH', "
	    "'EMP', 'PDI', 'JIS', 'GQW', 'SMX', 'UCU', 'IFF', 'VZI', 'DPN', 'LIK', 'CJO', 'DOC', 'BJP', 'NUB', 'GPH', "
	    "'GRQ', 'SOU', 'TZH', 'WUN', 'PDC', 'EWB', 'YMX', 'OEF', 'MEB', 'QBU', 'JUQ', 'HEQ', 'UJF', 'QDF', 'MMK', "
	    "'ZVU', 'JRP', 'JOS', 'TCC', 'TIO', 'RPM', 'GVB', 'GOH', 'CUV', 'DUW', 'EAK', 'CII', 'PGT', 'CHN', 'HMM', "
	    "'RLO', 'ARC', 'EFW', 'HVX', 'HJV', 'OHY', 'GPG', 'MQO', 'GBU', 'YPD', 'FNW', 'MDP', 'KXQ', 'PSB', 'NVZ', "
	    "'BYC', 'IUC', 'URT', 'BVP', 'VZP', 'UTL', 'BXW', 'FKT', 'PGM', 'DGD', 'AIV', 'TMY', 'RUN', 'QYG', 'GXF', "
	    "'VPF', 'UNY', 'MOZ', 'INN', 'VEV', 'PWC', 'AWY', 'MKI', 'TXG', 'BQU', 'QAU', 'ILO', 'IBH', 'PCO', 'RDH', "
	    "'UNP', 'LLB', 'DER', 'YRP', 'MWZ', 'NIH', 'MCS', 'ZCL', 'IOI', 'UPU', 'MNS', 'JDR', 'OWF', 'XTA', 'OAZ', "
	    "'WCV', 'FFI', 'NLI', 'BFD', 'LDD', 'FZR', 'HNP', 'IRL', 'WEA', 'PDE', 'IEN', 'GGR', 'YUP', 'VWA', 'WEM', "
	    "'QPZ', 'ZBA', 'GLU', 'CUH', 'VDK', 'KLF', 'OLF', 'PMD', 'FLZ', 'JQV', 'SGP', 'JZZ', 'HIO', 'LFV', 'PTG', "
	    "'FEL', 'WNX', 'LDU', 'EXI', 'JMT', 'RID', 'TXZ', 'PGC', 'GLC', 'JLI', 'CLT', 'VDK', 'FBI', 'WSW', 'ELJ', "
	    "'YJY', 'YXU', 'XWC', 'MII', 'YXM', 'HCJ', 'JVC', 'RPF', 'NXE', 'UFO', 'BYH', 'EFA', 'EGO', 'XVS', 'VWG', "
	    "'LOY', 'LCG', 'TRQ', 'IKL', 'OQA', 'HTU', 'XNU', 'AZS', 'BOR', 'XWR', 'UQN', 'YFH', 'TDS', 'RLP', 'PHC', "
	    "'AYP', 'BVL', 'CQI', 'HSC', 'JRS', 'YZU', 'YFY', 'WEI', 'ITW', 'MYU', 'ORF', 'OTD', 'CAO', 'UJN', 'UFW', "
	    "'UUP', 'IQG', 'SEG', 'BAQ', 'KJD', 'DNG', 'BZL', 'DVT', 'KJY', 'XLW', 'TMH', 'DCX', 'MVJ', 'JFU', 'VCL', "
	    "'FQX', 'WRS', 'OXS', 'QGT', 'UPJ', 'QTI', 'AOU', 'CHR', 'AMC', 'VCN', 'OUC', 'JDK', 'CNL', 'HVI', 'WTS', "
	    "'VPC', 'THK', 'SVV', 'IGZ', 'ZPC', 'OBU', 'EQP', 'UMD', 'BQX', 'CKN', 'FOW', 'RGD', 'LNP', 'YEG', 'GVT', "
	    "'BSV', 'KKI', 'JEZ', 'GQH', 'DHB', 'TFV', 'TFS', 'WVS', 'ZFS', 'ZPG', 'PLN', 'OBM', 'BRE', 'HXC', 'JBF', "
	    "'GKA', 'RKS', 'PUS', 'XGS', 'QYD', 'IOK', 'DZR', 'IRX', 'NIJ', 'EAJ', 'SCY', 'KXB', 'VII', 'ETT', 'UPQ', "
	    "'JPW', 'ZTS', 'CZC', 'DFE', 'JLR', 'ZAC', 'XTQ', 'JZY', 'MUW', 'EXR', 'OQC', 'QLF', 'VAL', 'CBQ', 'SQO', "
	    "'HXA', 'SCI', 'ZAM', 'EDK', 'HVB', 'LSH', 'EOR', 'KZF', 'TLA', 'JIK', 'ULN', 'CJY', 'YKP', 'RQQ', 'XQJ', "
	    "'EFK', 'WEA', 'VHL', 'ANA', 'VYI', 'GTV', 'FRN', 'BXI', 'QYM', 'WUJ', 'FHW', 'RAH', 'RCK', 'RXY', 'KXC', "
	    "'IFA', 'OCZ', 'PTB', 'IHO', 'SDL', 'YEP', 'RFX', 'ZUR', 'FKV', 'WFA', 'VAF', 'ZHK', 'KZO', 'AAN', 'IME', "
	    "'MIF', 'VCG', 'BYQ', 'QOY', 'LSG', 'IRT', 'PUI', 'UAB', 'MPY', 'CKS', 'PZL', 'EUW', 'JIO', 'WXL', 'JEV', "
	    "'SBT', 'WUQ', 'GBH', 'QEG', 'ZCJ', 'OKQ', 'AAX', 'LJN', 'OYH', 'EIJ', 'ZNK', 'JIA', 'TDN', 'DPU', 'BTR', "
	    "'SEP', 'VKZ', 'HEX', 'WFB', 'DKR', 'FQH', 'CTQ', 'NPX', 'DHG', 'ZSN', 'ZYF', 'MLP', 'IUH', 'ZIX', 'UYI', "
	    "'SRK', 'GEG', 'SCW', 'TED', 'XOP', 'PML', 'RNK', 'KTA', 'KJJ', 'UXS', 'MRX', 'FJH', 'NRA', 'ZJJ', 'WGU', "
	    "'NPD', 'GPA', 'UBR', 'GXI', 'YQP', 'SEI', 'MOZ', 'UIR', 'SIH', 'EAP', 'IXV', 'EKD', 'YVX', 'PPZ', 'MBK', "
	    "'NTQ', 'BSZ', 'BVD', 'LMV', 'MDB', 'BOK', 'XLL', 'QJJ', 'CTH', 'BLQ', 'ZOR', 'RVD', 'MNW', 'OCC', 'FCN', "
	    "'MAK', 'OMZ', 'PLJ', 'QMP', 'PTZ', 'AXB', 'YJG', 'JZX', 'HUH', 'FOL', 'BUV', 'BEI', 'ZYN', 'JHS', 'JND', "
	    "'TIB', 'JMM', 'UTZ', 'GEZ', 'LTQ', 'IBJ', 'WOY', 'IGT', 'ZUV', 'MWP', 'GSJ', 'FET', 'LXM', 'ZBE', 'VTI', "
	    "'WLW', 'PYT', 'DJQ', 'NFU', 'BKX', 'MDD', 'YPX', 'SBG', 'TKI', 'QRU', 'IKG', 'SRQ', 'AOB', 'KHN', 'IZN', "
	    "'NQU', 'NQC', 'HVS', 'JCP', 'HUL', 'MXN', 'DOW', 'HXR', 'GZT', 'GNL', 'INF', 'AOU', 'RXX', 'UEP', 'OXY', "
	    "'VWA', 'GLH', 'WPM', 'NVR', 'HIP', 'RIM', 'JJJ', 'JDN', 'RBB', 'BDI', 'XVJ', 'ERO', 'RXJ', 'ZMA', 'TVU', "
	    "'HPE', 'VAU', 'GVU', 'ZGK', 'KCK', 'DHB', 'RNZ', 'HDO', 'CZF', 'UTI', 'SAQ', 'SWA', 'VXK', 'FEH', 'RFJ', "
	    "'ZKQ', 'DOJ', 'DKK', 'SDL', 'FSX', 'ZJF', 'KTA', 'NLG', 'PHJ', 'WOH', 'AXU', 'HSI', 'SZW'";

	result = con.Query("SELECT * FROM strings WHERE s IN (" + in_list + ") ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"HXR", "NUT"}));

	result = con.Query("SELECT s, s IN (" + in_list + ") AS in_list FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "HXR", "NUT", "ZZZ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, false}));

	result = con.Query("SELECT s, s IN (" + in_list + ", NULL) AS in_list FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "HXR", "NUT", "ZZZ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, Value()}));

	result = con.Query("SELECT s, s NOT IN (" + in_list + ") AS in_list FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "HXR", "NUT", "ZZZ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));

	result = con.Query("SELECT s, s NOT IN (" + in_list + ", NULL) AS in_list FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "HXR", "NUT", "ZZZ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, Value()}));
}
