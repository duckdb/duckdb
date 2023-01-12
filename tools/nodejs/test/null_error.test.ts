import * as sqlite3 from '..';
import * as helper from './support/helper';

describe('null error', function() {
    var filename = 'test/tmp/test_sqlite_ok_error.db';
    var db: sqlite3.Database;

    before(function(done) {
        helper.ensureExists('test/tmp');
        helper.deleteFile(filename);
        db = new sqlite3.Database(filename, done);
    });

    it('should create a table', function(done) {
        db.run("CREATE TABLE febp_data (leacode TEXT, leaname TEXT, state TEXT, postcode TEXT, fips TEXT, titleistim TEXT, ideastim TEXT, ideapool TEXT, ideapoolname TEXT, localebasis TEXT, localetype2 TEXT, version TEXT, leacount_2006 TEXT, ppexpend_2005 TEXT, ppexpend_2006 TEXT, ppexpend_2007 TEXT, ppexpend_2008 TEXT, ppexpendrank_2006 TEXT, ppexpendrank_2007 TEXT, ppexpendrank_2008 TEXT, rankppexpend_2005 TEXT, opbud_2004 TEXT, opbud_2006 TEXT, opbud_2007 TEXT, opbud_2008 TEXT, titlei_2004 TEXT, titlei_2006 TEXT, titlei_2007 TEXT, titlei_2008 TEXT, titlei_2009 TEXT, titlei_2010 TEXT, idea_2004 TEXT, idea_2005 TEXT, idea_2006 TEXT, idea_2007 TEXT, idea_2008 TEXT, idea_2009 TEXT, ideaest_2010 TEXT, impact_2007 TEXT, impact_2008 TEXT, impact_2009 TEXT, impact_2010 TEXT, fedrev_2006 TEXT, fedrev_2007 TEXT, fedrev_2008 TEXT, schonut_2006 TEXT, schonut_2007 TEXT, schomeal_2006 TEXT, schomeal_2007 TEXT, schoco_2006 TEXT, schocom_2007 TEXT, medicaid_2006 TEXT, medicaid_2007 TEXT, medicaid_2008 TEXT, cenpov_2004 TEXT, cenpov_2007 TEXT, cenpov_2008 TEXT, rankcenpov_2004 TEXT, rankcenpov_2007 TEXT, rankcenpov_2008 TEXT, enroll_2006 TEXT, enroll_2007 TEXT, enroll_2008 TEXT, white_2006 TEXT, white_2007 TEXT, white_2008 TEXT, afam_2006 TEXT, afam_2007 TEXT, afam_2008 TEXT, amin_2006 TEXT, amin_2007 TEXT, amin_2008 TEXT, asian_2006 TEXT, asian_2007 TEXT, asian_2008 TEXT, hisp_2006 TEXT, hisp_2007 TEXT, hisp_2008 TEXT, frpl_2006 TEXT, frpl_2007 TEXT, frpl_2008 TEXT, ell_2006 TEXT, ell_2007 TEXT, ell_2008 TEXT, sped_2006 TEXT, sped_2007 TEXT, sped_2008 TEXT, state4read_2005 TEXT, state4read_2006 TEXT, state4read_2007 TEXT, state4read_2008 TEXT, state4read_2009 TEXT, state4math_2005 TEXT, state4math_2006 TEXT, state4math_2007 TEXT, state4math_2008 TEXT, state4math_2009 TEXT, minor_2007 TEXT, minor_2008 TEXT, state8math_2006 TEXT, state8math_2007 TEXT, state8math_2008 TEXT, state8math_2009 TEXT, state8read_2006 TEXT, state8read_2007 TEXT, state8read_2008 TEXT, state8read_2009 TEXT, statehsmath_2006 TEXT, statehsmath_2007 TEXT, statehsmath_2008 TEXT, statehsmath_2009 TEXT, statehsread_2006 TEXT, statehsread_2007 TEXT, statehsread_2008 TEXT, statehsread_2009 TEXT)", done);
    });

    it('should insert rows with lots of null values', function(done) {
        var stmt = db.prepare('INSERT INTO febp_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', function(err: null | Error) {
            if (err) throw err;

            for (var i = 0; i < 100; i++) {
                stmt.run([ '100005', 'Albertville City School District', 'ALABAMA', 'AL', '1', '856031', '753000', 'NULL', 'NULL', '6-Small Town', 'Town', 21, '130', '6624', '7140', '8731', '8520', '102', '88', '100', '94', '23352000', '27280000', '30106000', '33028000', '768478', '845886', '782696', '1096819', '1279663', '1168521', '561522', '657649', '684366', '687531', '710543', '727276', '726647', 'N/A', 'N/A', 'N/A', 'N/A', '986', '977', '1006', '1080250', '1202325', '1009962', '1109310', '70287', '93015', '14693.56', '13634.58', 'N/A', '0.230', '0.301', '0.268882175', '73', '26', '29', '3718', '3747', '3790', '2663', '2615', '2575', '75', '82', '89', '3', '2', '6', '11', '9', '8', '955', '1028', '1102', '1991', '2061', '2146', '649', '729', '770', '443', '278', '267', '0.860', '0.86', '0.8474', '0.84', '0.8235', '0.810', '0.84', '0.7729', '0.75', '0.7843', '1121', '1205', '0.74', '0.6862', '0.72', '0.7317', '0.78', '0.7766', '0.79', '0.7387', '0.84', '0.9255', '0.86', '0.9302', '0.88', '0.9308', '0.84', '0.8605' ]);
            }

            stmt.finalize(function(err) {
                if (err) throw err;
                done();
            });
        });
    });

    it('should have created the database', function() {
        helper.fileExists(filename);
    });

    after(function() {
        helper.deleteFile(filename);
    });
});
