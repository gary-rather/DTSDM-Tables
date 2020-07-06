import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DcmntWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("DcmntWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01(){

        //check that the unknown record 0 is populated
        //EXPECT: Initial load will add the 'UNKNOWN' row to the table. DCMNT_WID = 0; TRIP_WID = 0; PERSON_WID = 0;
        //          PERSON_ATTRIB_WID = 0; DCMNT_TYPE_WID = 0; AO_PERSON_WID = 0; AGNCY_WID = 0; AGNCY_ORG_WID = 0;
        //          SUBORG_WID = 0; ADJSTR_PERSON_WID = 0; DELEGT_PERSON_WID = 0; NXT_RTNG_OFFCL_WID = 0;
        //          CURR_DCMNNT_STATUS_WID = 0; NXT_DCMNT_STATUS_WID = 0; EXT_SYSTEM_WID = 0; DUTY_COND_TYPE_WID = 0;
        //          TRIP_TYPE_WID = 0; SGND_PERSON_WID = 0; All WIDS = 0; DCMNT_NAME = 'UNKNOWN'; DCMNT_BASE_NAME = 'UNKNOWN';
        //          ADJSTMT_LVL = 0; others NULL.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Initial load must add  unspecified data row.  Provides the ability to traverse through the DTSDM. DCMNT_WH  table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select * from DTSDM.DCMNT_WH where DCMNT_WH.DCMNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DcmntWhTest.test1,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        number++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((number == 1),"(number == 1)");
        roList.add(ro);
        wr.logTestResults(roList);

        System.out.println("Test DcmntWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish DcmntWhTest.test1");

    }

    @Test
    public void test02(){

        //Check the population of the unique identifier (DCMNT_WH.DCMNT_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_WID  PK column";
        String reason = " Sequential ID PK";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from " +
                      "( \n" +
                      "select distinct DCMNT_WH.DCMNT_WID, count(*) from DTSDM.DCMNT_WH\n" +
                        "group by DCMNT_WH.DCMNT_WID having count(*) > 1 \n" +
                      ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

         wr.logSql(theSql);

        int count = 0;
        int distinctCount = 0;
        int duplicateCount = 0;

        System.out.println("Starting DcmntWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        duplicateCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test2 sql1 failed");
            e.printStackTrace();
        }


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((0 == duplicateCount),"(0 == duplicateCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);
        assertEquals(0, duplicateCount);


        System.out.println("Finish DcmntLocatnWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the overall population of the DCMNT_WH table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the overall population of the DCMNT_WH table";
        String reason = " One row per unique document number and adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DCMNT_WH";

        String sql2 = "select sum(table_row_cnt) from \n" +
                        "(select distinct U##VCHNUM, ADJ_LEVEL, count(*) table_row_cnt \n" +
                        "from DTSDM_SRC_STG.VOUCHER group by U##VCHNUM, ADJ_LEVEL)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test3 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 3: Test Count = " + testCount);
        System.out.println("Test 3: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntWhTest.test3");
        System.out.println();

    }

    @Test
    public void test04(){

        //Check the population of the DCMNT_WH.TRIP_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_WID column";
        String reason = " Populate in ETL workflow that populates TRIP_WH table; authorization data used to build trip data, so write back the TRIP_WID as part of populating the TRIP_WH table";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct TRIP_WID,count(*) from DTSDM.DCMNT_WH group by TRIP_WID";
        String sql2 = "select distinct TRIP_WID,count(*) from DTSDM.TRIP_WH group by TRIP_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DTSDM.MNT_WH (table being tested)
        int testCount = 0; //comes from TRIP_WH (load being tested)

        System.out.println("Starting DcmntWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test4 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 4: Comparison Count = " + comparisonCount);
        System.out.println("Test 4: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test4");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the DCMNT_WH.PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.PERSON_WID column";
        String reason = " The latest PERSON_WID associated with the adjustment level. Update value if change is found to person data (new PERSON row is created), for adj_level 0 only; do not update if new person data is found and adj_level != 0, and do not update if return date is in the past";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct PERSON_WID from DTSDM.DCMNT_WH";
        String sql2 = "select distinct PERSON_WID from DTSDM.PERSON_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DTSDM.MNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH (load being tested)

        System.out.println("Starting DcmntWhTest.test5,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test5,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test5 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 5: Comparison Count = " + comparisonCount);
        System.out.println("Test 5: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test5");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the DCMNT_WH.PERSON_ATTRIB_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.PERSON_ATTRIB_WID column";
        String reason = " Compare values in all columns of PERSON_ATTRIB_HIST_WH to the source columns in DTSDM_SRC_STG.VOUCHER and get the ID of the matching row";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "Select distinct PERSON_ATTRIB_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_ATTRIB_WID, count(*) table_row_cnt\n" +
                        "from DTSDM.PERSON_ATTRIB_HIST_WH a, DTSDM_SRC_STG.VOUCHER b, DTSDM.PERSON_WH c\n" +
                        "where a.SSN_FULL = b.U##SSN and b.ADJ_LEVEL = 0 \n" +
                        "and a.PERSON_WID = c.PERSON_WID and c.SSN_FULL = b.U##SSN\n" +
                        "and b.U##DOCTYPE in ('VCH','AUTH', 'LVCH') and a.FNAME = b.FNAME\n" +
                        "and a.LNAME = b.LNAME and a.FULL_ORG_CD = b.U##ORG\n" +
                        "group by a.PERSON_ATTRIB_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_ATTRIB_WH, PERSON_WH, and DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test6,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test6 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 6: Comparison Count = " + comparisonCount);
        System.out.println("Test 6: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test6");
        System.out.println();

    }

    @Test
    public void test07(){

        // Check the population of the DCMNT_WH.DCMNT_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_TYPE_WID column";
        String reason = " FK to the TYPE_CONSOLDTD_RFRNC_WH table. Derive by looking up DOCSTAT.U##DOCTYPE";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct DCMNT_TYPE_WID, count(*) table_row_cnt \n" +
                        "from DTSDM.DCMNT_WH group by DCMNT_TYPE_WID";

        String sql2 = "select distinct a.TYPE_WID\n" +
                        "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH a, DTSDM_SRC_STG.DOCSTAT b \n" +
                        "where a.TYPE_CD = b.U##DOCTYPE";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from TYPE_CONSOLODTD_RFRNC_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test7,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test7,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test7 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 7: Comparison Count = " + comparisonCount);
        System.out.println("Test 7: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test7");
        System.out.println();

    }

    @Test
    public void test08(){

        // Check the population of the DCMNT_WH.AO_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.AO_PERSON_WID column";
        String reason = " FK to PERSON_WH table. Derive by looking up DOCSTAT.APPROVE_SSN";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct AO_PERSON_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_WID \n" +
                        "from DTSDM.PERSON_WH a, DTSDM_SRC_STG.DOCSTAT b \n" +
                        "where a.SSN_FULL = b.APPROVE_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test8,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test8 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 8: Comparison Count = " + comparisonCount);
        System.out.println("Test 8: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test8");
        System.out.println();

    }

    @Test
    public void test09(){

        // Check the population of the DCMNT_WH.DELEGT_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DELEGT_PERSON_WID column";
        String reason = " FK to PERSON table. Derive by looking up VCHSTAT.DELEGATE_SSN";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct DELEGT_PERSON_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH a, DTSDM_SRC_STG.VCHSTAT b\n" +
                        "where a.SSN_FULL = b.DELEGATE_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH and DTSDM_SRC_STG.VCHSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test9,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test9 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 9: Comparison Count = " + comparisonCount);
        System.out.println("Test 9: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test9");
        System.out.println();

    }

    @Test
    public void test10(){

        // Check the population of the DCMNT_WH.NXT_RTNG_OFCL_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.NXT_RTNG_OFCL_WID column";
        String reason = " FK to PERSON table. Derive by looking up VCHSTAT.U##NEXT_SSN";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct nvl(NXT_RTNG_OFFCL_WID,0) from DTSDM.DCMNT_WH";

        String sql2 = "select distinct p.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH p, DTSDM_SRC_STG.VCHSTAT vs\n" +
                        "where p.SSN_FULL = vs.U##NEXT_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH and DTSDM_SRC_STG.VCHSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test10,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test10 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 10: Comparison Count = " + comparisonCount);
        System.out.println("Test 10: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11(){

        // Check the population of the DCMNT_WH.AGNCY_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.AGNCY_WID column";
        String reason = " FK to AGNCY_WH. Derive by looking up VOUCHER.U##ORG in SUBORG_WH, then looking up AGNCY_WID in AGNCY_ORG_WH via AGNCY_ORG_WID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct nvl(AGNCY_WID,0), count(*) from DTSDM.DCMNT_WH group by AGNCY_WID";

        String sql2 = "select distinct ao.AGNCY_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH so, DTSDM_SRC_STG.VOUCHER v, DTSDM.AGNCY_ORG_WH ao\n" +
                        "where v.U##ORG = so.FULL_ORG_CD\n" +
                        "and so.AGNCY_ORG_WID = ao.AGNCY_ORG_WID\n" +
                        "group by ao.AGNCY_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LUBORG_WH, AGNCY_ORG_WH and DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test11,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test11,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test11 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 11: Comparison Count = " + comparisonCount);
        System.out.println("Test 11: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12(){

        // Check the population of the DCMNT_WH.SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SUBORG_WID column";
        String reason = " FK to SUBORG_WH; lookup ID on ORG = FULL_ORG_CD";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WID, count(*) from DTSDM.DCMNT_WH group by SUBORG_WID";

        String sql2 = "select distinct so.SUBORG_WID \n" +
                        "from DTSDM.SUBORG_WH so, DTSDM_SRC_STG.VOUCHER v \n" +
                        "where v.U##ORG = so.FULL_ORG_CD";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LUBORG_WH and DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test12 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 12: Comparison Count = " + comparisonCount);
        System.out.println("Test 12: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13(){

        // Check the population of the DCMNT_WH.SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SUBORG_WID column";
        String reason = " Lookup ID in STATUS_CONSOLDTD_RFRNC";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct CURR_DCMNNT_STATUS_WID, count(*) \n" +
                        "from DTSDM.DCMNT_WH group by CURR_DCMNNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, DTSDM_SRC_STG.DOCSTAT d \n" +
                        "where scr.STATUS_DESCR = d.CUR_STATUS \n" +
                        "and d.U##DOCTYPE in ('VCH','AUTH', 'LVCH')";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LTATUS_CONSOLDTD_RFRNC_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test13,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test13,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test13 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 13: Comparison Count = " + comparisonCount);
        System.out.println("Test 13: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14(){

        // Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select dc.CURR_DCMNNT_STATUS_DT as etl_curr_dcmnt_status_dt, \n" +
                        "    d.CUR_STATUS_DATE as test_curr_dcmnt_status_dt\n" +
                        "  from DTSDM.DCMNT_WH dc, DTSDM_SRC_STG.DOCSTAT d\n" +
                        "  where dc.DCMNT_NAME = d.U##VCHNUM\n" +
                        "  and dc.SRC_SSN = d.U##SSN\n" +
                        "  and dc.SRC_DOCTYPE = d.U##DOCTYPE\n" +
                        "  and dc.ADJSTMT_LVL = d.ADJ_LEVEL\n" +
                        "  and dc.CURR_DCMNNT_STATUS_DT != d.CUR_STATUS_DATE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test14,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test14 sql1 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 14: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15(){

        // Check the population of the DCMNT_WH.NXT_DCMNT_STATUS_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.NXT_DCMNT_STATUS_WID column";
        String reason = " Lookup ID in STATUS_CONSOLDTD_RFRNC";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct NXT_DCMNT_STATUS_WID, count(*) from DTSDM.DCMNT_WH group by NXT_DCMNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, DTSDM_SRC_STG.DOCSTAT d\n" +
                        "where scr.STATUS_DESCR = d.AWAITING\n" +
                        "and d.U##DOCTYPE in ('VCH','AUTH', 'LVCH')";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LTATUS_CONSOLDTD_RFRNC_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test15,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test15,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test15 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 15: Comparison Count = " + comparisonCount);
        System.out.println("Test 15: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test15");
        System.out.println();

    }

    @Ignore
    @Test
    public void test16() {
        //no test right now (no business rules)
    }

    @Test
    public void test17(){

        // Check the population of the DCMNT_WH.DCMNT_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_NAME column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct DCMNT_NAME, count(*) from DTSDM.DCMNT_WH group by DCMNT_NAME";
        String sql2 = "select distinct VCHNUM, count(*) from DTSDM_SRC_STG.VOUCHER group by VCHNUM";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test17,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test17,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test17 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 17: Comparison Count = " + comparisonCount);
        System.out.println("Test 17: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the DCMNT_WH.DCMNT_BASE_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_BASE_NAME column";
        String reason = " Get substring up to the underscore followed by voucher type indicator and authorization number, i.e. _A01";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME), count(*)\n" +
                        "from DTSDM.DCMNT_WH group by NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME)";

        String sql2 = "select distinct NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1), VCHNUM), count(*)\n" +
                        "from DTSDM_SRC_STG.VOUCHER group by NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1),VCHNUM)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test18,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test18,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test18 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 18: Comparison Count = " + comparisonCount);
        System.out.println("Test 18: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test18");
        System.out.println();

    }

    @Test
    public void test19(){

        // Check the population of the DCMNT_WH.ADJSTMT_LVL column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.ADJSTMT_LVL column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct ADJSTMT_LVL, count(*) from DTSDM.DCMNT_WH\n" +
                        "group by ADJSTMT_LVL order by ADJSTMT_LVL";

        String sql2 = "select distinct ADJ_LEVEL, count(*) from DTSDM_SRC_STG.VOUCHER\n" +
                        "group by ADJ_LEVEL order by ADJ_LEVEL";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM_SRC_STG.VOUCHER table (load being tested)

        System.out.println("Starting DcmntWhTest.test19,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test19,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test19 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 19: Comparison Count = " + comparisonCount);
        System.out.println("Test 19: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test19");
        System.out.println();

    }

    @Ignore
    @Test
    public void test20(){

    }

    @Ignore
    @Test
    public void test21(){

    }

    @Ignore
    @Test
    public void test22(){

    }

    @Test
    public void test23(){

        // Check the population of the DCMNT_WH.TRIP_PURPOSE_DESCR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_PURPOSE_DESCR column";
        String reason = " RETDATE - DEPDATE plus 1 day; get for specified adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select dc.trip_purpose_descr as etl_purpose_descr, " +
                        "    t.purpose as test_purpose_descr \n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.trip t \n" +
                        "  where dc.dcmnt_name = t.u##vchnum \n" +
                        "  and dc.src_ssn = t.u##ssn \n" +
                        "  and dc.src_doctype = t.u##doctype \n" +
                        "  and dc.adjstmt_lvl = t.adj_level \n" +
                        "  and dc.trip_purpose_descr != t.purpose" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test23,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test23 sql1 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 23: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test23");
        System.out.println();

    }

    @Test
    public void test24(){

        // Check the population of the DCMNT_WH.TRIP_TDY_DAYS_CNT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_TDY_DAYS_CNT column";
        String reason = " straight pull; get purpose for specified adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select (dc.trip_rtrn_dt - dc.trip_dprt_dt)+1 as etl_trip_tdy_days_cnt, \n" +
                        "    (d.retdate - depdate)+1 as test_tdy_days_cnt \n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and (dc.trip_rtrn_dt - dc.trip_dprt_dt)+1 != (d.retdate - depdate)+1 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test24,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test24 sql1 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 24: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test24");
        System.out.println();

    }

    @Test
    public void test25(){

        // Check the population of the DCMNT_WH.TRIP_CP_CR_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_CP_CR_AMT column";
        String reason = " Get the sum of TCOST values where INTINRY.MODE IN('CP', 'CR') AND U##LINE_TYPE = 'X' AND ADJ_LEVEL = 0;  get for specified adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select dc.trip_cp_cr_amt as etl_cp_cr_amt \n, " +
                        "    i.tcost as test_cp_cr_amt \n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.ITINRY i \n" +
                        "  where dc.dcmnt_name = i.u##vchnum \n" +
                        "  and dc.src_ssn = i.u##ssn \n" +
                        "  and dc.src_doctype = i.u##doctype \n" +
                        "  and dc.adjstmt_lvl = i.adj_level \n" +
                        "  and i.mode_ in ('CP','CR') \n" +
                        "  and i.u##line_type = 'X' \n" +
                        "  and i.adj_level = 0 \n" +
                        "  and dc.trip_cp_cr_amt != i.tcost" +
                        "  group by dc.trip_cp_cr_amt, i.tcost \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test25,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test25 sql1 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 25: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test25");
        System.out.println();

    }

    @Test
    public void test26(){

        // Check the population of the DCMNT_WH.TRIP_CPC_CRC_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_CPC_CRC_AMT column";
        String reason = " Get the sum of TCOST values where INTINRY.MODE IN('CP-C', 'CR-C') AND U##LINE_TYPE = 'X' AND ADJ_LEVEL = 0;  get for specified adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.trip_cpc_crc_amt as etl_cpc_crc_amt, \n" +
                        "    i.tcost as test_cpc_crc_amt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.ITINRY i \n" +
                        "\n" +
                        "  where dc.dcmnt_name = i.u##vchnum \n" +
                        "  and dc.src_ssn = i.u##ssn \n" +
                        "  and dc.src_doctype = i.u##doctype \n" +
                        "  and dc.adjstmt_lvl = i.adj_level \n" +
                        "  and i.mode_ in ('CP_C','CR_C') \n" +
                        "  and i.u##line_type = 'X' \n" +
                        "  and i.adj_level = 0 \n" +
                        "  and dc.trip_cpc_crc_amt != i.tcost \n" +
                        "\n" +
                        "  group by dc.trip_cpc_crc_amt,i.tcost \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test26,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test26 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 26: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test26");
        System.out.println();

    }

    @Test
    public void test27(){

        // Check the population of the DCMNT_WH.ADJSTR_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.ADJSTR_PERSON_WID column";
        String reason = " FK to PERSON table. Derive by looking up VCHADJ.SSN. No incremental updates";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.adjstr_person_wid as etl_adjstr_person_wid, " +
                        "    p.person_wid as test_adjstr_person_wid \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, dtsdm.person_wh p, DTSDM_SRC_STG.vchadj v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn \n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and p.ssn_full = v.u##ssn \n" +
                        "  and dc.adjstr_person_wid != p.person_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test27,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test27 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 27: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test27");
        System.out.println();

    }

    @Test
    public void test28(){

        // Check the population of the DCMNT_WH.ADJSTMT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.ADJSTMT_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.adjstmt_dt as etl_adjstmt_dt, \n" +
                        "    v.adj_date as test_adjstmt_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.vchadj v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn \n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and dc.adjstmt_dt != v.adj_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test28,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test28 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 28: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test28");
        System.out.println();

    }

    @Test
    public void test29(){

        // Check the population of the DCMNT_WH.SGND_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SGND_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.sgnd_dt as etl_sgnd_dt, \n" +
                        "    d.sign_date as test_sgnd_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.sgnd_dt != d.sign_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test29,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test29 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 29: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test29");
        System.out.println();

    }

    @Test
    public void test30(){

        // Check the population of the DCMNT_WH.APPRVD_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.APPRVD_DT column";
        String reason = " Last approved date where d.approve_ssn not in 'AUTO', 'CTO','PROCESSED', DOCSTAT joins to VOUCHER on VCHNUM and ADJ_LEVEL";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.apprvd_dt as etl_apprvd_dt, \n" +
                        "    d.approve_date as test_apprvd_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and d.u##vchnum = v.u##vchnum \n" +
                        "  and d.adj_level = v.adj_level \n" +
                        "  and dc.apprvd_dt != d.approve_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test30,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test30 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 30: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test30");
        System.out.println();

    }

    @Test
    public void test31(){

        // Check the population of the DCMNT_WH.CREATE_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CREATE_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  Select dc.create_dt as etl_create_dt, \n" +
                        "    d.create_date as test_create_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.create_dt != d.create_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test31,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test31 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 31: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test31");
        System.out.println();

    }

    @Test
    public void test32(){

        // Check the population of the DCMNT_WH.CNCLD_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CNCLD_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.cncld_dt as etl_cncld_dt, \n" +
                        "    d.cancel_date as test_cncld_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.cncld_dt != d.cancel_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test32,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test32 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 32: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test32");
        System.out.println();

    }

    @Ignore
    @Test
    public void test33(){

    }

    @Ignore
    @Test
    public void test34(){

    }

    @Ignore
    @Test
    public void test35(){

    }

    @Ignore
    @Test
    public void test36(){

    }

    @Ignore
    @Test
    public void test37(){

    }

    @Ignore
    @Test
    public void test38(){

    }

    @Ignore
    @Test
    public void test39(){

    }

    @Ignore
    @Test
    public void test40(){

    }

    @Test
    public void test41(){

        // Check the population of the DCMNT_WH.ADVNC_RJCT_DATE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.ADVNC_RJCT_DATE column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.advnc_rjct_date as etl_advnc_rjct_date, \n" +
                        "    d.adv_reject_date as test_advnc_rjct_date \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.advnc_rjct_date is not null \n" +
                        "  and dc.advnc_rjct_date != d.adv_reject_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test41,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test41 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 41: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test41");
        System.out.println();

    }

    @Test
    public void test42(){

        // Check the population of the DCMNT_WH.CLAIM_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CLAIM_AMT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.claim_amt as etl_override_flag, \n" +
                        "    v.amt_claim as test_claim_amt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn \n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and dc.claim_amt != v.amt_claim \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test42,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test42 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 42: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test42");
        System.out.println();

    }

    @Ignore
    @Test
    public void test43(){

    }

    @Ignore
    @Test
    public void test44(){

    }

    @Ignore
    @Test
    public void test45(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test46(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test47(){
        //no test right now (no business rules)
    }

    @Test
    public void test48(){

        // Check the population of the DCMNT_WH.TOT_ENTRD_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TOT_ENTRD_AMT column";
        String reason = " Sum of all ENTERED_AMT values for all records in INTINRY for the document and adjustment level";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.tot_entrd_amt as etl_tot_entrd_amt, " +
                        "    i.entered_amt as test_tot_entrd_amt\n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.itinry i\n" +
                        "\n" +
                        "  where dc.dcmnt_name = i.u##vchnum\n" +
                        "  and dc.src_ssn = i.u##ssn\n" +
                        "  and dc.src_doctype = i.u##doctype\n" +
                        "  and dc.adjstmt_lvl = i.adj_level\n" +
                        "  and dc.tot_entrd_amt = i.entered_amt" +
                        "  group by dc.tot_entrd_amt, i.entered_amt" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test48,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test48 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 48: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test48");
        System.out.println();

    }

    @Ignore
    @Test
    public void test49(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test50(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test51(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test52(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test53(){

    }

    @Test
    public void test54(){

        // Check the population of the DCMNT_WH.PREV_DCMNT_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.PREV_DCMNT_NAME column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.prev_dcmnt_name as etl_prev_dcmnt_name, \n" +
                        "    d.prior_vchnum as test_prev_dcmnt_name \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.prev_dcmnt_name != d.prior_vchnum" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test54,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test54 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 54: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test54");
        System.out.println();

    }

    @Test
    public void test55(){

        // Check the population of the DCMNT_WH.SRC_ORIG_CREATE_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SRC_ORIG_CREATE_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.src_orig_create_dt as etl_src_orig_create_dt, \n" +
                        "    d.orig_create_date as test_src_orig_create_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.src_orig_create_dt != d.orig_create_date" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test55,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test55 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 55: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test55");
        System.out.println();

    }

    @Test
    public void test56(){

        // Check the population of the DCMNT_WH.SRC_LAST_UPDT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SRC_LAST_UPDT_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.src_last_updt_dt as etl_src_last_updt_dt, \n" +
                        "    d.last_update_date as test_src_last_updt_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn\n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.src_last_updt_dt!= d.last_update_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test56,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test56 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 56: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test56");
        System.out.println();

    }

    @Test
    public void test57(){

        // Check the population of the DCMNT_WH.FRST_TCKT_ISSUED_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.FRST_TCKT_ISSUED_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.frst_tckt_issued_dt as etl_frst_tckt_issued_dt, \n" +
                        "    d.ticketed_date as test_frst_tckt_issued_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.frst_tckt_issued_dt != d.ticketed_date" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test57,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test57 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 57: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test57");
        System.out.println();

    }

    @Test
    public void test58(){

        // Check the population of the DCMNT_WH.SGND_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SGND_PERSON_WID column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.sgnd_person_wid as etl_sgnd_person_wid, \n" +
                        "    d.sign_ssn as test_sgnd_person_wid\n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.sgnd_person_wid != d.sign_ssn" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test58,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test58 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 58: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test58");
        System.out.println();

    }

    @Test
    public void test59(){

        // Check the population of the DCMNT_WH.AUDIT_FAIL_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.AUDIT_FAIL_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.audit_fail_dt as etl_audit_fail_dt, \n" +
                        "    d.audit_fail_date as test_audit_fail_dt\n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d\n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum\n" +
                        "  and dc.src_ssn = d.u##ssn\n" +
                        "  and dc.src_doctype = d.u##doctype\n" +
                        "  and dc.adjstmt_lvl = d.adj_level\n" +
                        "  and dc.audit_fail_dt is not null\n" +
                        "  and dc.audit_fail_dt != d.audit_fail_date" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test59,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test59 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 59: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test59");
        System.out.println();

    }

    @Test
    public void test60(){

        // Check the population of the DCMNT_WH.CTO_SUBMT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CTO_SUBMT_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.cto_submt_dt as etl_cto_submt_dt, \n" +
                        "    d.cto_submit_date as test_cto_submt_dt\n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d\n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum\n" +
                        "  and dc.src_ssn = d.u##ssn\n" +
                        "  and dc.src_doctype = d.u##doctype\n" +
                        "  and dc.adjstmt_lvl = d.adj_level\n" +
                        "  and dc.cto_submt_dt is not null\n" +
                        "  and dc.cto_submt_dt != d.cto_submit_date" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test60,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test60 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 60: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test60");
        System.out.println();

    }

    @Test
    public void test61(){

        // Check the population of the DCMNT_WH.VCHR_SUBMT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.VCHR_SUBMT_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.vchr_submt_dt as etl_vchr_submt_dt, \n" +
                        "    d.voucher_submitted_date as test_vchr_submt_dt \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.docstat d \n" +
                        "\n" +
                        "  where dc.dcmnt_name = d.u##vchnum \n" +
                        "  and dc.src_ssn = d.u##ssn \n" +
                        "  and dc.src_doctype = d.u##doctype \n" +
                        "  and dc.adjstmt_lvl = d.adj_level \n" +
                        "  and dc.vchr_submt_dt is not null \n" +
                        "  and dc.vchr_submt_dt != d.voucher_submitted_date" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test61,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test61 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 61: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test61");
        System.out.println();

    }

    @Test
    public void test62(){

        // Check the population of the DCMNT_WH.TRIP_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.TRIP_NUM column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.trip_num as etl_trip_num, va.tripnum as test_trip_num \n" +
                        "\n" +
                        "    from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.vchadj va \n" +
                        "\n" +
                        "  where dc.dcmnt_name = va.u##vchnum \n" +
                        "  and dc.src_ssn = va.u##ssn \n" +
                        "  and dc.src_doctype = va.u##doctype \n" +
                        "  and dc.adjstmt_lvl = va.adj_level \n" +
                        "  and dc.trip_num != 0 \n" +
                        "  and dc.trip_num != va.tripnum" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test62,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test62 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 62: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test62");
        System.out.println();

    }

    @Ignore
    @Test
    public void test63(){

        // Check the population of the DCMNT_WH.DCMNT_REF column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_REF column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select ltrim(dc.dcmnt_ref) as etl_dcmnt_ref,\n " +
                        "    ltrim(v.reference) as test_dcmnt_ref \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn \n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and dc.dcmnt_ref != v.reference" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test63,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test63 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 63: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test63");
        System.out.println();

    }

    @Test
    public void test64(){

        // Check the population of the DCMNT_WH.SRC_PROGRESS_RECID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.SRC_PROGRESS_RECID column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.src_progress_recid as etl_src_progress_recid, \n" +
                        "    v.progress_recid as test_src_progress_recid \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn \n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and dc.src_progress_recid != v.progress_recid" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test64,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test64 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 64: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test64");
        System.out.println();

    }

    @Test
    public void test65(){

        // Check the population of the DCMNT_WH.DCMNT_CTGRY column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.DCMNT_CTGRY column";
        String reason = " static value = 'TDY'";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DCMNT_WH";

        String sql2 = "select distinct dcmnt_ctgry, count(*)\n" +
                        "from dtsdm.dcmnt_wh group by dcmnt_ctgry";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test65,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test65 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test65,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test65 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 65: Comparison Count = " + comparisonCount);
        System.out.println("Test 65: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test65");
        System.out.println();

    }

    @Test
    public void test66(){

        // Check the population of the DCMNT_WH.AMENDMENT_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.AMENDMENT_FLAG column";
        String reason = " When TRANS_TYPE = 'AMENDMENT' then 'Y'";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DCMNT_WH";

        String sql2 = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.amendmnt_flag as etl_amendmnt_flag, v.tran_type \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn\n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and v.tran_type = 'AMENDMENT' \n" +
                        "  and dc.amendmnt_flag != 'Y'" +
                        ")";

        String sql3 = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.amendmnt_flag as etl_amendmnt_flag, v.tran_type \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.voucher v \n" +
                        "\n" +
                        "  where dc.dcmnt_name = v.u##vchnum \n" +
                        "  and dc.src_ssn = v.u##ssn\n" +
                        "  and dc.src_doctype = v.u##doctype \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and v.tran_type = 'AMENDMENT' \n" +
                        "  and dc.amendmnt_flag != 'Y'" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int countAmendYes = 0; //comes from DTSDM_SRC_STG.VOUCHER table (load being tested) where amendment flag is 'Y' for yes
        int countAmendNo = 0; //comes from DTSDM_SRC_STG.VOUCHER table (load being tested) where amendment flag is 'N' for no

        System.out.println("Starting DcmntWhTest.test66,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test66 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test66,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countAmendYes = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test66 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test66,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countAmendNo= rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test66 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countAmendYes + countAmendNo == comparisonCount-1),
                "(countAmendYes + countAmendNo == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 66: Amendment Flag 'Yes' Count = " + countAmendYes);
        System.out.println("Test 66: Amendment Flag 'No' Count = " + countAmendNo);
        System.out.println("Test 66: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount-1, countAmendYes + countAmendNo);

        System.out.println("Finish DcmntLocatnWhTest.test66");
        System.out.println();

    }

    @Ignore
    @Test
    public void test67(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test68(){
        //no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test69(){
        //no test right now (no business rules)
    }

    @Test
    public void test70(){

        // Check the population of the DCMNT_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CURR_SW column";
        String reason = " Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not current";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DCMNT_WH";

        String sql2 = "select distinct DCMNT_WH.CURR_SW, count(*)\n" +
                        "from DTSDM.DCMNT_WH group by DCMNT_WH.CURR_SW";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from DTSDM.LERSON_WH and DTSDM_SRC_STG.DOCSTAT table (load being tested)

        System.out.println("Starting DcmntWhTest.test70,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test70 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test70,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test70 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 70: Comparison Count = " + comparisonCount);
        System.out.println("Test 70: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test70");
        System.out.println();

    }

    @Ignore
    @Test
    public void test71(){

    }

    @Ignore
    @Test
    public void test72(){

    }

    @Ignore
    @Test
    public void test73(){

    }

    @Ignore
    @Test
    public void test74(){

    }

    @Ignore
    @Test
    public void test75(){

    }
    
    @Test
    @Ignore
    public void test76(){

        // Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_PRSN_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_PRSN_WID column";
        String reason = " Get value for the most recent status record for the document. Lookup SSN in DTSDM.PERSON_WH";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        wr.printYellowDiv("Column Does Not Exist");

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select dc.curr_dcmnt_status_prsc_wid as etl_curr_dcmnt_status_prsc_wid, \n" +
                        "    p.ssn_full as test_curr_dcmnt_status_prsc_wid \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, dtsdm.person_wh p, DTSDM_SRC_STG.vchstat vs \n" +
                        "\n" +
                        "  where dc.dcmnt_name = vs.u##vchnum \n" +
                        "  and dc.src_ssn = vs.u##ssn \n" +
                        "  and dc.src_doctype = vs.u##doctype \n" +
                        "  and dc.adjstmt_lvl = vs.adj_level \n" +
                        "  and p.ssn_full = vs.u##ssn \n" +
                        "  and dc.curr_dcmnt_status_prsc_wid != p.ssn_full" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test76,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test76 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 76: Comparison Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish DcmntWhTest.test76");
        System.out.println();

    }

    @Test
    /**
     * --DCMNT_WH ROW COUNT Althea
     */
    public void test77() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql = "select src.src_cnt - trgt.trgt_cnt as rcd_cnt_discrepancy\n" +
                "from\n" +
                "    (\n" +
                "        select count(*) src_cnt\n" +
                "        from\n" +
                "            (\n" +
                "                select distinct\n" +
                "                u##vchnum,\n" +
                "                u##doctype,\n" +
                "                u##ssn, \n" +
                "                adj_level\n" +
                "                from dtsdm_src_stg.voucher\n" +
                "                --where u##doctype != 'SAUTH'\n" +
                "            ) \n" +
                "        ) src,\n" +
                "    (\n" +
                "        select count(*) trgt_cnt\n" +
                "        from dtsdm.dcmnt_wh\n" +
                "        where dcmnt_wid != 0\n" +
                "    ) trgt";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int rcd_cnt_discrepancy = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql" );
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        rcd_cnt_discrepancy = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == rcd_cnt_discrepancy),"(0 == rcd_cnt_discrepancy)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test DcmntWh  rcd_cnt_discrepancy == " + rcd_cnt_discrepancy);
        assertEquals(0, rcd_cnt_discrepancy);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test77");
        System.out.println();
    }

}
