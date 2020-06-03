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
        wr = new WriteResults("TripLegWhTest.html");
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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct DCMNT_WH.DCMNT_WID, count(*) from DTSDM. DCMNT_WH\n" +
                        "group by DCMNT_WH.DCMNT_WID having count(*) > 1";

        String sql2 = "select count (distinct DCMNT_WH.DCMNT_WID) from DTSDM. DCMNT_WH";
        String sql3 = "select count(*) from DTSDM.DCMNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

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

        System.out.println("Starting DcmntWhTest.test2,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test2,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test2 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((distinctCount == count),"(distinctCount == count)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0),"(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 2: Count = " + count);
        System.out.println("Test 2: Distinct Count = " + distinctCount);
        assertEquals(count, distinctCount);

        assertEquals(0, duplicateCount);
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        System.out.println("Finish DcmntLocatnWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the overall population of the DCMNT_WH table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.DCMNT_WH";

        String sql2 = "select sum(table_row_cnt) from \n" +
                        "(select distinct U##VCHNUM, ADJ_LEVEL, count(*) table_row_cnt \n" +
                        "from FRED.VOUCHER group by U##VCHNUM, ADJ_LEVEL)";

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct TRIP_WID,count(*) from DTSDM.DCMNT_WH group by TRIP_WID";
        String sql2 = "select distinct TRIP_WID,count(*) from DTSDM.TRIP_WH group by TRIP_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct PERSON_WID from DTSDM.DCMNT_WH";
        String sql2 = "select distinct PERSON_WID from DTSDM.PERSON_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from PERSON_WH (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "Select distinct PERSON_ATTRIB_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_ATTRIB_WID, count(*) table_row_cnt\n" +
                        "from DTSDM.PERSON_ATTRIB_HIST_WH a, FRED.VOUCHER b, DTSDM.PERSON_WH c\n" +
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
        int testCount = 0; //comes from PERSON_ATTRIB_WH, PERSON_WH, and FRED.VOUCHER table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct DCMNT_TYPE_WID, count(*) table_row_cnt \n" +
                        "from DTSDM.DCMNT_WH group by DCMNT_TYPE_WID";

        String sql2 = "select distinct a.TYPE_WID\n" +
                        "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH a, FRED.DOCSTAT b \n" +
                        "where a.TYPE_CD = b.U##DOCTYPE";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from TYPE_CONSOLODTD_RFRNC_WH and FRED.DOCSTAT table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct AO_PERSON_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_WID \n" +
                        "from DTSDM.PERSON_WH a, FRED.DOCSTAT b \n" +
                        "where a.SSN_FULL = b.APPROVE_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from PERSON_WH and FRED.DOCSTAT table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct DELEGT_PERSON_WID from DTSDM.DCMNT_WH";

        String sql2 = "select distinct a.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH a, FRED.VCHSTAT b\n" +
                        "where a.SSN_FULL = b.DELEGATE_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from PERSON_WH and FRED.VCHSTAT table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct nvl(NXT_RTNG_OFFCL_WID,0) from DTSDM.DCMNT_WH";

        String sql2 = "select distinct p.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH p, FRED.VCHSTAT vs\n" +
                        "where p.SSN_FULL = vs.U##NEXT_SSN";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from PERSON_WH and FRED.VCHSTAT table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct nvl(AGNCY_WID,0), count(*) from DTSDM.DCMNT_WH group by AGNCY_WID";

        String sql2 = "select distinct ao.AGNCY_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH so, FRED.VOUCHER v, DTSDM.AGNCY_ORG_WH ao\n" +
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
        int testCount = 0; //comes from SUBORG_WH, AGNCY_ORG_WH and FRED.VOUCHER table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct SUBORG_WID, count(*) from DTSDM.DCMNT_WH group by SUBORG_WID";

        String sql2 = "select distinct so.SUBORG_WID \n" +
                        "from DTSDM.SUBORG_WH so, FRED.VOUCHER v \n" +
                        "where v.U##ORG = so.FULL_ORG_CD";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from SUBORG_WH and FRED.VOUCHER table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct CURR_DCMNNT_STATUS_WID, count(*) \n" +
                        "from DTSDM.DCMNT_WH group by CURR_DCMNNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, FRED.DOCSTAT d \n" +
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
        int testCount = 0; //comes from STATUS_CONSOLDTD_RFRNC_WH and FRED.DOCSTAT table (load being tested)

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

    @Ignore
    @Test
    public void test14(){

    }

    @Test
    public void test15(){

        // Check the population of the DCMNT_WH.NXT_DCMNT_STATUS_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct NXT_DCMNT_STATUS_WID, count(*) from DTSDM.DCMNT_WH group by NXT_DCMNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, FRED.DOCSTAT d\n" +
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
        int testCount = 0; //comes from STATUS_CONSOLDTD_RFRNC_WH and FRED.DOCSTAT table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct DCMNT_NAME, count(*) from DTSDM.DCMNT_WH group by DCMNT_NAME";
        String sql2 = "select distinct VCHNUM, count(*) from FRED.VOUCHER group by VCHNUM";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from FRED.VOUCHER table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME), count(*)\n" +
                        "from DTSDM.DCMNT_WH group by NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME)";

        String sql2 = "select distinct NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1), VCHNUM), count(*)\n" +
                        "from FRED.VOUCHER group by NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1),VCHNUM)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from FRED.VOUCHER table (load being tested)

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
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ADJSTMT_LVL, count(*) from DTSDM.DCMNT_WH\n" +
                        "group by ADJSTMT_LVL order by ADJSTMT_LVL";

        String sql2 = "select distinct ADJ_LEVEL, count(*) from FRED.VOUCHER\n" +
                        "group by ADJ_LEVEL order by ADJ_LEVEL";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int testCount = 0; //comes from FRED.VOUCHER table (load being tested)

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

    @Ignore
    @Test
    public void test23(){

    }

    @Ignore
    @Test
    public void test24(){

    }

    @Ignore
    @Test
    public void test25(){

    }

    @Ignore
    @Test
    public void test26(){

    }

    @Ignore
    @Test
    public void test27(){

    }

    @Ignore
    @Test
    public void test28(){

    }

    @Ignore
    @Test
    public void test29(){

    }

    @Ignore
    @Test
    public void test30(){

    }

    @Ignore
    @Test
    public void test31(){

    }

    @Ignore
    @Test
    public void test32(){

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

    @Ignore
    @Test
    public void test41(){

    }

    @Ignore
    @Test
    public void test42(){

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

    @Ignore
    @Test
    public void test48(){

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

    @Ignore
    @Test
    public void test54(){

    }

    @Ignore
    @Test
    public void test55(){

    }

    @Ignore
    @Test
    public void test56(){

    }

    @Ignore
    @Test
    public void test57(){

    }

    @Ignore
    @Test
    public void test58(){

    }

    @Ignore
    @Test
    public void test59(){

    }

    @Ignore
    @Test
    public void test60(){

    }

    @Ignore
    @Test
    public void test61(){

    }

    @Ignore
    @Test
    public void test62(){

    }

    @Ignore
    @Test
    public void test63(){

    }

    @Ignore
    @Test
    public void test64(){

    }

    @Ignore
    @Test
    public void test65(){

    }

    @Ignore
    @Test
    public void test66(){

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

    @Ignore
    @Test
    public void test70(){

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

    @Ignore
    @Test
    public void test76(){

    }

}
