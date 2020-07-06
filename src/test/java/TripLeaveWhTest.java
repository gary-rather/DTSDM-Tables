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
public class TripLeaveWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("TripLeaveWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
                String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "SELECT * FROM DTSDM.TRIP_LEAVE_WH WHERE TRIP_LEAVE_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting TripLeaveWhTest.test1,sql");
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
        ResultObject ro = new ResultObject((number == 1), "(number == 1)");
        roList.add(ro);
        wr.logTestResults(roList);

        System.out.println("Test TripLeaveWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish TripLeaveWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (TRIP_LEAVE_WH.TRIP_LEAVE_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
                String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "SELECT TRIP_LEAVE_WID FROM DTSDM.TRIP_LEAVE_WH \n" +
                "GROUP BY TRIP_LEAVE_WID HAVING COUNT(*) > 1";

        String sql2 = "select count (distinct TRIP_LEAVE_WH.TRIP_LEAVE_WID) from DTSDM.TRIP_LEAVE_WH";
        String sql3 = "select count(*) from DTSDM.TRIP_LEAVE_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int count = 0;
        int distinctCount = 0;
        int duplicateCount = 0;

        System.out.println("Starting TripLeaveWhTest.test2,sql1");
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
            System.out.println("TripLeaveWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveWhTest.test2,sql2");
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
            System.out.println("TripLeaveWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveWhTest.test2,sql3");
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
            System.out.println("TripLeaveWh.test2 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((distinctCount == count), "(distinctCount == count)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0), "(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 2: Count = " + count);
        System.out.println("Test 2: Distinct Count = " + distinctCount);
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        assertEquals(count, distinctCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish TripLeaveWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03() {

        // Check the population of the TRIP_LEAVE_WH.TRIP_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TRIP_LEAVE_WH";

        String sql2 = "select count(*) from" +
                "( \n" +
                "  SELECT DISTINCT A.TRIP_LEAVE_WID, \n" +
                "    A.TRIP_WID AS TEST_TRIP_WID, \n" +
                "    B.TRIP_WID AS ETL_TRIP_WID \n" +
                "\n" +
                "  FROM DTSDM.TRIP_LEAVE_WH A, \n" +
                "    DTSDM.DCMNT_WH B, DTSDM_SRC_STG.TLEAVE C \n" +
                "\n" +
                "  WHERE A.TRIP_WID = B.TRIP_WID \n" +
                "  AND B.DCMNT_NAME = C.U##VCHNUM \n" +
                "  AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                "  AND B.SRC_SSN = C.U##SSN \n" +
                "  AND B.ADJSTMT_LVL = C.ADJ_LEVEL \n" +
                "  ORDER BY A.TRIP_LEAVE_WID" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test03,sql1");
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
            System.out.println("TripLeaveWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TripLeaveWh.test03 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 03: Comparison Count = " + comparisonCount);
        System.out.println("Test 03: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish TripLeaveWhTest.test03");
        System.out.println();

    }

    @Ignore
    @Test
    public void test04() {

        // Check the population of the TRIP_LEAVE_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TRIP_LEAVE_WH";

        String sql2 = "select count(*) from" +
                "( \n" +
                "  SELECT DISTINCT A.TRIP_LEAVE_WID, \n" +
                "    B.DCMNT_WID AS ETL_DCMNT_WID \n" +
                "\n" +
                "  FROM DTSDM.TRIP_LEAVE_WH A, \n" +
                "    DTSDM.DCMNT_WH B, DTSDM_SRC_STG.TLEAVE C \n" +
                "\n" +
                "  WHERE A.TRIP_WID = B.TRIP_WID \n" +
                "  AND B.DCMNT_NAME = C.U##VCHNUM \n" +
                "  AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                "  AND B.SRC_SSN = C.U##SSN \n" +
                "  AND B.ADJSTMT_LVL = C.ADJ_LEVEL \n" +
                "  ORDER BY A.TRIP_LEAVE_WID" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test04,sql1");
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
            System.out.println("TripLeaveWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveTest.test04,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TripLeaveWh.test04 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 04: Comparison Count = " + comparisonCount);
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish TripLeaveWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05() {

        // Check the population of the TRIP_LEAVE_WH.TRIP_LEG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TRIP_LEAVE_WH";

        String sql2 = "select count(*) from" +
                "( \n" +
                "  SELECT DISTINCT A.TRIP_LEAVE_WID, \n" +
                "    A.TRIP_LEG_WID AS TEST_TRIP_LEG_WID, \n" +
                "    B.TRIP_LEG_WID AS ETL_TRIP_LEG_WID \n" +
                "\n" +
                "  FROM DTSDM.TRIP_LEAVE_WH A, DTSDM.TRIP_LEG_WH B, DTSDM_SRC_STG.TLEAVE C \n" +
                "\n" +
                "  WHERE A.TRIP_LEG_WID = B.TRIP_LEG_WID \n" +
                "  AND C.LVDATE BETWEEN B.LEG_DPRT_DT AND B.LEG_ARRV_DT \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test05,sql1");
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
            System.out.println("TripLeaveWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveTest.test05,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TripLeaveWh.test05 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 05: Comparison Count = " + comparisonCount);
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TripLeaveWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06() {

        // Check the population of the TRIP_LEAVE_WH.LV_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TRIP_LEAVE_WH";

        String sql2 = "select count(*) from" +
                "( \n" +
                "  SELECT DISTINCT A.TRIP_LEAVE_WID, \n" +
                "    A.LV_TYPE_WID AS TEST_LV_TYPE_WID, \n" +
                "    B.TYPE_WID AS ETL_LV_TYPE_WID \n" +
                "\n" +
                "  FROM DTSDM.TRIP_LEAVE_WH A, \n" +
                "    DTSDM.TYPE_CONSOLDTD_RFRNC_WH B, DTSDM_SRC_STG.TLEAVE C \n" +
                "  WHERE A.LV_TYPE_WID = B.TYPE_WID \n" +
                "  AND B.TYPE_CD = C.LVTYPE \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test06,sql1");
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
            System.out.println("TripLeaveWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TripLeaveTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TripLeaveWh.test06 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 06: Comparison Count = " + comparisonCount);
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TripLeaveWhTest.test06");
        System.out.println();

    }

    @Ignore
    @Test
    public void test07(){
        // no test right now (column does not exist)
    }

    @Ignore
    @Test
    public void test08(){
        // no test right now (column does not exist)
    }

    @Test
    public void test09() {

        // Check the population of the TRIP_LEAVE_WH.LV_STRT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                "( \n" +
                "SELECT DISTINCT A.TRIP_LEAVE_WID, A.LV_STRT_DT, B.LVDATE \n" +
                "\n" +
                "FROM DTSDM.TRIP_LEAVE_WH A, DTSDM_SRC_STG.TLEAVE B, DTSDM_SRC_STG.TLEAVE C, DTSDM_SRC_STG.STATE D \n" +
                "\n" +
                "WHERE A.LV_STRT_DT != B.LVDATE \n" +
                "AND A.LV_END_DT = C.LVDATE \n" +
                "AND A.OCONUS_FLAG = D.CONUS \n" +
                "AND A.TOT_LV_HOURS = B.LVHOUR \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test09,sql");
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
            System.out.println("ExtSystemWh.test09 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 09: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TripLeaveWhTest.test09");
        System.out.println();

    }

    @Test
    public void test10() {

        // Check the population of the TRIP_LEAVE_WH.LV_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                "( \n" +
                "SELECT DISTINCT A.TRIP_LEAVE_WID, A.LV_END_DT, C.LVDATE \n" +
                "\n" +
                "FROM DTSDM.TRIP_LEAVE_WH A, DTSDM_SRC_STG.TLEAVE B, DTSDM_SRC_STG.TLEAVE C, DTSDM_SRC_STG.STATE D \n" +
                "\n" +
                "WHERE A.LV_STRT_DT = B.LVDATE \n" +
                "AND A.LV_END_DT != C.LVDATE \n" +
                "AND A.OCONUS_FLAG = D.CONUS \n" +
                "AND A.TOT_LV_HOURS = B.LVHOUR \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test10,sql");
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
            System.out.println("ExtSystemWh.test10 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 10: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TripLeaveWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11() {

        // Check the population of the TRIP_LEAVE_WH.OCONUS_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                "( \n" +
                "SELECT DISTINCT A.TRIP_LEAVE_WID, A.OCONUS_FLAG, D.CONUS \n" +
                "\n" +
                "FROM DTSDM.TRIP_LEAVE_WH A, DTSDM_SRC_STG.TLEAVE B, DTSDM_SRC_STG.TLEAVE C, DTSDM_SRC_STG.STATE D \n" +
                "\n" +
                "WHERE A.LV_STRT_DT = B.LVDATE \n" +
                "AND A.LV_END_DT = C.LVDATE \n" +
                "AND A.OCONUS_FLAG != D.CONUS \n" +
                "AND A.TOT_LV_HOURS = B.LVHOUR \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test11,sql");
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
            System.out.println("ExtSystemWh.test11 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 11: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TripLeaveWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12() {

        // Check the population of the TRIP_LEAVE_WH.TOT_LEAVE_HOURS column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                "( \n" +
                "SELECT DISTINCT A.TRIP_LEAVE_WID, A.TOT_LV_HOURS, B.LVHOUR \n" +
                "\n" +
                "FROM DTSDM.TRIP_LEAVE_WH A, DTSDM_SRC_STG.TLEAVE B, DTSDM_SRC_STG.TLEAVE C, DTSDM_SRC_STG.STATE D \n" +
                "\n" +
                "WHERE A.LV_STRT_DT != B.LVDATE \n" +
                "AND A.LV_END_DT = C.LVDATE \n" +
                "AND A.OCONUS_FLAG = D.CONUS \n" +
                "AND A.TOT_LV_HOURS = B.LVHOUR \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TripLeaveWhTest.test12,sql");
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
            System.out.println("ExtSystemWh.test12 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 12: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TripLeaveWhTest.test12");
        System.out.println();

    }

}
