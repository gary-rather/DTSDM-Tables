import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DcmntStatusHistWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("DcmntStatusHistWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select * from DTSDM.DCMNT_STATUS_HIST_WH A where A.DCMNT_STATUS_HIST_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test1,sql");
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

        System.out.println("Test DcmntStatusHistWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish DcmntStatusHistWhTest.test1");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (DCMNT_STATUS_HIST_WH.DCMNT_STATUS_HIST_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.DCMNT_STATUS_HIST_WID, count(*) from DTSDM.DCMNT_STATUS_HIST_WH A \n" +
                        "group by A.DCMNT_STATUS_HIST_WID having count(*) > 1";

        String sql2 = "select count (distinct A.DCMNT_STATUS_HIST_WID) from DTSDM.DCMNT_STATUS_HIST_WH A";
        String sql3 = "select count(*) from DTSDM.HTL_TAX_EXMPT_WH";

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

        System.out.println("Starting DcmntStatusHistWhTest.test2,sql1");
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
            System.out.println("DcmntStatusHistWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test2,sql2");
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
            System.out.println("DcmntStatusHistWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test2,sql3");
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
            System.out.println("DcmntStatusHistWh.test2 sql3 failed");
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

        System.out.println("Finish DcmntStatusHistWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        // Check the population of the DCMNT_STATUS_HIST_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.DCMNT_WID, B.DCMNT_WID \n" +
                        "  FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM.DCMNT_WH B \n" +
                        "  WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test3,sql1");
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
            System.out.println("DcmntStatusHistWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test3 sql2 failed");
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

        System.out.println("Finish DcmntStatusHistWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        " SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_WID, B.STATUS_WID\n" +
                        "\n" +
                        "FROM DTSDM.DCMNT_STATUS_HIST_WH A,\n" +
                        " DTSDM.STATUS_CONSOLDTD_RFRNC_WH B,\n" +
                        " DTSDM_SRC_STG.VCHSTAT C\n" +
                        "\n" +
                        " WHERE A.STATUS_WID = B.STATUS_WID\n" +
                        " AND B.STATUS_DESCR = C.U##VSTAT \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test4,sql1");
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
            System.out.println("DcmntStatusHistWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test4 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 4: Test Count = " + testCount);
        System.out.println("Test 4: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_DT, B.VDATE \n" +
                        " FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        "WHERE A.STATUS_DT = B.VDATE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test5,sql1");
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
            System.out.println("DcmntStatusHistWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test5,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test5 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 5: Test Count = " + testCount);
        System.out.println("Test 5: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_TM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        " SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_TIME, B.VTIME \n" +
                        " FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        " WHERE A.STATUS_TIME = B.VTIME \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test6,sql1");
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
            System.out.println("DcmntStatusHistWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test6 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 6: Test Count = " + testCount);
        System.out.println("Test 6: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_UPDTD_BY_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_UPDTD_BY_NAME, B.NAME \n" +
                        "  FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        "  WHERE A.STATUS_UPDTD_BY_NAME = B.NAME \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test7,sql1");
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
            System.out.println("DcmntStatusHistWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test7,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test7 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 7: Test Count = " + testCount);
        System.out.println("Test 7: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test7");
        System.out.println();

    }

    @Test
    public void test8(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_RMRKS column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_RMRKS, B.REMARKS \n" +
                        "  FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        "  WHERE A.STATUS_RMRKS = B.REMARKS \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test8,sql1");
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
            System.out.println("DcmntStatusHistWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test8 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 8: Test Count = " + testCount);
        System.out.println("Test 8: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test8");
        System.out.println();

    }

    @Test
    public void test9(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_SIGNTR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_SIGNTR, B.U##SIGNATURE \n" +
                        "  FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        "  WHERE A.STATUS_SIGNTR = B.U##SIGNATURE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test9,sql1");
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
            System.out.println("DcmntStatusHistWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test9 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 9: Test Count = " + testCount);
        System.out.println("Test 9: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test9");
        System.out.println();

    }

    @Test
    public void test10(){

        // Check the population of the DCMNT_STATUS_HIST_WH.STATUS_SEQNC_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_STATUS_HIST_WH A WHERE A.DCMNT_STATUS_HIST_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.DCMNT_STATUS_HIST_WID, A.STATUS_SEQNC_NUM, B.SEQUENCE \n" +
                        "  FROM DTSDM.DCMNT_STATUS_HIST_WH A, DTSDM_SRC_STG.VCHSTAT B \n" +
                        "  WHERE A.STATUS_SEQNC_NUM = B.SEQUENCE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntStatusHistWhTest.test10,sql1");
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
            System.out.println("DcmntStatusHistWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntStatusHistWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntStatusHistWh.test10 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 10: Test Count = " + testCount);
        System.out.println("Test 10: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish DcmntStatusHistWhTest.test10");
        System.out.println();

    }

}
