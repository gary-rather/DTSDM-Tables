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
public class GovLdgNonuseWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("GovLdgNonuseWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01(){

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting GovLdgNonuseWhTest.test1,sql");
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

        System.out.println("Test GovLdgNonuseWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish GovLdgNonuseWhTest.test1");

    }

    @Test
    public void test02(){

        //Check the population of the unique identifier (GOV_LDG_NONUSE_WH.GOV_LDG_NONUSE_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select distinct A.GOV_LDG_NONUSE_WID, count(*) from DTSDM.GOV_LDG_NONUSE_WH A \n" +
                        "group by A.GOV_LDG_NONUSE_WID having count(*) > 1";

        String sql2 = "select count (distinct A.GOV_LDG_NONUSE_WID) from DTSDM.GOV_LDG_NONUSE_WH A";
        String sql3 = "select count(*) from DTSDM.GOV_LDG_NONUSE_WH";

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

        System.out.println("Starting GovLdgNonuseWhTest.test2,sql1");
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
            System.out.println("GovLdgNonuseWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test2,sql2");
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
            System.out.println("GovLdgNonuseWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test2,sql3");
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
            System.out.println("GovLdgNonuseWh.test2 sql3 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the GOV_LDG_NONUSE_WH.TRIP_LEG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT A.GOV_LDG_NONUSE_WID, A.TRIP_LEG_WID, B.TRIP_LEG_WID \n" +
                        "\n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM.TRIP_LEG_WH B, \n" +
                        "    DTSDM_SRC_STG.LDG_RES_NONUSE C \n" +
                        "\n" +
                        "  WHERE A.TRIP_LEG_WID = B.TRIP_LEG_WID \n" +
                        "  AND B.LEG_NUM = C.LEG \n" +
                        "  AND B.ARRV_LOCATN_NAME = C.ARR_LOCATION \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test3,sql1");
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
            System.out.println("GovLdgNonuseWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test3,sql2");
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
            System.out.println("GovLdgNonuseWh.test3 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test3");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the GOV_LDG_NONUSE_WH.DOD_LDG_CNA_DLS_SYS column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.DOD_LDG_CNA_DLS_SYS, B.DOD_LDG_CNA \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.DOD_LDG_CNA_DLS_SYS = SUBSTR(TRIM(B.DOD_LDG_CNA),3,2) \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test4,sql1");
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
            System.out.println("GovLdgNonuseWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test4,sql2");
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
            System.out.println("GovLdgNonuseWh.test4 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test4");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the GOV_LDG_NONUSE_WH.DOD_LDG_CNA_NBR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.DOD_LDG_CNA_NBR, B.DOD_LDG_CNA \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.DOD_LDG_CNA_NBR = TRIM(B.DOD_LDG_CNA) \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test5,sql1");
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
            System.out.println("GovLdgNonuseWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test5,sql2");
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
            System.out.println("GovLdgNonuseWh.test5 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test5");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the GOV_LDG_NONUSE_WH.DOD_LDG_CNCL_NBR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.DOD_LDG_CNCL_NBR, B.DOD_LDG_CANCEL_NUM \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.DOD_LDG_CNCL_NBR = B.DOD_LDG_CANCEL_NUM \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test6,sql1");
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
            System.out.println("GovLdgNonuseWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test6,sql2");
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
            System.out.println("GovLdgNonuseWh.test6 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test6");
        System.out.println();

    }

    @Test
    public void test07(){

        // Check the population of the GOV_LDG_NONUSE_WH.DOD_LDG_DECLINE_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.DOD_LDG_DECLINE_FLG, B.DOD_LDG_DECLINE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.DOD_LDG_DECLINE_FLG = B.DOD_LDG_DECLINE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test7,sql1");
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
            System.out.println("GovLdgNonuseWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test7,sql2");
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
            System.out.println("GovLdgNonuseWh.test7 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test7");
        System.out.println();

    }

    @Test
    public void test08(){

        // Check the population of the GOV_LDG_NONUSE_WH.DOD_LDG_SRVC_UNAVLBL_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.DOD_LDG_SRVC_UNAVLBL_FLG, \n" +
                        "    B.DOD_LDG_SERVICE_NOT_AVAIL \n" +
                        "\n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.DOD_LDG_DECLINE_FLG = B.DOD_LDG_DECLINE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test8,sql1");
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
            System.out.println("GovLdgNonuseWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test8,sql2");
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
            System.out.println("GovLdgNonuseWh.test8 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test8");
        System.out.println();

    }

    @Test
    public void test09(){

        // Check the population of the GOV_LDG_NONUSE_WH.HAS_DOD_LDG_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.HAS_DOD_LDG_FLG, B.HAS_DOD_LDG_FLG \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.HAS_DOD_LDG_FLG = B.HAS_DOD_LDG \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test9,sql1");
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
            System.out.println("GovLdgNonuseWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test9,sql2");
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
            System.out.println("GovLdgNonuseWh.test9 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test9");
        System.out.println();

    }

    @Test
    public void test10(){

        // Check the population of the GOV_LDG_NONUSE_WH.PREF_LDG_DECLINE_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.PREF_LDG_DECLINE_FLG, B.PREF_LDG_DECLINE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.PREF_LDG_DECLINE_FLG = B.PREF_LDG_DECLINE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test10,sql1");
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
            System.out.println("GovLdgNonuseWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test10,sql2");
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
            System.out.println("GovLdgNonuseWh.test10 sql2 failed");
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

        System.out.println("Finish GovLdgNonuseWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11(){

        // Check the population of the GOV_LDG_NONUSE_WH.PREF_LDG_AVLBL_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.PREF_LDG_AVLBL_FLG, B.PREF_LDG_AVAILABLE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.PREF_LDG_AVLBL_FLG = B.PREF_LDG_AVAILABLE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test11,sql1");
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
            System.out.println("GovLdgNonuseWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test11,sql2");
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
            System.out.println("GovLdgNonuseWh.test11 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 11: Test Count = " + testCount);
        System.out.println("Test 11: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12(){

        // Check the population of the GOV_LDG_NONUSE_WH.HAS_PREF_LDG_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.HAS_PREF_LDG_FLG, B.HAS_PREFERRED_LDG \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.HAS_PREF_LDG_FLG = B.HAS_PREFERRED_LDG \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test12,sql1");
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
            System.out.println("GovLdgNonuseWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test12,sql2");
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
            System.out.println("GovLdgNonuseWh.test12 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 12: Test Count = " + testCount);
        System.out.println("Test 12: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13(){

        // Check the population of the GOV_LDG_NONUSE_WH.PPV_LDG_DECLINE_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.PPV_LDG_DECLINE_FLG, B.PPV_LDG_DECLINE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.PPV_LDG_DECLINE_FLG = B.PPV_LDG_DECLINE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test13,sql1");
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
            System.out.println("GovLdgNonuseWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test13,sql2");
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
            System.out.println("GovLdgNonuseWh.test13 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 13: Test Count = " + testCount);
        System.out.println("Test 13: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14(){

        // Check the population of the GOV_LDG_NONUSE_WH.PPV_LDG_AVAIL_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.PPV_LDG_AVAIL_FLG, B.PPV_LDG_DECLINE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.PPV_LDG_AVAIL_FLG = B.PPV_LDG_DECLINE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test14,sql1");
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
            System.out.println("GovLdgNonuseWh.test14 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test14,sql2");
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
            System.out.println("GovLdgNonuseWh.test14 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 14: Test Count = " + testCount);
        System.out.println("Test 14: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15(){

        // Check the population of the GOV_LDG_NONUSE_WH.HAS_PPV_LDG_FLG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.HAS_PPV_LDG_FLG, B.PPV_LDG_AVAILABLE \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.HAS_PPV_LDG_FLG = B.PPV_LDG_AVAILABLE \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test15,sql1");
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
            System.out.println("GovLdgNonuseWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test15,sql2");
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
            System.out.println("GovLdgNonuseWh.test15 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 15: Test Count = " + testCount);
        System.out.println("Test 15: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16(){

        // Check the population of the GOV_LDG_NONUSE_WH.SRC_LDG_RES_NONUSE_ID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.SRC_LDG_RES_NONUSE_ID, B.LDG_RES_NONUSE_ID \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.SRC_LDG_RES_NONUSE_ID = B.LDG_RES_NONUSE_ID \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test16,sql1");
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
            System.out.println("GovLdgNonuseWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test16,sql2");
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
            System.out.println("GovLdgNonuseWh.test16 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 16: Test Count = " + testCount);
        System.out.println("Test 16: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17(){

        // Check the population of the GOV_LDG_NONUSE_WH.SRC_ARR_LOCATION column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.SRC_ARR_LOCATION, B.ARR_LOCATION \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM_SRC_STG.LDG_RES_NONUSE B \n" +
                        "  WHERE A.SRC_ARR_LOCATION = B.ARR_LOCATION \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test17,sql1");
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
            System.out.println("GovLdgNonuseWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test17,sql2");
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
            System.out.println("GovLdgNonuseWh.test17 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 17: Test Count = " + testCount);
        System.out.println("Test 17: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the GOV_LDG_NONUSE_WH.LOCATN_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select * from DTSDM.GOV_LDG_NONUSE_WH A where A.GOV_LDG_NONUSE_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.GOV_LDG_NONUSE_WID, A.LOCATN_WID, B.LOCATN_WID \n" +
                        "  FROM DTSDM.GOV_LDG_NONUSE_WH A, DTSDM.LOCATN_WH B, DTSDM_SRC_STG.LDG_RES_NONUSE C \n" +
                        "  WHERE A.LOCATN_WID = B.LOCATN_WID \n" +
                        "  AND B.LOCATN_NAME = C.ARR_LOCATION \n" +
                        "  AND C.ARR_LOCATION = A.SRC_ARR_LOCATION \n" +
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

        System.out.println("Starting GovLdgNonuseWhTest.test18,sql1");
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
            System.out.println("GovLdgNonuseWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting GovLdgNonuseWhTest.test18,sql2");
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
            System.out.println("GovLdgNonuseWh.test18 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 18: Test Count = " + testCount);
        System.out.println("Test 18: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish GovLdgNonuseWhTest.test18");
        System.out.println();

    }

}
