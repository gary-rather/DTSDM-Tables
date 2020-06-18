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
public class ExtSystemWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("ExtSystemWhTest.html");
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


        String sql = "SELECT * FROM DTSDM.EXT_SYSTEM_WH WHERE EXT_SYSTEM_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting ExtSystemWhTest.test1,sql");
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

        System.out.println("Test ExtSystemWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish ExtSystemWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (EXT_SYSTEM_WH.EXT_SYSTEM_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "SELECT EXT_SYSTEM_WID FROM DTSDM.EXT_SYSTEM_WH \n" +
                "GROUP BY EXT_SYSTEM_WID HAVING COUNT(*) > 1";

        String sql2 = "select count (distinct EXT_SYSTEM_WH.EXT_SYSTEM_WID) from DTSDM.EXT_SYSTEM_WH";
        String sql3 = "select count(*) from DTSDM.EXT_SYSTEM_WH";

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

        System.out.println("Starting ExtSystemWhTest.test2,sql1");
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
            System.out.println("ExtSystemWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExtSystemWhTest.test2,sql2");
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
            System.out.println("ExtSystemWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExtSystemWhTest.test2,sql3");
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
            System.out.println("ExtSystemWh.test2 sql3 failed");
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

        System.out.println("Finish ExtSystemWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03() {

        // Check the population of the EXT_SYSTEM_WH.EXT_SYSTEM_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.EXT_SYSTEM_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.EXT_SYSTEM_WID, A.EXT_SYSTEM_CD, \n" +
                        "\t\t\t B.U##PARTNER_SYSTEM_CODE, C.ERP_SYSTEM_CD \n" +
                        "\n" +
                        "\t FROM DTSDM.EXT_SYSTEM_WH A, DTSDM_SRC_STG.PARTNER_SYSTEM B, \n" +
                        "\t\t\t DTSDM_SRC_STG.DM_ERP_SYSTEM C \n" +
                        "\n" +
                        "\t WHERE A.EXT_SYSTEM_CD = B.U##PARTNER_SYSTEM_CODE\n" +
                        "\t AND A.EXT_SYSTEM_CD = C.ERP_SYSTEM_CD" +
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

        System.out.println("Starting ExtSystemWhTest.test03,sql1");
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
            System.out.println("ExtSystemWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExtSystemTest.test03,sql2");
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
            System.out.println("ExtSystemWh.test03 sql2 failed");
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

        System.out.println("Finish ExtSystemWhTest.test03");
        System.out.println();

    }

    @Ignore
    @Test
    public void test04(){
        // no test right now (no business rules)
    }

    @Test
    public void test05() {

        // Check the population of the EXT_SYSTEM_WH.EXT_SYSTEM_TYPE_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "SELECT B.EXT_SYSTEM_WID, B.EXT_SYSTEM_TYPE_CD, A.TYPE_CD \n" +
                        "\n" +
                        "FROM DTSDM.TYPE_CONSOLDTD_RFRNC_WH A, \n" +
                        "\t\t\t DTSDM.EXT_SYSTEM_WH B, DTSDM_SRC_STG.PARTNER_SYSTEM C \n" +
                        "\n" +
                        "WHERE C.U##PARTNER_SYSTEM_CODE = B.EXT_SYSTEM_CD \n" +
                        "AND B.EXT_SYSTEM_TYPE_CD != A.TYPE_CD \n" +
                        "AND B.EXT_SYSTEM_TYPE_DESCR = A.TYPE_DESCR \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExtSystemWhTest.test05,sql");
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
            System.out.println("ExtSystemWh.test05 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExtSystemWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06() {

        // Check the population of the EXT_SYSTEM_WH.EXT_SYSTEM_TYPE_DESCR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "SELECT B.EXT_SYSTEM_WID, B.EXT_SYSTEM_TYPE_CD, A.TYPE_CD \n" +
                        "\n" +
                        "FROM DTSDM.TYPE_CONSOLDTD_RFRNC_WH A, \n" +
                        "\t\t\t DTSDM.EXT_SYSTEM_WH B, DTSDM_SRC_STG.PARTNER_SYSTEM C \n" +
                        "\n" +
                        "WHERE C.U##PARTNER_SYSTEM_CODE = B.EXT_SYSTEM_CD \n" +
                        "AND B.EXT_SYSTEM_TYPE_CD = A.TYPE_CD \n" +
                        "AND B.EXT_SYSTEM_TYPE_DESCR ! = A.TYPE_DESCR \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting ExtSystemWhTest.test06,sql");
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
            System.out.println("ExtSystemWh.test06 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish ExtSystemWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07() {

        // Check the population of the EXT_SYSTEM_WH.SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.EXT_SYSTEM_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.EXT_SYSTEM_WID, A.SUBORG_WID, B.SUBORG_WID \n" +
                        "\t FROM DTSDM.EXT_SYSTEM_WH A, DTSDM.SUBORG_WH B, DTSDM_SRC_STG.PARTNER_SYSTEM C \n" +
                        "\t WHERE A.SUBORG_WID = B.SUBORG_WID \n" +
                        "\t AND B.FULL_ORG_CD = C.DTS_ORG \n" +
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

        System.out.println("Starting ExtSystemWhTest.test07,sql1");
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
            System.out.println("ExtSystemWh.test07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExtSystemTest.test07,sql2");
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
            System.out.println("ExtSystemWh.test07 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 07: Comparison Count = " + comparisonCount);
        System.out.println("Test 07: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExtSystemWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08() {

        // Check the population of the EXT_SYSTEM_WH.SRC_ID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.EXT_SYSTEM_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.EXT_SYSTEM_WID, A.SRC_ID, B.ID, C.ERP_SYSTEM_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.EXT_SYSTEM_WH A, DTSDM_SRC_STG.PARTNER_SYSTEM B, \n" +
                        "\t\t\t DTSDM_SRC_STG.DM_ERP_SYSTEM C \n" +
                        "\n " +
                        "\t WHERE A.SRC_ID = B.ID \n" +
                        "\t AND B.ID = C.ERP_SYSTEM_WID \n" +
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

        System.out.println("Starting ExtSystemWhTest.test08,sql1");
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
            System.out.println("ExtSystemWh.test08 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExtSystemTest.test08,sql2");
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
            System.out.println("ExtSystemWh.test08 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 08: Comparison Count = " + comparisonCount);
        System.out.println("Test 08: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExtSystemWhTest.test08");
        System.out.println();

    }

}
