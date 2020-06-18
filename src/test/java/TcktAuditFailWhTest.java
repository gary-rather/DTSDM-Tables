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
public class TcktAuditFailWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("TcktAuditFailWhTest.html");
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


        String sql = "SELECT * FROM DTSDM.TCKT_AUDIT_FAIL_WH WHERE TCKT_AUDIT_FAIL_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting TcktAuditFailWhTest.test1,sql");
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

        System.out.println("Test TcktAuditFailWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish TcktAuditFailWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (TCKT_AUDIT_FAIL_WH.TCKT_AUDIT_FAIL_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "SELECT TCKT_AUDIT_FAIL_WID FROM DTSDM.TCKT_AUDIT_FAIL_WH \n" +
                        "GROUP BY TCKT_AUDIT_FAIL_WID HAVING COUNT (*) > 1";

        String sql2 = "select count (distinct TCKT_AUDIT_FAIL_WH.TCKT_AUDIT_FAIL_WID) \n" +
                        "from DTSDM.TCKT_AUDIT_FAIL_WH";

        String sql3 = "select count(*) from DTSDM.TCKT_AUDIT_FAIL_WH";

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

        System.out.println("Starting TcktAuditFailWhTest.test2,sql1");
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
            System.out.println("TcktAuditFailWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TcktAuditFailWhTest.test2,sql2");
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
            System.out.println("TcktAuditFailWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TcktAuditFailWhTest.test2,sql3");
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
            System.out.println("TcktAuditFailWh.test2 sql3 failed");
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

        System.out.println("Finish TcktAuditFailWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03() {

        // Check the population of the TCKT_AUDIT_FAIL_WH.TICKET_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TCKT_AUDIT_FAIL_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TCKT_AUDIT_FAIL_WID, \n" +
                        "\t\t\t A.TICKET_WID AS TEST_TICKET_WID, \n" +
                        "\t\t\t B.TICKET_WID AS ETL_TICKET_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TCKT_AUDIT_FAIL_WH A, \n" +
                        "\t\t\t DTSDM.TICKET_WH B, DTSDM_SRC_STG.AUDITDTL C \n" +
                        "\n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\n" +
                        "\t ORDER BY A.TCKT_AUDIT_FAIL_WID \n" +
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

        System.out.println("Starting TcktAuditFailWhTest.test03,sql1");
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
            System.out.println("TcktAuditFailWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TcktAuditFailWhTest.test03,sql2");
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
            System.out.println("TcktAuditFailWh.test03 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 03: Comparison Count = " + comparisonCount);
        System.out.println("Test 03: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TcktAuditFailWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04() {

        // Check the population of the TCKT_AUDIT_FAIL_WH.REASON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TCKT_AUDIT_FAIL_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TCKT_AUDIT_FAIL_WID, \n" +
                        "\t\t\t A.REASON_WID AS TEST_REASON_WID, \n" +
                        "\t\t\t B.REASON_WID AS ETL_REASON_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TCKT_AUDIT_FAIL_WH A, \n" +
                        "\t\t\t DTSDM.REASON_RFRNC_WH B, DTSDM_SRC_STG.AUDITDTL C \n" +
                        "\n" +
                        "\t WHERE B.REASON_WID = A.REASON_WID \n" +
                        "\t AND A.JUSTIFCTN_TXT = C.JUSTIFY \n" +
                        "\n" +
                        "\t ORDER BY A.TCKT_AUDIT_FAIL_WID \n" +
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

        System.out.println("Starting TcktAuditFailWhTest.test04,sql1");
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
            System.out.println("TcktAuditFailWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TcktAuditFailWhTest.test04,sql2");
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
            System.out.println("TcktAuditFailWh.test04 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 04: Comparison Count = " + comparisonCount);
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TcktAuditFailWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05() {

        // Check the population of the TCKT_AUDIT_FAIL_WH.JUSTIFCTN_TXT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TCKT_AUDIT_FAIL_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TCKT_AUDIT_FAIL_WID, \n" +
                        "\t\t\t A.JUSTIFCTN_TXT AS TEST_JUSTIFY, \n" +
                        "\t\t\t C.JUSTIFY AS ETL_JUSTIFY \n" +
                        "\n" +
                        "\t FROM DTSDM.TCKT_AUDIT_FAIL_WH A, \n" +
                        "\t\t\t DTSDM.REASON_RFRNC_WH B, DTSDM_SRC_STG.AUDITDTL C \n" +
                        "\n" +
                        "\t WHERE B.REASON_WID = A.REASON_WID \n" +
                        "\t AND A.JUSTIFCTN_TXT = C.JUSTIFY \n" +
                        "\n" +
                        "\t ORDER BY A.TCKT_AUDIT_FAIL_WID \n" +
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

        System.out.println("Starting TcktAuditFailWhTest.test05,sql1");
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
            System.out.println("TcktAuditFailWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TcktAuditFailWhTest.test05,sql2");
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
            System.out.println("TcktAuditFailWh.test05 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 05: Comparison Count = " + comparisonCount);
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TcktAuditFailWhTest.test05");
        System.out.println();

    }

}
