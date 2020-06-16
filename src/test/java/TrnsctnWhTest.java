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
public class TrnsctnWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("TrnsctnWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "SELECT * FROM DTSDM.TRNSCTN_WH WHERE TRNSCTN_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting TrnsctnWhTest.test1,sql");
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

        System.out.println("Test TrnsctnWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish TrnsctnWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (TRNSCTN_WH.TRNSCTN_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "SELECT TRNSCTN_WID FROM DTSDM.TRNSCTN_WH GROUP BY TRNSCTN_WID HAVING COUNT(*) > 1";
        String sql2 = "select count (distinct TRNSCTN_WH.TRNSCTN_WID) from DTSDM.TRNSCTN_WH";
        String sql3 = "select count(*) from DTSDM.TRNSCTN_WH";

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

        System.out.println("Starting TrnsctnWhTest.test2,sql1");
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
            System.out.println("TrnsctnWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test2,sql2");
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
            System.out.println("TrnsctnWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test2,sql3");
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
            System.out.println("TrnsctnWh.test2 sql3 failed");
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

        System.out.println("Finish TrnsctnWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03() {

        // Check the population of the TRNSCTN_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.TRNSCTN_WID, \n" +
                        "\t\t\t A.DCMNT_WID AS TEST_DOC_WID, \n" +
                        "\t\t\t B.DCMNT_WID AS ETL_DOC_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, DTSDM.DCMNT_WH B, FRED.PM_DOC C\n" +
                        "\n" +
                        "\t WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        "\t AND B.DCMNT_NAME = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.ADJSTMT_LVL = C.ADJ_LEVEL \n" +
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

        System.out.println("Starting TrnsctnWhTest.test03,sql1");
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
            System.out.println("TrnsctnWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test03,sql2");
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
            System.out.println("TrnsctnWh.test03 sql2 failed");
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

        System.out.println("Finish TrnsctnWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04() {

        // Check the population of the TRNSCTN_WH.PREV_VCHR_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.TRNSCTN_WID, A.PREV_VCHR_NAME, C.PRIOR_VCHNUM \n" +
                        "\n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, DTSDM.DCMNT_WH B, FRED.DOCSTAT C \n" +
                        "\n" +
                        "\t WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        "\t AND B.PREV_DCMNT_NAME = C.PRIOR_VCHNUM \n" +
                        "\t AND A.PREV_VCHR_NAME != C.PRIOR_VCHNUM \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TrnsctnWhTest.test04,sql");
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
            System.out.println("TrnsctnWh.test04 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TrnsctnWhTest.test04");
        System.out.println();

    }

    @Ignore
    @Test
    public void test05(){
        // EXT_SYSTEM_WID
    }

    @Test
    public void test06() {

        // Check the population of the TRNSCTN_WH.TRNSCTN_DCMNT_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.TRNSCTN_WID, A.TRNSCTN_DCMNT_TYPE_WID, B.TYPE_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, DTSDM.TYPE_CONSOLDTD_RFRNC_WH B, FRED.PM_DOC C \n" +
                        "\n" +
                        "\t WHERE A.TRNSCTN_DCMNT_TYPE_WID = B.TYPE_WID \n" +
                        "\t AND B.TYPE_DESCR = C.PM_DOCTYPE \n" +
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

        System.out.println("Starting TrnsctnWhTest.test06,sql1");
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
            System.out.println("TrnsctnWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test06,sql2");
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
            System.out.println("TrnsctnWh.test06 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 06: Comparison Count = " + comparisonCount);
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish TrnsctnWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07() {

        // Check the population of the TRNSCTN_WH.TRNSCTN_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.TRNSCTN_WID, A.TRNSCTN_TYPE_WID, B.TYPE_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, DTSDM.TYPE_CONSOLDTD_RFRNC_WH B, FRED.PM_DOC C \n" +
                        "\n" +
                        "\t WHERE A.TRNSCTN_TYPE_WID = B.TYPE_WID \n" +
                        "\t AND B.TYPE_DESCR = C.PM_DOCTYPE \n" +
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

        System.out.println("Starting TrnsctnWhTest.test07,sql1");
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
            System.out.println("TrnsctnWh.test07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test07,sql2");
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
            System.out.println("TrnsctnWh.test07 sql2 failed");
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

        System.out.println("Finish TrnsctnWhTest.test07");
        System.out.println();

    }

    @Ignore // column does not exist
    @Test
    public void test08() {

        // Check the population of the TRNSCTN_WH.ORG_ACCNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT A.TRNSCTN_WID, A.ORG_ACCNT_WID, B.ORG_ACCNT_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, DTSDM.ORG_ACCNT_WH B, FRED.PM_LEDGER C \n" +
                        "\n" +
                        "\t WHERE A.ORG_ACCNT_WID = B.ORG_ACCNT_WID \n" +
                        "\t AND B.ACCNT_LABEL = C.ACCLABEL \n" +
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

        System.out.println("Starting TrnsctnWhTest.test08,sql1");
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
            System.out.println("TrnsctnWh.test08 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test08,sql2");
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
            System.out.println("TrnsctnWh.test08 sql2 failed");
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

        System.out.println("Finish TrnsctnWhTest.test08");
        System.out.println();

    }

    @Ignore
    @Test
    public void test09(){
        // TRNSCTN_CREATE_DT
    }

    @Ignore
    @Test
    public void test10(){
        // XML_SBMT_DT
    }

    @Test
    public void test11() {

        // Check the population of the TRNSCTN_WH.TRNSCTN_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.TRNSCTN_AMT, B.TOTAL_COST \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.TRNSCTN_AMT = B.TOTAL_COST \n" +
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

        System.out.println("Starting TrnsctnWhTest.test11,sql1");
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
            System.out.println("TrnsctnWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test11,sql2");
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
            System.out.println("TrnsctnWh.test11 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 11: Comparison Count = " + comparisonCount);
        System.out.println("Test 11: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12() {

        // Check the population of the TRNSCTN_WH.TRNSCTN_SEQ_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.TRNSCTN_SEQ_NUM, B.U##TASEQ \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.TRNSCTN_SEQ_NUM = B.U##TASEQ \n" +
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

        System.out.println("Starting TrnsctnWhTest.test12,sql1");
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
            System.out.println("TrnsctnWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test12,sql2");
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
            System.out.println("TrnsctnWh.test12 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 account for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 12: Comparison Count = " + comparisonCount);
        System.out.println("Test 12: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13() {

        // Check the population of the TRNSCTN_WH.ACCNT_STATUS_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.ACCNT_STATUS_CODE, B.ACCT_STATUS \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.ACCNT_STATUS_CODE = B.ACCT_STATUS \n" +
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

        System.out.println("Starting TrnsctnWhTest.test13,sql1");
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
            System.out.println("TrnsctnWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test13,sql2");
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
            System.out.println("TrnsctnWh.test13 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 account for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 13: Comparison Count = " + comparisonCount);
        System.out.println("Test 13: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14() {

        // Check the population of the TRNSCTN_WH.ACCNTG_SYS_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.ACCNTG_SYS_NAME, B.ACCTG_SYSTEM_NAME \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.ACCNTG_SYS_NAME = B.ACCTG_SYSTEM_NAME \n" +
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

        System.out.println("Starting TrnsctnWhTest.test14,sql1");
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
            System.out.println("TrnsctnWh.test14 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test14,sql2");
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
            System.out.println("TrnsctnWh.test14 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 account for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 14: Comparison Count = " + comparisonCount);
        System.out.println("Test 14: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test14");
        System.out.println();

    }

    @Ignore
    @Test
    public void test15(){
        // ACCNTG_SYS_DSPLY_NAME
    }

    @Ignore
    @Test
    public void test16() {

        // Check the population of the TRNSCTN_WH.LST_UPDT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.LST_UPDT_DT, B.UPDATE_DATE \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.LST_UPDT_DT = B.UPDATE_DATE \n" +
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

        System.out.println("Starting TrnsctnWhTest.test16,sql1");
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
            System.out.println("TrnsctnWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test16,sql2");
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
            System.out.println("TrnsctnWh.test16 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 account for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 16: Comparison Count = " + comparisonCount);
        System.out.println("Test 16: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17() {

        // Check the population of the TRNSCTN_WH.SETTLMENT_TYPE_DESCR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TRNSCTN_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.TRNSCTN_WID, A.SETTLMENT_TYPE_DESCR, B.SETTLEMENT_TYPE \n" +
                        "\t FROM DTSDM.TRNSCTN_WH A, FRED.PM_DOC B \n" +
                        "\t WHERE A.SETTLMENT_TYPE_DESCR = B.SETTLEMENT_TYPE \n" +
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

        System.out.println("Starting TrnsctnWhTest.test17,sql1");
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
            System.out.println("TrnsctnWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsctnWhTest.test17,sql2");
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
            System.out.println("TrnsctnWh.test17 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 account for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1), "(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 17: Comparison Count = " + comparisonCount);
        System.out.println("Test 17: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish TrnsctnWhTest.test17");
        System.out.println();

    }

}
