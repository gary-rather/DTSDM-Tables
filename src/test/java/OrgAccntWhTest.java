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
public class OrgAccntWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("OrgAccntWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "SELECT * FROM DTSDM.ORG_ACCNT_WH WHERE ORG_ACCNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting OrgAccntWhTest.test1,sql");
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

        System.out.println("Test OrgAccntWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish OrgAccntWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (ORG_ACCNT_WH.ORG_ACCNT_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from \n" +
                "(\n" +
                "select distinct FULL_ORG_CD,ACCNT_LABEL, count (*)\n" +
                "from ORG_ACCNT_WH\n" +
                "group by FULL_ORG_CD,ACCNT_LABEL\n" +
                "having count(*) > 1\n" +
                "\n" +
                ")";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        wr.logSql(theSql);

        int duplicateCount = 0;

        System.out.println("Starting OrgAccntWhTest.test2,sql1");
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
            System.out.println("OrgAccntWh.test2 sql1 failed");
            e.printStackTrace();
        }


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((0 == duplicateCount), "(0 == duplicateCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish OrgAccntWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the ORG_ACCNT_WH.SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.ORG_ACCNT_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "\t SELECT B.SUBORG_WID AS ETL_SUBORG_WID, A.SUBORG_WID AS TEST_SUBORG_WID \n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM.SUBORG_WH B, DTSDM_SRC_STG.ACCOUNT C \n" +
                        "\t WHERE A.ACCNT_LABEL = C.ACCLABEL \n" +
                        "\t AND B.SUBORG_WID = A.SUBORG_WID \n" +
                        "\t AND A.FULL_ORG_CD = C.ORG \n" +
                        "\t AND B.FULL_ORG_CD = C.ORG \n" +
                        "\t AND A.FULL_ORG_CD = B.FULL_ORG_CD" +
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

        System.out.println("Starting OrgAccntWhTest.test03,sql1");
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
            System.out.println("OrgAccntWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OrgAccntWhTest.test03,sql2");
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
            System.out.println("OrgAccntWh.test03 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 03: Comparison Count = " + comparisonCount);
        System.out.println("Test 03: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish OrgAccntWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04() {

        // Check the population of the ORG_ACCNT_WH.FULL_ORG_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT B.FULL_ORG_CD AS ETL_SUBORG_WID, A.FULL_ORG_CD AS TEST_SUBORG_WID \n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM.SUBORG_WH B, DTSDM_SRC_STG.ACCOUNT C \n" +
                        "\t WHERE A.ACCNT_LABEL = C.ACCLABEL \n" +
                        "\t AND B.SUBORG_WID = A.SUBORG_WID \n" +
                        "\t AND A.FULL_ORG_CD = C.ORG \n" +
                        "\t AND B.FULL_ORG_CD = C.ORG \n" +
                        "\t AND A.FULL_ORG_CD != B.FULL_ORG_CD" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test04,sql");
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
            System.out.println("OrgAccntWh.test04 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05() {

        // Check the population of the ORG_ACCNT_WH.ACCT_LABEL column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL != B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test05,sql");
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
            System.out.println("OrgAccntWh.test05 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06() {

        // Check the population of the ORG_ACCNT_WH.ACC1 column (1st accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 != B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test06,sql");
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
            System.out.println("OrgAccntWh.test06 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07() {

        // Check the population of the ORG_ACCNT_WH.ACC2 column (2nd accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 != B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test07,sql");
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
            System.out.println("OrgAccntWh.test07 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 07: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08() {

        // Check the population of the ORG_ACCNT_WH.ACC3 column (3rd accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 != B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test08,sql");
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
            System.out.println("OrgAccntWh.test08 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 08: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test08");
        System.out.println();

    }

    @Test
    public void test09() {

        // Check the population of the ORG_ACCNT_WH.ACC4 column (4th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 != B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test09,sql");
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
            System.out.println("OrgAccntWh.test09 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 09: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test09");
        System.out.println();

    }

    @Test
    public void test10() {

        // Check the population of the ORG_ACCNT_WH.ACC5 column (5th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 != B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test10,sql");
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
            System.out.println("OrgAccntWh.test10 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 10: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11() {

        // Check the population of the ORG_ACCNT_WH.ACC6 column (6th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 != B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test11,sql");
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
            System.out.println("OrgAccntWh.test11 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 11: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12() {

        // Check the population of the ORG_ACCNT_WH.ACC7 column (7th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 != B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test12,sql");
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
            System.out.println("OrgAccntWh.test12 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 12: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13() {

        // Check the population of the ORG_ACCNT_WH.ACC8 column (8th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 != B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test13,sql");
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
            System.out.println("OrgAccntWh.test13 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 13: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14() {

        // Check the population of the ORG_ACCNT_WH.ACC9 column (9th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 != B.ACC9 AND A.ACC10 = B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test14,sql");
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
            System.out.println("OrgAccntWh.test14 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 14: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15() {

        // Check the population of the ORG_ACCNT_WH.ACC10 column (10th accounting label)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from" +
                        "( \n" +
                        "\t SELECT DISTINCT A.ACCNT_LABEL AS ETL_ACCNT_LABEL, \n" +
                        "\t\t\t B.ACCLABEL AS TEST_ACCNT_LABEL \n" +
                        "\n" +
                        "\t FROM DTSDM.ORG_ACCNT_WH A, DTSDM_SRC_STG.ACCOUNT B \n" +
                        "\n" +
                        "\t WHERE A.ACCNT_LABEL = B.ACCLABEL \n" +
                        "\t AND A.ACC1 = B.ACC1 AND A.ACC2 = B.ACC2 \n" +
                        "\t AND A.ACC3 = B.ACC3 AND A.ACC4 = B.ACC4 \n" +
                        "\t AND A.ACC5 = B.ACC5 AND A.ACC6 = B.ACC6 \n" +
                        "\t AND A.ACC7 = B.ACC7 AND A.ACC8 = B.ACC8 \n" +
                        "\t AND A.ACC9 = B.ACC9 AND A.ACC10 != B.ACC10 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting OrgAccntWhTest.test15,sql");
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
            System.out.println("OrgAccntWh.test15 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 15: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish OrgAccntWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16() {

        // Check the population of the ORG_ACCNT_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.ORG_ACCNT_WH";
        String sql2 = "select ORG_ACCNT_WH.CURR_SW from DTSDM.ORG_ACCNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting ExpnsSmryWhTest.test16,sql1");
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
            System.out.println("ExpnsSmryWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting ExpnsSmryWhTest.test16,sql2");
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
            System.out.println("ExpnsSmryWh.test16 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 16: Comparison Count = " + comparisonCount);
        System.out.println("Test 16: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish ExpnsSmryWhTest.test16");
        System.out.println();

    }

}
