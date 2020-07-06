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
public class PersonEftAccntWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("PersonEftAccntWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Initial load must add  unspecified data row. Provides the ability to traverse through the DTSDM.EXT_SYSTEM_WH table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "SELECT * FROM DTSDM.PERSON_EFT_ACCNT_WH WHERE PERSON_EFT_ACCNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting PersonEftAccntWhTest.test1,sql");
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

        System.out.println("Test PersonEftAccntWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish PersonEftAccntWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (PERSON_EFT_ACCNT_WH.PERSON_EFT_ACCNT_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the unique identifier (PERSON_EFT_ACCNT_WH.PERSON_EFT_ACCNT_WID (PK) column)";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "SELECT PERSON_EFT_ACCNT_WID FROM DTSDM.PERSON_EFT_ACCNT_WH \n" +
                        "GROUP BY PERSON_EFT_ACCNT_WID HAVING COUNT(*) > 1";

        String sql2 = "select count (distinct PERSON_EFT_ACCNT_WH.PERSON_EFT_ACCNT_WID) \n" +
                        "from DTSDM.PERSON_EFT_ACCNT_WH";

        String sql3 = "select count(*) from DTSDM.PERSON_EFT_ACCNT_WH";

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

        System.out.println("Starting PersonEftAccntWhTest.test2,sql1");
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
            System.out.println("PersonEftAccntWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonEftAccntWhTest.test2,sql2");
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
            System.out.println("PersonEftAccntWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PersonEftAccntWhTest.test2,sql3");
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
            System.out.println("PersonEftAccntWh.test2 sql3 failed");
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

        System.out.println("Finish PersonEftAccntWhTest.test2");
        System.out.println();

    }

    @Ignore
    @Test
    public void test03(){

        // Check the population of the PERSON_EFT_ACCNT_WH.PERSON_MASTER_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.PERSON_MASTER_WID column";
        String reason = "  (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.PERSON_EFT_ACCNT_WH";

        String sql2 = "select count(*) from" +
                        "( \n" +
                        "  SELECT A.PERSON_MASTER_WID, B.U##SSN \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM_SRC_STG.TPEREFT B \n" +
                        "  WHERE A.PERSON_MASTER_WID = B.U##SSN \n" +
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

        System.out.println("Starting PersonEftAccntWhTest.test03,sql1");
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
            System.out.println("PersonEftAccntWh.test03 sql1 failed");
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
            System.out.println("PersonEftAccntWh.test03 sql2 failed");
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

        System.out.println("Finish PersonEftAccntWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the PERSON_EFT_ACCNT_WH.EFT_ACCNT_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.EFT_ACCNT_TYPE_WID column";
        String reason = " FK to TYPE_CONSOLDTD_RFRNC_WH";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.PERSON_EFT_ACCNT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.PERSON_EFT_ACCNT_WID, A.EFT_ACCNT_TYPE_WID, B.TYPE_WID \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM.TYPE_CONSOLDTD_RFRNC_WH B, DTSDM_SRC_STG.TPEREFT C \n" +
                        "  WHERE A.EFT_ACCNT_TYPE_WID = B.TYPE_WID \n" +
                        "  AND B.TYPE_DESCR = C.U##EFT_TYPE \n" +
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

        System.out.println("Starting PersonEftAccntWhTest.test04,sql1");
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
            System.out.println("PersonEftAccntWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OrgAccntWhTest.test04,sql2");
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
            System.out.println("PersonEftAccntWh.test04 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 04: Comparison Count = " + comparisonCount);
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(comparisonCount-1, testCount);

        System.out.println("Finish PersonEftAccntWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05() {

        // Check the population of the PERSON_EFT_ACCNT_WH.EFT_ACCNT_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.EFT_ACCNT_NUM column";
        String reason = "  (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "  SELECT A.PERSON_EFT_ACCNT_WID, A.EFT_ACCNT_NUM, B.EFT_ACCOUNT \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM_SRC_STG.TPEREFT B \n" +
                        "  WHERE A.EFT_ACCNT_NUM != B.EFT_ACCOUNT \n" +
                        "  AND A.ACCNT_ROUTNG_NUM = B.EFT_ROUTING \n" +
                        "  AND A.EXPRTN_DT = B.EXPDATE \n" +
                        "  AND A.ACCNT_ACTV_FLAG = B.CCACTIVE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting PersonEftAccntWhTest.test05,sql");
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
            System.out.println("PersonEftAccntWh.test05 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish PersonEftAccntWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06() {

        // Check the population of the PERSON_EFT_ACCNT_WH.ACCNT_ROUTNG_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.ACCNT_ROUTNG_NUM column";
        String reason = " (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "  SELECT A.PERSON_EFT_ACCNT_WID, A.EFT_ACCNT_NUM, B.EFT_ACCOUNT \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM_SRC_STG.TPEREFT B \n" +
                        "  WHERE A.EFT_ACCNT_NUM = B.EFT_ACCOUNT \n" +
                        "  AND A.ACCNT_ROUTNG_NUM != B.EFT_ROUTING \n" +
                        "  AND A.EXPRTN_DT = B.EXPDATE \n" +
                        "  AND A.ACCNT_ACTV_FLAG = B.CCACTIVE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting PersonEftAccntWhTest.test06,sql");
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
            System.out.println("PersonEftAccntWh.test06 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish PersonEftAccntWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07() {

        // Check the population of the PERSON_EFT_ACCNT_WH.EXPRTN_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.EXPRTN_DT column";
        String reason = " (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "  SELECT A.PERSON_EFT_ACCNT_WID, A.EFT_ACCNT_NUM, B.EFT_ACCOUNT \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM_SRC_STG.TPEREFT B \n" +
                        "  WHERE A.EFT_ACCNT_NUM = B.EFT_ACCOUNT \n" +
                        "  AND A.ACCNT_ROUTNG_NUM = B.EFT_ROUTING \n" +
                        "  AND A.EXPRTN_DT != B.EXPDATE \n" +
                        "  AND A.ACCNT_ACTV_FLAG = B.CCACTIVE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting PersonEftAccntWhTest.test07,sql");
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
            System.out.println("PersonEftAccntWh.test07 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 07: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish PersonEftAccntWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08() {

        // Check the population of the PERSON_EFT_ACCNT_WH.ACCNT_ACTV_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the PERSON_EFT_ACCNT_WH.ACCNT_ACTV_FLAG column";
        String reason = "  (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from" +
                        "( \n" +
                        "  SELECT A.PERSON_EFT_ACCNT_WID, A.EFT_ACCNT_NUM, B.EFT_ACCOUNT \n" +
                        "  FROM DTSDM.PERSON_EFT_ACCNT_WH A, DTSDM_SRC_STG.TPEREFT B \n" +
                        "  WHERE A.EFT_ACCNT_NUM = B.EFT_ACCOUNT \n" +
                        "  AND A.ACCNT_ROUTNG_NUM = B.EFT_ROUTING \n" +
                        "  AND A.EXPRTN_DT = B.EXPDATE \n" +
                        "  AND A.ACCNT_ACTV_FLAG != B.CCACTIVE \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting PersonEftAccntWhTest.test08,sql");
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
            System.out.println("PersonEftAccntWh.test08 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0), "(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 08: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish PersonEftAccntWhTest.test08");
        System.out.println();

    }

}
