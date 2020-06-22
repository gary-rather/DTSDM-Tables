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
public class OtherAuthWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("OtherAuthWhTest.html");
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


        String sql = "select * from DTSDM.DCMNT_OTHER_AUTH_WH A where A.DCMNT_OTHER_AUTH_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting OtherAuthWhTest.test01,sql");
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

        System.out.println("Test OtherAuthWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish OtherAuthWhTest.test01");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (DCMNT_OTHER_AUTH_WH.DCMNT_OTHER_AUTH_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.DCMNT_OTHER_AUTH_WID, count(*) from DTSDM.DCMNT_OTHER_AUTH_WH A\n" +
                        "group by A.DCMNT_OTHER_AUTH_WID having count(*) > 1";

        String sql2 = "select count (distinct A.DCMNT_OTHER_AUTH_WID) from DTSDM.DCMNT_OTHER_AUTH_WH A";
        String sql3 = "select count(*) from DTSDM.DCMNT_OTHER_AUTH_WH";

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

        System.out.println("Starting OtherAuthWhTest.test02,sql1");
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
            System.out.println("OtherAuthWh.test02 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test02,sql2");
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
            System.out.println("OtherAuthWh.test02 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test02,sql3");
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
            System.out.println("OtherAuthWh.test02 sql3 failed");
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

        System.out.println("Finish OtherAuthWhTest.test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the DCMNT_OTHER_AUTH_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_OTHER_AUTH_WH A where A.DCMNT_OTHER_AUTH_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.DCMNT_OTHER_AUTH_WID, A.DCMNT_WID, B.DCMNT_WID \n" +
                        "\t FROM DTSDM.DCMNT_OTHER_AUTH_WH A, DTSDM.DCMNT_WH B, FRED.TAOTHER C \n" +
                        "\t WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        "\t AND B.DCMNT_NAME = C.U##VCHNUM \n" +
                        "\t AND B.ADJSTMT_LVL = C.ADJ_LEVEL \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.TRIP_NUM = C.TRIPNUM \n" +
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

        System.out.println("Starting OtherAuthWhTest.test03,sql1");
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
            System.out.println("OtherAuthWh.tes03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test03,sql2");
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
            System.out.println("OtherAuthWh.test03 sql2 failed");
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

        System.out.println("Finish OtherAuthWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the DCMNT_OTHER_AUTH_WH.OTHER_AUTH_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_OTHER_AUTH_WH A where A.DCMNT_OTHER_AUTH_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.DCMNT_OTHER_AUTH_WID, \n" +
                        "\t\t\t A.OTHER_AUTH_TYPE_WID, B.TYPE_WID \n" +
                        "\n" +
                        "\t FROM DTSDM.DCMNT_OTHER_AUTH_WH A, \n" +
                        "\t\t\t DTSDM.TYPE_CONSOLDTD_RFRNC_WH B, \n" +
                        "\t\t\t FRED.TAOTHER C \n" +
                        "\n" +
                        "\t WHERE A.OTHER_AUTH_TYPE_WID = B.TYPE_WID \n" +
                        "\t AND B.TYPE_DESCR = C.AUTHORIZED \n" +
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

        System.out.println("Starting OtherAuthWhTest.test04,sql1");
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
            System.out.println("OtherAuthWh.tes04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test04,sql2");
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
            System.out.println("OtherAuthWh.test04 sql2 failed");
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

        System.out.println("Finish OtherAuthWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the DCMNT_OTHER_AUTH_WH.OTHER_AUTH_RMRKS_TXT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_OTHER_AUTH_WH A where A.DCMNT_OTHER_AUTH_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.DCMNT_OTHER_AUTH_WID, \n" +
                        "\t\t\t A.OTHER_AUTH_RMRKS_TXT, B.AUTH_REM \n" +
                        "\n" +
                        "\t FROM DTSDM.DCMNT_OTHER_AUTH_WH A, FRED.TAOTHER B \n" +
                        "\t WHERE A.OTHER_AUTH_RMRKS_TXT = B.AUTH_REM \n" +
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

        System.out.println("Starting OtherAuthWhTest.test05,sql1");
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
            System.out.println("OtherAuthWh.tes05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test05,sql2");
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
            System.out.println("OtherAuthWh.test05 sql2 failed");
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

        System.out.println("Finish OtherAuthWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the DCMNT_OTHER_AUTH_WH.PERSTEMPO_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.DCMNT_OTHER_AUTH_WH A where A.DCMNT_OTHER_AUTH_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "\t SELECT A.DCMNT_OTHER_AUTH_WID, A.PERSTEMPO_TXT, B.AUTHORIZED \n" +
                        "\t FROM DTSDM.DCMNT_OTHER_AUTH_WH A, FRED.TAOTHER B \n" +
                        "\t WHERE A.PERSTEMPO = ( \n" +
                        "\t\t\t\t\t SELECT \n" +
                        "\t\t\t\t\t\t CASE \n" +
                        "\t\t\t\t\t\t\t WHEN FRED.AUTHORIZED LIKE 'PERSTEMPO%' \n" +
                        "\t\t\t\t\t\t\t THEN SUBSTR(16,1) \n" +
                        "\t\t\t\t\t\t\t ELSE NULL \n" +
                        "\t\t\t\t\t\t END\n" +
                        "\t\t\t\t\t FROM FRED.TAOTHER \n" +
                        "\t\t\t\t\t ) \n" +
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

        System.out.println("Starting OtherAuthWhTest.test06,sql1");
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
            System.out.println("OtherAuthWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting OtherAuthWhTest.test06,sql2");
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
            System.out.println("OtherAuthWh.test06 sql2 failed");
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

        System.out.println("Finish OtherAuthWhTest.test06");
        System.out.println();

    }

}
