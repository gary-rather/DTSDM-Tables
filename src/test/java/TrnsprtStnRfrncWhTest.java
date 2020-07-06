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
public class TrnsprtStnRfrncWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("TrnsprtStnRfrncWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "Select count(*) from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql" );
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        number = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test TrnsprtStnRfrncWhTest  Row 0 = " + number);
        assertEquals(1, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
        System.out.println();

    }

    @Test
    public void test02(){

        //Check the population of the unique identifier (TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_RFRNC_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.TRNSPRT_STN_RFRNC_WID, count(*) \n" +
                        "from DTSDM.TRNSPRT_STN_RFRNC A \n" +
                        "group by A.TRNSPRT_STN_RFRNC_WID having count(*) > 1";

        String sql2 = "select count (distinct A.TRNSPRT_STN_RFRNC_WH) from DTSDM.TRNSPRT_STN_RFRNC_WH A";
        String sql3 = "select count(*) from DTSDM.TRNSPRT_STN_RFRNC_WH";

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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test2,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test2,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test2,sql3");
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
            System.out.println("TrnsprtStnRfrncWh.test2 sql3 failed");
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
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        assertEquals(count, distinctCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3_Arr(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_NAME column (for arrival location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_NAME, B.ARR_AIRPORT \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_NAME = B.ARR_AIRPORT \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test3_Arr,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test3_Arr sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test3_Arr,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test3_Arr sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test3_Arr: Test Count = " + testCount);
        System.out.println("Test3_Arr: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test3_Arr");
        System.out.println();

    }

    @Test
    public void test4_Arr(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_CODE column (for arrival location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_CODE, B.ARR_AIR \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_CODE = B.ARR_AIR \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test4_Arr,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test4_Arr sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test4_Arr,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test4_Arr sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test4_Arr: Test Count = " + testCount);
        System.out.println("Test4_Arr: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test4_Arr");
        System.out.println();

    }

    @Test
    public void test5_Arr(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_LATITD column (for arrival location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_LATITD, B.ARR_LAT \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_LATITD = B.ARR_LAT \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test5_Arr,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test5_Arr sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test5_Arr,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test5_Arr sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test5_Arr: Test Count = " + testCount);
        System.out.println("Test5_Arr: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test5_Arr");
        System.out.println();

    }

    @Test
    public void test6_Arr(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_LONGTD column (for arrival location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_LONGTD, B.ARR_LONG \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_LONGTD = B.ARR_LONG \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test6_Arr,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test6_Arr sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test6_Arr,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test6_Arr sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test6_Arr: Test Count = " + testCount);
        System.out.println("Test6_Arr: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test6_Arr");
        System.out.println();

    }

    @Test
    public void test3_Dep(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_NAME column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_NAME, B.DEP_AIRPORT \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_NAME = B.DEP_AIRPORT \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test3_Dep,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test3_Dep sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test3_Dep,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test3_Dep sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test3_Dep: Test Count = " + testCount);
        System.out.println("Test3_Dep: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test3_Dep");
        System.out.println();

    }

    @Test
    public void test4_Dep(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_CODE column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_CODE, B.DEP_AIR \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_CODE = B.DEP_AIR \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test4_Dep,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test4_Dep sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test4_Dep,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test4_Dep sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test4_Dep: Test Count = " + testCount);
        System.out.println("Test4_Dep: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test4_Dep");
        System.out.println();

    }

    @Test
    public void test5_Dep(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_LATITD column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_LATITD, B.DEP_LAT \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_LATITD = B.DEP_LAT \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test5_Dep,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test5_Dep sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test5_Dep,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test5_Dep sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test5_Dep: Test Count = " + testCount);
        System.out.println("Test5_Dep: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test5_Dep");
        System.out.println();

    }

    @Test
    public void test6_Dep(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_LONGTD column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.TRNSPRT_STN_RFRNC_WID, \n" +
                        "    A.TRNSPRT_STN_LONGTD, B.DEP_LONG \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_LONGTD = B.DEP_LONG \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test6_Dep,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test6_Dep sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test6_Dep,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test6_Dep sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test6_Arr: Test Count = " + testCount);
        System.out.println("Test6_Arr: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish TrnsprtStnRfrncWhTest.test6_Dep");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_TYPE_CD column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT A.TRNSPRT_STN_RFRNC_WID, A.TRNSPRT_STN_TYPE_CD, B.U##RES_TYPE\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B\n" +
                        "  WHERE A.TRNSPRT_STN_TYPE_CD IN (SELECT \n" +
                        "          CASE\n" +
                        "           WHEN B.U##RES_TYPE = 'COMM-RAIL' THEN 'R'\n" +
                        "           WHEN B.U##RES_TYPE = 'COMM-CARR' THEN 'A'\n" +
                        "           WHEN B.U##RES_TYPE = 'COMM-BUS' THEN 'B'\n" +
                        "           WHEN B.U##RES_TYPE = 'COMM-SHIP' THEN 'S'\n" +
                        "           ELSE NULL\n" +
                        "          END\n" +
                        "         FROM DTSDM_SRC_STG.TICKSUB B) \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test7,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test7,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test7 sql2 failed");
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

        System.out.println("Finish TrnsprtStnRfrncWhTest.test7");
        System.out.println();

    }

    @Test
    public void test8(){

        // Check the population of the TRNSPRT_STN_RFRNC_WH.TRNSPRT_STN_TYPE_DESCR column (for departing location)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.TRNSPRT_STN_RFRNC_WH A \n" +
                        "where A.TRNSPRT_STN_RFRNC_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT A.TRNSPRT_STN_RFRNC_WID, " +
                        "    A.TRNSPRT_STN_TYPE_DESCR, B.U##RES_TYPE \n" +
                        "\n" +
                        "  FROM DTSDM.TRNSPRT_STN_RFRNC_WH A, DTSDM_SRC_STG.TICKSUB B \n" +
                        "  WHERE A.TRNSPRT_STN_TYPE_DESCR = B.U##RES_TYPE \n" +
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

        System.out.println("Starting TrnsprtStnRfrncWhTest.test8,sql1");
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
            System.out.println("TrnsprtStnRfrncWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TrnsprtStnRfrncWhTest.test8,sql2");
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
            System.out.println("TrnsprtStnRfrncWh.test8 sql2 failed");
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

        System.out.println("Finish TrnsprtStnRfrncWhTest.test8");
        System.out.println();

    }

}
