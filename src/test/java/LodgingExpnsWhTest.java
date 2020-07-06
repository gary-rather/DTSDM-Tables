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
public class LodgingExpnsWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("LodgingExpnsWhTest.html");
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


        String sql = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LodgingExpnsWhTest.test01,sql");
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

        System.out.println("Test LodgingExpnsWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish LodgingExpnsWhTest.test01");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (LODGING_EXPNS_WH.LODGING_EXPNS_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.LODGING_EXPNS_WID, count(*) from DTSDM.LODGING_EXPNS_WH A\n" +
                        "group by A.LODGING_EXPNS_WID having count(*) > 1";

        String sql2 = "select count (distinct A.LODGING_EXPNS_WID) from DTSDM.LODGING_EXPNS_WH A";
        String sql3 = "select count(*) from DTSDM.DTSDM.LODGING_EXPNS_WH";

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

        System.out.println("Starting LodgingExpnsWhTest.test02,sql1");
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
            System.out.println("LodgingExpnsWh.test02 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test02,sql2");
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
            System.out.println("LodgingExpnsWh.test02 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test02,sql3");
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
            System.out.println("LodgingExpnsWh.test02 sql3 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the LODGING_EXPNS_WH.TRIP_LEG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.TRIP_LEG_WID, B.TRIP_LEG_WID \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM.TRIP_LEG_WH B \n" +
                        "  WHERE A.TRIP_LEG_WID = B.TRIP_LEG_WID \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test03,sql1");
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
            System.out.println("LodgingExpnsWh.tes03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test03,sql2");
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
            System.out.println("LodgingExpnsWh.test03 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test03");
        System.out.println();

    }

    @Ignore
    @Test
    public void test04(){

    }

    @Test
    public void test05(){

        // Check the population of the LODGING_EXPNS_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.DCMNT_WID, B.DCMNTORG_ACCNT_WID \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM.DCMNT_WH B, DTSDM_SRC_STG.LODGE C \n" +
                        "  WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        "  AND B.DCMNT_NAME = C.U##VCHNUM \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test05,sql1");
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
            System.out.println("LodgingExpnsWh.tes05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test05,sql2");
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
            System.out.println("LodgingExpnsWh.test05 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the LODGING_EXPNS_WH.ORG_ACCNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.ORG_ACCNT_WID, B.ORG_ACCNT_WID \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM.ORG_ACCNT_WH B, DTSDM_SRC_STG.LODGE C \n" +
                        "  WHERE A.ORG_ACCNT_WID = B.ORG_ACCNT_WID \n" +
                        "  AND B.ACCT_LABEL = C.LDG_ACC_LABEL \n" +
                        "  AND B.FULL_ORG_CD = C.LDGACCORG \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test06,sql1");
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
            System.out.println("LodgingExpnsWh.tes06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test06,sql2");
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
            System.out.println("LodgingExpnsWh.test06 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07(){

        // Check the population of the LODGING_EXPNS_WH.TRAV_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.TRVL_DT, B.TRAVDATE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.TRVL_DT = B.TRAVDATE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test07,sql1");
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
            System.out.println("LodgingExpnsWh.tes07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test07,sql2");
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
            System.out.println("LodgingExpnsWh.test07 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08(){

        // Check the population of the LODGING_EXPNS_WH.RATE_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.RATE_AMT, B.LDGRATE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.RATE_AMT = B.LDGRATE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test08,sql1");
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
            System.out.println("LodgingExpnsWh.tes08 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test08,sql2");
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
            System.out.println("LodgingExpnsWh.test08 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test08");
        System.out.println();

    }

    @Test
    public void test09(){

        // Check the population of the LODGING_EXPNS_WH.FOR_RATE_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.FOR_RATE_AMT, B.FOR_LDGRATE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.FOR_RATE_AMT = B.FOR_LDGRATE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test09,sql1");
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
            System.out.println("LodgingExpnsWh.tes09 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test09,sql2");
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
            System.out.println("LodgingExpnsWh.test09 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test09");
        System.out.println();

    }

    @Ignore
    @Test
    public void test10(){

    }

    @Test
    public void test11(){

        // Check the population of the LODGING_EXPNS_WH.QTRS_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.QTRS_SW, B.QTRS \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.QTRS_SW = B.QTRS \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test11,sql1");
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
            System.out.println("LodgingExpnsWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test11,sql2");
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
            System.out.println("LodgingExpnsWh.test11 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12(){

        // Check the population of the LODGING_EXPNS_WH.LDG_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.LDG_COST_AMT, B.LDGCOST \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.LDG_COST_AMT = B.LDGCOST \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test12,sql1");
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
            System.out.println("LodgingExpnsWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test12,sql2");
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
            System.out.println("LodgingExpnsWh.test12 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13(){

        // Check the population of the LODGING_EXPNS_WH.FOR_LDG_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.FOR_LDG_COST_AMT, B.FOR_LDGCOST \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.FOR_LDG_COST_AMT = B.FOR_LDGCOST \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test13,sql1");
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
            System.out.println("LodgingExpnsWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test13,sql2");
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
            System.out.println("LodgingExpnsWh.test13 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14(){

        // Check the population of the LODGING_EXPNS_WH.ALLWD_LDG_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.ALLWD_LDG_COST_AMT, B.LDG_ALLOWED \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.ALLWD_LDG_COST_AMT = B.LDG_ALLOWED \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test14,sql1");
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
            System.out.println("LodgingExpnsWh.test14 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test14,sql2");
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
            System.out.println("LodgingExpnsWh.test14 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15(){

        // Check the population of the LODGING_EXPNS_WH.LDG_SUBCODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.LDG_SUBCODE, B.LDG_SUBCODE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.LDG_SUBCODE = B.LDG_SUBCODE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test15,sql1");
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
            System.out.println("LodgingExpnsWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test15,sql2");
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
            System.out.println("LodgingExpnsWh.test15 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16(){

        // Check the population of the LODGING_EXPNS_WH.PAYMNT_MTHD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                "( \n" +
                "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.PAYMNT_MTHD, B.LDG_PAYMETH \n" +
                "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                "  WHERE A.PAYMNT_MTHD = B.LDG_PAYMETH \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test16,sql1");
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
            System.out.println("LodgingExpnsWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test16,sql2");
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
            System.out.println("LodgingExpnsWh.test16 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17(){

        // Check the population of the LODGING_EXPNS_WH.LDG_STATUS_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.LDG_STATUS_CODE, B.LDG_STAT \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.LDG_STATUS_CODE = B.LDG_STAT \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test17,sql1");
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
            System.out.println("LodgingExpnsWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test17,sql2");
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
            System.out.println("LodgingExpnsWh.test17 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the LODGING_EXPNS_WH.VNDR_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT A.LODGING_EXPNS_WID, A.VNDR_WID, B.VNDR_WID \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM.VNDR_WHB, DTSDM_SRC_STG.LODGE C \n" +
                        "  WHERE A.VNDR_WID = B.VNDR_WID \n" +
                        "  AND B.VNDR_NAME = C.LDG_VENDOR \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test17,sql1");
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
            System.out.println("LodgingExpnsWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test17,sql2");
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
            System.out.println("LodgingExpnsWh.test17 sql2 failed");
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

        System.out.println("Finish LodgingExpnsWhTest.test17");
        System.out.println();

    }

    @Test
    public void test19(){

        // Check the population of the LODGING_EXPNS_WH.ADV_ALLWD_IND column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.ADV_ALLWD_IND, B.LADV_ALLOWED \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.ADV_ALLWD_IND = B.LADV_ALLOWED \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test19,sql1");
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
            System.out.println("LodgingExpnsWh.test19 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test19,sql2");
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
            System.out.println("LodgingExpnsWh.test19 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 19: Test Count = " + testCount);
        System.out.println("Test 19: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test19");
        System.out.println();

    }

    @Test
    public void test20(){

        // Check the population of the LODGING_EXPNS_WH.GELCO_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.GELCO_CODE, B.LDG_GELCO_CODE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.GELCO_CODE = B.LDG_GELCO_CODE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test20,sql1");
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
            System.out.println("LodgingExpnsWh.test20 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test20,sql2");
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
            System.out.println("LodgingExpnsWh.test20 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 20: Test Count = " + testCount);
        System.out.println("Test 20: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test20");
        System.out.println();

    }

    @Test
    public void test21(){

        // Check the population of the LODGING_EXPNS_WH.TOTAL_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.TOTAL_COST_AMT, B.TOTLDGCOST \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.TOTAL_COST_AMT = B.TOTLDGCOST \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test21,sql1");
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
            System.out.println("LodgingExpnsWh.test21 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test21,sql2");
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
            System.out.println("LodgingExpnsWh.test21 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 21: Test Count = " + testCount);
        System.out.println("Test 21: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test21");
        System.out.println();

    }

    @Test
    public void test22(){

        // Check the population of the LODGING_EXPNS_WH.DSTRBTN_MTHD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.DSTRBTN_MTHD, B.DISTMETH \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.DSTRBTN_MTHD = B.DISTMETH \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test22,sql1");
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
            System.out.println("LodgingExpnsWh.test22 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test22,sql2");
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
            System.out.println("LodgingExpnsWh.test22 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 22: Test Count = " + testCount);
        System.out.println("Test 22: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test22");
        System.out.println();

    }

    @Test
    public void test23(){

        // Check the population of the LODGING_EXPNS_WH.LIMTD_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.LIMTD_AMT, B.LIMITED_AMT \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.LIMTD_AMT = B.LIMITED_AMT \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test23,sql1");
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
            System.out.println("LodgingExpnsWh.test23 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test23,sql2");
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
            System.out.println("LodgingExpnsWh.test23 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 23: Test Count = " + testCount);
        System.out.println("Test 23: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test23");
        System.out.println();

    }

    @Test
    public void test24(){

        // Check the population of the LODGING_EXPNS_WH.ENTRD_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.ENRTD_AMT, B.ENTERED_AMT \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.ENTRD_AMT = B.ENTERED_AMT \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test24,sql1");
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
            System.out.println("LodgingExpnsWh.test24 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test24,sql2");
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
            System.out.println("LodgingExpnsWh.test24 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 24: Test Count = " + testCount);
        System.out.println("Test 24: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test24");
        System.out.println();

    }

    @Ignore
    @Test
    public void test25(){

    }

    @Test
    public void test26(){

        // Check the population of the LODGING_EXPNS_WH.COST_STATUS_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.COST_STATUS_CODE, B.COST_STAT \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.COST_STATUS_CODE = B.COST_STAT \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test26,sql1");
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
            System.out.println("LodgingExpnsWh.test26 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test26,sql2");
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
            System.out.println("LodgingExpnsWh.test26 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 26: Test Count = " + testCount);
        System.out.println("Test 26: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test26");
        System.out.println();

    }

    @Test
    public void test27(){

        // Check the population of the LODGING_EXPNS_WH.FOR_LDG_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.FOR_LDG_COST_AMT, B.FOR_LDGCOST \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.FOR_LDG_COST_AMT = B.FOR_LDGCOST \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test27,sql1");
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
            System.out.println("LodgingExpnsWh.test27 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test27,sql2");
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
            System.out.println("LodgingExpnsWh.test27 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 27: Test Count = " + testCount);
        System.out.println("Test 27: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test27");
        System.out.println();

    }

    @Test
    public void test28(){

        // Check the population of the LODGING_EXPNS_WH.FOR_LDG_RATE_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.FOR_LDG_RATE_AMT, B.FOR_LDGRATE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.FOR_LDG_RATE_AMT = B.FOR_LDGRATE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test28,sql1");
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
            System.out.println("LodgingExpnsWh.test28 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test28,sql2");
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
            System.out.println("LodgingExpnsWh.test28 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 28: Test Count = " + testCount);
        System.out.println("Test 28: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test28");
        System.out.println();

    }

    @Test
    public void test29(){

        // Check the population of the LODGING_EXPNS_WH.EXPNS_START_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LODGING_EXPNS_WH A where A.LODGING_EXPNS_WID != 0";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  SELECT DISTINCT A.LODGING_EXPNS_WID, A.EXPNS_START_DT, B.ACCRUED_START_DATE \n" +
                        "  FROM DTSDM.LODGING_EXPNS_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "  WHERE A.EXPNS_START_DT = B.ACCRUED_START_DATE \n" +
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

        System.out.println("Starting LodgingExpnsWhTest.test29,sql1");
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
            System.out.println("LodgingExpnsWh.test29 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LodgingExpnsWhTest.test29,sql2");
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
            System.out.println("LodgingExpnsWh.test29 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 29: Test Count = " + testCount);
        System.out.println("Test 29: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount,testCount);

        System.out.println("Finish LodgingExpnsWhTest.test29");
        System.out.println();

    }

}
