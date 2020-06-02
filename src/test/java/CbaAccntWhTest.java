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
public class CbaAccntWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("CbaAccntWhTest.html");
        wr.pageHeader();

    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated
        //EXPECT: CBA_ACCNT_WID = 0; AGNCY_WID = 0; CBA_ACCNT_NUM = 'UNKNOWN'; CBA_ACCNT_LABEL = 'UNKNOWN'; others NULL

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from DTSDM.CBA_ACCNT_WH where CBA_ACCNT_WH. CBA_ACCNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting CbaAccntWhTest.test1,sql");
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

        System.out.println("Test CbaAccntWh  Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish CbaAccntWhTest.test1");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (CBA_ACCNT_WH.CBA_ACCNT_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct cba_accnt_wid, count(*) from dtsdm.cba_accnt_wh\n" +
                        "group by cba_accnt_wid having count(*) > 1";

        String sql2 = "select distinct count(CBA_ACCNT_WID) from DTSDM.CBA_ACCNT_WH";

        String sql3 = "select count(*) from DTSDM.CBA_ACCNT_WH";

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

        System.out.println("Starting CbaAccntWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        duplicateCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test2,sql2");
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
            System.out.println("CbaAccntWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test2,sql3");
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
            System.out.println("CbaAccntWh.test2 sql3 failed");
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
        assertEquals(distinctCount, count);

        assertEquals(0, duplicateCount);
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        System.out.println("Finish CbaAccntWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        // Check the population of the CBA_ACCNT_WH.AGNCY_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct agncy_wid from dtsdm.cba_accnt_wh";

        String sql2 = "select distinct a.agncy_wid f\n" +
                        "from dtsdm.agncy_wh a, fred.cba_account fca\n" +
                        "where a.agncy_descr = fca.agency_code";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting CbaAccntWhTest.test3,sql1");
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
            System.out.println("CbaAccntWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test3 sql2 failed");
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

        System.out.println("Finish CbaAccntWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        // Check the population of the CBA_ACCNT_WH.CBA_ACCT_NUM column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ca.cba_accnt_num from dtsdm.cba_accnt_wh ca";
        String sql2 = "select distinct fca.cba_acct_no from fred.cba_account fca";

        String sql3 = "select distinct ca.cba_accnt_num from dtsdm.cba_accnt_wh ca\n" +
                        "where ca.cba_accnt_num not in\n" +
                        "(select distinct fca.cba_acct_no from fred.cba_account fca)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int dtsdmCount = 0;
        int fredCount = 0;
        int diffCount = 0;

        System.out.println("Starting CbaAccntWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dtsdmCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        fredCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test4 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test4,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        diffCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test4 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((diffCount == 0),"(diffCount == 0)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 4: Count From DTSDM.CBA_ACCNT_WH = " + dtsdmCount);
        System.out.println("Test 4: Count From FRED.CBA_ACCOUNT = " + fredCount);
        System.out.println("Test 4: Discrepancy Count = " + diffCount);

        assertEquals(0, diffCount);

        System.out.println("Finish CbaAccntWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        // Check the population of the CBA_ACCNT_WH.CBA_ACCT_LABEL column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ca.cba_acct_label from dtsdm.cba_accnt_wh ca";
        String sql2 = "select distinct fca.acct_label from fred.cba_account fca";

        String sql3 = "select distinct ca.cba_acct_label from dtsdm.cba_accnt_wh ca\n" +
                        "where ca.cba_acct_label not in\n" +
                        "(select distinct fca.acct_label from fred.cba_account fca)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int dtsdmCount = 0;
        int fredCount = 0;
        int diffCount = 0;

        System.out.println("Starting CbaAccntWhTest.test5,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dtsdmCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test5,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        fredCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test5 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test5,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        diffCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test5 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((diffCount == 0),"(diffCount == 0)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 5: Count From DTSDM.CBA_ACCNT_WH = " + dtsdmCount);
        System.out.println("Test 5: Count From FRED.CBA_ACCOUNT = " + fredCount);
        System.out.println("Test 5: Discrepancy Count = " + diffCount);

        assertEquals(0, diffCount);

        System.out.println("Finish CbaAccntWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        // Check the population of the CBA_ACCNT_WH.CBA_ACCNT_STATUS_CD column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ca.cba_accnt_status_cd from dtsdm.cba_accnt_wh ca";
        String sql2 = "Select distinct fca.acct_status from fred.cba_account fca";

        String sql3 = "select distinct ca.cba_accnt_status_cd from dtsdm.cba_accnt_wh ca\n" +
                        "where ca.cba_accnt_status_cd not in\n" +
                        "(select distinct fca.acct_status from fred.cba_account fca)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int dtsdmCount = 0;
        int fredCount = 0;
        int diffCount = 0;

        System.out.println("Starting CbaAccntWhTest.test6,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dtsdmCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        fredCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test6 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test6,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        diffCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test6 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((diffCount == 0),"(diffCount == 0)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 6: Count From DTSDM.CBA_ACCNT_WH = " + dtsdmCount);
        System.out.println("Test 6: Count From FRED.CBA_ACCOUNT = " + fredCount);
        System.out.println("Test 6: Discrepancy Count = " + diffCount);

        assertEquals(0, diffCount);

        System.out.println("Finish CbaAccntWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the CBA_ACCNT_WH.CURR_SW column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct CBA_ACCNT_WH.CURR_SW, count(*)\n" +
                        "from DTSDM.CBA_ACCNT_WH group by CBA_ACCNT_WH.CURR_SW";

        String sql2 = "select count(*)from cba_accnt_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;

        System.out.println("Starting CbaAccntWhTest.test7,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test7,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test7 sql2 failed");
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

        System.out.println("Finish CbaAccntWhTest.test7");
        System.out.println();

    }

    /*
    @Test
    public void test8(){

        // Check the population of the CBA_ACCNT_WH.EFT_START_DT column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct trunc(CBA_ACCNT_WH.EFF_START_DT), count (*)\n" +
                        "from DTSDM.CBA_ACCNT_WH group by trunc(CBA_ACCNT_WH.EFF_START_DT)";

        String sql2 = "select count (*) from DTSDM.CBA_ACCNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;

        System.out.println("Starting CbaAccntWhTest.test8,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test8 sql2 failed");
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

        System.out.println("Finish CbaAccntWhTest.test8");
        System.out.println();

    }

    @Test
    public void test9(){

        // Check the population of the CBA_ACCNT_WH.EFT_END_DT column.

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "Select distinct CBA_ACCNT_WH.EFF_END_DT, count (*)\n" +
                        "from DTSDM.CBA_ACCNT_WH group by CBA_ACCNT_WH.EFF_END_DT";

        String sql2 = "select count (*) from DTSDM.CBA_ACCNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;

        System.out.println("Starting CbaAccntWhTest.test9,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CbaAccntWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("CbaAccntWh.test9 sql2 failed");
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

        System.out.println("Finish CbaAccntWhTest.test9");
        System.out.println();

    }
    */

}
