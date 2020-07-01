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
public class DebtSmryWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("DebtSmryWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        // check that the unknown record 0 is populated
        // EXPECT: DEBT_SMRY_WID = 0 and DCMNT_WID = 0 RCRD_TYPE_CD = 'UNK'; RCRD_TYPE_DESCR = 'UNKNOWN'; others NULL

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check if the unknown record 0 is populated";
        String reason = " Provides the ability to traverse through the DEBT_SMRY_WH when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select * from DTSDM.DEBT_SMRY_WH \n" +
                        "where DEBT_SMRY_WH.DEBT_SMRY_WID = 0 and DEBT_SMRY_WH.DCMNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql", sql.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DebtSmryWhTest.test1,sql");
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

        System.out.println("Test DebtSmryWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtSmryWhTest.test1");

    }

    @Test
    public void test02() {

        //Check the population of the unique identifier (DEBT_SMRY_WH.DEBT_SMRY_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the unique identifier DEBT_SMRY_WH.DEBT_SMRY_WID (PK) column";
        String reason = " Sequential ID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct DEBT_SMRY_WID, count(*) from DTSDM.DEBT_SMRY_WH \n" +
                        "group by DEBT_SMRY_WID having count(*) > 1";

        String sql2 = "select count (distinct DEBT_SMRY_WH.DEBT_SMRY_WID) from DTSDM.DEBT_SMRY_WH";
        String sql3 = "select count(*) from DTSDM.DEBT_SMRY_WH";

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

        System.out.println("Starting DebtSmryTicketWhTest.test2,sql1");
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
            System.out.println("DebtSmryWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test2,sql2");
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
            System.out.println("DebtSmryWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test2,sql3");
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
            System.out.println("DebtSmryWh.test2 sql3 failed");
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

        System.out.println("Finish DebtSmryWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03() {

        // Check the population of the DEBT_SMRY_WH.DCMNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.DCMNT_WID column";
        String reason = " Lookup PM_DEBT.VCHNUM, U##DOCTYPE, U##SSN in VCHR_WH & related WH tables to get VHCR_WID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct dc.dcmnt_wid from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.pm_debt pd \n" +
                        "where dc.dcmnt_name = pd.u##vchnum and dc.src_ssn = pd.u##ssn \n" +
                        "and dc.src_doctype = pd.u##doctype and dc. adjstmt_lvl = pd.adj_level";

        String sql2 = "select distinct ds.dcmnt_wid from dtsdm.debt_smry_wh ds";

        String sql3 = "Select distinct ds.dcmnt_wid from dtsdm.debt_smry_wh ds \n" +
                        "where ds.dcmnt_wid not in (Select distinct dc.dcmnt_wid \n" +
                                                    "from dtsdm.dcmnt_wh dc, DTSDM_SRC_STG.pm_debt pd \n" +
                                                    "where dc.dcmnt_name = pd.u##vchnum and dc.src_ssn = pd.u##ssn \n" +
                                                    "and dc.src_doctype = pd.u##doctype and dc. adjstmt_lvl = pd.adj_level)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test3 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test3,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test3 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 3: Test Count = " + testCount);
        System.out.println("Test 3: Comparison Count = " + comparisonCount);
        System.out.println("Test 3: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test3");
        System.out.println();

    }

    @Test
    public void test04() {

        // Check the population of the DEBT_SMRY_WH.RCRD_TYPE_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.RCRD_TYPE_CD column";
        String reason = " Value = \"DT\" for all records";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DEBT_SMRY_WH";
        String sql2 = "select DEBt_SMRY_WH.RCRD_TYPE_CD from DTSDM.DEBT_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DebtSmryWhTest.test04,sql1");
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
            System.out.println("DebtSmryWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test04,sql2");
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
            System.out.println("DebtSmryWh.test04 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 04: Comparison Count = " + comparisonCount);
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DebtSmryWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05() {

        // Check the population of the DEBT_SMRY_WH.RCRD_TYPE_DESCR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.RCRD_TYPE_DESCR column";
        String reason = " Value = \"DTS Trip Debt\" for all records";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DEBT_SMRY_WH";
        String sql2 = "select DEBt_SMRY_WH.RCRD_TYPE_DESCR from DTSDM.DEBT_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DebtSmryWhTest.test05,sql1");
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
            System.out.println("DebtSmryWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test05,sql2");
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
            System.out.println("DebtSmryWh.test05 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 05: Comparison Count = " + comparisonCount);
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DebtSmryWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06() {

        // Check the population of the DEBT_SMRY_WH.DEBT_INCRD_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.DEBT_INCRD_DT column";
        String reason = "  straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from\n" +
                        "(\n" +
                        "select distinct pd.orig_debt_date, count(*) \n" +
                        "from DTSDM_SRC_STG.pm_debt pd group by pd.orig_debt_date \n" +
                        "--having count(*) > 1 \n" +
                        ")";

        String sql2 = "select sum(records_cnt) from\n" +
                        "(\n" +
                        "select distinct ds.debt_incrd_dt, count(*) records_cnt \n" +
                        "from dtsdm.debt_smry_wh ds group by ds.debt_incrd_dt \n" +
                        "--having count(*) > 1 \n" +
                        ")";

        String sql3 = "Select distinct ds.debt_incrd_dt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.debt_incrd_dt not in \n" +
                        "(select distinct pd.orig_debt_date from DTSDM_SRC_STG.pm_debt pd)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test6,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test6 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test6,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test6 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 6: Test Count = " + testCount);
        System.out.println("Test 6: Comparison Count = " + comparisonCount);
        System.out.println("Test 6: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test6");
        System.out.println();

    }

    @Ignore
    @Test
    public void test07(){
        // no test right now (no information provided)
    }

    @Test
    public void test08() {

        // Check the population of the DEBT_SMRY_WH.DEBT_ORIG_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.DEBT_ORIG_AMT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pd.orig_debt_amt from DTSDM_SRC_STG.pm_debt pd";
        String sql2 = "select ds.debt_orig_amt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.debt_orig_amt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.debt_orig_amt not in \n" +
                        "(select pd.orig_debt_amt from DTSDM_SRC_STG.pm_debt pd)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test8,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test8 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test8,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test8 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 8: Test Count = " + testCount);
        System.out.println("Test 8: Comparison Count = " + comparisonCount);
        System.out.println("Test 8: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test8");
        System.out.println();

    }

    @Ignore
    @Test
    public void test09() {

        // Check the population of the DEBT_SMRY_WH.COLLECT_TMR_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = "  Check the population of the DEBT_SMRY_WH.COLLECT_TMR_DT column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pd.pm_collect_timer from DTSDM_SRC_STG.pm_debt pd";
        String sql2 = "select ds.collect_tmr_txt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.collect_tmr_txt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.collect_tmr_txt not in \n" +
                        "(select pd.pm_collect_timer from DTSDM_SRC_STG.pm_debt pd)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test9,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test9 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test9,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test9 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 9: Test Count = " + testCount);
        System.out.println("Test 9: Comparison Count = " + comparisonCount);
        System.out.println("Test 9: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test9");
        System.out.println();

    }

    @Test
    public void test10() {

        // Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_STATUS_TXT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_STATUS_TXT column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pdh.status from DTSDM_SRC_STG.pm_debt_hist pdh";
        String sql2 = "select ds.lst_debt_trns_status_txt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.lst_debt_trns_status_txt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.lst_debt_trns_status_txt not in \n" +
                        "(select pdh.status from DTSDM_SRC_STG.pm_debt_hist pdh)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test10,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test10 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test10,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test10 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 10: Test Count = " + testCount);
        System.out.println("Test 10: Comparison Count = " + comparisonCount);
        System.out.println("Test 10: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11() {

        // Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_AMT column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pdh.trans_amt from DTSDM_SRC_STG.pm_debt_hist pdh";
        String sql2 = "select ds.lst_debt_trns_amt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.lst_debt_trns_amt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.lst_debt_trns_amt not in \n" +
                        "(select pdh.trans_amt from DTSDM_SRC_STG.pm_debt_hist pdh)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test11,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test11,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test11 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test11,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test11 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 11: Test Count = " + testCount);
        System.out.println("Test 11: Comparison Count = " + comparisonCount);
        System.out.println("Test 11: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12() {

        // Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.LST_DEBT_TRNS_DT column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pdh.seq_date from DTSDM_SRC_STG.pm_debt_hist pdh";
        String sql2 = "select ds.lst_debt_trns_dt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.lst_debt_trns_dt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.lst_debt_trns_dt not in \n" +
                        "(select pdh.seq_date from DTSDM_SRC_STG.pm_debt_hist pdh)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test12 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test12,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test12 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 12: Test Count = " + testCount);
        System.out.println("Test 12: Comparison Count = " + comparisonCount);
        System.out.println("Test 12: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13() {

        // Check the population of the DEBT_SMRY_WH.CURR_DUE_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.CURR_DUE_AMT column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pdh.curr_debt_bal from DTSDM_SRC_STG.pm_debt_hist pdh";
        String sql2 = "select ds.curr_due_amt from dtsdm.debt_smry_wh ds";

        String sql3 = "select ds.curr_due_amt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.curr_due_amt not in \n" +
                        "(select pdh.curr_debt_bal from DTSDM_SRC_STG.pm_debt_hist pdh)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test13,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test13,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test13 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test13,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test13 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 13: Test Count = " + testCount);
        System.out.println("Test 13: Comparison Count = " + comparisonCount);
        System.out.println("Test 13: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test13");
        System.out.println();

    }

    @Ignore
    @Test
    public void test14() {

        // Check the population of the DEBT_SMRY_WH.SRC_ID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.SRC_ID column";
        String reason = " straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select pd.pm_collect_timer from DTSDM_SRC_STG.pm_debt pd";
        String sql2 = "select ds.collect_tmr_dt from dtsdm.debt_smry_wh ds";

        String sql3 = "Select ds.collect_tmr_dt from dtsdm.debt_smry_wh ds \n" +
                        "where ds.collect_tmr_dt not in \n" +
                        "(select pd.pm_collect_timer from DTSDM_SRC_STG.pm_debt pd)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int testCount = 0;
        int comparisonCount = 0;
        int errorCount = 0;

        System.out.println("Starting DebtSmryTicketWhTest.test14,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test14 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test14,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test14 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test14,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        errorCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DebtSmryWh.test14 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((errorCount == 0), "(errorCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 14: Test Count = " + testCount);
        System.out.println("Test 14: Comparison Count = " + comparisonCount);
        System.out.println("Test 14: Error Count =  " + errorCount);

        assertEquals(comparisonCount, testCount);
        assertEquals(0, errorCount);

        System.out.println("Finish DebtSmryWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15() {

        // Check the population of the DEBT_SMRY_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.CURR_SW column";
        String reason = " Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not current";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DEBT_SMRY_WH";
        String sql2 = "select DEBT_SMRY_WH.CURR_SW from DTSDM.DEBT_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DebtSmryWhTest.test15,sql1");
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
            System.out.println("DebtSmryWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test15,sql2");
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
            System.out.println("DebtSmryWh.test15 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 15: Comparison Count = " + comparisonCount);
        System.out.println("Test 15: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DebtSmryWhTest.test15");
        System.out.println();

    }

    @Ignore
    @Test
    public void test16() {

        // Check the population of the DEBT_SMRY_WH.EFF_START_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.EFF_START_DT column";
        String reason = " Default EFF_STRT_DT = sysdate";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DEBT_SMRY_WH";
        String sql2 = "select trunc(DEBT_SMRY_WH.EFF_START_DT) from DTSDM.DEBT_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DebtSmryWhTest.test16,sql1");
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
            System.out.println("DebtSmryWh.test16 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test16,sql2");
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
            System.out.println("DebtSmryWh.test16 sql2 failed");
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

        System.out.println("Finish DebtSmryWhTest.test16");
        System.out.println();

    }

    @Ignore
    @Test
    public void test17() {

        // Check the population of the DEBT_SMRY_WH.EFF_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the DEBT_SMRY_WH.EFF_END_DT column";
        String reason = "  It should be 01-JAN-00";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.DEBT_SMRY_WH";
        String sql2 = "select DEBt_SMRY_WH.EFF_END_DT from DTSDM.DEBT_SMRY_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DebtSmryWhTest.test17,sql1");
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
            System.out.println("DebtSmryWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DebtSmryWhTest.test17,sql2");
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
            System.out.println("DebtSmryWh.test17 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount), "(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 17: Comparison Count = " + comparisonCount);
        System.out.println("Test 17: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DebtSmryWhTest.test17");
        System.out.println();

    }

}
