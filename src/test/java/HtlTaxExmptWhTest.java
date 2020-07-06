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
public class HtlTaxExmptWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("HtlTaxExmptWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " check that the unknown record 0 is populated";
        String reason = " Initial load must add  unspecified data row. Provides the ability to traverse through the DTSDM.HTL_TAX_EXMPT_WH table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select * from DTSDM.HTL_TAX_EXMPT_WH A where A.HTL_TAX_EXMPT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting HtlTaxExmptWhTest.test1,sql");
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

        System.out.println("Test HtlTaxExmptWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish HtlTaxExmptWhTest.test1");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (HTL_TAX_EXMPT_WH.HTL_TAX_EXMPT_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the unique identifier (HTL_TAX_EXMPT_WH.HTL_TAX_EXMPT_WID (PK))";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct A.HTL_TAX_EXMPT_WID, count(*) from DTSDM.HTL_TAX_EXMPT_WH A \n" +
                        "group by A.HTL_TAX_EXMPT_WID having count(*) > 1";

        String sql2 = "select count (distinct A.HTL_TAX_EXMPT_WID) from DTSDM.HTL_TAX_EXMPT_WH A";
        String sql3 = "select count(*) from DTSDM.HTL_TAX_EXMPT_WH";

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

        System.out.println("Starting HtlTaxExmptWhTest.test2,sql1");
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
            System.out.println("HtlTaxExmptWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test2,sql2");
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
            System.out.println("HtlTaxExmptWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test2,sql3");
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
            System.out.println("HtlTaxExmptWh.test2 sql3 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        // Check the population of the HTL_TAX_EXMPT_WH.STATE_COUNTRY_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the HTL_TAX_EXMPT_WH.STATE_COUNTRY_CD column";
        String reason = "  (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.HTL_TAX_EXMPT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  select a.htl_tax_exmpt_wid, a.state_country_cd, b.state_code \n" +
                        "  from dtsdm.htl_tax_exmpt_wh a, dtsdm_src_stg.htl_tax_exmpt_location b \n" +
                        "  where a.state_country_cd = b.state_code \n" +
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

        System.out.println("Starting HtlTaxExmptWhTest.test3,sql1");
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
            System.out.println("HtlTaxExmptWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test3,sql2");
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
            System.out.println("HtlTaxExmptWh.test3 sql2 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        // Check the population of the HTL_TAX_EXMPT_WH.STATE_COUNTRY_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the HTL_TAX_EXMPT_WH.STATE_COUNTRY_WID column";
        String reason = " Join HTL_TAX_EXMPT_LOCATION.STATE_CODE to STATE_COUNTRY_RFRNC_WH.STATE_COUNTRY_CD Default value = 0";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.HTL_TAX_EXMPT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  select a.htl_tax_exmpt_wid, \n" +
                        "    a.state_country_wid, \n" +
                        "    b.state_country_wid \n" +
                        "\n" +
                        "  from dtsdm.htl_tax_exmpt_wh a, \n" +
                        "    dtsdm.state_country_rfrnc_wh b, \n" +
                        "    dtsdm_src_stg.htl_tax_exmpt_location c \n" +
                        "\n" +
                        "  where a.state_country_wid = b.state_country_wid \n" +
                        "  and b.state_country_cd = c.state_code \n" +
                        "  and c.state_code = a.state_country_cd \n" +
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

        System.out.println("Starting HtlTaxExmptWhTest.test4,sql1");
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
            System.out.println("HtlTaxExmptWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test4,sql2");
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
            System.out.println("HtlTaxExmptWh.test4 sql2 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        // Check the population of the HTL_TAX_EXMPT_WH.TAX_EXMPT_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the HTL_TAX_EXMPT_WH.TAX_EXMPT_CD column";
        String reason = " HTL_TAX_EXMPT_LOCATION.STATE_CODE joins to STATE.STCODE";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.HTL_TAX_EXMPT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  select a.htl_tax_exmpt_wid, \n" +
                        "    a.htl_tax_exmpt_cd, \n" +
                        "    b.tax_exmpt_type \n" +
                        "\n" +
                        "  from dtsdm.htl_tax_exmpt_wh a, \n" +
                        "    dtsdm_src_stg.htl_tax_exmpt_location b, \n" +
                        "    dtsdm_src_stg.state c \n" +
                        "\n" +
                        "  where a.htl_tax_exmpt_cd = b.tax_exmpt_type \n" +
                        "  and b.state_code = c.stcode \n" +
                        "  and c.stcode = a.state_country_cd \n" +
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

        System.out.println("Starting HtlTaxExmptWhTest.test5,sql1");
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
            System.out.println("HtlTaxExmptWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test5,sql2");
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
            System.out.println("HtlTaxExmptWh.test5 sql2 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        // Check the population of the HTL_TAX_EXMPT_WH.HTL_TAX_EXP_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the HTL_TAX_EXMPT_WH.HTL_TAX_EXP_DT column";
        String reason = " (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.HTL_TAX_EXMPT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  select a.htl_tax_exmpt_wid, a.htl_tax_exp_dt, b.date_expiry \n" +
                        "  from dtsdm.htl_tax_exmpt_wh a, dtsdm_src_stg.htl_tax_exmpt_location b \n" +
                        "  where a.htl_tax_exp_dt = b.date_expiry \n" +
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

        System.out.println("Starting HtlTaxExmptWhTest.test6,sql1");
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
            System.out.println("HtlTaxExmptWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test6,sql2");
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
            System.out.println("HtlTaxExmptWh.test6 sql2 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the HTL_TAX_EXMPT_WH.HTL_TAX_EXP_SRC_ID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the HTL_TAX_EXMPT_WH.HTL_TAX_EXP_SRC_ID column";
        String reason = " (straight pull)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.HTL_TAX_EXMPT_WH";

        String sql2 = "select count(*) from \n" +
                        "( \n" +
                        "  select a.htl_tax_exmpt_wid, a.htl_tax_exmpt_src_id, b.htl_tax_exmpt_location_id \n" +
                        "  from dtsdm.htl_tax_exmpt_wh a, dtsdm_src_stg.htl_tax_exmpt_location b \n" +
                        "  where a.htl_tax_exmpt_src_id = b.htl_tax_exmpt_location_id \n" +
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

        System.out.println("Starting HtlTaxExmptWhTest.test7,sql1");
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
            System.out.println("HtlTaxExmptWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting HtlTaxExmptWhTest.test7,sql2");
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
            System.out.println("HtlTaxExmptWh.test7 sql2 failed");
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

        System.out.println("Finish HtlTaxExmptWhTest.test7");
        System.out.println();

    }

}
