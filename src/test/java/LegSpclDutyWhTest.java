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
public class LegSpclDutyWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("LegSpclDutyWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "Select count(*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test01,sql");
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

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test01");
        System.out.println();

    }

    @Test
    public void test02() {

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select count (distinct LEG_SPCL_DUTY_WH.LEG_SPCL_DUTY_WID) \n" +
                        "from DTSDM. LEG_SPCL_DUTY_WH";

        String sql2 = "select count(*) from DTSDM. LEG_SPCL_DUTY_WH\n";

        String sql3 = "select distinct LEG_SPCL_DUTY_WID, count(*) \n" +
                        "from DTSDM.LEG_SPCL_DUTY_WH \n" +
                        "group by LEG_SPCL_DUTY_WID having count(*) > 1";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();

        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);

        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);

        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);

        wr.logSql(theSql);

        int distinctCount = 0;
        int totalCount = 0;
        int dupeCount = 0;

        System.out.println("Starting LegSpclDutyWhTest.test02,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test02,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test02,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dupeCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();

        ResultObject ro1 = new ResultObject((totalCount == distinctCount),"(totalCount == distinctCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
        roList.add(ro2);
 
        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  totalCount == distinctCount " + totalCount);
        assertEquals(distinctCount, totalCount);

        System.out.println("Test LegSpclDutyWh  0 == dupeCount " + dupeCount);
        assertEquals(0, dupeCount);

        System.out.println("Finish LegSpclDutyWhTest.test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the TRIP_LEG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count (*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.LEG_SPCL_DUTY_WID, A.TRIP_LEG_WID, B.TRIP_LEG_WID \n" +
                        "\t FROM DTSDM.LEG_SPCL_DUTY_WH A, DTSDM.TRIP_LEG_WH B, DTSDM_SRC_STG.LODGE C \n" +
                        "\t WHERE A.TRIP_LEG_WID = B.TRIP_LEG_WID \n" +
                        "\t AND B.LEG_NUM = C.LEG \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
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

        System.out.println("Starting LegSpclDutyWhTest.test03,sql1");
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
            System.out.println("LegSpclDutyWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test03,sql2");
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
            System.out.println("LegSpclDutyWh.test03 sql2 failed");
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

        System.out.println("Finish LegSpclDutyWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the DUTY_CONDTN_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count (*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.LEG_SPCL_DUTY_WID, A.DUTY_CONDTN_TYPE_WID, C.TYPE_WID \n" +
                        "\t FROM DTSDM.LEG_SPCL_DUTY_WH A, DTSDM_SRC_STG.ACTUAL B, \n" +
                        "\t\t\t DTSDM.TYPE_CONSOLDTD_RFRNC_WH C, DTSDM_SRC_STG.LODGE D \n" +
                        "\t WHERE A.DUTY_COUNDTN_TYPE_WID = C.TYPE_WID \n" +
                        "\t AND B.U##TYPE = C.TYPE_DESCR \n" +
                        "\t AND C.TYPE_DESCR = D.DUTY_COND \n" +
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

        System.out.println("Starting LegSpclDutyWhTest.test04,sql1");
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
            System.out.println("LegSpclDutyWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test04,sql2");
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
            System.out.println("LegSpclDutyWh.test04 sql2 failed");
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

        System.out.println("Finish LegSpclDutyWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the SPCL_DUTY_STRT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count (*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "select distinct a.leg_spcl_duty_wid, a.spcl_duty_strt_dt, b.travdate\n" +
                        "from dtsdm.leg_spcl_duty_wh a, fred.lodge b\n" +
                        "where a.spcl_duty_strt_dt = b.travdate\n" +
                        "and a.spcl_duty_strt_dt in (select min(travdate)from fred.lodge\n" +
                        "                                where duty_cond is not null\n" +
                        "                                and duty_cond != ' '\n" +
                        "                                group by duty_cond)" +
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

        System.out.println("Starting LegSpclDutyWhTest.test05,sql1");
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
            System.out.println("LegSpclDutyWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test05,sql2");
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
            System.out.println("LegSpclDutyWh.test05 sql2 failed");
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

        System.out.println("Finish LegSpclDutyWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the SPCL_DUTY_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count (*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "select distinct a.leg_spcl_duty_wid, a.spcl_duty_end_dt, b.travdate\n" +
                        "from dtsdm.leg_spcl_duty_wh a, fred.lodge b\n" +
                        "where a.spcl_duty_end_dt = b.travdate\n" +
                        "and a.spcl_duty_strt_dt in (select max(travdate)from fred.lodge\n" +
                        "                                where duty_cond is not null\n" +
                        "                                and duty_cond != ' '\n" +
                        "                                group by duty_cond)" +
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

        System.out.println("Starting LegSpclDutyWhTest.test06,sql1");
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
            System.out.println("LegSpclDutyWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test06,sql2");
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
            System.out.println("LegSpclDutyWh.test06 sql2 failed");
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

        System.out.println("Finish LegSpclDutyWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07(){

        // Check the population of the SPCL_DUTY_CITY_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count (*) from DTSDM.LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.LEG_SPCL_DUTY_WID, A.SPCL_DUTY_CITY_NAME, \n" +
                        "\t\t\t NVL2(TRIM(B.LOCATE), B.LOCATE, B.LOCATION) \n" +
                        "\t FROM DTSDM.LEG_SPCL_DUTY_WH A, DTSDM_SRC_STG.LODGE B \n" +
                        "\t WHERE A.SPCL_DUTY_CITY_NAME = B.LOCATE \n" +
                        "\t OR A.SPCL_DUTY_CITY_NAME = B.LOCATION \n" +
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

        System.out.println("Starting LegSpclDutyWhTest.test07,sql1");
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
            System.out.println("LegSpclDutyWh.test07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LegSpclDutyWhTest.test07,sql2");
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
            System.out.println("LegSpclDutyWh.test07 sql2 failed");
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

        System.out.println("Finish LegSpclDutyWhTest.test07");
        System.out.println();

    }

}
