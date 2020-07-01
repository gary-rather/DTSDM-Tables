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
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Initial load must add  unspecified data row.  Provides the ability to traverse through the DTSDM. LEG_SPCL_DUTY_WH  table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql = "Select count(*) from DTSDM. LEG_SPCL_DUTY_WH where LEG_SPCL_DUTY_WH. LEG_SPCL_DUTY_WID = 0";

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
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.LEG_SPCL_DUTY_WID  (PK) column";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "Select count (distinct LEG_SPCL_DUTY_WH.LEG_SPCL_DUTY_WID) \n" +
                "from DTSDM. LEG_SPCL_DUTY_WH\n";
        String sql2 = "Select count(*)\n" +
                "from DTSDM. LEG_SPCL_DUTY_WH\n";
        String sql3 = "select distinct LEG_SPCL_DUTY_WH.LEG_SPCL_DUTY_WID, count(*)\n" +
                "from DTSDM.LEG_SPCL_DUTY_WH\n" +
                "group by LEG_SPCL_DUTY_WH.LEG_SPCL_DUTY_WID\n" +
                "having count(*) > 1\n";

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
    public void test03() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.TRIP_LEG_WID columns";
        String reason = " FK to TRIP_LEG_WH; lookup TRIP_LEG_WID using LODGE. LEG, U##VCHNUM, U##DOCTYPE, U##SSN in TRIP_LEG_WH and related tables";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test03,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test03,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test03");
        System.out.println();
    }

    @Test
    public void test04() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.DUTY_CONDTN_TYPE_WID column";
        String reason = " FK to TYPE_CONSOLDTD_RFRNC_WH; lookup DUTY_COND, U##TYPE values in TYPE_CD to get TYPE_WID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test04,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test04,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test04,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test04");
        System.out.println();
    }

    @Test
    public void test05() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH. SPCL_DUTY_STRT_DT column";
        String reason = " Get TRAVDATE from the first record having the duty condition, when sorted by date (for the start date)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test05,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test05,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test05,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test05");
        System.out.println();
    }

    @Test
    public void test06() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH. SPCL_DUTY_END_DT column";
        String reason = " Get TRAVDATE from the last consecutive record having the duty condition, when sorted by date (for the end date)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test06,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test06,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test06");
        System.out.println();
    }

    @Test
    public void test07() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.CURR_SW column";
        String reason = "  Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not currect";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test07,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test07,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test07,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test07");
        System.out.println();
    }

    @Test
    public void test08() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.EFF_START_DT column";
        String reason = " Default EFF_STRT_DT = sysdate";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test08,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test08,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test08,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test08");
        System.out.println();
    }

    @Test
    public void test09() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LEG_SPCL_DUTY_WH.EFF_END_DT column";
        String reason = " It should be 01-JAN-00";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test09,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test09,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test09,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test09");
        System.out.println();
    }

    @Test
    public void test10() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test10,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test10,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test10");
        System.out.println();
    }



    @Test
    public void test11() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test11,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test11,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test11,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test11");
        System.out.println();
    }

    @Test
    public void test12() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test12,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test12");
        System.out.println();
    }

    @Test
    public void test13() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test13,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test13,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test13,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test13");
        System.out.println();
    }

    @Test
    public void test14() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test14,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test14,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test14,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test14");
        System.out.println();
    }

    @Test
    public void test15() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test15,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test15,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test15,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test15");
        System.out.println();
    }

    @Test
    public void test16() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test16,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test16,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test16,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test16");
        System.out.println();
    }

    @Test
    public void test17() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test17,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test17,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test17,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test17");
        System.out.println();
    }

    @Test
    public void test18() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test18,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test18,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test18,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test18");
        System.out.println();
    }

    @Test
    public void test19() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "";
        String sql2 = "";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj3);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LegSpclDutyWhTest.test19,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test19,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
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
        System.out.println("Starting LegSpclDutyWhTest.test19,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
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
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test LegSpclDutyWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish LegSpclDutyWhTest.test19");
        System.out.println();
    }



}
