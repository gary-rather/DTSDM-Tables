import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DebtWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("DebtWhTest.html");
        wr.pageHeader();
    }


    @Test
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql = "Select count(*) \n" + "from DTSDM.DEBT_WH \n" + " where DEBT_WH.DEBT_WID = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DebtWhTest.test01,sql");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test01");
        System.out.println();
    }

    @Test
    public void test02() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "Select count (distinct DEBT_WH.DEBT_WID) \n" +
                "from DTSDM. DEBT_WH";
        String sql2 = "Select count(*)\n" +
                "from DTSDM.DEBT_WH\n";
        String sql3 = "select distinct DEBT_WH.DEBT_WID, count(*)\n" +
                "from DTSDM. DEBT_WH\n" +
                "group by DEBT_WH.DEBT_WID\n" +
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

        System.out.println("Starting DebtWhTest.test02,sql1");
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
        System.out.println("Starting DebtWhTest.test02,sql2");
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
        System.out.println("Starting DebtWhTest.test02,sql3");
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount),"(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
        roList.add(ro2);


        wr.logTestResults(roList);

        System.out.println("Test DebtWh  distinctCount == totalCount " + (distinctCount == totalCount));
        assertEquals(distinctCount , totalCount);

        System.out.println("Test DebtWh  0 == dupeCount " + (0 == dupeCount));
        assertEquals(0 , dupeCount);

        System.out.println("Finish DebtWhTest.test02");
        System.out.println();
    }

    @Test
    public void test03() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "Select count(*)\n" +
                "from DTSDM.DEBT_WH\n";

        String sql2 = "select count(*) from \n" +
                "(\n" +
                "select A.DCMNT_WID, B.DCMNT_WID\n" +
                "from DTSDM.DEBT_WH A,\n" +
                "       DTSDM.DCMNT_WH B\n" +
                "where  A.DCMNT_WID = B.DCMNT_WID      \n" +
                ")";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);



        wr.logSql(theSql);

        int totalCount = 0;
        int distinctCount = 0;

        System.out.println("Starting DebtWhTest.test03,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting DebtWhTest.test03,sql2");
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
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((totalCount == distinctCount),"(totalCount == distinctCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test DebtWh  totalCount == distinctCount " + (totalCount == distinctCount));
        assertEquals(totalCount, distinctCount);

        System.out.println("Finish DebtWhTest.test03");
        System.out.println();
    }

    @Test
    @Ignore
    public void test04() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printYellowDiv("Querys not defined");

        String sql1 = "";

        String sql2 = "" ;

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

        System.out.println("Starting DebtWhTest.test04,sql1");
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
        System.out.println("Starting DebtWhTest.test04,sql2");
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
        System.out.println("Starting DebtWhTest.test04,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test04");
        System.out.println();
    }

    @Test
    public void test05() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printYellowDiv("Querys not working correctly");

        String sql1 = "Select distinct a.TYPE_WID\n" +
                "From DTSDM.TYPE_CONSOLDTD_RFRNC_WH a,\n" +
                "           DTSDM_SRC_STG.PM_DEBT_HIST b,\n" +
                "           DTSDM_SRC_STG.PM_DOC c\n" +
                "Where a.TYPE_DESCR =  b.TYPE \n" +
                "and        a.TYPE_DESCR != c.TRANS_TYPE";

        String sql2 = "Select distinct a.TYPE_WID\n" +
                "From DTSDM.TYPE_CONSOLDTD_RFRNC_WH a,\n" +
                "           DTSDM_SRC_STG.PM_DEBT_HIST b,\n" +
                "           DTSDM_SRC_STG.PM_DOC c\n" +
                "Where a.TYPE_DESCR !=  b.TYPE \n" +
                "and        a.TYPE_DESCR = c.TRANS_TYPE";
        String sql3 = "select distinct TRNSCTN_TYPE_WID, count (*)\n" +
                "from debt_wh\n" +
                "group by TRNSCTN_TYPE_WID\n" +
                "order by TRNSCTN_TYPE_WID";


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

        System.out.println("Starting DebtWhTest.test05,sql1");
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
        System.out.println("Starting DebtWhTest.test05,sql2");
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
        System.out.println("Starting DebtWhTest.test05,sql3");
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

        //System.out.println("Test DebtWh  Row 0 1= " + number);
        //assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test05");
        System.out.println();
    }

    @Test
    public void test06() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "select count(*) from\n" +
                "(\n" +
                "Select distinct DEBT_SMRY_WID\n" +
                "From DTSDM.DEBT_SMRY_WH\n" +
                ")";
        String sql2 = "select count(table_row_cnt),SUM(table_row_cnt) from \n" +
                "(\n" +
                "select distinct DEBT_SMRY_WID, count (*) as table_row_cnt\n" +
                "from DTSDM. DEBT_WH\n" +
                "group by DEBT_SMRY_WID \n" +
                "order by DEBT_SMRY_WID \n" +
                ")";
        String sql3 = "select count(*) from \n" +
                "(\n" +
                "select distinct DEBT_SMRY_WID, count (*) as table_row_cnt\n" +
                "from DTSDM.DEBT_WH\n" +
                "group by DEBT_SMRY_WID \n" +
                "order by DEBT_SMRY_WID \n" +
                ")";

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
        int sql2Count  = 0;
        int totalCount = 0;
        int sql3Count  = 0;

        System.out.println("Starting DebtWhTest.test06,sql1");
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
        System.out.println("Starting DebtWhTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        sql2Count = rs.getInt(1);
                        totalCount = rs.getInt(2);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting DebtWhTest.test06,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        sql3Count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == sql2Count),"(distinctCount == sql2Count)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((distinctCount == sql3Count),"(distinctCount == sql3Count)");
        roList.add(ro2);
         wr.logTestResults(roList);

        System.out.println("Test DebtWh  distinctCount == sql2Count " + (distinctCount == sql2Count));
        assertEquals(distinctCount, sql2Count);

        System.out.println("Test DebtWh  distinctCount == sql3Count " + (distinctCount == sql3Count));
        assertEquals(distinctCount, sql3Count);

        System.out.println("Finish DebtWhTest.test06");
        System.out.println();
    }

    @Test
    @Ignore
    public void test07() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test07,sql1");
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
        System.out.println("Starting DebtWhTest.test07,sql2");
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
        System.out.println("Starting DebtWhTest.test07,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test07");
        System.out.println();
    }

    @Test
    @Ignore
    public void test08() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test08,sql1");
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
        System.out.println("Starting DebtWhTest.test08,sql2");
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
        System.out.println("Starting DebtWhTest.test08,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test08");
        System.out.println();
    }

    @Test
    @Ignore
    public void test09() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test09,sql1");
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
        System.out.println("Starting DebtWhTest.test09,sql2");
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
        System.out.println("Starting DebtWhTest.test09,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test09");
        System.out.println();
    }

    @Test
    @Ignore
    public void test10() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test10,sql1");
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
        System.out.println("Starting DebtWhTest.test10,sql2");
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
        System.out.println("Starting DebtWhTest.test10,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test10");
        System.out.println();
    }



    @Test
    @Ignore
    public void test11() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql = "Select count(*) \n" + "from DTSDM.DEBT_WH \n" + " where DEBT_WH.DEBT_WID = 0 \n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DebtWhTest.test11,sql");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test11");
        System.out.println();
    }

    @Test
    @Ignore
    public void test12() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test12,sql1");
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
        System.out.println("Starting DebtWhTest.test12,sql2");
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
        System.out.println("Starting DebtWhTest.test12,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test12");
        System.out.println();
    }

    @Test
    @Ignore
    public void test13() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test13,sql1");
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
        System.out.println("Starting DebtWhTest.test13,sql2");
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
        System.out.println("Starting DebtWhTest.test13,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test13");
        System.out.println();
    }

    @Test
    @Ignore
    public void test14() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test14,sql1");
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
        System.out.println("Starting DebtWhTest.test14,sql2");
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
        System.out.println("Starting DebtWhTest.test14,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test14");
        System.out.println();
    }

    @Test
    @Ignore
    public void test15() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test15,sql1");
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
        System.out.println("Starting DebtWhTest.test15,sql2");
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
        System.out.println("Starting DebtWhTest.test15,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test15");
        System.out.println();
    }

    @Test
    public void test16() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "select distinct DEBT_WH.RCRD_TYPE_CD , count(*)\n" +
                "from DTSDM.DEBT_WH\n" +
                "group by DEBT_WH.RCRD_TYPE_CD ";
        String sql2 = "select count(*) from DEBT_WH";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);


        wr.logSql(theSql);

        int dtsCount = 0;
        int unkCount = 0;
        int totalCount = 0;

        System.out.println("Starting DebtWhTest.test16,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String type = rs.getString(1);
                        int count = rs.getInt(2);
                        if ("DTS".equalsIgnoreCase(type)) dtsCount = count;
                        if ("UNK".equalsIgnoreCase(type)) unkCount = count;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting DebtWhTest.test16,sql2");
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

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((totalCount == (dtsCount + unkCount)),"(totalCount == (dtsCount + unkCount))");
        roList.add(ro1);


        wr.logTestResults(roList);
        System.out.println("Test DebtWh  totalCount , dtsCount , unkCount) " + totalCount + ", " + dtsCount  + ", " + unkCount);
        System.out.println("Test DebtWh  (totalCount == (dtsCount + unkCount) " + (totalCount == (dtsCount + unkCount)));
        assertEquals(totalCount, dtsCount + unkCount);

        System.out.println("Finish DebtWhTest.test16");
        System.out.println();
    }

    @Test
    public void test17() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "select count(*) from DEBT_WH";

        String sql2 = "select distinct  DEBT_WH.RCRD_TYPE_DESCR , count(*)\n" +
                "from DTSDM.DEBT_WH\n" +
                "group by DEBT_WH.RCRD_TYPE_DESCR ";
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

        int distinctCount = 0;
        int totalCount = 0;
        int dtsCount = 0;
        int unkCount = 0;

        System.out.println("Starting DebtWhTest.test17,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        System.out.println("Starting DebtWhTest.test17,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String type = rs.getString(1);
                        int count = rs.getInt(2);
                        if ("DTS Debt".equalsIgnoreCase(type)) dtsCount = count;
                        if ("UNKNOWN".equalsIgnoreCase(type)) unkCount = count;


                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((totalCount == (dtsCount + unkCount)),"(totalCount == (dtsCount + unkCount))");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test DebtWh  totalCount , dtsCount , unkCount) " + totalCount + ", " + dtsCount  + ", " + unkCount);
        System.out.println("Test DebtWh  (totalCount == (dtsCount + unkCount) " + (totalCount == (dtsCount + unkCount)));
        assertEquals(totalCount, dtsCount + unkCount);

        System.out.println("Finish DebtWhTest.test17");
        System.out.println();
    }

    @Test
    @Ignore
    public void test18() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test18,sql1");
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
        System.out.println("Starting DebtWhTest.test18,sql2");
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
        System.out.println("Starting DebtWhTest.test18,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test18");
        System.out.println();
    }

    @Test
    @Ignore
    public void test19() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


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

        System.out.println("Starting DebtWhTest.test19,sql1");
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
        System.out.println("Starting DebtWhTest.test19,sql2");
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
        System.out.println("Starting DebtWhTest.test19,sql3");
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

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test19");
        System.out.println();
    }

    @Test
    public void test20() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "Select distinct DEBT_WH.CURR_SW, count(*)\n" +
                "From DTSDM. DEBT_WH\n" +
                "Group by DEBT_WH.CURR_SW\n";
        String sql2 = "select count(*) from DEBT_WH";
        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);


        wr.logSql(theSql);

        int totalCount = 0;
        int oneCount = 0;
        int nullCount = 0;

        System.out.println("Starting DebtWhTest.test20,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String type = rs.getString(1);
                        int count = rs.getInt(2);
                        if ("1".equalsIgnoreCase(type)) oneCount = count;
                        if (type == null) nullCount = count;


                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Starting DebtWhTest.test20,sql2");
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


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((totalCount == (oneCount + nullCount)),"(totalCount == (oneCount + nullCount))");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test DebtWh  totalCount , oneCount , nullCount) " + totalCount + ", " + oneCount  + ", " + nullCount);
        System.out.println("Test DebtWh  (totalCount == (oneCount + nullCount) " + (totalCount == (oneCount + nullCount)));
        assertEquals(totalCount, oneCount + nullCount);

        System.out.println("Finish DebtWhTest.test20");
        System.out.println();
    }



    @Test
    @Ignore
    public void test21() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "Select count (*)\n" +
                "From DTSDM. DEBT_WH;";

        String sql2 = "Select distinct trunc(DEBT_WH.EFF_START_DT), count (*)\n" +
                "From DTSDM. DEBT_WH\n" +
                "Group by trunc(DEBT_WH.EFF_START_DT)";
;
        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);

        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DebtWhTest.test21,sql");
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

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test21");
        System.out.println();
    }

    @Test
    @Ignore
    public void test22() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql1 = "Select distinct DEBT_WH.EFF_END_DT, count (*)\n" +
                "From DTSDM. DEBT_WH\n" +
                "Group by DEBT_WH.EFF_END_DT\n";
        String sql2 = "Select count(DEBT_WH.EFF_END_DT)\n" +
                "From DTSDM. DEBT_WH\n" +
                "Where DEBT_WH.EFF_END_DT is not null\n";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj2);


        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DebtWhTest.test22,sql1");
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
        System.out.println("Starting DebtWhTest.test22,sql2");
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


        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro2);
        ResultObject ro3 = new ResultObject((1 == number),"(1 == number)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("Test DebtWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish DebtWhTest.test22");
        System.out.println();
    }


}
