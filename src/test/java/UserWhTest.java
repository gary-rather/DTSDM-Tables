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
public class UserWhTest extends TableTest {

    @BeforeClass
    public static void openResults() {
        wr = new WriteResults("UserWhTest.html");
        wr.pageHeader();
    }

    @Test
    /*
     */
    public void test01() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        wr.logSql(theSql);

        int row0Count = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        row0Count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((1 == row0Count), "(1 == row0Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (1 == row0Count) " + row0Count);
        assertEquals(1, row0Count);

        System.out.println("Finish Trip_wh.test1");
    }

    @Test
    /*
     */
    public void test02() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";

        String sql3 = " ";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;
        int minusCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String wid = rs.getString(1);
                        int rCont = rs.getInt(1);
                         minusCount++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == minusCount), "(0 == minusCount)");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Test assert  (0 == minusCount) " + minusCount  );
        assertEquals(0 ,minusCount);

        System.out.println("Finish Trip_wh.test2");
    }

    @Test
    /*
     */
    public void test03() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "s";

        String sql2 = "";

        String sql3 = "";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql2.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;
        int minusCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String wid = rs.getString(1);
                        int rCont = rs.getInt(1);
                        minusCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == minusCount), "(0 == minusCount)");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Test assert  (0 == minusCount) " + minusCount  );
        assertEquals(0 ,minusCount);

        System.out.println("Finish Trip_wh.test3");
    }

    @Test
    /*
     */
    public void test04() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";

        String sql3 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        SqlObject sqlObj3 = new SqlObject("sql3", sql3.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj3);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;
        int minusCount = 0;
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        totalCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        String wid = rs.getString(1);
                        int rCont = rs.getInt(1);
                        minusCount++;

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        ResultObject ro2 = new ResultObject((0 == minusCount), "(0 == minusCount)");
        roList.add(ro2);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Test assert  (0 == minusCount) " + minusCount  );
        assertEquals(0 ,minusCount);

        System.out.println("Finish Trip_wh.test4");
    }

    @Test
    /*
     */
    public void test05() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test5");
    }

    @Test
    /*
     */
    public void test06() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test6");
    }

    @Test
    /*
     */

    public void test07() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test7");
    }

    @Test
    /*
     */
    @Ignore
    public void test08() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test8");
    }

    @Test
    /*
     */

    public void test09() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int sql1Count = -1;
        int sql2Count = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        sql1Count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        sql2Count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((sql1Count == sql2Count), "(sql1Count == sql2Count)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (sql1Count == sql2Count) " + sql1Count  + " == " + sql2Count);
        assertEquals(sql1Count ,sql2Count);

        System.out.println("Finish Trip_wh.test9");
    }

    @Test
    /*
     */
    public void test10() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        //System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        //assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test10");
    }

    @Test
    /*
     */
    public void test11() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        //System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        //assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test11");
    }

    @Test
    /*
     */
    public void test12() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        //System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        //assertEquals(distinctCount ,totalCount);

        System.out.println("Finish Trip_wh.test12");
    }

    @Test
    /*
     */

    public void test13() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        //System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        //assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    public void test14() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test15() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test16() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test17() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test18() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test19() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test20() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test21() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test22() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = " ";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test23() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test24() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test25() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test26() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "n";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test27() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "s";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test28() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test29() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test30() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test31() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test32() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test33() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

    @Test
    /*
     */
    @Ignore
    public void test34() {
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printComment(" ");

        String sql1 = "";

        String sql2 = "";


        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj1);
        SqlObject sqlObj2 = new SqlObject("sql2", sql1.replaceAll("\n", "\n<br>"));
        theSql.add(sqlObj2);
        wr.logSql(theSql);

        int distinctCount = -1;
        int totalCount = -1;

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    //System.out.println("Size of results = " + rs.getInt(1));
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
        ResultObject ro1 = new ResultObject((distinctCount == totalCount), "(distinctCount == totalCount)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test assert  (distinctCount == totalCount) " + distinctCount  + " == " + totalCount);
        assertEquals(distinctCount ,totalCount);

        System.out.println("Finish " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
    }

}
