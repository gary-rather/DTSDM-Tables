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
public class SrvcWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("SrvcWhTest.html");
        wr.pageHeader();

    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated
        //EXPECT: SRVC_WID = 0; SRVC_CD = 'UNK'; SRVC_NAME = 'UNKNOWN'; PARNT_SRVC_NAME=?

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from DTSDM.SRVC_WH where SRVC_WH.SRVC_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting SrvcWhTest.test1,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
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
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((number == 1),"(number == 1)");
        roList.add(ro);
        wr.logTestResults(roList);

        System.out.println("Test SrvcWh  Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish SrvcWhTest.test1");
        System.out.println();

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (SRVC_WH.SRVC_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count (distinct SRVC_WH.SRVC_WID) from DTSDM.SRVC_WH";

        String sql2 = "select count(*) from DTSDM.SRVC_WH";

        String sql3 = "select distinct SRVC_WH. SRVC_WID, count(*)\n" +
                        "from DTSDM.SRVC_WH\n" +
                        "group by SRVC_WH. SRVC_WID\n" +
                        "having count(*) > 1";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql1",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql1",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int count = 0;
        int distinctCount = 0;
        int duplicateCount = 0;

        System.out.println("Starting SrvcWhTest.test2,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SrvcWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SrvcWhTest.test2,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SrvcWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SrvcWhTest.test2,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        duplicateCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SrvcWh.test2 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((count == distinctCount),"(count == distinctCount)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0),"(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 2: Count = " + count);
        System.out.println("Test 2: Distinct Count = " + distinctCount);
        assertEquals(count, distinctCount);

        assertEquals(0, duplicateCount);
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        System.out.println("Finish SrvcWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        //Check the population of the SRVC_WH.SRVC_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(NVL(srvc_cd,0)) from dtsdm.srvc_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting SrvcWhTest.test3,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == 7),"(count == 7)");
        roList.add(ro);
        wr.logTestResults(roList);

        //Count should be 7 (b/c there are 7 records w/ unknown), denoting proper population of this column
        System.out.println("Test SrvcWh: Number of srvc codes = " + count);
        assertEquals(7, count);

        System.out.println("Finish SrvcWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        //Check the population of the SRVC_WH.SRVC_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct srvc_wh.srvc_name,\n" +
                        "srvc_wh.parnt_srvc_name,\n" +
                        "count(*)\n" +
                        "from dtsdm.srvc_wh\n" +
                        "group by srvc_wh.srvc_name,\n" +
                        "srvc_wh.parnt_srvc_name";

        String sql2 = "select distinct site_to_org_mapping.service, site_to_org_mapping.parent_service, count(*)\n" +
                        "from dtsdm_src_stg.site_to_org_mapping\n" +
                        "where service != 'UNKNOWN'\n" +
                        "group by site_to_org_mapping.service, site_to_org_mapping.parent_service";

        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting SrvcWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Starting SrvcWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == comparisonCount),"(count == comparisonCount)");
        roList.add(ro);
        wr.logTestResults(roList);

        //The combination of these two columns should be unique
        //If the two counts are equal, then the combination of columns is indeed unique
        System.out.println("Sql1 count should equal Sql2 count. This indicates that the combination of columns is unique");
        System.out.println("Test SrvcWh: Sql1 count = " + count + "; Sql2 count = " + comparisonCount);
        assertEquals(comparisonCount, count);

        System.out.println("Finish SrvcWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        //Check the population of the SRVC_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select distinct SRVC_WH.CURR_SW, count(*)\n" +
                        "from DTSDM.SRVC_WH\n" +
                        "group by SRVC_WH.CURR_SW";

        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting SrvcWhTest.test5,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == 1),"(count == 1)");
        roList.add(ro);
        wr.logTestResults(roList);

        //Count should be 1, representing a unique curr_sw 1 (for active record)
        System.out.println("Sql count should equal 1, a unique curr_sw of 1 for active");
        System.out.println("Test SrvcWh: Sql count = " + count);
        assertEquals(1, count);

        System.out.println("Finish SrvcWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        //Check the population of the SRVC_WH.INSERT_DATE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from dtsdm.srvc_wh where srvc_wh.insert_date is NULL";

        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting SrvcWhTest.test6,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == 0),"(count == 0)");
        roList.add(ro);
        wr.logTestResults(roList);

        //Count should be 0, denoting proper population of this column
        System.out.println("Sql count should equal 0 when this column populates properly");
        System.out.println("Test SrvcWh: Test 6 Sql count = " + count);
        assertEquals(0, count);

        System.out.println("Finish SrvcWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        //Check the population of the SRVC_WH.UPDATE_DATE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from dtsdm.srvc_wh where srvc_wh.update_date is NULL";

        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting SrvcWhTest.test7,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == 0),"(count == 0)");
        roList.add(ro);
        wr.logTestResults(roList);

        //Count should be 0, denoting proper population of this column
        System.out.println("Sql count should equal 0 when this column populates properly");
        System.out.println("Test SrvcWh: Test 7 Sql count = " + count);
        assertEquals(0, count);

        System.out.println("Finish SrvcWhTest.test7");
        System.out.println();

    }

    @Test
    public void test8(){

        //Check the population of the SRVC_WH.DELETED_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from dtsdm.srvc_wh where srvc_wh.deleted_flag != 'N'";

        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting SrvcWhTest.test8,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();
        ResultObject ro = new ResultObject((count == 7),"(count == 7)");
        roList.add(ro);
        wr.logTestResults(roList);

        //Count should be 7 (b/c there are 7 records w/ unknown), denoting proper population of this column
        System.out.println("Sql count should equal 7 when this column populates properly");
        System.out.println("Test SrvcWh: Test 7 Sql count = " + count);
        assertEquals(7, count);

        System.out.println("Finish SrvcWhTest.test7");
        System.out.println();

    }

}
