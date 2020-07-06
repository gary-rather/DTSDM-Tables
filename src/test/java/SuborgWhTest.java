import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuborgWhTest extends TableTest{

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("SuborgWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the unknown record 0 is populated";
        String reason = " Provides the ability to traverse through the SUBORG_WH when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "Select * from DTSDM.SUBORG_WH where SUBORG_WH.SUBORG_WID = 0";
        int number = 0;

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        System.out.println("Starting SuborgWhTest.test1,sql");
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
        ResultObject ro1 = new ResultObject((number == 1),"(number == 1)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test SuborgWh  Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish SuborgWhTest.test1");
        System.out.println();

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (SUBORG_WH.SUBORG_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the unique identifier SUBORG_WH.SUBORG_WID (PK) column";
        String reason = " Unique identifier of the record";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "Select count(*) from DTSDM.SUBORG_WH";

        String sql2 = "select count (distinct SUBORG_WH.SUBORG_WID) as Distinct_Count from DTSDM.SUBORG_WH";

        String sql3 = "select distinct SUBORG_WH.SUBORG_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "group by SUBORG_WH.SUBORG_WID\n" +
                        "having count(*) > 1";

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

        System.out.println("Starting SuborgWhTest.test2,sql1");
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
            System.out.println("SuborgWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test2,sql2");
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
            System.out.println("SuborgWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting AgncyWhTest.test2,sql3");
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
            System.out.println("SuborgWh.test2 sql3 failed");
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

        System.out.println("Finish SuborgWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        //Check the population of the SUBORG_WH.AGNCY_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.AGNCY_WID column";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct agncy_wid from DTSDM.Luborg_wh where agncy_wid != 0";

        String sql2 = "select distinct orglist.agency, agncy_wh.agncy_wid\n" +
                        "from DTSDM_SRC_STG.orglist\n" +
                        "LEFT OUTER JOIN DTSDM.AGNCY_WH\n" +
                        "ON AGNCY_WH.AGNCY_CD = orglist.AGENCY";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting SuborgWhTest.test3,sql1");
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
            System.out.println("SuborgWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test3 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((count == comparisonCount),"(count == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("SuborgWh sql1 count = " + comparisonCount);
        System.out.println("SuborgWh sql2 count = " + count);
        assertEquals(comparisonCount,count);

        System.out.println("Finish SuborgWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        //Check the population of the SUBORG_WH.AGNCY_ORG_WID columns

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.AGNCY_ORG_WID column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.AGNCY_ORG_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "where SUBORG_WH.AGNCY_ORG_WID != 0\n" +
                        "group by SUBORG_WH.AGNCY_ORG_WID\n" +
                        "order by SUBORG_WH.AGNCY_ORG_WID";

        String sql2 = "Select distinct AGNCY_ORG_WH.AGNCY_ORG_WID,\n" +
                        "AGNCY_ORG_WH.AGNCY_SHRT_CD,\n" +
                        "substr(ORGLIST.U##ORG,2,1)\n" +
                        "from DTSDM.AGNCY_ORG_WH, DTSDM_SRC_STG.ORGLIST\n" +
                        "where AGNCY_ORG_WH.AGNCY_SHRT_CD = substr(ORGLIST.U##ORG,2,1)\n" +
                        "and substr(ORGLIST.U##ORG, 1,2) like 'D%'\n" +
                        "and substr(ORGLIST.U##ORG, 2,1) != 'D'\n" +
                        "order by AGNCY_ORG_WH.AGNCY_ORG_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting SuborgWhTest.test4,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test4,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test4 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((count == comparisonCount),"(count == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("SuborgWh sql1 count = " + comparisonCount);
        System.out.println("SuborgWh sql2 count = " + count);
        assertEquals(comparisonCount,count);

        System.out.println("Finish SuborgWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        //Check the population of the SUBORG_WH.SRVC_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.SRVC_WID column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.SRVC_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "group by SUBORG_WH.SRVC_WID\n" +
                        "order by SUBORG_WH.SRVC_WID";

        String sql2 = "select distinct SRVC_WH.SRVC_WID,\n" +
                        "SRVC_WH.SRVC_NAME, SITE_TO_ORG_MAPPING.SERVICE\n" +
                        "from DTSDM.SRVC_WH,\n" +
                        "DTSDM_SRC_STG. SITE_TO_ORG_MAPPING\n" +
                        "where SRVC_WH.SRVC_NAME = SITE_TO_ORG_MAPPING.SERVICE\n" +
                        "order by SRVC_WH.SRVC_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting SuborgWhTest.test5,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test5,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test5 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((count == comparisonCount),"(count == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("SuborgWh sql1 count = " + comparisonCount);
        System.out.println("SuborgWh sql2 count = " + count);
        assertEquals(comparisonCount,count);

        System.out.println("Finish SuborgWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        //Check the population of the SUBORG_WH.DUTY_STN_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.DUTY_STN_WID column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.DUTY_STN_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "where SUBORG_WH.DUTY_STN_WID != 0\n" +
                        "group by SUBORG_WH.DUTY_STN_WID";

        String sql2 = "select DUTY_STN_WH.DUTY_STN_WID,\n" +
                        "DUTY_STN_WH.DUTY_STN_NAME, ORGLIST.STATION\n" +
                        "from DTSDM. DUTY_STN_WH, DTSDM_SRC_STG.ORGLIST\n" +
                        "where DUTY_STN_WH.DUTY_STN_NAME = ORGLIST.STATION\n" +
                        "and ORGLIST.STATION is not NULL";

        String sql3 = "select distinct ORGLIST.STATION,\n" +
                        "count(ORGLIST.SITE_ID)\n" +
                        "from DTSDM_SRC_STG.ORGLIST\n" +
                        "where ORGLIST.STATION is not null\n" +
                        "group by ORGLIST.STATION\n" +
                        "order by ORGLIST.STATION";

        String sql4 = "select distinct a.STATION, a.SITE_ID,\n" +
                        "b.SITE_NAME, count(*)\n" +
                        "from DTSDM_SRC_STG.ORGLIST a, DTSDM_SRC_STG.SITE b\n" +
                        "where a.SITE_ID = b.SITE_ID\n" +
                        "group by a.STATION, a.SITE_ID, b.SITE_NAME\n" +
                        "order by a.STATION";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        SqlObject sql4Obj = new SqlObject("sql4",sql4.replaceAll("\n","\n<br>"));
        theSql.add(sql4Obj);

        wr.logSql(theSql);

        int rowCountTest6Sql1 = 0;
        int rowCountTest6Sql2 = 0;
        int rowCountTest6Sql3 = 0;
        int rowCountTest6Sql4 = 0;

        System.out.println("Starting SuborgWhTest.test6,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        rowCountTest6Sql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        rowCountTest6Sql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test6 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test6,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        rowCountTest6Sql3 = rs.getInt("count(ORGLIST.SITE_ID)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test6 sql3 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test6,sql4");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql4)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        rowCountTest6Sql4 = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test6 sql4 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((rowCountTest6Sql1 == rowCountTest6Sql2),"(rowCountTest6Sql1 == rowCountTest6Sql2)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((rowCountTest6Sql3 == rowCountTest6Sql4),"(rowCountTest6Sql3 == rowCountTest6Sql4)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test Eight: Row Count 1 should equal Row Count 1\n");
        System.out.println(rowCountTest6Sql1 + " = " + rowCountTest6Sql2) ;
        assertEquals(rowCountTest6Sql1, rowCountTest6Sql2);

        System.out.println("\nTest Five  Row Count 3 should equal Row Count 4\n");
        System.out.println(rowCountTest6Sql3 + " = " + rowCountTest6Sql4) ;
        assertEquals(rowCountTest6Sql3, rowCountTest6Sql4);

        System.out.println("Finish SuborgWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        //Check the population of the SUBORG_WH.ORG_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.ORG_CD column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.ORG_CD, count(*)\n" +
                        "from DTSDM. SUBORG_WH\n" +
                        "where SUBORG_WH.ORG_CD != 'UNK'\n" +
                        "group by SUBORG_WH.ORG_CD";

        String sql2 = "SELECT DISTINCT\n" +
                        "case when substr(ORGLIST.\"U##ORG\",0,2) in('DA','DF') then substr(ORGLIST.\"U##ORG\",3,3)\n" +
                        "when substr(ORGLIST.\"U##ORG\",0,2) in ('DN', 'DD', 'DM', 'DJ') then substr(ORGLIST.\"U##ORG\",3,2)\n" +
                        "else NULL\n" +
                        "end AS AGNCY_ORG_CD\n" +
                        "FROM DTSDM_SRC_STG.ORGLIST\n" +
                        "where substr(ORGLIST.\"U##ORG\",0,2) in('DA','DF','DN', 'DD', 'DM', 'DJ')";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting SuborgWhTest.test7,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test7,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test7 sql2 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((count == comparisonCount),"(count == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("SuborgWh sql1 count = " + comparisonCount);
        System.out.println("SuborgWh sql2 count = " + count);
        assertEquals(comparisonCount,count);

        System.out.println("Finish SuborgWhTest.test7");
        System.out.println();

    }

    @Test
    public void test8(){

        //Check the population of the SUBORG_WH.SUBORG_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.SUBORG_CD column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.SUBORG_CD, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "group by SUBORG_WH.SUBORG_CD\n" +
                        "having count(*)=1";

        String sql2 = "select distinct SUBORG_WH.SUBORG_CD, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "group by SUBORG_WH.SUBORG_CD\n" +
                        "having count(*)>1";

        String sql3 = "select case when substr(ORGLIST.U##ORG,0,2) in('DA','DF')\n" +
                        "then substr(ORGLIST.U##ORG,6)\n" +
                        "when substr(ORGLIST.U##ORG,0,2) in ('DN', 'DD', 'DM', 'DJ')\n" +
                        "then substr(ORGLIST.U##ORG,5)\n" +
                        "else substr(ORGLIST.U##ORG,3)\n" +
                        "end AS SUBORG_CD\n" +
                        "from DTSDM_SRC_STG.ORGLIST";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int suborgCdSrcCount = 0;
        int suborgCdSrcRepeatCount = 0;
        int suborgCdTargetCount = 0;

        System.out.println("Starting SuborgWhTest.test8,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        suborgCdSrcCount +=  rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWhTest.test8 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test8,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        suborgCdSrcRepeatCount =  rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWhTest.test8 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("SuborgWhTest.test8,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                //ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    //System.out.println("Size of results = " + rs.getInt(1));
                    while(rs.next()) {
                        suborgCdTargetCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWhTest.test8 sql3 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((suborgCdSrcCount == 473),"(suborgCdSrcCount == 473)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((suborgCdSrcRepeatCount == 32),"(suborgCdSrcRepeatCount == 32)");
        roList.add(ro2);

        ResultObject ro3 = new ResultObject((suborgCdTargetCount == 505),"(suborgCdTargetCount == 505)");
        roList.add(ro3);

        wr.logTestResults(roList);

        System.out.println("SuborgWh Source SuborgCd Count (having count > 1) (expect 473 rows) = " + suborgCdSrcCount);
        assertEquals(473, suborgCdSrcCount);

        System.out.println("SuborgWh Source SuborgCd Count (having count of 1) (expect 32 rows) = " + suborgCdSrcRepeatCount) ;
        assertEquals(32, suborgCdSrcRepeatCount);

        System.out.println("SuborgWh Target SuborgCd Count  (expect 505 rows) = " + suborgCdTargetCount) ;
        assertEquals(505, suborgCdTargetCount);

        System.out.println("Finish SuborgWhTest.test8");
        System.out.println();

    }

    @Test
    public void test9(){

        //Check the population of the SUBORG_WH.FULL_ORG_CD column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the SUBORG_WH.FULL_ORG_CD column";
        String reason = " See 'DTS Data Warehouse Table Definition.xls' document";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct SUBORG_WH.FULL_ORG_CD, count(*)\n" +
                        "from DTSDM.SUBORG_WH\n" +
                        "group by SUBORG_WH.FULL_ORG_CD";

        String sql2 = "select distinct ORGLIST.U##ORG, count (*)\n" +
                        "from DTSDM_SRC_STG.ORGLIST\n" +
                        "group by ORGLIST.U##ORG";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int dtsdmCount = 0;
        int DTSDM_SRC_STGCount = 0;

        System.out.println("Starting SuborgWhTest.test9,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dtsdmCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test9 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting SuborgWhTest.test9,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        DTSDM_SRC_STGCount = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("SuborgWh.test9 sql2 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((DTSDM_SRC_STGCount == dtsdmCount),"(DTSDM_SRC_STGCount == dtsdmCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("SuborgWh sql1 count = " + dtsdmCount);
        System.out.println("SuborgWh sql2 count = " + DTSDM_SRC_STGCount);
        assertEquals(dtsdmCount,DTSDM_SRC_STGCount);

        System.out.println("Finish SuborgWhTest.test9");
        System.out.println();

    }

}