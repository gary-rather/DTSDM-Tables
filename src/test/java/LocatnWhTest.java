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
public class LocatnWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("LocatnWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test1(){

        //check that the unknown record 0 is populated
        //EXPECT: LOCATN_WID = 0; STATE_COUNTRY_WID=0; STATE_COUNTRY_CD = 'UNK'; LOCATN_NAME_CD = 'UNKNOWN';

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check that the unknown record 0 is populated";
        String reason = " Initial load must add  unspecified data row.  Provides the ability to traverse through the LOCATN_WH table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select * from DTSDM.LOCATN_WH where LOCATN_WH.LOCATN_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting LocatnWhTest.test1,sql");
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

        System.out.println("Test LocatnWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish LocatnWhTest.test1");

    }

    @Test
    public void test2(){

        //Check the population of the unique identifier (LOCATN_WH.LOCATN_WID (PK))

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the unique identifier (LOCATN_WH.LOCATN_WID (PK))";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select distinct LOCATN_WH. LOCATN_WID, count(*) from DTSDM.LOCATN_WH\n" +
                        "group by LOCATN_WH. LOCATN_WID having count(*) > 1";

        String sql2 = "select count (distinct LOCATN_WH.LOCATN_WID) from DTSDM.LOCATN_WH";
        String sql3 = "select count(*) from DTSDM.LOCATN_WH";

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

        System.out.println("Starting LocatnWhTest.test2,sql1");
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
            System.out.println("LocatnWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test2,sql2");
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
            System.out.println("LocatnWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test2,sql3");
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
            System.out.println("LocatnWh.test2 sql3 failed");
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

        System.out.println("Finish LocatnWhTest.test2");
        System.out.println();

    }

    @Test
    public void test3(){

        // Check the population of the LOCATN_WH.LOCATN_NAME, LOCATN_WH.CITY_NAME, LOCATN_WH.STATE_COUNTRY_CD columns

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LOCATN_WH.LOCATN_NAME, LOCATN_WH.CITY_NAME, LOCATN_WH.STATE_COUNTRY_CD columns";
        String reason = " straight pull from DTSDM_SRC_STG ( ITINRY.LOCATION)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from\n" +
                        "(select distinct LOCATN_WH.STATE_COUNTRY_WID,\n" +
                        "LOCATN_WH.LOCATN_NAME, LOCATN_WH.STATE_COUNTRY_CD\n" +
                        "from DTSDM.LOCATN_WH)";

        String sql2 = "select count(*) from\n" +
                        "(SELECT DISTINCT upper(replace (ITINRY.LOCATION ,', ',',')) AS LOCATION,\n" +
                        "nvl(ITINRY. U##LOCST,'') AS U##LOCST, nvl(ITINRY. U##LOCATE,'') AS U##LOCATE\n" +
                        "FROM DTSDM_SRC_STG.ITINRY ITINRY WHERE trim(ITINRY.LOCATION) is not null)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int dtsdmCount = 0;
        int DTSDM_SRC_STGCount = 0;

        System.out.println("Starting LocatnWhTest.test3,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        dtsdmCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("LocatnWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test3,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        DTSDM_SRC_STGCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("LocatnWh.test3 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((DTSDM_SRC_STGCount == dtsdmCount-1),"(DTSDM_SRC_STGCount == dtsdmCount-1)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 3: DTSDM_SRC_STG Table Count = " + DTSDM_SRC_STGCount);
        System.out.println("Test 3: DTSDM Table Count = " + dtsdmCount);
        assertEquals(dtsdmCount-1,DTSDM_SRC_STGCount);

        System.out.println("Finish LocatnWhTest.test3");
        System.out.println();

    }

    @Test
    public void test5(){

        // Check the population of the LOCATN_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LOCATN_WH.CURR_SW column";
        String reason = "  Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not current";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LOCATN_WH";

        String sql2 = "select distinct LOCATN_WH.CURR_SW, count(*)\n" +
                        "from DTSDM.LOCATN_WH group by LOCATN_WH.CURR_SW";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting LocatnWhTest.test5,sql1");
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
            System.out.println("LocatnWh.test5 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test5,sql2");
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
            System.out.println("LocatnWh.test5 sql2 failed");
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

        System.out.println("Finish LocatnWhTest.test5");
        System.out.println();

    }

    @Test
    public void test6(){

        // Check the population of the LOCATN_WH.EFF_START_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the TRIP_WH.EFF_START_DT column";
        String reason = " Default EFF_STRT_DT = sysdate";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LOCATN_WH";

        String sql2 = "select distinct trunc(LOCATN_WH.EFF_STRT_DT), count(*)\n" +
                        "from DTSDM.LOCATN_WH group by trunc(LOCATN_WH.EFF_STRT_DT)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting LocatnWhTest.test6,sql1");
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
            System.out.println("LocatnWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test6,sql2");
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
            System.out.println("LocatnWh.test6 sql2 failed");
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

        System.out.println("Finish LocatnWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the LOCATN_WH.EFF_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " Check the population of the LOCATN_WH.EFF_END_DT column";
        String reason = " It should be 01-JAN-00";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.LOCATN_WH";

        String sql2 = "Select distinct LOCATN_WH.EFF_END_DT, count (*)\n" +
                        "from DTSDM.LOCATN_WH group by LOCATN_WH.EFF_END_DT";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting LocatnWhTest.test7,sql1");
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
            System.out.println("LocatnWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting LocatnWhTest.test7,sql2");
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
            System.out.println("LocatnWh.test7 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro = new ResultObject((testCount == comparisonCount-1),"(testCount == comparisonCount)");
        roList.add(ro);

        wr.logTestResults(roList);

        System.out.println("Test 7: Test Count = " + testCount);
        System.out.println("Test 7: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount-1,testCount);

        System.out.println("Finish LocatnWhTest.test7");
        System.out.println();

    }

}
