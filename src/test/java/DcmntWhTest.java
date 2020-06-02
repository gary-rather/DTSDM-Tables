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
public class DcmntWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("DcmntWhTest.html");
        wr.pageHeader();

    }

    @Test
    public void test01(){

        //check that the unknown record 0 is populated

        //EXPECT: Initial load will add the 'UNKNOWN' row to the table. DCMNT_WID = 0; TRIP_WID = 0; PERSON_WID = 0;
        // PERSON_ATTRIB_WID = 0; DCMNT_TYPE_WID = 0; AO_PERSON_WID = 0; AGNCY_WID = 0; AGNCY_ORG_WID = 0;
        // SUBORG_WID = 0; ADJSTR_PERSON_WID = 0; DELEGT_PERSON_WID = 0; NXT_RTNG_OFFCL_WID = 0; CURR_DCMNNT_STATUS_WID = 0;
        // NXT_DCMNT_STATUS_WID = 0; EXT_SYSTEM_WID = 0; DUTY_COND_TYPE_WID = 0; TRIP_TYPE_WID = 0; SGND_PERSON_WID = 0;
        // All WIDS = 0; DCMNT_NAME = 'UNKNOWN'; DCMNT_BASE_NAME = 'UNKNOWN'; ADJSTMT_LVL = 0; others NULL

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select * from DTSDM.DCMNT_WH where DCMNT_WH.DCMNT_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting DcmntWhTest.test01,sql");
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

        System.out.println("Test DcmntWh  Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish DcmntWhTest.test01");
        System.out.println();

    }

    @Test
    public void test02(){

        //Check the population of the DCMNT_WH.DCMNT_WID (PK) column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "Select count (distinct DCMNT_WH.DCMNT_WID) from DTSDM. DCMNT_WH";

        String sql2 = "Select count(*) from DTSDM.DCMNT_WH"; //10086 results including 'unknown' record

        String sql3 = "select distinct DCMNT_WH.DCMNT_WID, count(*)\n" +
                        "from DTSDM. DCMNT_WH\n" +
                        "group by DCMNT_WH.DCMNT_WID\n" +
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

        System.out.println("Starting DcmntWhTest.test02,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        distinctCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test02 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test02,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        count = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test02 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test02,sql3");
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
            System.out.println("DcmntWh.test02 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((distinctCount == count),"(distinctCount == count)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((duplicateCount == 0),"(duplicateCount == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 02: Count = " + count);
        System.out.println("Test 02: Distinct Count = " + distinctCount);
        assertEquals(distinctCount, count);

        assertEquals(0, duplicateCount);
        System.out.println("Test 02: Duplicate Count =  " + duplicateCount);

        System.out.println("Finish DcmntWhTest.test02");
        System.out.println();

    }

    @Test
    public void test03(){

        //Check the overall population of the DCMNT_WH table
        //Business rule: * One row per unique document number and adjustment level

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select sum(table_row_cnt) from\n" +
                        "(\n" +
                        "select distinct U##VCHNUM, ADJ_LEVEL, count(*) table_row_cnt\n" +
                        "from FRED.VOUCHER\n" +
                        "group by U##VCHNUM, ADJ_LEVEL\n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int count = 0;

        System.out.println("Starting DcmntWhTest.test03,sql");
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
        ResultObject ro = new ResultObject((count == 10085),"(count == 10085)");
        roList.add(ro);
        wr.logTestResults(roList);

        System.out.println("Test DcmntWh: General Row Count = " + count);
        assertEquals(10085, count);

        System.out.println("Finish DcmntWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        //Check the population of the DCMNT_WH.TRIP_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct TRIP_WID, count(*) from DTSDM.DCMNT_WH group by TRIP_WID";
        String sql2 = "select distinct TRIP_WID, count(*) from DTSDM.TRIP_WH group by TRIP_WID";

        String sql3 = "select distinct TRIP_WID from DTSDM.DCMNT_WH\n" +
                        "minus\n" +
                        "select distinct TRIP_WID from DTSDM.TRIP_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int countSql1 = 0; //count from dcmnt.wh
        int countSql2 = 0; //count from trip.wh
        int countSql3 = 0; //result of count from dcmnt_wh - count from trip_wh: should be 0 for this test to pass.

        System.out.println("Starting DcmntWhTest.test04,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test04 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test04,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test04 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test04,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql3++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test04 sql3 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql3 == countSql1-countSql2),"(countSql3 == countSql1-countSql2)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((countSql3 == 0),"(count3Sql == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 04: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 04: TRIP_WH Count = " + countSql2);
        System.out.println("Test 04: Minus Result = " + countSql3);
        assertEquals(countSql3, countSql1-countSql2-1); //-1 accounts for the unknown

        assertEquals(0, countSql3);
        System.out.println("Test 4: Minus Count = " + countSql3);

        System.out.println("Finish DcmntWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        //Check the population of the DCMNT_WH.PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct PERSON_WID from DTSDM.DCMNT_WH";
        String sql2 = "select distinct PERSON_WID from DTSDM.PERSON_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from person_wh

        System.out.println("Starting DcmntWhTest.test05,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test05 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test05,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test05 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1),"(countSql2 == countSql1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 05: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 05: PERSON_WH Count = " + countSql2);
        assertEquals(countSql2, countSql1);

        System.out.println("Finish DcmntWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        //Check the population of the DCMNT_WH.PERSON_ATTRIB_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct a.PERSON_ATTRIB_WID, count(*) table_row_cnt\n" +
                        "from DTSDM.PERSON_ATTRIB_HIST_WH a,\n" +
                        "FRED.VOUCHER b,\n" +
                        "DTSDM.PERSON_WH c\n" +
                        "where a.SSN_FULL = b.U##SSN\n" +
                        "and b.ADJ_LEVEL = 0\n" +
                        "and a.PERSON_WID = c.PERSON_WID\n" +
                        "and c.SSN_FULL = b.U##SSN\n" +
                        "and b.U##DOCTYPE in ('VCH','AUTH', 'LVCH')\n" +
                        "and a.FNAME = b.FNAME\n" +
                        "and a.LNAME = b.LNAME\n" +
                        "and a.FULL_ORG_CD = b.U##ORG\n" +
                        "group by a.PERSON_ATTRIB_WID";

        String sql2 = "select distinct PERSON_ATTRIB_WID from DTSDM.DCMNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from person_attrib_wh
        int countSql2 = 0; //count from person_wh

        System.out.println("Starting DcmntWhTest.test06,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1 = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test06 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test06,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2 = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test06 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql1 == countSql2),"(countSql1 == countSql2)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 06: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 06: PERSON_WH Count = " + countSql2);
        assertEquals(countSql1, countSql2);

        System.out.println("Finish DcmntWhTest.test06");
        System.out.println();

    }

    @Test
    public void test07(){

        //Check the population of the DCMNT_WH.DCMNT_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct a.TYPE_WID\n" +
                        "from DTSDM.TYPE_CONSOLDTD_RFRNC_WH a,\n" +
                        "FRED.DOCSTAT b\n" +
                        "where a.TYPE_CD = b.U##DOCTYPE";

        String sql2 = "Select distinct DCMNT_TYPE_WID,\n " +
                        "count(*)\n" +
                        "from DTSDM.DCMNT_WH\n" +
                        "where DCMNT_TYPE_WID != 0\n" +
                        "group by DCMNT_TYPE_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from type_consoldtd_rfrnc_wh
        int countSql2 = 0; //count from dcmnt_wh

        System.out.println("Starting DcmntWhTest.test07,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test07 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test07,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2 = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test07 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql1 == countSql2),"(countSql1 == countSql2)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 07: TYPE_WH Count = " + countSql1);
        System.out.println("Test 07: DCMNT_WH Count w/ Unknown Record = " + countSql2);
        assertEquals(countSql1, countSql2);

        System.out.println("Finish DcmntWhTest.test07");
        System.out.println();

    }

    @Test
    public void test08(){

        //Check the population of the DCMNT_WH.AO_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct AO_PERSON_WID from DTSDM.DCMNT_WH where AO_PERSON_WID != 0";

        String sql2 = "select distinct a.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH a,\n" +
                        "FRED.DOCSTAT b\n" +
                        "where a.SSN_FULL = b.APPROVE_SSN";

        String sql3 = "select distinct a.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH a,\n" +
                        "FRED.DOCSTAT b\n" +
                        "where a.SSN_FULL = b.APPROVE_SSN\n" +
                        "minus\n" +
                        "select distinct AO_PERSON_WID from DTSDM.DCMNT_WH where AO_PERSON_WID != 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int countSql1 = 0; //count from dcmnt.wh
        int countSql2 = 0; //count from person.wh
        int countSql3 = 0; //result of count from dcmnt_wh - count from person_wh: should be 0 for this test to pass.

        System.out.println("Starting DcmntWhTest.test08,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test08 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test08,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test08 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test08,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql3++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test08 sql3 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql3 == countSql1-countSql2),"(countSql3 == countSql1-countSql2)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((countSql3 == 0),"(count3Sql == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 08: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 08: PERSON_WH Count = " + countSql2);
        System.out.println("Test 08: Minus Result = " + countSql3);
        assertEquals(countSql3, countSql1-countSql2-1); //-1 accounts for the unknown record

        assertEquals(0, countSql3);
        System.out.println("Test 8: Minus Count = " + countSql3);

        System.out.println("Finish DcmntWhTest.test08");
        System.out.println();

    }

    @Test
    public void test09(){

        //Check the population of the DCMNT_WH.DELEGT_PERSON_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct a.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH a,\n" +
                        "FRED.VCHSTAT b\n" +
                        "where a.SSN_FULL = b.DELEGATE_SSN";

        String sql2 = "select distinct DELEGT_PERSON_WID\n " +
                        "from DTSDM.DCMNT_WH\n " +
                        "where DELEGT_PERSON_WID != 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from person_wh
        int countSql2 = 0; //count from dcmnt_wh

        System.out.println("Starting DcmntWhTest.test09,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test09 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test09,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test09 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql1 == countSql2),"(countSql1 == countSql2)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 09: PERSON_WH Count = " + countSql1);
        System.out.println("Test 09: DCMNT_WH Count w/ Unknown Record = " + countSql2);
        assertEquals(countSql1, countSql2);

        System.out.println("Finish DcmntWhTest.test09");
        System.out.println();

    }

    @Test
    public void test10(){

        //Check the population of the DCMNT_WH.NXT_RTNG_OFFCL_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct nvl(dc.NXT_RTNG_OFFCL_WID ,0)\n" +
                        "from DTSDM.DCMNT_WH dc\n";

        String sql2 = "select distinct p.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH p, FRED.VCHSTAT vs\n" +
                        "where p.SSN_FULL = vs.U##NEXT_SSN";

        String sql3 = "select distinct p.PERSON_WID\n" +
                        "from DTSDM.PERSON_WH p, FRED.VCHSTAT vs\n" +
                        "where p.SSN_FULL = vs.U##NEXT_SSN\n" +
                        "minus\n" +
                        "select distinct nvl(dc.NXT_RTNG_OFFCL_WID ,0)\n" +
                        "from DTSDM.DCMNT_WH dc";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int countSql1 = 0; //count from dcmnt.wh
        int countSql2 = 0; //count from person.wh
        int countSql3 = 0; //result of count from dcmnt_wh - count from person_wh: should be 0 for this test to pass.

        System.out.println("Starting DcmntWhTest.test10,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test10 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test10,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test10 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test10,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql3++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test10 sql3 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql3 == countSql1-countSql2),"(countSql3 == countSql1-countSql2)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((countSql3 == 0),"(count3Sql == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 10: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 10: PERSON_WH Count = " + countSql2);
        System.out.println("Test 10: Minus Result = " + countSql3);
        assertEquals(countSql3, countSql1-countSql2-1); //-1 accounts for the 'unknown' record

        assertEquals(0, countSql3);
        System.out.println("Test 10: Minus Count = " + countSql3);

        System.out.println("Finish DcmntWhTest.test10");
        System.out.println();

    }

    @Test
    public void test11(){

        //Check the population of the DCMNT_WH.AGNCY_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ao.AGNCY_WID, count(*)\n" +
                        "from DTSDM.SUBORG_WH so, FRED.VOUCHER v, DTSDM.AGNCY_ORG_WH ao\n" +
                        "where v.U##ORG = so.FULL_ORG_CD\n" +
                        "and so.AGNCY_ORG_WID = ao.AGNCY_ORG_WID\n" +
                        "group by ao.AGNCY_WID";

        String sql2 = "select distinct nvl(AGNCY_WID,0), count(*)\n" +
                        "from DTSDM.DCMNT_WH\n" +
                        "group by AGNCY_WID";

        String sql3 = "select distinct nvl(AGNCY_WID,0)\n" +
                        "from DTSDM.DCMNT_WH\n" +
                        "MINUS\n" +
                        "select distinct ao.AGNCY_WID\n" +
                        "from DTSDM.SUBORG_WH so, FRED.VOUCHER v, DTSDM.AGNCY_ORG_WH ao\n" +
                        "where v.U##ORG = so.FULL_ORG_CD\n" +
                        "and so.AGNCY_ORG_WID = ao.AGNCY_ORG_WID";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int countSql1 = 0; //count from agncy_org.wh
        int countSql2 = 0; //count from dcmnt.wh
        int countSql3 = 0; //result of count from dcmnt_wh - count from person_wh: should be 0 for this test to pass.

        System.out.println("Starting DcmntWhTest.test11,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1 = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test11 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test11,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2 = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test11 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test11,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql3++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test11 sql3 failed");
            e.printStackTrace();
        }

        // log the sql
        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql3 == countSql2-countSql1-1),"(countSql3 == countSql2-countSql1-1)");
        roList.add(ro1);

        ResultObject ro2 = new ResultObject((countSql3 == 0),"(count3Sql == 0)");
        roList.add(ro2);

        wr.logTestResults(roList);

        System.out.println("Test 11: AGNCY_ORG_WH Count = " + countSql1);
        System.out.println("Test 11: DCMNT_WH Count w/ Unknown Record = " + countSql2);
        System.out.println("Test 11: Minus Result = " + countSql3);
        assertEquals(countSql3, countSql2-countSql1-1); //-1 accounts for the 'unknown' record

        assertEquals(0, countSql3);
        System.out.println("Test 11: Minus Count = " + countSql3);

        System.out.println("Finish DcmntWhTest.test11");
        System.out.println();

    }

    @Test
    public void test12(){

        //Check the population of the DCMNT_WH.SUBORG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct SUBORG_WID, count(*) from DTSDM.DCMNT_WH group by SUBORG_WID";

        String sql2 = "select distinct so.SUBORG_WID\n" +
                        "from DTSDM.SUBORG_WH so, FRED.VOUCHER v\n" +
                        "where v.U##ORG = so.FULL_ORG_CD";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from suborg_wh

        System.out.println("Starting DcmntWhTest.test12,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1 = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test12 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test12,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test12 sql2 failed");
            e.printStackTrace();
        }

        // log the sql

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1),"(countSql2 == countSql1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 12: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 12: SUBORG_WH Count = " + countSql2);
        assertEquals(countSql1, countSql2);

        System.out.println("Finish DcmntWhTest.test12");
        System.out.println();

    }

    @Test
    public void test13(){

        //Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct CURR_DCMNNT_STATUS_WID, count(*)\n" +
                        "from DTSDM.DCMNT_WH\n" +
                        "group by CURR_DCMNNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, FRED.DOCSTAT d\n" +
                        "where scr.STATUS_DESCR = d.CUR_STATUS\n" +
                        "and d.U##DOCTYPE in ('VCH','AUTH', 'LVCH')";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from fred.docstat

        System.out.println("Starting DcmntWhTest.test13,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test13 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test13,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test13 sql2 failed");
            e.printStackTrace();
        }

        // log the sql

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1),"(countSql2 == countSql1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 13: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 13: DOCSTAT Count = " + countSql2);
        assertEquals(countSql1, countSql2);

        System.out.println("Finish DcmntWhTest.test13");
        System.out.println();

    }

    /*
    @Test
    public void test14(){

        //Check the population of the DCMNT_WH.CURR_DCMNT_STATUS_DT table

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select dc.CURR_DCMNNT_STATUS_DT as etl_curr_dcmnt_status_dt,\n" +
                        "d.CUR_STATUS_DATE as test_curr_dcmnt_status_dt\n" +
                        "from DTSDM.DCMNT_WH dc, FRED.DOCSTAT d\n" +
                        "where dc.DCMNT_NAME = d.U##VCHNUM\n" +
                        "and dc.SRC_SSN = d.U##SSN\n" +
                        "and dc.SRC_DOCTYPE = d.U##DOCTYPE\n" +
                        "and dc.ADJSTMT_LVL = d.ADJ_LEVEL\n" +
                        "and dc.CURR_DCMNNT_STATUS_DT != d.CUR_STATUS_DATE";

        String sql2 = "select count(distinct DCMNT_WH.CURR_DCMNNT_STATUS_DT) from DTSDM.DCMNT_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql1",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int count = 0;
        int comparisonCount = 0;

        System.out.println("Starting DcmntWhTest.test14,sql1");
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
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test14,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        comparisonCount++;
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

        System.out.println("Test 14: Count for CURR_DCMNT_STATUS_DT Column = " + count);
        assertEquals(comparisonCount, count);

        System.out.println("Finish DcmntWhTest.test14");
        System.out.println();

    }
    */

    @Test
    public void test15(){

        //Check the population of the DCMNT_WH.NXT_DCMNT_STATUS_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct NXT_DCMNT_STATUS_WID\n" +
                        "from DTSDM.DCMNT_WH\n" +
                        "group by NXT_DCMNT_STATUS_WID";

        String sql2 = "select distinct scr.STATUS_WID\n" +
                        "from DTSDM.STATUS_CONSOLDTD_RFRNC_WH scr, FRED.DOCSTAT d\n" +
                        "where scr.STATUS_DESCR = d.AWAITING\n" +
                        "and d.U##DOCTYPE in ('VCH','AUTH', 'LVCH')";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from status_consoldtd_rfrnc_wh

        System.out.println("Starting DcmntWhTest.test15,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test15 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test15,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test15 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        //NOTE: -1 accounts for the 'unknown' record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1-1),"(countSql2 == countSql1-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 15: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 15: STATUS_WH Count = " + countSql2);
        assertEquals(countSql1-1, countSql2);

        System.out.println("Finish DcmntWhTest.test15");
        System.out.println();

    }

    @Test
    public void test17(){

        //Check the population of the DCMNT_WH.DCMNT_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct DCMNT_NAME, count(*) from DTSDM.DCMNT_WH group by DCMNT_NAME";
        String sql2 = "select distinct VCHNUM, count(*) from FRED.VOUCHER group by VCHNUM";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from fred.voucher

        System.out.println("Starting DcmntWhTest.test17,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test17 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test17,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test17 sql2 failed");
            e.printStackTrace();
        }

        // log the sql
        //NOTE: -1 accounts for the 'unknown' record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1-1),"(countSql2 == countSql1-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 17: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 17: VOUCHER Count = " + countSql2);
        assertEquals(countSql1-1, countSql2);

        System.out.println("Finish DcmntWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        //Check the population of the DCMNT_WH.DCMNT_BASE_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME)\n" +
                        "From DTSDM.DCMNT_WH\n" +
                        "group by NVL(SUBSTR(DCMNT_NAME, 0, INSTR(DCMNT_NAME, '_')-1), DCMNT_NAME)";

        String sql2 = "select distinct NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1), VCHNUM)\n" +
                        "From FRED.VOUCHER\n" +
                        "group by NVL(SUBSTR(VCHNUM, 0, INSTR(VCHNUM, '_')-1),VCHNUM)";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from fred.voucher

        System.out.println("Starting DcmntWhTest.test18,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test18 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test18,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test18 sql2 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1-1),"(countSql2 == countSql1-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 18: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 18: VOUCHER Count = " + countSql2);
        assertEquals(countSql1-1, countSql2);

        System.out.println("Finish DcmntWhTest.test18");
        System.out.println();

    }

    @Test
    public void test19(){

        //Check the population of the DCMNT_WH.ADJSTMT_LVL column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct ADJSTMT_LVL, count(*) from DTSDM.DCMNT_WH\n" +
                        "group by ADJSTMT_LVL order by ADJSTMT_LVL";

        String sql2 = "select distinct ADJ_LEVEL, count(*) from FRED.VOUCHER\n" +
                        "group by ADJ_LEVEL order by ADJ_LEVEL";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        //these two counts must be equal for the test to pass
        int countSql1 = 0; //count from dcmnt_wh
        int countSql2 = 0; //count from fred.voucher

        System.out.println("Starting DcmntWhTest.test19,sql1");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql1++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test19 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test19,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countSql2++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("DcmntWh.test19 sql2 failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countSql2 == countSql1-1),"(countSql2 == countSql1-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 19: DCMNT_WH Count w/ Unknown Record = " + countSql1);
        System.out.println("Test 19: VOUCHER Count = " + countSql2);
        assertEquals(countSql1-1, countSql2);

        System.out.println("Finish DcmntWhTest.test19");
        System.out.println();

    }

}
