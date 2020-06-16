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
public class TicketWhTest extends TableTest {

    @BeforeClass
    public static void openResults(){
        wr = new WriteResults("TicketWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01(){

        //check that the unknown record 0 is populated
        //EXPECT: Unspecified data row: Initial load must add the 'Unspecified' row to the table.
        //          TICKET_WID = 0; TRIP_LEG_WID = 0; CBA_ACCNT_WID = 0; CTO_VNDR_WID = 0;
        //          RSVTN_TYPE_WID = 0; TCKT_NUM = 'Unspecified'; others NULL.
        //          As of 05/10/2020: TCKT_NUM = ‘UNKNOWN’

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "Select * from DTSDM.TICKET_WH where TICKET_WH.TICKET_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting TicketWhTest.test1,sql");
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

        System.out.println("Test TicketWh Row 0 1 = " + number);
        assertEquals(1, number);

        System.out.println("Finish TicketWhTest.test1");

    }

    @Test
    public void test02(){

        //Check the population of the unique identifier (TICKET_WH.TICKET_WID (PK) column)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select distinct TICKET_WH.TICKET_WID, count(*) from DTSDM.TICKET_WH\n" +
                        "group by TICKET_WH. TICKET_WID having count(*) > 1";

        String sql2 = "select count (distinct TICKET_WH.TICKET_WID) from DTSDM.TICKET_WH";
        String sql3 = "select count(*) from DTSDM.TICKET_WH";

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

        System.out.println("Starting TicketWhTest.test2,sql1");
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
            System.out.println("TicketWh.test2 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TicketWhTest.test2,sql2");
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
            System.out.println("TicketWh.test2 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TicketWhTest.test2,sql3");
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
            System.out.println("TicketWh.test2 sql3 failed");
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
        System.out.println("Test 2: Duplicate Count =  " + duplicateCount);

        assertEquals(count, distinctCount);
        assertEquals(0, duplicateCount);

        System.out.println("Finish TicketWhTest.test2");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the TICKET_WH.TRIP_LEG_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "\t select distinct t.trip_leg_wid as etl_trip_leg_wid, \n" +
                        "\t\t\t tl.trip_leg_wid as test_trip_leg_wid \n" +
                        "\n" +
                        "\t from dtsdm.dcmnt_wh dc, dtsdm.trip_leg_wh tl, \n" +
                        "\t\t\t DTSDM_SRC_STG.ticksub ts, dtsdm.ticket_wh t \n" +
                        "\n" +
                        "\t where dc.dcmnt_wid = tl.dcmnt_wid \n" +
                        "\t and dc.dcmnt_name = ts.u##vchnum \n" +
                        "\t and tl.leg_num = ts.leg\n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        "\t and ts.u##doctype = dc.src_doctype \n" +
                        "\t and ts.u##ssn = dc.src_ssn \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test03,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test03 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 03: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test03");
        System.out.println();

    }

    @Test
    public void test04(){

        // Check the population of the TICKET_WH.CBA_ACCNT_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.cba_accnt_wid as etl_cba_accnt_wid, \n" +
                        "\t\t\t ca.cba_accnt_wid as test_cba_accnt_num, c.ticknum \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.cbatick c, dtsdm.cba_accnt_wh ca, \n" +
                        "\t\t\t DTSDM_SRC_STG.itinry i, dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where c.ticknum = i.ticknum \n" +
                        "\t and c.cba_acct_no = ca.cba_accnt_num \n" +
                        "\t and t.src_vchnum = i.u##vchnum \n" +
                        "\t and t.src_snn = i.u##ssn \n" +
                        "\t and t.src_doctype = i.u##doctype \n" +
                        "\t and t.src_legnum = i.leg \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.adjstmt_lvl = i.adj_level \n" +
                        "\t and t.cba_accnt_wid != ca.cba_accnt_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test04,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test04 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 04: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test04");
        System.out.println();

    }

    @Test
    public void test05(){

        // Check the population of the TICKET_WH.CTO_VNDR_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "\t select t.cto_vndr_wid as etl_cto_vndr_wid, \n" +
                        "\t\t\t v.vndr_wid as test_cto_vndr_wid, tc.ticknum\n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.pnrtouch p, \n" +
                        "\t dtsdm.vndr_wh v, DTSDM_SRC_STG.ticket tc \n" +
                        "\n" +
                        "\t where p.cto = v.vndr_name \n" +
                        "\t and p.u##vchnum = tc.u##vchnum \n" +
                        "\t and upper(p.ssn) = tc.u##ssn \n" +
                        "\t and p.u##doctype = tc.u##doctype \n" +
                        "\t and p.adj_level = tc.adj_level \n" +
                        "\t and t.src_vchnum = p.u##vchnum \n" +
                        "\t and t.src_snn = upper(p.ssn) \n" +
                        "\t and t.src_doctype = p.u##doctype \n" +
                        "\t and t.cto_vndr_wid != v.vndr_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test05,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test05 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 05: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test05");
        System.out.println();

    }

    @Test
    public void test06(){

        // Check the population of the TICKET_WH.RSVTN_TYPE_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "\t select t.rsvtn_type_wid as etl_rsvtn_type_wid, \n" +
                        "\t\t\t tcr.type_wid as test_rsvtn_type_wid \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, dtsdm.type_consoldtd_rfrnc_wh tcr, DTSDM_SRC_STG.ticksub tc \n" +
                        "\t where tc.u##res_type = tcr.type_descr \n" +
                        "\t and t.src_vchnum = tc.u##vchnum \n" +
                        "\t and t.src_snn = tc.u##ssn \n" +
                        "\t and t.src_doctype = tc.u##doctype \n" +
                        "\t and t.rsvtn_type_wid != tcr.type_wid \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test06,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test06 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 06: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test06");
        System.out.println();

    }

    @Ignore
    @Test
    public void test07(){
        //no test right now (column not in table)
    }

    @Test
    public void test08(){

        // Check the population of the TICKET_WH.CARRIER_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "\t select t.carrier_code as etl_carrier_code, \n" +
                        "\t\t\t tc.carrier as test_carrier_code \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub tc \n" +
                        "\n" +
                        "\t where t.src_vchnum = tc.u##vchnum \n" +
                        "\t and t.src_snn = tc.u##ssn \n" +
                        "\t and t.src_doctype = tc.u##doctype \n" +
                        "\t and t.carrier_code != tc.carrier \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test08,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test08 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 08: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test08");
        System.out.println();

    }

    @Test
    public void test09(){

        // Check the population of the TICKET_WH.TCKT_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "(\n" +
                        "\t select t.tckt_num as etl_tckt_num, \n" +
                        "\t\t\t upper(tc.ticknum) as test_tckt_num \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub tc \n" +
                        "\n" +
                        "\t where t.src_vchnum = tc.u##vchnum \n" +
                        "\t and t.src_snn = tc.u##ssn \n" +
                        "\t and t.src_doctype = tc.u##doctype \n" +
                        "\t and t.tckt_num != upper(tc.ticknum) \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test09,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test09 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 09: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test09");
        System.out.println();

    }

    @Test
    public void test10(){

        // Check the population of the TICKET_WH.TCKT_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select tw.tckt_dt as etl_tckt_dt, \n" +
                        "\t\t\t tc.ticket_date as test_tckt_dt \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh tw, DTSDM_SRC_STG.ticket tc \n" +
                        "\t where tw.src_vchnum = tc.u##vchnum \n" +
                        "\t and tw.src_snn = tc.u##ssn \n" +
                        "\t and tw.src_doctype = tc.u##doctype \n" +
                        "\t and tw.tckt_dt != tc.ticket_date \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test10,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test10 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 10: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test10");
        System.out.println();

    }

    @Ignore
    @Test
    public void test11(){
        // no test right now
    }

    @Ignore
    @Test
    public void test12(){
        // no test right now
    }

    @Test
    public void test13(){

        // Check the population of the TICKET_WH.DPRT_AIRPRT_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.dprt_airprt_name as etl_dprt_airprt_name, \n" +
                        "\t\t\t ts.dep_airport as test_dprt_airprt_name \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = ts.u##vchnum \n" +
                        "\t and t.src_snn = ts.u##ssn \n" +
                        "\t and t.src_doctype = ts.u##doctype \n" +
                        "\t and t.src_legnum = ts.leg \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        "\t and t.dprt_airprt_name != ts.dep_airport \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test13,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test13 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 13: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test13");
        System.out.println();

    }

    @Test
    public void test14(){

        // Check the population of the TICKET_WH.ARVL_AIRPRT_NAME column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.arvl_airprt_name as etl_arvl_airprt_name, \n" +
                        "\t\t\t ts.arr_airport as test_arvl_airprt_name \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = ts.u##vchnum \n" +
                        "\t and t.src_snn = ts.u##ssn \n" +
                        "\t and t.src_doctype = ts.u##doctype \n" +
                        "\t and t.src_legnum = ts.leg \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        "\t and t.arvl_airprt_name != ts.dep_airport \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test14,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test14 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 14: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test14");
        System.out.println();

    }

    @Test
    public void test15(){

        // Check the population of the TICKET_WH.FLGHT_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.flght_num as etl_flght_num, \n" +
                        "\t\t\t ts.flight as test_flght_num \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = ts.u##vchnum \n" +
                        "\t and t.src_snn = ts.u##ssn \n" +
                        "\t and t.src_doctype = ts.u##doctype \n" +
                        "\t and t.src_legnum = ts.leg \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        "\t and t.flght_num != ts.flight" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test15,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test15 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 15: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test15");
        System.out.println();

    }

    @Test
    public void test16(){

        // Check the population of the TICKET_WH.PNR_LOCATOR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.pnr_locator as etl_pnr_locator, " +
                        "\t\t\t tc.locator as test_pnr_locator\n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, " +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc\n" +
                        "\n" +
                        "\t where t.src_vchnum = tc.u##vchnum\n" +
                        "\t and t.src_snn = tc.u##ssn\n" +
                        "\t and t.src_doctype = tc.u##doctype\n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid\n" +
                        "\t and t.src_vchnum = dc.dcmnt_name\n" +
                        "\t and dc.adjstmt_lvl = tc.adj_level\n" +
                        "\t and t.pnr_locator != tc.locator" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test16,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test16 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 16: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test16");
        System.out.println();

    }

    @Test
    public void test17(){

        // Check the population of the TICKET_WH.TCKT_COST_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.tckt_cost_amt as etl_tckt_cost_amt, \n" +
                        "\t\t\t tc.cost as test_tckt_cost_amt \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = tc.u##vchnum \n" +
                        "\t and t.src_snn = tc.u##ssn \n" +
                        "\t and t.src_doctype = tc.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = tc.adj_level \n" +
                        "\t and t.tckt_cost_amt != tc.cost \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test17,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test17 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 17: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test17");
        System.out.println();

    }

    @Test
    public void test18(){

        // Check the population of the TICKET_WH.PYMT_MODE_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.pymt_mode_code as etl_pymt_mode_code, \n" +
                        "\t\t\t ltrim(i.mode_) as test_pymt_mode_code \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.itinry i, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = i.u##vchnum \n" +
                        "\t and t.src_snn = i.u##ssn \n" +
                        "\t and t.src_doctype = i.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = i.adj_level \n" +
                        "\t and t.pymt_mode_code != ltrim(i.mode_) \n" +
                        "\t and i.mode_ != ' ' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test18,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test18 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 18: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test18");
        System.out.println();

    }

    @Test
    public void test19(){

        // Check the population of the TICKET_WH.PYMT_MTHD_DESCR column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.pymt_mthd_descr as etl_pymt_mthd_descr, \n" +
                        "\t\t\t ltrim(i.pay_method) as test_pymt_mthd_descr \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.itinry i, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = i.u##vchnum \n" +
                        "\t and t.src_snn = i.u##ssn \n" +
                        "\t and t.src_doctype = i.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = i.adj_level \n" +
                        "\t and t.pymt_mthd_descr != i.pay_method \n" +
                        "\t and ltrim(i.pay_method) != ' ' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test19,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test19 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 19: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test19");
        System.out.println();

    }

    @Ignore
    @Test
    public void test20(){

        // Check the population of the TICKET_WH.FEMA_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.fema_code as etl_fema_code, \n" +
                        "\t\t\t ts.fema_code as test_fema_code, ts.pd_flag \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = ts.u##vchnum \n" +
                        "\t and t.src_snn = ts.u##ssn \n" +
                        "\t and t.src_doctype = ts.u##doctype \n" +
                        "\t and t.src_legnum = ts.leg \n" +
                        "\t and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test20,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test20 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 20: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test20");
        System.out.println();

    }

    @Test
    public void test21(){

        // Check the population of the TICKET_WH.TCKT_VAL_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.tckt_val_amt as etl_tckt_val_amt, \n" +
                        "\t\t\t tc.tickval as test_tckt_val_amt \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = tc.u##vchnum \n" +
                        "\t and t.src_snn = tc.u##ssn \n" +
                        "\t and t.src_doctype = tc.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = tc.adj_level \n" +
                        "\t and t.tckt_val_amt != tc.tickval \n" +
                        "\t and tc.tickval != 0 \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test21,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test21 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 21: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test21");
        System.out.println();

    }

    @Ignore
    @Test
    public void test22(){
        // no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test23(){

        // Check the population of the TICKET_WH.CNCLD_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TICKET_WH";

        String sql2 = "select count(*) from \n" +
                        "(\n" +
                        "\t select t.cncld_flag as etl_cncld_flag, v.u##pnrstatus \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = v.u##vchnum \n" +
                        "\t and t.src_snn = v.u##ssn \n" +
                        "\t and t.src_doctype = v.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = v.adj_level \n" +
                        "\t and v.u##pnrstatus = 'CANCELLED' \n" +
                        "\t and t.cncld_flag = 'Y' \n" +
                        ")";

        String sql3 = "select count(*) from \n" +
                        "(\n" +
                        "\t Select t.cncld_flag as etl_cncld_flag, v.u##pnrstatus \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "\t\t\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = v.u##vchnum \n" +
                        "\t and t.src_snn = v.u##ssn \n" +
                        "\t and t.src_doctype = v.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = v.adj_level \n" +
                        "\t and v.u##pnrstatus != 'CANCELLED' \n" +
                        "\t and t.cncld_flag = 'N' \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        SqlObject sql3Obj = new SqlObject("sql3",sql3.replaceAll("\n","\n<br>"));
        theSql.add(sql3Obj);

        wr.logSql(theSql);

        int comparisonCount = 0; //comes from DCMNT_WH (table being tested)
        int countCancelledYes = 0; //comes from DTSDM_SRC_STG.VCHPNR table (load being tested) where cancelled flag is 'Y' for yes
        int countCancelledNo = 0; //comes from DTSDM_SRC_STG.VCHONR table (load being tested) where cancelled flag is 'N' for no

        System.out.println("Starting TicketWhTest.test23,sql1");
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
            System.out.println("TicketWh.test23 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TicketWhTest.test23,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countCancelledYes = rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test23 sql2 failed");
            e.printStackTrace();
        }

        System.out.println("Starting TicketWhTest.test23,sql3");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql3)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        countCancelledNo= rs.getInt("count(*)");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test23 sql3 failed");
            e.printStackTrace();
        }

        // Log the results before
        // NOTE: -1 accounts for the unknown record

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((countCancelledYes + countCancelledNo == comparisonCount-1),
                "(countCancelledYes + countCancelledNo == comparisonCount-1)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 23: Cancelled Flag 'Yes' Count = " + countCancelledYes);
        System.out.println("Test 23: Cancelled Flag 'No' Count = " + countCancelledNo);
        System.out.println("Test 23: Comparison Count = " + comparisonCount);
        assertEquals(comparisonCount-1, countCancelledYes + countCancelledNo);

        System.out.println("Finish TicketWhTest.test23");
        System.out.println();

    }

    @Test
    public void test24(){

        // Check the population of the TICKET_WH.EXCPTN_FLAG column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TICKET_WH";
        String sql2 = "select excptn_flag, tckt_cost_amt, tckt_dt, tckt_num from ticket_wh";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test24,sql1");
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
            System.out.println("DcmntWh.test24 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test24,sql2");
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
            System.out.println("DcmntWh.test24 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 24: Comparison Count = " + comparisonCount);
        System.out.println("Test 24: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test24");
        System.out.println();

    }

    @Test
    public void test25(){

        // Check the population of the TICKET_WH.JSTFCTN_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.jstfctn_code as etl_jstfctn_code, \n" +
                        "\t\t\t v.just_code as test_jstfctn_code \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "\t dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where t.src_vchnum = v.u##vchnum \n" +
                        "\t and t.src_snn = v.u##ssn \n" +
                        "\t and t.src_doctype = v.u##doctype \n" +
                        "\t and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "\t and t.src_vchnum = dc.dcmnt_name \n" +
                        "\t and dc.adjstmt_lvl = v.adj_level \n" +
                        "\t and t.jstfctn_code != v.just_code \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test25,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test25 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 25: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test25");
        System.out.println();

    }

    @Ignore
    @Test
    public void test26(){
        // no test right now (no business rules)
    }

    @Ignore
    @Test
    public void test27(){
        // no test right now (no business rules)
    }

    @Test
    public void test28(){

        // Check the population of the TICKET_WH.SRC_VCHNUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                "( \n" +
                "\t select distinct t.src_vchnum as etl_src_vchnum, \n" +
                "\t\t\t tc.u##vchnum as test_src_vchnum \n" +
                "\n" +
                "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                "\n" +
                "\t where dc.dcmnt_name = tc.u##vchnum \n" +
                "\t and dc.src_ssn = tc.u##ssn \n" +
                "\t and dc.src_doctype = tc.u##doctype \n" +
                "\t and dc.adjstmt_lvl = tc.adj_level \n" +
                "\t and dc.src_ssn = t.src_snn \n" +
                "\t and dc.src_doctype = t.src_doctype \n" +
                "\t and t.src_vchnum != tc.u##vchnum \n" +
                ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test28,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test28 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 28: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test28");
        System.out.println();

    }

    @Test
    public void test29(){

        // Check the population of the TICKET_WH.SRC_DOCTYPE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.src_doctype as etl_src_doctype, \n" +
                        "\t\t\t tc.u##doctype as test_src_doctype \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where dc.dcmnt_name = tc.u##vchnum \n" +
                        "\t and dc.src_ssn = tc.u##ssn \n" +
                        "\t and dc.adjstmt_lvl = tc.adj_level \n" +
                        "\t and dc.src_ssn = t.src_snn \n" +
                        "\t and dc.dcmnt_name = t.src_vchnum \n" +
                        "\t and t.src_doctype != tc.u##doctype \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test29,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test29 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 29: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test29");
        System.out.println();

    }

    @Test
    public void test30(){

        // Check the population of the TICKET_WH.SRC_SSN column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select distinct t.src_snn as etl_src_ssn, \n" +
                        "\t\t\t tc.u##ssn as test_src_ssn \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "\t where dc.dcmnt_name = tc.u##vchnum \n" +
                        "\t and dc.src_ssn = tc.u##ssn \n" +
                        "\t and dc.adjstmt_lvl = tc.adj_level \n" +
                        "\t and dc.src_doctype = tc.u##doctype \n" +
                        "\t and dc.dcmnt_name = t.src_vchnum \n" +
                        "\t and dc.src_ssn != t.src_snn \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test30,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test30 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 30: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test30");
        System.out.println();

    }

    @Test
    public void test31(){

        // Check the population of the TICKET_WH.SRC_LEGNUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql = "select count(*) from \n" +
                        "( \n" +
                        "\t select t.src_legnum as etl_src_legnum, \n" +
                        "\t\t\t ts.leg as test_src_legnum \n" +
                        "\n" +
                        "\t from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "\t\t\t dtsdm.dcmnt_wh dc, dtsdm.trip_leg_wh tl \n" +
                        "\n" +
                        "\t where t.src_vchnum = ts.u##vchnum \n" +
                        "\t and t.src_snn = ts.u##ssn \n" +
                        "\t and t.src_doctype = ts.u##doctype \n" +
                        "\t and dc.adjstmt_lvl = ts.adj_level \n" +
                        "\t and dc.dcmnt_name = t.src_vchnum \n" +
                        "\t and tl.trip_leg_wid = t.trip_leg_wid \n" +
                        "\t and t.src_legnum != ts.leg \n" +
                        ")";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);

        wr.logSql(theSql);

        int testCount = 0;

        System.out.println("Starting TicketWhTest.test31,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("TicketWh.test31 sql failed");
            e.printStackTrace();
        }

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == 0),"(testCount == 0)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Count for this test should be 0. This will confirm proper population of this column.");
        System.out.println("Test 31: Test Count = " + testCount);
        assertEquals(0, testCount);

        System.out.println("Finish TicketWhTest.test31");
        System.out.println();

    }

    @Test
    public void test32(){

        // Check the population of the TICKET_WH.CURR_SW column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TICKET_WH";
        String sql2 = "select TICKET_WH.CURR_SW from DTSDM.TICKET_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test32,sql1");
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
            System.out.println("DcmntWh.test32 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test32,sql2");
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
            System.out.println("DcmntWh.test32 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 32: Comparison Count = " + comparisonCount);
        System.out.println("Test 32: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test32");
        System.out.println();

    }

    @Ignore
    @Test
    public void test33(){

        // Check the population of the TICKET_WH.EFF_START_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TICKET_WH";
        String sql2 = "select trunc(TICKET_WH.EFF_START_DT) from DTSDM.TICKET_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test33,sql1");
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
            System.out.println("DcmntWh.test33 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test33,sql2");
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
            System.out.println("DcmntWh.test33 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 33: Comparison Count = " + comparisonCount);
        System.out.println("Test 33: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test33");
        System.out.println();

    }

    @Ignore
    @Test
    public void test34(){

        // Check the population of the TICKET_WH.EFF_END_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());

        String sql1 = "select count(*) from DTSDM.TICKET_WH";
        String sql2 = "select TICKET_WH.EFF_END_DT from DTSDM.TICKET_WH";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();

        SqlObject sql1Obj = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sql1Obj);

        SqlObject sql2Obj = new SqlObject("sql2",sql2.replaceAll("\n","\n<br>"));
        theSql.add(sql2Obj);

        wr.logSql(theSql);

        int comparisonCount = 0;
        int testCount = 0;

        System.out.println("Starting DcmntWhTest.test34,sql1");
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
            System.out.println("DcmntWh.test34 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting DcmntWhTest.test34,sql2");
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
            System.out.println("DcmntWh.test34 sql2 failed");
            e.printStackTrace();
        }

        // Log the results before

        ArrayList<ResultObject> roList = new ArrayList<ResultObject>();

        ResultObject ro1 = new ResultObject((testCount == comparisonCount),"(testCount == comparisonCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test 34: Comparison Count = " + comparisonCount);
        System.out.println("Test 34: Test Count = " + testCount);
        assertEquals(comparisonCount, testCount);

        System.out.println("Finish DcmntLocatnWhTest.test34");
        System.out.println();

    }

}
