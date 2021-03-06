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
        String condition = " Check that the \"unknown\" record 0 is populated";
        String reason = " Initial load must add  unspecified data row.  Provides the ability to traverse through the DTSDM. TICKET_WH table when no value is matched";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


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
        String condition = " Check the population of the unique identifier TICKET_WH.TICKET_WID (PK) column";
        String reason = " Sequential ID (PK)";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


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
        String condition = " Check the population of the TICKET_WH.TRIP_LEG_WID column";
        String reason = " Lookup TRIP_LEG_WID using LEG_NUM via voucher num, adj level, ssn, doc type";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select distinct t.trip_leg_wid as etl_trip_leg_wid, \n" +
                        "    tl.trip_leg_wid as test_trip_leg_wid \n" +
                        "\n" +
                        "  from dtsdm.dcmnt_wh dc, dtsdm.trip_leg_wh tl, \n" +
                        "    DTSDM_SRC_STG.ticksub ts, dtsdm.ticket_wh t \n" +
                        "\n" +
                        "  where dc.dcmnt_wid = tl.dcmnt_wid \n" +
                        "  and dc.dcmnt_name = ts.u##vchnum \n" +
                        "  and tl.leg_num = ts.leg\n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
                        "  and ts.u##doctype = dc.src_doctype \n" +
                        "  and ts.u##ssn = dc.src_ssn \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
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
        String condition = " Check the population of the TICKET_WH.CBA_ACCNT_WID column";
        String reason = " Lookup CBATICK.CBA_ACCT_ NO in CBA_ACCNT_WH.CBA_ACCNT_NUM to get WID; value = 0 if TICKNUM does not exist in CBATICK";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.cba_accnt_wid as etl_cba_accnt_wid, \n" +
                        "    ca.cba_accnt_wid as test_cba_accnt_num, c.ticknum \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.cbatick c, dtsdm.cba_accnt_wh ca, \n" +
                        "    DTSDM_SRC_STG.itinry i, dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where c.ticknum = i.ticknum \n" +
                        "  and c.cba_acct_no = ca.cba_accnt_num \n" +
                        "  and t.src_vchnum = i.u##vchnum \n" +
                        "  and t.src_snn = i.u##ssn \n" +
                        "  and t.src_doctype = i.u##doctype \n" +
                        "  and t.src_legnum = i.leg \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and dc.adjstmt_lvl = i.adj_level \n" +
                        "  and t.cba_accnt_wid != ca.cba_accnt_wid \n" +
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
        String condition = " Check the population of the TICKET_WH.CTO_VNDR_WID column";
        String reason = " Lookup PNRTOUCH.CTO in VNDR_WH.VNDR_NAME to get CTO_VNDR_WID; value = 0 if TICKNUM does not exist in PNRTOUCH";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select t.cto_vndr_wid as etl_cto_vndr_wid, \n" +
                        "    v.vndr_wid as test_cto_vndr_wid, tc.ticknum\n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.pnrtouch p, \n" +
                        "  dtsdm.vndr_wh v, DTSDM_SRC_STG.ticket tc \n" +
                        "\n" +
                        "  where p.cto = v.vndr_name \n" +
                        "  and p.u##vchnum = tc.u##vchnum \n" +
                        "  and upper(p.ssn) = tc.u##ssn \n" +
                        "  and p.u##doctype = tc.u##doctype \n" +
                        "  and p.adj_level = tc.adj_level \n" +
                        "  and t.src_vchnum = p.u##vchnum \n" +
                        "  and t.src_snn = upper(p.ssn) \n" +
                        "  and t.src_doctype = p.u##doctype \n" +
                        "  and t.cto_vndr_wid != v.vndr_wid \n" +
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
        String condition = " Check the population of the TICKET_WH.RSVTN_TYPE_WID column";
        String reason = " Look up U##RESTYPE value in TYPE_CONSOLDTD_RFRNC_WH.TYPE_DESCR to get ID";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select t.rsvtn_type_wid as etl_rsvtn_type_wid, \n" +
                        "    tcr.type_wid as test_rsvtn_type_wid \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, dtsdm.type_consoldtd_rfrnc_wh tcr, DTSDM_SRC_STG.ticksub tc \n" +
                        "  where tc.u##res_type = tcr.type_descr \n" +
                        "  and t.src_vchnum = tc.u##vchnum \n" +
                        "  and t.src_snn = tc.u##ssn \n" +
                        "  and t.src_doctype = tc.u##doctype \n" +
                        "  and t.rsvtn_type_wid != tcr.type_wid \n" +
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
        String condition = " Check the population of the TICKET_WH.CARRIER_CODE column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select t.carrier_code as etl_carrier_code, \n" +
                        "    tc.carrier as test_carrier_code \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub tc \n" +
                        "\n" +
                        "  where t.src_vchnum = tc.u##vchnum \n" +
                        "  and t.src_snn = tc.u##ssn \n" +
                        "  and t.src_doctype = tc.u##doctype \n" +
                        "  and t.carrier_code != tc.carrier \n" +
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
        String condition = " Check the population of the TICKET_WH.TCKT_NUM column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "(\n" +
                        "  select t.tckt_num as etl_tckt_num, \n" +
                        "    upper(tc.ticknum) as test_tckt_num \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub tc \n" +
                        "\n" +
                        "  where t.src_vchnum = tc.u##vchnum \n" +
                        "  and t.src_snn = tc.u##ssn \n" +
                        "  and t.src_doctype = tc.u##doctype \n" +
                        "  and t.tckt_num != upper(tc.ticknum) \n" +
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
        String condition = " Check the population of the TICKET_WH.TCKT_DT column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select tw.tckt_dt as etl_tckt_dt, \n" +
                        "    tc.ticket_date as test_tckt_dt \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh tw, DTSDM_SRC_STG.ticket tc \n" +
                        "  where tw.src_vchnum = tc.u##vchnum \n" +
                        "  and tw.src_snn = tc.u##ssn \n" +
                        "  and tw.src_doctype = tc.u##doctype \n" +
                        "  and tw.tckt_dt != tc.ticket_date \n" +
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
        String condition = " Check the population of the TICKET_WH.DPRT_AIRPRT_NAME column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.dprt_airprt_name as etl_dprt_airprt_name, \n" +
                        "    ts.dep_airport as test_dprt_airprt_name \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = ts.u##vchnum \n" +
                        "  and t.src_snn = ts.u##ssn \n" +
                        "  and t.src_doctype = ts.u##doctype \n" +
                        "  and t.src_legnum = ts.leg \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
                        "  and t.dprt_airprt_name != ts.dep_airport \n" +
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
        String condition = " Check the population of the TICKET_WH.ARVL_AIRPRT_NAME column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.arvl_airprt_name as etl_arvl_airprt_name, \n" +
                        "    ts.arr_airport as test_arvl_airprt_name \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = ts.u##vchnum \n" +
                        "  and t.src_snn = ts.u##ssn \n" +
                        "  and t.src_doctype = ts.u##doctype \n" +
                        "  and t.src_legnum = ts.leg \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
                        "  and t.arvl_airprt_name != ts.dep_airport \n" +
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
        String condition = " Check the population of the TICKET_WH.FLGHT_NUM column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.flght_num as etl_flght_num, \n" +
                        "    ts.flight as test_flght_num \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = ts.u##vchnum \n" +
                        "  and t.src_snn = ts.u##ssn \n" +
                        "  and t.src_doctype = ts.u##doctype \n" +
                        "  and t.src_legnum = ts.leg \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
                        "  and t.flght_num != ts.flight" +
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
        String condition = " Check the population of the TICKET_WH.PNR_LOCATOR column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.pnr_locator as etl_pnr_locator, " +
                        "    tc.locator as test_pnr_locator\n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, " +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc\n" +
                        "\n" +
                        "  where t.src_vchnum = tc.u##vchnum\n" +
                        "  and t.src_snn = tc.u##ssn\n" +
                        "  and t.src_doctype = tc.u##doctype\n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid\n" +
                        "  and t.src_vchnum = dc.dcmnt_name\n" +
                        "  and dc.adjstmt_lvl = tc.adj_level\n" +
                        "  and t.pnr_locator != tc.locator" +
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
        String condition = " Check the population of the TICKET_WH.TCKT_COST_AMT column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.tckt_cost_amt as etl_tckt_cost_amt, \n" +
                        "    tc.cost as test_tckt_cost_amt \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = tc.u##vchnum \n" +
                        "  and t.src_snn = tc.u##ssn \n" +
                        "  and t.src_doctype = tc.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = tc.adj_level \n" +
                        "  and t.tckt_cost_amt != tc.cost \n" +
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
        String condition = " Check the population of the TICKET_WH.PYMT_MODE_CODE column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.pymt_mode_code as etl_pymt_mode_code, \n" +
                        "    ltrim(i.mode_) as test_pymt_mode_code \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.itinry i, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = i.u##vchnum \n" +
                        "  and t.src_snn = i.u##ssn \n" +
                        "  and t.src_doctype = i.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = i.adj_level \n" +
                        "  and t.pymt_mode_code != ltrim(i.mode_) \n" +
                        "  and i.mode_ != ' ' \n" +
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
        String condition = " Check the population of the TICKET_WH.PYMT_MTHD_DESCR column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.pymt_mthd_descr as etl_pymt_mthd_descr, \n" +
                        "    ltrim(i.pay_method) as test_pymt_mthd_descr \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.itinry i, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = i.u##vchnum \n" +
                        "  and t.src_snn = i.u##ssn \n" +
                        "  and t.src_doctype = i.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = i.adj_level \n" +
                        "  and t.pymt_mthd_descr != i.pay_method \n" +
                        "  and ltrim(i.pay_method) != ' ' \n" +
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
        String condition = " Check the population of the TICKET_WH.FEMA_CODE column";
        String reason = " when pd_flag = 'F' then value = 'FPLP' when pd_flag != 'F' and fema_code is not null then value = fema_code";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.fema_code as etl_fema_code, \n" +
                        "    ts.fema_code as test_fema_code, ts.pd_flag \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = ts.u##vchnum \n" +
                        "  and t.src_snn = ts.u##ssn \n" +
                        "  and t.src_doctype = ts.u##doctype \n" +
                        "  and t.src_legnum = ts.leg \n" +
                        "  and t.trip_leg_wid = tl.trip_leg_wid \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
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
        String condition = " Check the population of the TICKET_WH.TCKT_VAL_AMT column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.tckt_val_amt as etl_tckt_val_amt, \n" +
                        "    tc.tickval as test_tckt_val_amt \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = tc.u##vchnum \n" +
                        "  and t.src_snn = tc.u##ssn \n" +
                        "  and t.src_doctype = tc.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = tc.adj_level \n" +
                        "  and t.tckt_val_amt != tc.tickval \n" +
                        "  and tc.tickval != 0 \n" +
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
        String condition = " Check the population of the TICKET_WH.CNCLD_FLAG column";
        String reason = " when U##PNRSTATUS = 'Cancelled' then 'Y'";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TICKET_WH";

        String sql2 = "select count(*) from \n" +
                        "(\n" +
                        "  select t.cncld_flag as etl_cncld_flag, v.u##pnrstatus \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = v.u##vchnum \n" +
                        "  and t.src_snn = v.u##ssn \n" +
                        "  and t.src_doctype = v.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and v.u##pnrstatus = 'CANCELLED' \n" +
                        "  and t.cncld_flag = 'Y' \n" +
                        ")";

        String sql3 = "select count(*) from \n" +
                        "(\n" +
                        "  Select t.cncld_flag as etl_cncld_flag, v.u##pnrstatus \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "    dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = v.u##vchnum \n" +
                        "  and t.src_snn = v.u##ssn \n" +
                        "  and t.src_doctype = v.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and v.u##pnrstatus != 'CANCELLED' \n" +
                        "  and t.cncld_flag = 'N' \n" +
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
        String condition = " Check the population of the TICKET_WH.EXCPTN_FLAG column";
        String reason = " When there is a null ticket number and there is no cost, then 'E'. When there is a null ticket number and there is no ticket date, then 'E'. When there is a null ticket number, but there is cost and a ticket date, then '*' .Else (if ticket number is not null) then 'E'";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.TICKET_WH";
        String sql2 = "select excptn_flag, tckt_cost_amt, tckt_dt, tckt_num from DTSDM.Licket_wh";

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
        String condition = " Check the population of the TICKET_WH.JSTFCTN_CODE column";
        String reason = "  List of justification codes associated with the ticket; if multiple justification codes exist for a ticket they are captured as a concatenated list";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.jstfctn_code as etl_jstfctn_code, \n" +
                        "    v.just_code as test_jstfctn_code \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.vchpnr v, \n" +
                        "  dtsdm.trip_leg_wh tl, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where t.src_vchnum = v.u##vchnum \n" +
                        "  and t.src_snn = v.u##ssn \n" +
                        "  and t.src_doctype = v.u##doctype \n" +
                        "  and tl.dcmnt_wid = dc.dcmnt_wid \n" +
                        "  and t.src_vchnum = dc.dcmnt_name \n" +
                        "  and dc.adjstmt_lvl = v.adj_level \n" +
                        "  and t.jstfctn_code != v.just_code \n" +
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
        String condition = " Check the population of the TICKET_WH.SRC_VCHNUM column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                "( \n" +
                "  select distinct t.src_vchnum as etl_src_vchnum, \n" +
                "    tc.u##vchnum as test_src_vchnum \n" +
                "\n" +
                "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                "\n" +
                "  where dc.dcmnt_name = tc.u##vchnum \n" +
                "  and dc.src_ssn = tc.u##ssn \n" +
                "  and dc.src_doctype = tc.u##doctype \n" +
                "  and dc.adjstmt_lvl = tc.adj_level \n" +
                "  and dc.src_ssn = t.src_snn \n" +
                "  and dc.src_doctype = t.src_doctype \n" +
                "  and t.src_vchnum != tc.u##vchnum \n" +
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
        String condition = " Check the population of the TICKET_WH.SRC_DOCTYPE column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.src_doctype as etl_src_doctype, \n" +
                        "    tc.u##doctype as test_src_doctype \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where dc.dcmnt_name = tc.u##vchnum \n" +
                        "  and dc.src_ssn = tc.u##ssn \n" +
                        "  and dc.adjstmt_lvl = tc.adj_level \n" +
                        "  and dc.src_ssn = t.src_snn \n" +
                        "  and dc.dcmnt_name = t.src_vchnum \n" +
                        "  and t.src_doctype != tc.u##doctype \n" +
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
        String condition = " Check the population of the TICKET_WH.SRC_SSN column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select distinct t.src_snn as etl_src_ssn, \n" +
                        "    tc.u##ssn as test_src_ssn \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticket tc, dtsdm.dcmnt_wh dc \n" +
                        "\n" +
                        "  where dc.dcmnt_name = tc.u##vchnum \n" +
                        "  and dc.src_ssn = tc.u##ssn \n" +
                        "  and dc.adjstmt_lvl = tc.adj_level \n" +
                        "  and dc.src_doctype = tc.u##doctype \n" +
                        "  and dc.dcmnt_name = t.src_vchnum \n" +
                        "  and dc.src_ssn != t.src_snn \n" +
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
        String condition = " Check the population of the TICKET_WH.SRC_LEGNUM column";
        String reason = " Straight pull";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql = "select count(*) from \n" +
                        "( \n" +
                        "  select t.src_legnum as etl_src_legnum, \n" +
                        "    ts.leg as test_src_legnum \n" +
                        "\n" +
                        "  from dtsdm.ticket_wh t, DTSDM_SRC_STG.ticksub ts, \n" +
                        "    dtsdm.dcmnt_wh dc, dtsdm.trip_leg_wh tl \n" +
                        "\n" +
                        "  where t.src_vchnum = ts.u##vchnum \n" +
                        "  and t.src_snn = ts.u##ssn \n" +
                        "  and t.src_doctype = ts.u##doctype \n" +
                        "  and dc.adjstmt_lvl = ts.adj_level \n" +
                        "  and dc.dcmnt_name = t.src_vchnum \n" +
                        "  and tl.trip_leg_wid = t.trip_leg_wid \n" +
                        "  and t.src_legnum != ts.leg \n" +
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
        String condition = " Check the population of the TICKET_WH.CURR_SW column";
        String reason = " Indicates whether the record is the current record for the agency.  value = 1 if current, 0 if not current";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


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
        String condition = " Check the population of the TICKET_WH.EFF_START_DT column";
        String reason = "  Default EFF_STRT_DT = sysdate";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


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
        String condition = " Check the population of the TICKET_WH.EFF_END_DT column";
        String reason = " It should be 01-JAN-00";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


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
