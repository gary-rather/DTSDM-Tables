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
public class CtoFeeWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("CtoFeeWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql = "select count(*) from DTSDM.CTO_FEE_WH where CTO_FEE_WID = 0";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql" );
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

        System.out.println("Test CtoFeeWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
        System.out.println();

    }

    @Test
    public void test02() {

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql1 = "select distinct TICKET_WID, count (*) from DTSDM.CTO_FEE_WH \n" +
                        "group by TICKET_WID having count (*) > 1";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);

        wr.logSql(theSql);

        int number = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql1" );
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
        ResultObject ro1 = new ResultObject((0 == number),"(0 == number)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test CtoFeeWh group by count > 0 " + number);
        assertEquals(0, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test02");
        System.out.println();

    }

    @Test
    public void test3(){

        // Check the population of the CTO_FEE_WH.TICKET_WID column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.CTO_FEE_WH where CTO_FEE_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.CTO_FEE_WID, A.TICKET_WID, B.TICKET_WID \n" +
                        "\t FROM DTSDM.CTO_FEE_WH A, DTSDM.TICKET_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.TICKET C \n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.PNR_LOCATOR = C.LOCATOR \n" +
                        "\t AND C.ADJ_LEVEL = 0 \n" +
                        "\t AND C.TICKNUM LIKE ‘890%’ \n" +
                        "\t AND C.LOCATOR IS NOT NULL \n" +
                        "\t AND C.LOCATOR != 0 \n" +
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

        System.out.println("Starting CtoFeeWhTest.test3,sql1");
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
            System.out.println("CtoFeeWh.test3 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CtoFeeWhTest.test3,sql2");
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
            System.out.println("CtoFeeWh.test3 sql2 failed");
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

        System.out.println("Finish CtoFeeWhTest.test3");
        System.out.println();

    }

    @Test
    public void test4(){

        // Check the population of the CTO_FEE_WH.CTO_FEE_AMT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.CTO_FEE_WH where CTO_FEE_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.CTO_FEE_WID, A.CTO_FEE_AMT, C.COST \n" +
                        "\t FROM DTSDM.CTO_FEE_WH A, DTSDM.TICKET_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.TICKET C \n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.PNR_LOCATOR = C.LOCATOR \n" +
                        "\t AND C.ADJ_LEVEL = 0 \n" +
                        "\t AND C.TICKNUM LIKE ‘890%’ \n" +
                        "\t AND C.LOCATOR IS NOT NULL \n" +
                        "\t AND C.LOCATOR != 0 \n" +
                        "\t AND A.CTO_FEE_AMT = C.COST \n" +
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

        System.out.println("Starting CtoFeeWhTest.test4,sql1");
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
            System.out.println("CtoFeeWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CtoFeeWhTest.test4,sql2");
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
            System.out.println("CtoFeeWh.test4 sql2 failed");
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

        System.out.println("Finish CtoFeeWhTest.test4");
        System.out.println();

    }

    @Test
    public void test5(){

        // Check the population of the CTO_FEE_WH.PYMT_MODE_CODE column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.CTO_FEE_WH where CTO_FEE_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.CTO_FEE_WID, A.PYMT_MODE_CODE, D.MODE_ \n" +
                        "\t FROM DTSDM.CTO_FEE_WH A, DTSDM.TICKET_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.TICKET C, DTSDM_SRC_STG.ITINRY D \n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.PNR_LOCATOR = C.LOCATOR \n" +
                        "\t AND C.ADJ_LEVEL = 0 \n" +
                        "\t AND C.TICKNUM LIKE ‘890%’ \n" +
                        "\t AND C.LOCATOR IS NOT NULL \n" +
                        "\t AND C.LOCATOR != 0 \n" +
                        "\t AND A.PYMT_MODE_CODE = D.MODE_ \n" +
                        "\t AND D.MODE IN (‘CF’, ‘CF-C’, ‘CF-I’) \n" +
                        "\t AND D.U##VCHNUM = C.U##VCHNUM \n" +
                        "\t AND D.U##DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND D.U##SSN = C.U##SSN \n" +
                        "\t AND D.ADJ_LEVEL = C.ADJ_LEVEL \n" +
                        "\t AND D.TICKNUM = C.TICKNUM \n" +
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

        System.out.println("Starting CtoFeeWhTest.test4,sql1");
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
            System.out.println("CtoFeeWh.test4 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CtoFeeWhTest.test4,sql2");
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
            System.out.println("CtoFeeWh.test4 sql2 failed");
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

        System.out.println("Finish CtoFeeWhTest.test4");
        System.out.println();

    }

    @Test
    public void test6(){

        // Check the population of the CTO_FEE_WH.CTO_FEE_NUM column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.CTO_FEE_WH where CTO_FEE_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.CTO_FEE_WID, A.CTO_FEE_NUM, C.TICKNUM \n" +
                        "\t FROM DTSDM.CTO_FEE_WH A, DTSDM.TICKET_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.TICKET C \n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.PNR_LOCATOR = C.LOCATOR \n" +
                        "\t AND C.ADJ_LEVEL = 0 \n" +
                        "\t AND C.TICKNUM LIKE ‘890%’ \n" +
                        "\t AND C.LOCATOR IS NOT NULL \n" +
                        "\t AND C.LOCATOR != 0 \n" +
                        "\t AND A.CTO_FEE_NUM = C.TICKNUM \n" +
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

        System.out.println("Starting CtoFeeWhTest.test6,sql1");
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
            System.out.println("CtoFeeWh.test6 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CtoFeeWhTest.test6,sql2");
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
            System.out.println("CtoFeeWh.test6 sql2 failed");
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

        System.out.println("Finish CtoFeeWhTest.test6");
        System.out.println();

    }

    @Test
    public void test7(){

        // Check the population of the CTO_FEE_WH.CTO_FEE_DT column

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select * from DTSDM.CTO_FEE_WH where CTO_FEE_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.CTO_FEE_WID, A.CTO_FEE_DT, C.TICKET_DATE \n" +
                        "\t FROM DTSDM.CTO_FEE_WH A, DTSDM.TICKET_WH B, \n" +
                        "\t\t\t DTSDM_SRC_STG.TICKET C \n" +
                        "\t WHERE A.TICKET_WID = B.TICKET_WID \n" +
                        "\t AND B.SRC_VCHNUM = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.PNR_LOCATOR = C.LOCATOR \n" +
                        "\t AND C.ADJ_LEVEL = 0 \n" +
                        "\t AND C.TICKNUM LIKE ‘890%’ \n" +
                        "\t AND C.LOCATOR IS NOT NULL \n" +
                        "\t AND C.LOCATOR != 0 \n" +
                        "\t AND A.CTO_FEE_DT = C.TICKET_DATE \n" +
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

        System.out.println("Starting CtoFeeWhTest.test7,sql1");
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
            System.out.println("CtoFeeWh.test7 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting CtoFeeWhTest.test7,sql2");
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
            System.out.println("CtoFeeWh.test7 sql2 failed");
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

        System.out.println("Finish CtoFeeWhTest.test7");
        System.out.println();

    }

}
