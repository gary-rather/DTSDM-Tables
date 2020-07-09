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
public class PerstempoAggrWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("PerstempoAggrWhTest.html");
        wr.pageHeader();
    }

    @Test
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "Select count(*) from DTSDM.PERSTEMPO_AGGR_WH where PERSTEMPO_AGGR_WID = 0 \n";

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

        System.out.println("Test PerstempoAggrWhTest Row 0 = " + number);
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

        String sql1 = "select PERSTEMPO_AGGR_WID from DTSDM.PERSTEMPO_AGGR_WH" +
                        "group by PERSTEMPO_AGGR_WID having count (*) > 1";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<>();
        SqlObject sqlObj1 = new SqlObject("sql1",sql1.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj1);

        wr.logSql(theSql);

        int dupeCount = 0;

        System.out.println("Starting "+ this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName() + " sql1" );
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql1)) {
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
        ResultObject ro1 = new ResultObject((0 == dupeCount),"(0 == dupeCount)");
        roList.add(ro1);

        wr.logTestResults(roList);

        System.out.println("Test PerstempoAggrWhTest dupeCount= " + dupeCount);
        assertEquals(0, dupeCount);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test02");
        System.out.println();

    }

    @Test
    public void test03(){

        // Check the population of the DCMNT_WID and PERSTEMPO_CODE_LST columns
        // (looking for correct distinct combinations)

        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);


        String sql1 = "select count(*) from DTSDM.PERSTEMPO_AGGR_WH where PERSTEMPO_AGGR_WID != 0";

        String sql2 = "select count (*) from \n" +
                        "( \n" +
                        "\t SELECT DISTINCT A.PERSTEMPO_AGGR_WID, A.DCMNT_WID, B.DCMNT_WID \n" +
                        "\t\t\t\t\t\t\t\t\t A.PERSTEMPO_CODE_LST, D.PERSTEMPO_CODE \n" +
                        "\t FROM DTSDM.PERSTEMPO_AGGR_WH A, DTSDM.DCMNT_WH B, \n" +
                        "\t\t\t\t DTSDM_SRG_STG.TAOTHER C, DTSDM.DCMNT_OTHER_AUTH_WH D \n" +
                        "\t WHERE A.DCMNT_WID = B.DCMNT_WID \n" +
                        "\t AND B.DCMNT_WID = D.DCMNT_WID \n" +
                        "\t AND B.DCMNT_NAME = C.U##VCHNUM \n" +
                        "\t AND B.SRC_DOCTYPE = C.U##DOCTYPE \n" +
                        "\t AND B.SRC_SSN = C.U##SSN \n" +
                        "\t AND B.TRIP_NUM = C.TRIPNUM \n" +
                        "\t AND B.ADJSTMT_LVL = C.ADJ_LEVEL \n" +
                        "\t A.PERSTEMPO_CODE_LST = D.PERSTEMPO_CODE \n" +
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

        System.out.println("Starting PerstempoAggrWhTest.test03,sql1");
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
            System.out.println("PerstempoAggrWh.test03 sql1 failed");
            e.printStackTrace();
        }

        System.out.println("Starting PerstempoAggrWhTest.test03,sql2");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql2)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        testCount++;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("PerstempoAggrWh.test03 sql2 failed");
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

        System.out.println("Finish PerstempoAggrWhTest.test03");
        System.out.println();

    }

}
