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
public class AccntBdgtLabelWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("AccntBdgtLabelWhTest.html");
        wr.pageHeader();
    }


    @Test
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);

        String sql = "select count(*) from ACCNT_BDGT_LABEL_WH where ACCNT_BDGT_LABEL_WID = 0";

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

        System.out.println("Test AccntBdgtLabelWh  Row 0 1= " + number);
        assertEquals(1, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test01");
        System.out.println();
    }

    @Test
    public void test02() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " one";
        String reason = " two";
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition,reason);



        String sql1 = "select count(*) from (\n" +
                "select distinct SRC_BUDGET_LEDGER_ID, count (*)\n" +
                "from ACCNT_BDGT_LABEL_WH\n" +
                "group by src_budget_ledger_id\n" +
                "having count(*) > 1\n" +
                ")\n" ;

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

        System.out.println("Test AccntBdgtLabelWh  group by count > 0 " + number);
        assertEquals(0, number);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test02");
        System.out.println();
    }



}
