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

public class TripExpnsFactTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("TripExpnsFactTest.html");
        wr.pageHeader();
    }


    @Test
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        String condition = " ReplaceConditionText";
        String reason = " ReplaceReasonText";

        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName(), condition, reason);



        String sql1 = "select count(*) from \n" +
                "(\n" +
                "select distinct  EXPNS_SMRY_WID, count(*)\n" +
                "from DTSDM_BI.TRIP_EXPNS_FACT\n" +
                "group by EXPNS_SMRY_WID \n" +
                "having count(*) > 1\n" +
                ")";

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

        System.out.println("Test TripExpnsFactTest  dupeCount= " + dupeCount);
        assertEquals(0, dupeCount);

        System.out.println("Finish " +  this.getClass().getSimpleName() + ".test02");
        System.out.println();
    }



}
