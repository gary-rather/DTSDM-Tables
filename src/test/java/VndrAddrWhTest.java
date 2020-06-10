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
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VndrAddrWhTest extends TableTest {

    @BeforeClass
    public  static void openResults(){
        wr = new WriteResults("VndrWhTest.html");
        wr.pageHeader();
    }


    @Test
    /**
     * --VNDR_ADDR_WH ROW COUNT Althea
     */
    public void test01() {
        // Log the Class and method
        System.out.println("Starting " + this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());
        wr.printDiv(this.getClass().getSimpleName() + " " + new Throwable().getStackTrace()[0].getMethodName());


        String sql = "select src.src_cnt - trgt.trgt_cnt rcd_cnt_discrepancy\n" +
                "from\n" +
                "    (\n" +
                "        select count(*) src_cnt\n" +
                "        from\n" +
                "            (\n" +
                "                select distinct nvl(v_address, 'X') v_address,\n" +
                "                nvl(v_city, 'X') v_city,\n" +
                "                nvl(v_state, 'X') v_state,\n" +
                "                v_country,\n" +
                "                nvl(v_zip, 'X') v_zip,\n" +
                "                nvl(v_phone, 'X') v_phone,\n" +
                "                nvl(trim(dep_lat), 'X') dep_lat,\n" +
                "                nvl(trim(dep_long), 'X') dep_long\n" +
                "                from dtsdm_src_stg.ticksub a\n" +
                "                where exists\n" +
                "                    (select vndr_name\n" +
                "                     from dtsdm.vndr_wh b\n" +
                "                     where b.vndr_name  = a.vendor)\n" +
                "            )\n" +
                "        ) src,\n" +
                "        (\n" +
                "            select count(*) trgt_cnt\n" +
                "            from dtsdm.vndr_addr_wh\n" +
                "            where vndr_addr_wid != 0\n" +
                "        ) trgt\n";

        // log the Sql
        ArrayList<SqlObject> theSql = new ArrayList<SqlObject>();
        SqlObject sqlObj = new SqlObject("sql",sql.replaceAll("\n","\n<br>"));
        theSql.add(sqlObj);
        wr.logSql(theSql);

        int rcdCntDiscrepancy = 0;

        System.out.println("Starting VndrWhTest.test1,sql");
        try {
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                // ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery();) {
                    // System.out.println("Size of results = " + rs.getInt(1));
                    while (rs.next()) {
                        rcdCntDiscrepancy = rs.getInt(1);

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Log the results before
        ArrayList<ResultObject> roList = new ArrayList<>();
        ResultObject ro1 = new ResultObject((0 == rcdCntDiscrepancy),"(0 == rcdCntDiscrepancy)");
        roList.add(ro1);
        wr.logTestResults(roList);

        System.out.println("Test VndrWh  rcdCntDiscrepancy == " + rcdCntDiscrepancy);
        assertEquals(0, rcdCntDiscrepancy);

        System.out.println("Finish VndrWhTest.test1");
        System.out.println();
    }



}