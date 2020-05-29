import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class WriteResults {
    PrintWriter printWriter = null;

    public WriteResults(String filename) {
        try {
            File outDIr = new File("target/out");
            if (!outDIr.exists()) outDIr.mkdir();
            File results = new File("target/out", filename);
            System.out.println("Open File: " + results.getAbsolutePath());
            FileWriter fw = new FileWriter(results, false);
            printWriter = new PrintWriter(fw);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void logTestResults(ArrayList<ResultObject> theTests) {

        StringBuffer sb = new StringBuffer();
        sb.append("<table id='tbl'>");
        for (ResultObject aRow : theTests) {
            if (aRow.theResult) {
                sb.append("<tr><td bgcolor='lightgreen'> Pass </td>");
            } else {
                sb.append("<tr><td bgcolor='red'> Fail </td>");
            }
            sb.append("<td>" + aRow.theTest + "</td></tr>");

        }
        sb.append("</table>");
        printWriter.println(sb.toString());

    }


    public void logSql(ArrayList<SqlObject> theSql) {

        StringBuffer sb = new StringBuffer();
        sb.append("<table id='tbl'>");
        for (SqlObject row : theSql) {
            sb.append("<tr><td>" + row.getLabel() + "</td>" + "<td>" + row.getSql() + "</td></tr>");
        }
        sb.append("</table>");
        printWriter.println(sb.toString());
    }


    public void printDiv(String text) {
        printWriter.println("<div id=\"divtitle\">" + text + "</div>");
    }
    public void printComment(String text) {
        printWriter.println("<div id=\"divcomment\">" + text + "</div>");
    }

    public void printYellowDiv(String text) {
        printWriter.println("<div id=\"divyellow\">" + text + "</div>");
    }

    public void openTable() {

        File aFile = new File("src/main/resources/tableStart.txt");
        try (Scanner sc = new Scanner(aFile)) {
            while (sc.hasNextLine()) {

                String s = sc.nextLine(); //Add line separator
                this.printWriter.println(s);
            }
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void closeTable() {
        printWriter.println("</table>");
        printWriter.flush();

    }

    public void closePage() {
        printWriter.println("    </body>");
        printWriter.println("</html>");
        printWriter.flush();
    }

    public void pageHeader() {

        File aFile = new File("src/main/resources/pageHeader.txt");
        try (Scanner sc = new Scanner(aFile)) {
            while (sc.hasNextLine()) {

                String s = sc.nextLine(); //Add line separator
                this.printWriter.println(s);
            }
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
