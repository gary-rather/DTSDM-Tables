import java.util.ArrayList;

public class SqlObject {

    String label = "&nbsp;";
    String sql = "&nbsp;";

    public SqlObject(String label, String sql){
        this.label = label;
        this.sql = sql;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

}
