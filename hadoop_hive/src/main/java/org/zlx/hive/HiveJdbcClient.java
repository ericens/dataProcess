package org.zlx.hive;

/**
 * Created by @author linxin on 14/06/2018.  <br>
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

/**
 * Created by wmky_kk on 2017-09-14.
 */

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver"; // 如果写成org.apache.hadoop.hive.jdbc.HiveDriver 会报java.lang.ClassNotFoundException

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try{

            UserGroupInformation.loginUserFromKeytab("dengsc@HADOOP.COM", "src/main/Resources/dengsc.keytab");
        }catch (IOException e){
            System.out.println(e);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.250.40:10000/tmp;principal=hive/nfjd-hadoop001@HADOOP.COM", "", "");
        Statement stmt = con.createStatement();
        String tableName = "test_table";
        // load
        String sql2 = "load data inpath '/user/dengsc/bcd.txt' into table " + tableName;
        stmt.execute(sql2);
        System.out.println("sql2 over!");
        String sql1 = "load data local inpath '/home/dengsc/abc.txt' into table " + tableName;
        stmt.execute(sql1);
        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
        }

    }
}
