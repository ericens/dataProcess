package org.zlx.hive;

import java.sql.DriverManager;

import java.sql.*;

/**
 * Created by @author linxin on 07/06/2018.  <br>
 */
public class BlockingWord {

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try{
            Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/default","ericens",null);
            PreparedStatement sta = con.prepareStatement("select * from test1");
            ResultSet result = sta.executeQuery();
            while(result.next()){
                System.out.println(result.getString(1));
                System.out.println(result.getString(2));
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

}
