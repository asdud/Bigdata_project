package day0706.blacklist;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class BlackListMySQLBolt extends BaseRichBolt {
	private static String driver = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql://192.168.157.21:3306/demo";
	private static String user = "demo";
	private static String password = "Welcome_1";
	
	//注册驱动
	static{
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	

	public void execute(Tuple tuple) {
		//每10秒钟，统计过去30秒内访问频率超过4次的用户信息 ----> 写到MySQL中
		//得到数据
		int userid = tuple.getIntegerByField("userid");
		int pv = tuple.getIntegerByField("PV");
		
		//如果MySQL不存在该userid，就执行插入；如果已经存在就执行更新操作。
		String sql = "insert into myresult(userid,PV) values("+userid+","+pv+") on duplicate key update PV=PV+"+pv;
		
		Connection conn = null;
		Statement st = null;
		try{
			conn = DriverManager.getConnection(url, user, password);
			st = conn.createStatement();
			st.execute(sql);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(st != null){
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}












