package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL="+dbURL);
		Connection con = null;
		try {
			if (dbURL.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		} catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public int getTotalHostNo(Connection con){
		Statement stmt;
		int NoOfHost=0;
		try{
			String sql = "SELECT DISTINCT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()){
				NoOfHost=rs.getInt(1);
				LOG.info("Total hosts="+NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return NoOfHost;
	}
	
	public ArrayList<String> getHostsMT(Connection con,int seq,int Total){
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try{
			int NoOfHost=getTotalHostNo(con);
			int sliceTerm=(int) Math.ceil(NoOfHost/(Total*1.0));
			LOG.info("GAP=>"+sliceTerm);
			int sliceStart=0;
			int sliceEnd=0;
			sliceStart=sliceStart+sliceTerm*seq;
			sliceEnd=sliceStart+sliceTerm-1;
			LOG.info(seq+":"+sliceStart+"~"+sliceEnd);
			String sql="SELECT DISTINCT HOSTNAME FROM HOST_INFOS WHERE HOST_NO > "+sliceStart+" and HOST_NO <"+sliceEnd ;
			stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery(sql);
			while (rs.next()){
				String host = rs.getString("HOSTNAME");
				LOG.info(seq+":hostname"+host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return hostList;
	}
	
	public ArrayList<String> getHostsTest(Connection conR){
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}
	
	
	public boolean insertEvent(Connection conR, String host, String allLines,String dbType) {
		PreparedStatement pst = null;
		if (allLines.length()<=0){
			return false;
		}
		try {
			conR.setAutoCommit(false);
			String[] lines=allLines.split("\n");
			for (String line:lines){
				LOG.info(line);
				int repeatCnt=0;
				int localID=-1;
				int rows=-1;
				String[] items=line.toString().split(",");
				String sql=null;
				if (dbType.matches("oracle")){
					LOG.info(items[4]);
					String[] time=items[4].split("\\.");
					LOG.info(time[0]);
					sql = "SELECT DISTINCT REPEAT_CNT,LOCAL_ID FROM HOST_EVENT WHERE HOSTNAME='"+host+"' AND "
							+ " EVENT_CODE='"+items[1]+"' AND (to_date ('"+time[0]+"','YYYY-MM-DD HH24:MI:SS')- "
							+ "LAST_EVENT_TIME < NUMTODSINTERVAL('10','MINUTE') )"; //oracle
				}else if (dbType.matches("postgresql")){
					sql = "SELECT DISTINCT REPEAT_CNT,LOCAL_ID FROM HOST_EVENT WHERE HOSTNAME='"+host+"' AND "
						+ " EVENT_CODE='"+items[1]+"' AND (timestamp '"+items[4]+"'- LAST_EVENT_TIME  < time '0:10')"; //pgsql
				}else{
					LOG.fatal("Can't find right JDBC. please check you config.xml");
					System.exit(0);
				}
				
				LOG.info("sql="+sql);
				Statement stmt = conR.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					rows=rs.getRow();
					repeatCnt=rs.getInt("REPEAT_CNT");
					localID=rs.getInt("LOCAL_ID");
					LOG.info("hostnanme="+items[1]+":"+repeatCnt+":"+localID);
					
				}
				rs.close();
				stmt.close();
				LOG.info(rows);
				if (rows<=0){ // new arrival event
					//START_TIME TIMESTAMP, LAST_EVENT_TIME TIMESTAMP, REPEAT_CNT BIGINT
				 	//FIRST INTPUT
					sql = "INSERT INTO HOST_EVENT "+
							"       (HOSTNAME,ARRIVAL_TIME,LOCAL_ID,EVENT_CODE,SEVERITY,MESSAGE,START_TIME,LAST_EVENT_TIME,REPEAT_CNT) "+
							"VALUES ('"+host+"',CURRENT_TIMESTAMP,"+items[0]+",'"+items[1]+"','"+items[2]+"','"+items[3]+"','"+items[4]+"','"+items[4]+"',"+repeatCnt+")";
					LOG.trace(sql);
					pst = conR.prepareStatement(sql);
					pst.executeUpdate();
					conR.commit();
					pst.close();
				}else{
					repeatCnt++;
					sql = "UPDATE HOST_EVENT "+
							"SET LOCAL_ID="+items[0]+",MESSAGE='"+items[3]+"',LAST_EVENT_TIME='"+items[4]+"',REPEAT_CNT="+repeatCnt+" "+
						    "WHERE HOSTNAME='"+host+"' AND EVENT_CODE='"+items[1]+"' AND LOCAL_ID="+localID;
					LOG.trace(sql);
					pst = conR.prepareStatement(sql);
					pst.executeUpdate();
					conR.commit();
					pst.close();
					
				}
			}
		}catch (SQLException sqle) {
			printSQLException(sqle);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error("IndexOut "+host+" "+allLines);
			
		}finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
		return true;
	}
	
	public void setWorkingTimestamp(Connection conR, Conf cf,String dbType){
		PreparedStatement pst =null;
		try{
			conR.setAutoCommit(false);
			int svrNo=cf.getSinglefValue("service_no");
			String sqlLastUpdateTime =null;
			
			if (dbType.matches("oracle")){
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='EventCollector"+svrNo+"'"; //oracle
			}else if (dbType.matches("postgresql")){
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=NOW() WHERE SERVICE_NAME='EventCollector"+svrNo+"'";//pgsql
			}else{
				LOG.fatal("Can't find right JDBC. please check you config.xml");
				System.exit(0);
			}
			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		}catch (SQLException sqle) {
			printSQLException(sqle);
		}catch (ArrayIndexOutOfBoundsException e){
			
		}finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}
	
}