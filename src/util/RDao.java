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
		}catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		}catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		}catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public ArrayList<String> getHosts(Connection con,Conf cf) {
		Statement stmt;
		int startNo=cf.getSinglefValue("start_host_no");
		int endNo=cf.getSinglefValue("end_host_no");
		ArrayList<String> hostList = new ArrayList<String>();
		int i=0;
		try {
			
			//TODO String sql = "SELECT DISTINCT HOSTNAME FROM HOST_INFOS WHERE "; add condition
			String sql = "SELECT DISTINCT HOSTNAME FROM HOST_INFOS WHERE STATUS = 'up'  and (HOST_NO >="+startNo+
					" and HOST_NO <="+endNo+")";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				i++;
				String host=rs.getString("HOSTNAME");
				LOG.info(i+":hostnanme="+host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		}catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}
/*
	public long getOldEventID(Connection con, String hostname) {
		long id = 0;
		Statement stmt;
		LOG.info(hostname);
		try {
			String sql = "SELECT MAX(LOCAL_ID) FROM EVENT WHERE HOSTNAME='"
					+ hostname + "'";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				id = rs.getLong(1);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		} 
		LOG.trace(id);
		return id;
	}*/

	public void disableHosts(Connection conR,ArrayList<String> failed_hosts) {
		for (String host:failed_hosts){
			String sql = null;
			PreparedStatement pst = null;
			LOG.fatal("Manager can't connect to "+host);
			LOG.fatal("No more connection try to "+host);
			sql = "UPDATE HOST_INFOS SET STATUS ='NotConnected' WHERE HOSTNAME='"+host+"'";
			LOG.trace(sql);
			
			try {
				pst = conR.prepareStatement(sql);
				conR.setAutoCommit(false);
				pst.executeUpdate();
				conR.commit();
				pst.close();
			}catch (SQLException sqle) {
				printSQLException(sqle);
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

	public void updateLastUpdateTime(Connection conR, String host){
		PreparedStatement pst =null;
		try{
			conR.setAutoCommit(false);
			String sqlLastUpdateTime = "UPDATE HOST_INFOS SET LAST_UPDATED_TIME=SYSDATE WHERE HOSTNAME='"+host+"'";
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
