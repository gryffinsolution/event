package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;

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

	public static void printSQLExceptionHost(String host, SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Host:" + host + ",Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL=" + dbURL);
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

	public int getTotalHostNo(Connection con) {
		Statement stmt;
		int NoOfHost = 0;
		try {
			String sql = "SELECT DISTINCT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				NoOfHost = rs.getInt(1);
				LOG.info("Total hosts=" + NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return NoOfHost;
	}

	public ArrayList<String> getHostsMT(Connection con, int seq, int Total,
			String sql) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try {
			int NoOfHost = getTotalHostNo(con);
			int sliceTerm = (int) Math.ceil(NoOfHost / (Total * 1.0));
			LOG.info("GAP=>" + sliceTerm);
			int sliceStart = 0;
			int sliceEnd = 0;
			sliceStart = sliceStart + sliceTerm * seq;
			sliceEnd = sliceStart + sliceTerm - 1;
			LOG.info(seq + 1 + ":" + sliceStart + "~" + sliceEnd);
			sql = sql + " AND HOST_NO > " + sliceStart + " AND HOST_NO < "
					+ sliceEnd;
			LOG.info(sql);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				LOG.info(seq + 1 + ":hostname" + host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}

	public ArrayList<String> getHostsTest() {
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}

	public void setWorkingTimestamp(Connection conR, String dbURL, int thNo) {
		PreparedStatement pst = null;
		try {
			conR.setAutoCommit(false);

			String sqlLastUpdateTime = null;

			if (dbURL.startsWith("jdbc:postgresql:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=NOW() WHERE SERVICE_NAME='event"
						+ thNo + "'"; // pgsql
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='event"
						+ thNo + "'";// oracle
			} else {
				LOG.fatal("Can't find right JDBC. please check you config.xml");
				System.exit(0);
			}

			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public void setEventStartTimestamp(Connection conn, String hostname) {
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			String sql = "UPDATE HOST_INFOS SET EVENT_START_TIME=SYSDATE WHERE HOSTNAME='"
					+ hostname + "'";
			LOG.trace(sql);
			pst = conn.prepareStatement(sql);
			pst.executeUpdate();
			conn.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public void setEventEndTimestamp(Connection conn, String hostname) {
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			String sql = "UPDATE HOST_INFOS SET EVENT_LAST_TIME=SYSDATE WHERE HOSTNAME='"
					+ hostname + "'";
			LOG.trace(sql);
			pst = conn.prepareStatement(sql);
			pst.executeUpdate();
			conn.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public boolean insertEventOra(Connection conR, String host,
			String allLines, String dbType) {
		PreparedStatement pst = null;
		if (allLines.length() <= 0) {
			return false;
		}
		try {
			conR.setAutoCommit(false);
			String[] lines = allLines.split("\n");
			for (String line : lines) {
				LOG.info(line);
				String[] items = line.toString().split(",");
				String sql = null;

				LOG.info("date=" + items[4]);
				String[] dates = items[4].split("\\.");
				LOG.info("date=" + dates[0]);
				sql = "INSERT INTO HOST_EVENT "
						+ "       (HOSTNAME,ARRIVAL_TIME,LOCAL_ID,EVENT_CODE,SEVERITY,MESSAGE,START_TIME,LAST_EVENT_TIME,REPEAT_CNT) "
						+ "VALUES ('" + host + "',CURRENT_TIMESTAMP,"
						+ items[0] + ",'" + items[1] + "','" + items[2] + "','"
						+ items[3] + "',TO_DATE('" + dates[0]
						+ "', 'YYYY-MM-DD HH24:MI:SS'),TO_DATE('" + dates[0]
						+ "', 'YYYY-MM-DD HH24:MI:SS')," + 0 + ")";
				LOG.trace(sql);
				pst = conR.prepareStatement(sql);
				pst.executeUpdate();
				conR.commit();
				pst.close();
			}
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error("IndexOut " + host + " " + allLines);
		} finally {
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

	public void insertEventOraV3(Connection conR, String host, String allLines,
			String dbType, String gmtBase) {
		PreparedStatement pst = null;
		if (allLines.length() <= 0) {
			return;
		}
		try {
			conR.setAutoCommit(false);
			String[] lines = allLines.split("\n");
			for (String line : lines) {
				LOG.info(line);
				String[] items = line.toString().split(",");
				String sql = null;

				LOG.info("date=" + items[4]);
				long epTime = Long.parseLong(items[4]) * 1000L;
				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss");
				sdf.setTimeZone(TimeZone.getTimeZone(gmtBase));
				String strDate = sdf.format(epTime);

				LOG.info("date=" + strDate);
				sql = "INSERT INTO HOST_EVENT "
						+ "       (HOSTNAME,ARRIVAL_TIME,LOCAL_ID,EVENT_CODE,SEVERITY,MESSAGE,START_TIME,LAST_EVENT_TIME,REPEAT_CNT) "
						+ "VALUES ('" + host + "',CURRENT_TIMESTAMP,"
						+ items[0] + ",'" + items[1] + "','" + items[2] + "','"
						+ items[3] + "',TO_DATE('" + strDate
						+ "', 'YYYY-MM-DD HH24:MI:SS'),TO_DATE('" + strDate
						+ "', 'YYYY-MM-DD HH24:MI:SS')," + 0 + ")";
				LOG.trace(sql);
				pst = conR.prepareStatement(sql);
				pst.executeUpdate();
				conR.commit();
				pst.close();
			}
		} catch (SQLException sqle) {
			printSQLExceptionHost(host, sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error("IndexOut " + host + " " + allLines);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public HashMap<String, Boolean> getV3Info(Connection conn) {
		HashMap<String, Boolean> isV3 = new HashMap<String, Boolean>();
		Statement stmt;
		try {
			String sql = "SELECT DISTINCT HOSTNAME,IS_V3 FROM HOST_INFOS WHERE IS_V3=1 ";
			LOG.info(sql);
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				isV3.put(host, true);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return isV3;
	}
}