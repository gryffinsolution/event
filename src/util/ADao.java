package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ADao {
	private static final Logger LOG = LogManager.getLogger(ADao.class);

	public boolean isWorking() {
		return true;
	}

	public static String printSQLException(SQLException e) {
		LOG.error("\n----- SQLException -----");
		LOG.error("  SQL State:  " + e.getSQLState());
		LOG.error("  Error Code: " + e.getErrorCode());
		LOG.error("  Message:    " + e.getMessage());
		if (e.getMessage().contains("Table/View")
				|| e.getMessage().contains(" does not exist.")) {
			LOG.fatal(e.getMessage());
			return "error__NoTable";
		}
		if (e.getMessage().contains("Error connecting to server")) {
			LOG.info(e.getMessage());
			return "error__connection";
		}
		return "error__unknown";
	}

	public String getEvent(int port, String host) {
		Connection conn = null;
		Properties props = new Properties();
		props.put("user", "agent");
		props.put("password", "catallena7");
		String protocol = null;
		StringBuffer sb = new StringBuffer();
		if (host.matches("localhost.localdomain")) {// TEST
			host = "192.168.178.131";
		}
		protocol = "jdbc:derby://" + host + ":" + port + "/";

		PreparedStatement pst = null;
		ResultSet rs = null;
		Statement s = null;
		long lastId = 0L;
		try {
			DriverManager.setLoginTimeout(5);
			conn = DriverManager.getConnection(
					protocol + "derbyDB;create=true", props);
			String sql = "SELECT ID,EVENT_CODE,SEVERITY,MESSAGE,TIME FROM AGENT_EVENT WHERE ID> (SELECT LAST_SENT_EVENT_ID FROM AGENT_MGR WHERE ID=0)  ORDER BY ID";
			LOG.trace(host + ":" + sql);
			s = conn.createStatement();
			rs = s.executeQuery(sql);

			while (rs.next()) {
				lastId = rs.getLong("ID");
				sb.append(lastId);
				sb.append(",");
				sb.append(rs.getString("EVENT_CODE"));
				sb.append(",");
				sb.append(rs.getString("SEVERITY"));
				sb.append(",");
				sb.append(rs.getString("MESSAGE"));
				sb.append(",");
				sb.append(rs.getTimestamp("TIME"));
				sb.append("\n");
			}
			if (sb.length() > 0)
				LOG.info(host + ":res=" + sb);
			if (sb.length() > 3 && lastId > 0) {
				sql = "UPDATE AGENT_MGR SET LAST_SENT_EVENT_ID=" + lastId
						+ " WHERE ID=0";
				LOG.trace(host + ":" + sql);
				pst = conn.prepareStatement(sql);
				pst.executeUpdate();
				conn.commit();
			}
		} catch (SQLException sqle) {
			return (printSQLException(sqle));
		} finally {
			try {
				if (pst != null) {
					pst.close();
					pst = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
					return sb.toString();
				}
			} catch (SQLException e) {
				return (printSQLException(e));
			}
		}
		return sb.toString();
	}
}