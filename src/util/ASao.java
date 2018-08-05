package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ASao {
	private static final Logger LOG = LogManager.getLogger(ASao.class);

	public boolean isWorking() {
		return true;
	}

	public String getEventGet( String host,int port){
		String url ="http://"+host+":"+port+"/getEvent";
		HttpURLConnection con = null;
		try{
            URL myurl = new URL(url);
            con = (HttpURLConnection) myurl.openConnection();
            con.setRequestMethod("GET");
            StringBuilder content;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {

                String line;
                content = new StringBuilder();

                while ((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
            }
            System.out.println(content.toString());
            con.disconnect();
            return content.toString();
        }catch (MalformedURLException e){
        	LOG.error(e);
        	return null;
        }catch (ProtocolException e){
        	LOG.error(e);
        	return null;
        }catch (IOException e){
        	LOG.error(e);
        	return null;
        }catch (Exception e){
        	LOG.error(e);
        	return null;
        }finally {
            con.disconnect();
		}
	}
	
	public boolean setEventEndTimestamp(String host, int port,long id) {
		String url ="http://"+host+":"+port+"/setLastEventId?eventID="+id;
		HttpURLConnection con = null;
		try{
            URL myurl = new URL(url);
            con = (HttpURLConnection) myurl.openConnection();
            con.setRequestMethod("GET");
            con.setReadTimeout(100);//TEST
            StringBuilder content;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {

                String line;
                content = new StringBuilder();

                while ((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
            }
            System.out.println(content.toString());
            con.disconnect();
            return true;
        }catch (MalformedURLException e){
        	LOG.error(e);
        	return false;
        }catch (ProtocolException e){
        	LOG.error(e);
        	return false;
        }catch (IOException e){
        	LOG.error(e);
        	return false;
        }catch (Exception e){
        	LOG.error(e);
        	return false;
        }finally {
            con.disconnect();
		}
	}
	

	public String getHostInfo( String host,int port){
		String url ="http://"+host+":"+port+"/getHostDataAgntMgr";
		HttpURLConnection con = null;
		try{
            URL myurl = new URL(url);
            con = (HttpURLConnection) myurl.openConnection();
            con.setRequestMethod("GET");
            StringBuilder content;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {

                String line;
                content = new StringBuilder();

                while ((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
            }
            System.out.println(content.toString());
            con.disconnect();
            return content.toString();
        }catch (MalformedURLException e){
        	LOG.error(e);
        	return null;
        }catch (ProtocolException e){
        	LOG.error(e);
        	return null;
        }catch (IOException e){
        	LOG.error(e);
        	return null;
        }catch (Exception e){
        	LOG.error(e);
        	return null;
        }finally {
            con.disconnect();
		}
	}
	
/*
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
	}*/

	
}