package main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.ADao;
import util.ASao;
import util.RDao;

public class Worker implements Callable<Boolean> {
	private static final Logger LOG = LogManager.getLogger(Worker.class);
	int thNo;
	int thAll;
	String rdbUrl;
	String rdbUser;
	String rdbPassword;
	String dbType;
	int agentPort;
	String sql;
	String gmtBase;

	public Worker(int thNo, int thAll, String rdbUrl, String rdbUser,
			String rdbPasswd, int agentPort, String dbType, String sql,
			String gmtBase) {
		this.thNo = thNo;
		this.thAll = thAll;
		this.rdbUrl = rdbUrl;
		this.rdbUser = rdbUser;
		this.rdbPassword = rdbPasswd;
		this.agentPort = agentPort;
		this.dbType = dbType;
		this.sql = sql;
		this.gmtBase = gmtBase;
	}

	@Override
	public Boolean call() throws Exception {
		RDao rDao = new RDao();
		Connection conn = rDao.getConnection(rdbUrl, rdbUser, rdbPassword);
		ArrayList<String> hosts = rDao.getHostsMT(conn, thNo - 1, thAll, sql);
		// ArrayList<String> hosts = rDao.getHostsTest();
		HashMap<String, Boolean> isV3 = rDao.getV3Info(conn);

		int i = 0;
		ADao adao = new ADao();
		ASao asao = new ASao();
		DateTime start = new DateTime();
		for (String host : hosts) {
			rDao.setEventStartTimestamp(conn, host);
			if (isV3.containsKey(host) && isV3.get(host)) {
				LOG.trace("V3:"+thNo + "-" + i + ":Checking:" + host);
				i++;
				String line = asao.getEventGet(host, agentPort);
				LOG.info("outFrADaoV3=" + line);
				if (line.length() > 0) {
					rDao.insertEventOraV3(conn, host, line, dbType, gmtBase);
				}
			} else {
				LOG.trace("V2:" + thNo + "-" + i + ":Checking:" + host);
				i++;
				String line = adao.getEvent(agentPort, host);
				LOG.info("outFrADaoV3=" + line);
				if (line.length() > 0) {
					rDao.insertEventOraV3(conn, host, line, dbType, gmtBase);
				}
			}
			rDao.setEventEndTimestamp(conn, host);
		}
		rDao.setWorkingTimestamp(conn, rdbUrl, thNo);
		DateTime end = new DateTime();
		Duration elapsedTime = new Duration(start, end);
		LOG.info(elapsedTime);
		rDao.disconnect(conn);
		return true;
	}
}