package main;

import java.sql.Connection;
import java.util.ArrayList;
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

	public Worker(int thNo, int thAll, String rdbUrl, String rdbUser,
			String rdbPasswd, int agentPort, String dbType, String sql) {
		this.thNo = thNo;
		this.thAll = thAll;
		this.rdbUrl = rdbUrl;
		this.rdbUser = rdbUser;
		this.rdbPassword = rdbPasswd;
		this.agentPort = agentPort;
		this.dbType = dbType;
		this.sql = sql;
	}

	@Override
	public Boolean call() throws Exception {
		RDao rDao = new RDao();
		//Connection conn = rDao.getConnection(rdbUrl, rdbUser, rdbPassword);
		//ArrayList<String> hosts = rDao.getHostsMT(conn, thNo-1, thAll, sql);
		ArrayList <String> hosts = rDao.getHostsTest();

		int i = 0;
		//ADao adao = new ADao();
		ASao asao = new ASao();
		DateTime start = new DateTime();
		for (String host : hosts) {
			//rDao.setEventStartTimestamp(conn, host);
			LOG.trace(thNo + "-" + i + ":Checking:" + host);
			i++;
			//String line = adao.getEvent(agentPort, host);
		//	String line = asao.getEventGet(host,agentPort);
			String line = asao.getHostInfo(host,agentPort);
			
			LOG.info("outFrADao=" + line);
			/*
			if (line.length() > 0) {
				//long id=rDao.insertEventOra(conn, host, line, dbType);
				int id=rDao.insertEventOraTest( host, line, dbType);
				if( id > 0){
					asao.setEventEndTimestamp(host,agentPort,id);		
				}
			}*/
			//rDao.setEventEndTimestamp(conn,host);
		}
		//rDao.setWorkingTimestamp(conn, rdbUrl, thNo);
		DateTime end = new DateTime();
		Duration elapsedTime = new Duration(start, end);
		LOG.info(elapsedTime);
		//rDao.disconnect(conn);
		return true;
	}
}