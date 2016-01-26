package main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.ADao;
import util.Conf;
import util.RDao;

public class EventCollector {
	private static final Logger LOG = LogManager.getLogger(EventCollector.class);
	
	public void setConfValue(Conf cf, String[] args){
		if (args.length <1){
			LOG.error("please input config.xml as args");
			System.exit(0);
		}
		cf.setConfFile(args[0]);
	}
	public static void printSQLException(SQLException e){
		while (e != null){
            LOG.error("\n----- SQLException -----");
            LOG.error("  SQL State:  " + e.getSQLState());
            LOG.error("  Error Code: " + e.getErrorCode());
            LOG.error("  Message:    " + e.getMessage());
            e = e.getNextException();
        }
    }
	public static void main(String[] args) {
		
		Conf cf = new Conf();
		EventCollector ctrl = new EventCollector();
		ctrl.setConfValue(cf,args);
		RDao rDao= new RDao();
		int connection_limit=cf.getSinglefValue("agent_connection_trial_limit");
		Connection conR=rDao.getConnection(cf.getDbURL(),cf.getSingleString("user"),cf.getSingleString("password"));
		
		String dbType=null;
		
		if (cf.getDbURL().startsWith("jdbc:postgresql:")){
			dbType="postgresql";
		}else if (cf.getDbURL().startsWith("jdbc:oracle:")) {
			dbType="oracle";
		}else{
			LOG.fatal("Can't find right JDBC. please check you config.xml");
			System.exit(0);
		}
		
		//-----------------------------Agent Dao Set----------
		ADao aDao = new ADao();
		
		HashMap <String,Integer> conFailedHosts= new HashMap<String, Integer>();
		ArrayList <String> failed_hosts=new ArrayList<String>();
		int port = cf.getSinglefValue("agent_port");
		
		while(true){
			int i=0;
			DateTime start = new DateTime();
			ArrayList <String> hosts=rDao.getHosts(conR,cf);
			rDao.setWorkingTimestamp(conR, cf,dbType);
			for (String host:hosts){
				i++;
				LOG.trace(i+":"+host);
				String lines=aDao.getEvent(port,host);
				
				if (lines.contains("error__")){ // fail or no return value TODO we have to distinguish
					int trial=0;
					if (conFailedHosts.containsKey(host)){
						trial =conFailedHosts.get(host);
						trial++;
						LOG.info("host connection error set "+host+" cnt="+trial);
						conFailedHosts.put(host,trial);
					}else{
						LOG.info("host connection error set "+host+" cnt=1");
						conFailedHosts.put(host,1);
					}
					if (trial >connection_limit){
						LOG.info("host connection error set "+host+" cnt=1");
						failed_hosts.add(host);
					}
					if (!failed_hosts.isEmpty()){
						LOG.info("host disable procedure started");
						rDao.disableHosts(conR,failed_hosts);
						failed_hosts.clear();
					}
				}else{
					
					if (conFailedHosts.containsKey(host)){
						conFailedHosts.remove(host);
					}
					LOG.info(lines);
					rDao.insertEvent(conR,host,lines,dbType);
				}
			}
			DateTime end = new DateTime();
			Duration elapsedTime= new Duration(start,end);
			LOG.info(elapsedTime);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//rDao.disconnect(conR);
	}
}
