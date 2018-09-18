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
}