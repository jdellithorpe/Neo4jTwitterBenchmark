package org.ellitron.neo4jbenchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class TwitterWorkloadThread extends Thread {
	
	public int serverNumber;
	public int threadNumber;
	public double runTime;
	public double streamProb;
	public boolean loadBalanced;
	public long totUsers; 
	public int streamTxPgSize;
	public String datFilePrefix;
	public String masterServer;
	public List<String> slaveServers;
	public int workingSetSize;
	public String outputDir;
	
	public TwitterWorkloadThread(int serverNumber, int threadNumber, double runTime, double streamProb, boolean loadBalanced, long totUsers, int streamTxPgSize, String datFilePrefix, String masterServer, List<String> slaveServers, int workingSetSize, String outputDir) {
		this.serverNumber = serverNumber;
		this.threadNumber = threadNumber;
		this.runTime = runTime;
		this.streamProb = streamProb;
		this.loadBalanced = loadBalanced;
		this.totUsers = totUsers;
		this.streamTxPgSize = streamTxPgSize;
		this.datFilePrefix = datFilePrefix;
		this.masterServer = masterServer;
		this.slaveServers = slaveServers;
		this.workingSetSize = workingSetSize;
		this.outputDir = outputDir;
	}
	
	public void run() {
		try {
			// stat counters
			int streamTxCount = 0;
			int tweetTxCount = 0;

			// cypher queries
			String streamTxQuery = "START n=node({id}) MATCH (n)-[s:STREAM]->(t) RETURN t ORDER BY s.time DESC LIMIT " + Integer.toString(streamTxPgSize);
			String tweetTxQuery = "START n=node({id}) MATCH (m)-[:FOLLOWS]->(n) CREATE UNIQUE (n)-[:TWEET { time:{time} }]->(t:STATUS { text:{text}, time:{time} }) CREATE (m)-[:STREAM { time:{time} }]->(t)";

			String tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";

			// jersey client api
			Client c = Client.create();
			WebResource masterWR = c.resource("http://" + masterServer + ":7474/db/data/cypher");
			WebResource[] slaveWR = new WebResource[slaveServers.size()];
			for(int i = 0; i<slaveServers.size(); i++)
				slaveWR[i] = c.resource("http://" + slaveServers.get(i) + ":7474/db/data/cypher");

			// stat recording
			SummaryStatistics stSumStats = new SummaryStatistics();	// for stream transaction latencies
			SummaryStatistics twSumStats = new SummaryStatistics(); // for tweet transaction latencies

			// open file for recording individual latency measurements
			String latFileName;
			if(datFilePrefix != null)
				latFileName = datFilePrefix + "_s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".lat";
			else
				latFileName = "s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".lat";

			BufferedWriter latFileWriter = new BufferedWriter(new FileWriter(outputDir + "/" + latFileName));
			latFileWriter.write(String.format("%12s%12s%12s%12s%12s\n", "#USERID", "TXTYPE", "SERVER", "LATENCY", "STATUS"));

			// perform transactions in a loop for runTime minutes
			long threadStartTime = System.currentTimeMillis();	
			while(System.currentTimeMillis() - threadStartTime < runTime*60*1000) {
				double randDouble = Math.random();

				if(randDouble < streamProb) {
					// read a user's stream
					// select user uniformly at random from the set of all users
					int userId;
					if( workingSetSize == 0 )
						userId = (int)(Math.random()*(double)totUsers);
					else {
						userId = (int)(Math.random()*(double)workingSetSize);
						userId = userId * (int)(totUsers/workingSetSize);
					}

					String streamTxParam = String.format("\"id\" : %d", userId);

					if(loadBalanced) {
						long startTime = System.nanoTime();
						ClientResponse response = slaveWR[userId % slaveServers.size()].accept( MediaType.APPLICATION_JSON )
								.type( MediaType.APPLICATION_JSON )
								.entity( "{ \"query\" : \"" + streamTxQuery + "\", \"params\" : { " + streamTxParam + " } }" )
								.post( ClientResponse.class );
						long endTime = System.nanoTime();

						response.close();

						// output measurement to file
						latFileWriter.write(String.format("%12d%12s%12s%12.2f%12d\n", userId, "ST", slaveServers.get(userId % slaveServers.size()).substring(11), (endTime - startTime)/1e6, response.getStatus()));

						// update stats
						stSumStats.addValue((endTime - startTime)/1e6);
						streamTxCount++;
					} else {
						long startTime = System.nanoTime();
						ClientResponse response = slaveWR[0].accept( MediaType.APPLICATION_JSON )
								.type( MediaType.APPLICATION_JSON )
								.entity( "{ \"query\" : \"" + streamTxQuery + "\", \"params\" : { " + streamTxParam + " } }" )
								.post( ClientResponse.class );
						long endTime = System.nanoTime();

						response.close();

						// output measurement to file
						latFileWriter.write(String.format("%12d%12s%12s%12.2f%12d\n", userId, "ST", slaveServers.get(0).substring(11), (endTime - startTime)/1e6, response.getStatus()));

						// update stats
						stSumStats.addValue((endTime - startTime)/1e6);
						streamTxCount++;
					}
				} else {
					// publish a tweet
					// select user uniformly at random from the set of all users
					int userId;
					if( workingSetSize == 0 )
						userId = (int)(Math.random()*(double)totUsers);
					else {
						userId = (int)(Math.random()*(double)workingSetSize);
						userId = userId * (int)(totUsers/workingSetSize);
					}

					String tweetTxParam = String.format("\"id\" : %d, \"text\" : \"%s\", \"time\" : %d", userId, tweetString.substring(0, (int)(Math.random()*140)), (new Date()).getTime()/1000l);

					long startTime = System.nanoTime();
					ClientResponse response = masterWR.accept( MediaType.APPLICATION_JSON )
							.type( MediaType.APPLICATION_JSON )
							.entity( "{ \"query\" : \"" + tweetTxQuery + "\", \"params\" : { " + tweetTxParam + " } }" )
							.post( ClientResponse.class );
					long endTime = System.nanoTime();
					
					response.close();

					// output measurement to file
					latFileWriter.write(String.format("%12d%12s%12s%12.2f%12d\n", userId, "TW", masterServer.substring(11), (endTime - startTime)/1e6, response.getStatus()));
					
					// update stats
					twSumStats.addValue((endTime - startTime)/1e6);
					tweetTxCount++;
				}
			}
			long threadEndTime = System.currentTimeMillis();

			
			// flush and close latency file 
			latFileWriter.flush();
			latFileWriter.close();

			String datFileName;
			if(datFilePrefix != null)
				datFileName = datFilePrefix + "_s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".dat";
			else
				datFileName = "s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".dat";

			BufferedWriter datFileWriter = new BufferedWriter(new FileWriter(outputDir + "/" + datFileName));

			// write out the experiment results
			datFileWriter.write(String.format("#%15s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s\n", "threadStartTime", "threadEndTime", "threadTotTime", "streamTxCount", "tweetTxCount", "totTxCount", "streamTxAvgTPS", "tweetTxAvgTPS", "streamTxTotTime", "tweetTxTotTime", "totTxTime", "streamTxAvgTime", "tweetTxAvgTime", "streamTxMinTime", "tweetTxMinTime", "streamTxMaxTime", "tweetTxMaxTime", "streamTxStdTime", "tweetTxStdTime"));
			datFileWriter.write(String.format("%16d%16d%16d%16d%16d%16d%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f\n", threadStartTime, threadEndTime, (threadEndTime - threadStartTime)/1000l, streamTxCount, tweetTxCount, streamTxCount + tweetTxCount, (double)streamTxCount/((threadEndTime - threadStartTime)/1000l), (double)tweetTxCount/((threadEndTime - threadStartTime)/1000l), stSumStats.getSum(), twSumStats.getSum(), stSumStats.getSum() + twSumStats.getSum(), stSumStats.getMean(), twSumStats.getMean(), stSumStats.getMin(), twSumStats.getMin(), stSumStats.getMax(), twSumStats.getMax(), stSumStats.getStandardDeviation(), twSumStats.getStandardDeviation()));

			// flush and close data file
			datFileWriter.flush();
			datFileWriter.close();

		} catch(Exception e) {
			System.out.println("Error: " + e);
			return;
		}
	}
}
