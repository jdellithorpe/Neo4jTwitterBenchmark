package org.ellitron.neo4jbenchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class TwitterWorkloadGenerator {
	
	public static void main(String[] args) {
		int serverNumber 		= Integer.parseInt(args[0]); // our server number
		int baseServerNumber 	= Integer.parseInt(args[1]); // base server number
		int numServers 			= Integer.parseInt(args[2]); // total number of servers used in the workload generation
		int numThreads 			= Integer.parseInt(args[3]); // total number of threads to use (spread evenly across the servers)
		double runTime 			= Double.parseDouble(args[4]); // time to run in minutes
		double streamProb		= Double.parseDouble(args[5]); // probability of stream transaction
		boolean loadBalanced	= Boolean.parseBoolean(args[6]); // whether or not we do load balancing for the reads
		long totUsers			= Long.parseLong(args[7]); // total number of users in the graph
		int streamTxPgSize		= Integer.parseInt(args[8]); // the number of tweets to read in a stream
		int workingSetSize		= Integer.parseInt(args[9]); // number of nodes to operate over
		String outputDir 		= args[10];
		String datFilePrefix = null;
		if(args.length > 11)
			datFilePrefix = args[11]; // prefix for output data files
		
		// calculate number of threads to spin up on this server
		int numLocalThreads = numThreads / numServers;
		numLocalThreads += ((numThreads%numServers) > (serverNumber-baseServerNumber)) ? 1 : 0;
		
		String masterServer = "0.0.0.0";
		List<String> slaveServers = new ArrayList<String>();
		
		for(int i=1; i<=80; i++) {
			String serverCandidate = "192.168.1." + String.format("%d", 100+i);
			
			WebResource resource = Client.create()
					.resource( "http://" + serverCandidate + ":7474/db/manage/server/ha/available" );
			
			try {
				ClientResponse response = resource
						.get( ClientResponse.class );
				
				if( response.getEntity( String.class ).equalsIgnoreCase("master") )
					masterServer = serverCandidate;
				else 
					slaveServers.add(serverCandidate);
				
				response.close();
			} catch(Exception e) {
				// do nothing
			}
		}
		
		if( masterServer.equals("0.0.0.0") ) {
			System.out.println("Count not find master server");
			return;
		}
		
		System.out.println("Detected master server: " + masterServer);
		System.out.println("Detected slave servers: " + slaveServers);
		System.out.println("Running workload...");
		
		for(int i = 0; i<numLocalThreads; i++) {
			// start up threads
			(new TwitterWorkloadThread(serverNumber, i, runTime, streamProb, loadBalanced, totUsers, streamTxPgSize, datFilePrefix, masterServer, slaveServers, workingSetSize, outputDir)).start();
//			TwitterWorkloadGenerator.workloadThread(serverNumber, i, runTime, datFilePrefix, masterServer, slaveServers);
		}

	}

}
