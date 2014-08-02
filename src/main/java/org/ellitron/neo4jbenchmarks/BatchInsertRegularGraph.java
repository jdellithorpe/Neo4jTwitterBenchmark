package org.ellitron.neo4jbenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class BatchInsertRegularGraph {

	public static void main(String[] args) {
		long TWEETS_PER_USER = Long.parseLong(args[0]);	// number of tweets to add to each user, added in round robin order
		String edgelistFilename = args[1];
		String databaseDirectory = args[2];
		
		// parameters
    	long STARTING_TWEET_TIME	= 1230800000;	// roughly seconds passed since 1970 on 2009-01-01
    	int TWEETS_PER_SECOND		= 1000;			// a single tweet takes up 1/TWEETS_PER_SECOND seconds of time
    	int NUM_DATASET_CLONES		= 1;			// how many clones of the dataset we want
		
		Pattern pattern = Pattern.compile("([0-9]+)");
		Matcher matcher = pattern.matcher(edgelistFilename);
		
		if(!matcher.find()) {
			System.out.println("Failed to parse filename >_<");
			return;
		}
		
		long NUM_USERS = Long.parseLong(matcher.group());
		
		if(!matcher.find()) {
			System.out.println("Failed to parse filename >_<");
			return;
		}
		
		long FRIENDS_AND_FOLLOWERS_PER_USER = Long.parseLong(matcher.group());
		
		// inserter configuration settings
    	Map<String, String> config = new HashMap<String, String>();
    	config.put("neostore.propertystore.db.index.keys.mapped_memory", "500M");
    	config.put("neostore.propertystore.db.index.mapped_memory", "500M");
    	config.put("neostore.nodestore.db.mapped_memory", "1000M");
    	config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
    	config.put("neostore.propertystore.db.mapped_memory", "1000M");
    	config.put("neostore.propertystore.db.strings.mapped_memory", "1000M");

    	BatchInserter inserter = BatchInserters.inserter(databaseDirectory, config);

    	Label userLabel = DynamicLabel.label("User");
    	Label tweetLabel = DynamicLabel.label("Tweet");
    	
    	RelationshipType followsRelType = DynamicRelationshipType.withName( "FOLLOWS" );
    	RelationshipType tweetRelType = DynamicRelationshipType.withName("TWEET");
    	RelationshipType streamRelType = DynamicRelationshipType.withName("STREAM");
    	
    	Map<String, Object> userNodeProperties = new HashMap<String, Object>();
    	Map<String, Object> tweetNodeProperties = new HashMap<String, Object>();
    	Map<String, Object> tweetRelProperties = new HashMap<String, Object>();
    	Map<String, Object> streamRelProperties = new HashMap<String, Object>();
    	Map<String, Object> followsRelProperties = new HashMap<String, Object>();
    	
    	String tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";

    	System.out.println("Creating database files for a regular graph of size " + NUM_USERS + " users and " + FRIENDS_AND_FOLLOWERS_PER_USER + " friends and followers per user with " + TWEETS_PER_USER + " tweets per user...");
    	
    	long tweetCount = 0;
    	
    	System.out.println("Creating users...");
    	long startTime = System.currentTimeMillis();
    	for(long userId = 0; userId < NUM_USERS; userId++) {
    		userNodeProperties.put("username", String.format("%012d", userId));
    		
    		inserter.createNode(userId, userNodeProperties, userLabel);
    		
    		for(int i = 0; i<TWEETS_PER_USER; i++) {
				long tweetIdOffset = (NUM_USERS * i) + userId;
				long tweetId = NUM_USERS + tweetIdOffset;
				long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);

				String tweetText = tweetString.substring(0, (int)(Math.random()*140));

				tweetNodeProperties.put("text", tweetText);
				tweetNodeProperties.put("time", tweetTimestamp);

				inserter.createNode(tweetId, tweetNodeProperties, tweetLabel);

				tweetRelProperties.put("time", tweetTimestamp);

				inserter.createRelationship(userId, tweetId, tweetRelType, tweetRelProperties);

				tweetCount++;
			}
    		
    		if(userId % 1000000 == 0)
				System.out.printf("%3d%% @%.2fminutes\n", 100*userId/NUM_USERS, (System.currentTimeMillis() - startTime)/60e3);
    	}
    	long endTime = System.currentTimeMillis(); 	
    	System.out.println();
    	
    	System.out.printf("Imported %d users, %d tweets, and %d tweet relationships in %.2f minutes\n\n", NUM_USERS, tweetCount, tweetCount, (endTime - startTime)/60e3);
    	
    	long numEdges = 0;
    	long numStreamRels = 0;
    	
    	System.out.println("Creating edges...");
    	startTime = System.currentTimeMillis();
    	try {
    		BufferedReader br = new BufferedReader(new FileReader(edgelistFilename));
    		String line;
    		while ((line = br.readLine()) != null) {
    			String[] lineParts = line.split(" ");
    			long userId1 = Long.parseLong(lineParts[0]);
    			long userId2 = Long.parseLong(lineParts[1]);
    			
    			inserter.createRelationship(userId1, userId2, followsRelType, followsRelProperties);
    			
    			numEdges++;

    			for(int i = 0; i<TWEETS_PER_USER; i++) {
					long tweetIdOffset = (NUM_USERS * i) + userId2;
					long tweetId = NUM_USERS + tweetIdOffset;
					long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);
					
					streamRelProperties.put("time", tweetTimestamp);
					
					inserter.createRelationship(userId1, tweetId, streamRelType, streamRelProperties);
					
					numStreamRels++;
				}
    			
    			inserter.createRelationship(userId2, userId1, followsRelType, followsRelProperties);
    			
    			numEdges++;
    			
    			for(int i = 0; i<TWEETS_PER_USER; i++) {
					long tweetIdOffset = (NUM_USERS * i) + userId1;
					long tweetId = NUM_USERS + tweetIdOffset;
					long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);
					
					streamRelProperties.put("time", tweetTimestamp);
					
					inserter.createRelationship(userId2, tweetId, streamRelType, streamRelProperties);
					
					numStreamRels++;
				}

    			if(numEdges % 1000000 == 0) {
    				System.out.printf("%3d%% @%.2fminutes\n", 100*numEdges/(FRIENDS_AND_FOLLOWERS_PER_USER*NUM_USERS), (System.currentTimeMillis() - startTime)/60e3);
    			}
    		}
    		br.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	endTime = System.currentTimeMillis(); 	
    	System.out.println();

    	System.out.printf("Imported %d follower relationships and %d stream relationships in %.2f minutes\n\n", numEdges, numStreamRels, (endTime - startTime)/60e3);


    	System.out.println("Shutting down...");
    	long shutdown_start = System.currentTimeMillis();
    	inserter.shutdown();
    	long shutdown_total = System.currentTimeMillis() - shutdown_start;
    	System.out.println("Shutting down took " + shutdown_total/1e3 + " seconds");
    	
	}

}
