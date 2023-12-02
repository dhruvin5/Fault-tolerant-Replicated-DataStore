package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.transport.Message.Request;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.*;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single fault-tolerant Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {
	protected final String myID;
	final private Session session;
    final private Cluster cluster;
	final private ZooKeeper zooKeeper;
	public static final String common_Znode_Path = "/LEADER";
	final private String myZnode;
	final private String serverCheckpoint;
	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;
	protected final MessageNIOTransport<String,String> serverMessenger;
	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;
	private static final String DELIMITER = "/n";
	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);
		
		this.myID = myID;
		this.myZnode = "/"+myID;
		this.serverCheckpoint = "/checkpoint"+myID;
		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect(myID);

		
		zooKeeper = new ZooKeeper("localhost:2181", 3000000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
				System.out.println("in process to ZooKeeper ensemble.");
				if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
					System.out.println("Reconnected to ZooKeeper ensemble.");
					
					restore();
				}
                //handleZNodeEvent(event);
            }
        });



		if(!checkNodeexist(myZnode))
		{
			try {
				zooKeeper.create(myZnode, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(!checkNodeexist(serverCheckpoint))
		{
			try {
				zooKeeper.create(serverCheckpoint, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);


		
	}

	public synchronized boolean checkNodeexist(String path)
	{
		
		try {
			if (zooKeeper.exists(path, false) == null) {
				return false;
			}
			return true;
		} catch (KeeperException | InterruptedException e) {
			
			e.printStackTrace();
		}
		return true;
	}

	public  ArrayList<String> getRequests()
	{
		byte[] data = null;
		
        
		try {
			data = zooKeeper.getData(common_Znode_Path, false,null);
		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String existingData = "";
		
		 
		 if (data != null) {

            try {
				existingData = new String(data, SingleServer.DEFAULT_ENCODING);
				
				ArrayList<String> requests = splitWithDelimiter(existingData);
				return requests;
			} catch (UnsupportedEncodingException e) {
				
				e.printStackTrace();
			}
			
				
        }
		return null; 


	}
	/**
	 * TODO: process bytes received from clients here.
	 */
	protected synchronized void handleMessageFromClient(byte[] bytes, NIOHeader header) {

        if (checkNodeexist(common_Znode_Path)) {
			
				
			ArrayList<String> requests  = getRequests();
			if(requests.size()==400)
			{
				
			}
			String request = new String(bytes);
			requests.add(request);
			String concatenatedRequests = concatenateWithDelimiter(requests);
			
			try {
				zooKeeper.setData(common_Znode_Path, concatenatedRequests.getBytes(), -1);
				} catch (KeeperException | InterruptedException e) {
					
					e.printStackTrace();
				}
        } 
		
		else {
			
            ArrayList<String> requests = new ArrayList<>();
			String request = new String(bytes);
			requests.add(request);
			String concatenatedRequests = concatenateWithDelimiter(requests);
			try {
				zooKeeper.create(common_Znode_Path, concatenatedRequests.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
        }
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            try {
                this.serverMessenger.send(node, bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
		}
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected synchronized void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		int value = 0;
		try {
			value = getCounterValue(zooKeeper, myZnode);
			ArrayList<String>request = getRequests();
			execute(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return;
	}

	private void handleZNodeEvent(String path) {
       
		System.out.println(this.myID + path);
       
		if(path == common_Znode_Path)
		{
           	 System.out.println("Znode updated: " + path);
			 ArrayList<String> requeStrings = getRequests();
			 execute(requeStrings);
         
		}
       
    }

	public synchronized void execute(List<String>requeStrings)
	{
		try {
			int executed = getCounterValue(zooKeeper, myZnode);
			for(int i=executed;i<requeStrings.size();i++)
			{
				//System.out.print(requeStrings.get(i));
				session.execute(requeStrings.get(i));
			}
			incrementCounter(zooKeeper, this.myZnode,requeStrings.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean checkpoint(){
		 
		System.out.println("checkpt");
		String query = "SELECT * FROM "+myID+".grade;";
		ResultSet result = session.execute(query);
		String all_results = result.all().toString();		
		try {
			zooKeeper.setData(serverCheckpoint,all_results.getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}

	public boolean restore(){
		if(!checkNodeexist(serverCheckpoint))
		{
			return true;
		}
		String all_results;
		try {
			byte[] data = zooKeeper.getData(serverCheckpoint, false, null);
			all_results = data.toString();
			Pattern pattern = Pattern.compile("Row\\[(-?\\d+), \\[([^\\]]+)\\]\\]");
			Matcher matcher = pattern.matcher(all_results);
			Map<Integer, Vector<Integer>> resultMap = new HashMap<>();

			while (matcher.find()) {
				int key = Integer.parseInt(matcher.group(1));
				String valuesString = matcher.group(2);

				String[] valuesArray = valuesString.split(", ");
				Vector<Integer> valuesVector = new Vector<>();
				for (String value : valuesArray) {
					valuesVector.add(Integer.parseInt(value));
				}
				resultMap.put(key, valuesVector);
			}
			int cnt = 0;
			for (Map.Entry<Integer, Vector<Integer>> entry : resultMap.entrySet()) {
				int key = entry.getKey();
				Vector<Integer> values = entry.getValue();

				String cql = String.format("INSERT INTO %s (id, events) VALUES (?, ?);", myID+"grade");

				PreparedStatement preparedStatement = session.prepare(cql);
				session.execute(preparedStatement.bind(key, values));
				cnt +=1;
			}
			//zooKeeper.setData(personalReqCounterPath,(Integer.toString(cnt)).getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
		
		return true;
	}

	private static int getCounterValue(ZooKeeper zooKeeper , String myZnode) throws Exception {
        byte[] counterData = zooKeeper.getData(myZnode, false, null);
        String counterValue = new String(counterData);
        return Integer.parseInt(counterValue);
    }

    private static void incrementCounter(ZooKeeper zooKeeper, String myZnode,int value) throws Exception {
        
        int updatedCounter = value;

        // Update the counter znode with the new value
        zooKeeper.setData(myZnode, String.valueOf(updatedCounter).getBytes(), -1);
    }

	    private String concatenateWithDelimiter(ArrayList<String> strings) {
        StringJoiner joiner = new StringJoiner(DELIMITER);
        for (String str : strings) {
            joiner.add(str);
        }
        return joiner.toString();
    }

	private synchronized ArrayList<String> splitWithDelimiter(String concatenatedString) {
		if(concatenatedString.length()==0)
		{
			ArrayList<String> req = new ArrayList<>();
			return req;
		}
		// Convert the array obtained from split to a List and then create a new ArrayList
		return new ArrayList<>(Arrays.asList(concatenatedString.split(DELIMITER)));
	}

	public void close() {
		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}