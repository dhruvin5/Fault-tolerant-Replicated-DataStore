package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.nio.file.Files;
import java.nio.file.Path;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;


import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1500;
	protected final String myID;
	final private Session session;
    final private Cluster cluster;
	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		this.myID = args [0];
		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect(myID);
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		return execute(request);
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		String query = request.toString();
		
		JSONObject json1;
		String query_toexec="";
		try {
			json1 = new JSONObject(query);
			query_toexec = json1.getString("QV");
			//System.out.println(query_toexec);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//ResultSet resultSet = session.execute(query);
		try{
			session.execute(query_toexec);
			//String p = this.checkpoint(myID);
		}
			catch(Exception e){
				return false;

			}
			return true;
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		//System.out.println("CHECKPOINT");

		ResultSet result =  session.execute("SELECT * FROM grade;");
		String all_results = result.all().toString();
		//System.out.println(myID + "---:+" + all_results);
		
		return  all_results;
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {

		String resultFromCheckPoint; 
		Pattern pat;
		Matcher match;
		Map<Integer, ArrayList<Integer>>  resultsRestore;

		if(s1.length()==2)
		{
		
			return true;
		}
		
		resultFromCheckPoint= s1;
		pat = Pattern.compile("Row\\[(-?\\d+), \\[([^\\]]+)\\]\\]");
		match = pat.matcher(resultFromCheckPoint);
		resultsRestore = new HashMap<>(); //storing result
			
			

			//IF MATCH FOUND
			while (match.find()) {
				int key = Integer.parseInt(match.group(1));
				String value = match.group(2);

			
				String[] entrieStrings = value.split(", ");
				ArrayList<Integer> vArrayList = new ArrayList<>();
				for (String v : entrieStrings) {
					vArrayList.add(Integer.parseInt(v));
				}

				
				resultsRestore.put(key, vArrayList);
			}

			for (Map.Entry<Integer, ArrayList<Integer>> entry : resultsRestore.entrySet()) {
				String cqlQuery;
				int key = entry.getKey();
				
				ArrayList<Integer> val = entry.getValue();

				
				 cqlQuery = String.format("INSERT INTO %s (id, events) VALUES (?, ?);", "grade");

				
				PreparedStatement preparedStatement = session.prepare(cqlQuery);

				// Execute the statement with the values
				session.execute(preparedStatement.bind(key, val));
			}

        
		return true;

	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}

	//@Override
	//public boolean execute(Request arg0) {
		// TODO Auto-generated method stub
	//	throw new UnsupportedOperationException("Unimplemented method 'execute'");
	//}
}