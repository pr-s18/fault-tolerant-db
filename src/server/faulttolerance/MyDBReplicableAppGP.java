package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
	public static final int SLEEP = 1000;
	protected Session session;
	protected int requests;
	private String keyspace;


	private void setupCassandra(String keyspace) {
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(keyspace);
	}

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
		// TODO: setup connection to the data store and keyspace
//		throw new RuntimeException("Not yet implemented");
		keyspace = args[0];
		setupCassandra(args[0]);
		requests = 0;
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
//		System.out.println("in bool exec");
		return execute(request);
//		throw new RntimeException("Not yet implemented");
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
//		System.out.println("in exec");
		String requestValue = ((RequestPacket) request).getRequestValue();
		String query = "";
		// deserialize request
		try {
			JSONObject jsonRequest = new JSONObject(requestValue);
			query = jsonRequest.getString("QV");
			requests++;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		// execute query
		ResultSet resultSet = session.execute(query);
		return resultSet.one() != null;
//		throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		int random = ThreadLocalRandom.current().nextInt();
		// using the copy command of cqlsh to take a snapshot of the cassandra table to be able to restore at a later point
		String filename = "abcd" + random + ".csv";
		System.out.println("helloe world checkpoint + "  + filename + " on " + keyspace);
		String query = "COPY " + keyspace + ".grade TO '" + filename + "' WITH HEADER = TRUE ;";
		Runtime runtime = Runtime.getRuntime();
		String[] commands = {"/System/Volumes/Data/opt/homebrew/bin/cqlsh", "-e", query};
		try {
			Process process = runtime.exec(commands);
		} catch (IOException e) {
			e.printStackTrace();
		}
//		throw new RuntimeException("Not yet implemented");
		return filename;
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
		// TODO:
		System.out.println("helloe world restore: " + s + ", " + s1);
		// using the copy from command of cqlsh to restore the table from a checkpointed state
		if(!s1.equals("{}")){
			String query = "COPY " + keyspace + ".grade FROM '" + s1 + "' WITH HEADER = TRUE ;";
			System.out.println(query);
			Runtime runtime = Runtime.getRuntime();
			String[] commands = {"/System/Volumes/Data/opt/homebrew/bin/cqlsh", "-e", query};
			try {
				Process process = runtime.exec(commands);
//				return process.exitValue() == 0;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
//		throw new RuntimeException("Not yet implemented");
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
}

