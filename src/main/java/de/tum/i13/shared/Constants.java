package de.tum.i13.shared;

public class Constants {
	public static final String TELNET_ENCODING = "ISO-8859-1"; // encoding for telnet
	public static final String END_OF_PACKET = "\r\n";
	public static final String LRU = "LRU";
	public static final String LFU = "LFU";
	public static final String FIFO = "FIFO";
	public static final String SERVER_STOPPED = "server_stopped" + END_OF_PACKET;
	public static final String SERVER_NOT_RESPONSIBLE = "server_not_responsible" + END_OF_PACKET;
	public static final String SERVER_ADD_SUCCESS = "addserver_success" + END_OF_PACKET;
	public static final String KEYRANGE_SUCCESS = "keyrange_success";
	public static final String KEYRANGE_READ_SUCCESS = "keyrange_read_success";
	public static final String SERVER_REMOVE_SUCCESS = "removeserver_success" + END_OF_PACKET;
	public static final String SERVER_WRITE_LOCK = "server_write_lock" + END_OF_PACKET;
	public static final String HEX_START_INDEX = "00000000000000000000000000000000";
	public static final String HEX_END_INDEX = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
	public static final String DONE = "done";  // handoff successfully done!
	public static final String ERROR = "error"; // handoff failed!
	public static final String METADA_Received = "metadata_received"; // handoff failed!
	public static final String OK = "Ok"; //standard answer message when no real meaningful answer is needed
	public static final String SEND_ALL_KEYS = "send_all"; //indicates that server must send all its keys
	public static final String YOUR_RANGE= "your_range"; //indicates that server must send the keys of its range
	public static final String ONLY_MY_RANGE= "only_my_range"; //indicates that server must keep only the keys in its range
	public static final String SET_REPLICA = "set_replica"; //command sent from server to another with key/values, the server that receives it copies all the keys
	public static final String REMOVE_REPLICA = "remove_replica"; //removes the replication from another server

	public static final String PUT_KEY_REPLICA= "put_key_replica"; //put command for replica
	public static final String REMOVE_KEY_REPLICA= "remv_key_replica"; //removes a key on a replcia

	public static final String ADD_EVENT_PUBLISHER= "eventpublisher";
	public static final String CHANGED = "changed";
	public static final String NOTOPICINDICATOR= "-nt";


	//COMMANDS
	public static final String SUBSCRIBE = "subscribe";
	public static final String UNSUBSCRIBE = "unsubscribe";
	public static final String PUT_SUCCESS = "putsuccess";
	public static final String REMOVE_SUCCESS = "removesuccess";
	public static final String GET = "get";
	public static final String PUT = "unsubscribe";


}
