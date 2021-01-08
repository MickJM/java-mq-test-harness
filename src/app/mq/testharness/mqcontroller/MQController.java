package app.mq.testharness.mqcontroller;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Hashtable;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

public class MQController {

	private String hostName;
	private String channelName;
	private int port;
	private String userId;
	private String password;
	private String queueName;
	private String queueManager;
	private String cipher;
	private boolean mqcsp;
	private boolean usetls;

	public String HostName() {
		return this.hostName;
	}
	public void HostName(String hostName) {
		this.hostName = hostName;
	}

	public String ChannelName() {
		return this.channelName;
	}
	public void ChannelName(String channelName) {
		this.channelName = channelName;
	}

	public int Port() {
		return port;
	}
	public void Port(int port) {
		this.port = port;
	}
	
	public String UserId() {
		return userId;
	}
	public void UserId(String userId) {
		this.userId = userId;
	}
	
	public String Password() {
		return password;
	}
	public void Password(String password) {
		this.password = password;
	}

	public  String QueueName() {
		return queueName;
	}
	public void QueueName(String queueName) {
		this.queueName = queueName;
	}

	//public String Msg() {
	//	return msg;
	//}
	//public void Msg(String msg) {
	//	this.msg = msg;
	//}

	public String QueueManagerName() {
		return queueManager;
	}
	public void QueueManagerName(String queueManager) {
		this.queueManager = queueManager;
	}

	public String Cipher() {
		return cipher;
	}
	public void Cipher(String cipher) {
		this.cipher = cipher;
	}

	public boolean MQCSP() {
		return mqcsp;
	}
	public void MQCSP(boolean v) {
		this.mqcsp = v;
	}
	
	public boolean UseTLS() {
		return usetls;
	}
	public void UseTLS(boolean tls) {
		this.usetls = tls;
	}
	
	private String keystore;
	public String KeyStore() {
		return keystore;
	}
	public void KeyStore(String v) {
		this.keystore = v;
	}
	private String storepass;
	public String StorePass() {
		return storepass;
	}
	public void StorePass(String v) {
		this.storepass = v;
	}
	private boolean ibmcipher;
	public boolean IBMCipher() {
		return ibmcipher;
	}
	public void IBMCipher(boolean v) {
		this.ibmcipher = v;
	}
	
	private boolean log;
	public boolean Log() {
		return log;
	}
	public void Log(boolean v) {
		this.log = v;
	}

	private boolean verbose;
	public boolean Verbose() {
		return verbose;
	}
	public void Verbose(boolean v) {
		this.verbose = v;
	}
	
	private String appname;
	public String AppName() {
		return appname;
	}
	public void AppName(String v) {
		this.appname = v;
	}
	
	private String version;
	public String Version() {
		return version;
	}
	public void Version(String v) {
		this.version = v;
	}

	private String ccdtfile;
	public String CCDTFile() {
		return ccdtfile;
	}
	public void CCDTFile(String v) {
		this.ccdtfile = v;
	}
	
	public MQController() {
	}
	
	
	/*
	 * Create a connection to the queue manager
	 */
	public MQQueueManager ConnectToQueueManager() throws MQException, MalformedURLException {

		Hashtable<String, Comparable> env = null;
		boolean localConn = true;
		
		env = new Hashtable<String, Comparable>();
		if (HostName() != null) {
			env.put(MQConstants.HOST_NAME_PROPERTY, HostName());
			localConn = false;

		}
		if (ChannelName() != null) {
			env.put(MQConstants.CHANNEL_PROPERTY, ChannelName());
		}
		if (Port() != 0) {
			env.put(MQConstants.PORT_PROPERTY, Port());
		}
		if (UserId() != null) {
			env.put(MQConstants.USER_ID_PROPERTY, UserId()); 
			env.put(MQConstants.PASSWORD_PROPERTY, Password());
			env.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, MQCSP());			
		} 
		
		env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES);
		env.put(MQConstants.APPNAME_PROPERTY,Version());

		if (UseTLS()) {
			System.setProperty("javax.net.ssl.trustStore", KeyStore());
	        System.setProperty("javax.net.ssl.trustStorePassword", StorePass());
	        System.setProperty("javax.net.ssl.trustStoreType","JKS");
	        if (IBMCipher()) {
	        	System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","true");
	        } else {
	        	System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","false");
	        	
	        }
	        System.setProperty("javax.net.ssl.keyStore", KeyStore());
	        System.setProperty("javax.net.ssl.keyStorePassword", StorePass());
	        System.setProperty("javax.net.ssl.keyStoreType","JKS");	
	        env.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, Cipher());
		}
		
		URL ccdtFileName = new URL("file:///" + "C:/ProgramData/IBM/MQ/qmgrs/QMAP01/@ipcc/AMQCLCHL.TAB");
		//URL ccdtFileName = null;
		boolean ccdt = false;
		MQQueueManager qmgr = null;
		
		if (localConn) {
			qmgr = new MQQueueManager(QueueManagerName());
			
		} else {
			if (ccdt) {
				qmgr = new MQQueueManager(QueueManagerName(), env, ccdtFileName);
				
			} else {
				env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES_CLIENT);
				qmgr = new MQQueueManager(QueueManagerName(),env);
			}
		}
        
        return qmgr;
	}

	/*
	 * Open a queue
	 */
	public MQQueue OpenQueue(MQQueueManager qmgr, int openOptions) throws MQException, IOException {
		
		//int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
		//				+ MQConstants.MQOO_INPUT_SHARED;
		
		return (qmgr.accessQueue(QueueName(), openOptions));
	}
	
	/*
	 * Read messages from a queue
	 */
	public MQMessage ReadFromQueue(MQQueue queue, MQGetMessageOptions gmo) throws MQException, IOException {
		
		MQMessage newmsg = new MQMessage();		
		queue.get(newmsg, gmo); 
		return newmsg;		
	}
	
	/*
	 * Write messages to a queue
	 */
	public void WriteMessage(MQQueue queue, MQMessage newmsg, MQPutMessageOptions pmo) throws MQException {

		queue.put(newmsg, pmo);
	}

	/*
	 * Disconnect from queue manager
	 */
	public void Disconnect(MQQueueManager qmgr) throws MQException {

		System.out.println("Disconnect");
		qmgr.disconnect();
	}
	
}
