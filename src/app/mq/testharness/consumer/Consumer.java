package app.mq.testharness.consumer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import app.mq.testharness.Constants;
import app.mq.testharness.mqcontroller.MQController;

public class Consumer {

	private MQController controller = null;
	private MQQueueManager qmgr = null;
	private MQQueue queue = null;

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

	private int per;
	private int function;
	private boolean stillProcessing = true;

	
	public MQController Controller() {
		return this.controller;
	}
	public void Controller(MQController c) {
		this.controller = c;
	}

	public MQQueueManager QueueManager() {
		return this.qmgr;
	}
	public void MQQueueManager(MQQueueManager qm) {
		this.qmgr = qm;
	}
	public MQQueue Queue() {
		return this.queue;
	}
	public void Queue(MQQueue q) {
		this.queue = q;
	}
	
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

	
	// ******************
	
	public int Per() {
		return per;
	}
	public void Persistent(int v) {
		this.per = v;
	}

	
	public void Function(int v) {
		this.function = v;
	}
	public int Function() {
		return this.function;
	}

	private int summerize;
	public int Summerize() {
		return summerize;
	}
	public void Summerize(int v) {
		this.summerize = v;
	}

	private int groupSeq;
	public void IncrementGroupSeq() {
		this.groupSeq++;
	}
	public void ResetGroupSeq() {
		this.groupSeq = 0;
	}

	private boolean simulation;
	public boolean Simulation() {
		return simulation;
	}
	public void Simulation(boolean v) {
		this.simulation = v;
	}

	private boolean splitmessages;
	public boolean SplitMessages() {
		return splitmessages;
	}
	public void SplitMessages(boolean v) {
		this.splitmessages = v;
	}
	private int splitmessagesize;
	public int SplitMessageSize() {
		return splitmessagesize;
	}
	public void SplitMessageSize(int v) {
		this.splitmessagesize = v;
	}

	private int messagesize;
	public int MessageSize() {
		return messagesize;
	}
	public void MesssageSize(int v) {
		this.messagesize = v;
	}

	private boolean groupmessages;
	public boolean GroupMessages() {
		return groupmessages;
	}
	public void GroupMessages(boolean v) {
		this.groupmessages = v;
	}
	private int groupmessagesize;
	public int GroupMessageSize() {
		return groupmessagesize;
	}
	public void GroupMessageSize(int v) {
		this.groupmessagesize = v;
	}	
	
	private int decimalorbinary;
	public int DecimalOrBinary() {
		return decimalorbinary;
	}
	public void DecimalOrBinary(int v) {
		this.decimalorbinary = v;
	}	
	
	private byte[] groupId;
	public void GenerateGroupId() throws NoSuchAlgorithmException {
		this.groupId = new byte[24];
		SecureRandom.getInstanceStrong().nextBytes(this.groupId);
	}
	
	private long numberofmessages;
	public long NumberOfMessages() {
		return numberofmessages;
	}
	public long NumberOfMessages(long v) {
		return this.numberofmessages = v;
	}

	private long sync;
	public long Sync() {
		return this.sync;
	}
	public void Sync(long v) {
		this.sync = v;
	}

	private long messagecount;
	public long Statistics() {
		return this.messagecount;
	}
	public void Statistics(long v) {
		this.messagecount = v;
	}
	
	private int waitinterval;
	public int WaitInterval() {
		return waitinterval;
	}
	public void WaitInterval(int v) {
		this.waitinterval = v;
	}
	
	private int codepage;
	public int CodePage() {
		return codepage;
	}
	public void CodePage(int v) {
		this.codepage = v;
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

	private String threadName;
	public String ThreadName() {
		return threadName;
	}
	public void ThreadName(String v) {
		this.threadName = v;
	}

	public void StillProcessing(boolean v) {
		System.out.println("Setting Still Processing ...");
		this.stillProcessing = false;
	}
	public boolean StillProcessing() {
		return this.stillProcessing;
	}

	private String version;
	public String Version() {
		return version;
	}
	public void Version(String v) {
		this.version = v;
	}
	
	private MQGetMessageOptions gmo;
	public MQGetMessageOptions Gmo() {
		return this.gmo;
	}
	public void Gmo(MQGetMessageOptions options) {
		this.gmo = options;
	}
	
	/*
	 * Start processing ...
	 */
	public void Start() throws NoSuchAlgorithmException, MQException, IOException, InterruptedException {
	
		Process();
	}

	/*
	 * Create a Controller object and connect try to connect to a queue manager
	 */
	private void Process() throws MQException, IOException, InterruptedException, NoSuchAlgorithmException {
		
		try {
			Controller(new MQController());
			Controller().QueueManagerName(QueueManagerName());
			Controller().QueueName(QueueName());
			Controller().HostName(HostName());
			Controller().ChannelName(ChannelName());
			Controller().Port(Port());
			Controller().UserId(UserId());
			Controller().Password(Password());
			Controller().UseTLS(UseTLS());
			Controller().KeyStore(KeyStore());
			Controller().StorePass(StorePass());
			Controller().Cipher(Cipher());
			Controller().IBMCipher(IBMCipher());
			
			Controller().Log(Log());
			Controller().Verbose(Verbose());

			if (HostName() != null) {
				System.out.println(String.format("CONSUMER: %s , Attempting to create a client connection to %s ", ThreadName(), QueueManagerName()));
			} else {
				System.out.println(String.format("CONSUMER: %s , Attempting to create a local binding connection to %s ", ThreadName(), QueueManagerName()));
			}
			Controller().Version(Version());

			MQQueueManager(Controller().ConnectToQueueManager());
			
			System.out.println(String.format("CONSUMER: %s , Connection established",ThreadName()));
			
		} catch (MQException e) {
			System.out.println("ERROR: Unable to connect to queue manager ... " + e.reasonCode + ": " + e.getLocalizedMessage());
			System.out.println("ERROR: See MQ logs for more details");
			StopProcessor();
			
		} catch (Exception e) {
			System.out.println("ERROR: Exception - Unable to connect to queue manager ... "  + e.getLocalizedMessage());
			System.out.println("ERROR: Exception - See MQ logs for more details");
			StopProcessor();
		}
		
		if (QueueManager() != null) {
			consumer();
		}
	}

	/*
	 * Read messsages
	 */
	private void consumer() throws MQException, IOException, InterruptedException {
		
		System.out.println(String.format("CONSUMER: %s , Sleeping for 2 seconds ... ", ThreadName()));		
		Thread.sleep(2000);
		
		Queue(null);
		try {
			openQueueForReading();
			
		} catch (MQException e1) {
			System.out.println(String.format("CONSUMER: %s , Unable to read messages from queue ... %s : %s", ThreadName(), e1.reasonCode, e1.getLocalizedMessage()));
			StopProcessor();
			
		} catch (IOException e1) {
			System.out.println(String.format("CONSUMER: %s , IOException when reading message ... : %s", ThreadName(), e1.getLocalizedMessage()));
			StopProcessor();
		}

		/*
		 * If messages are in 'groups'
		 * ... on the last 'group' message, commit the messages from the queue that we have successfully read them
		 * 
		 * otherwise, if we have processed 'x' number (sync), then commit them
		 * 
		 */
		long messages = 0;
		
		try {
			Gmo(GetGMO());
			SetBrowseGMO(MQConstants.MQGMO_BROWSE_FIRST);
			
			// MQConstants.MQGMO_BROWSE_FIRST
			Date startTime = new Date();
			Date intStartTime = startTime;
			
			final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
			String date = dateFormat.format(startTime);	
			System.out.println(String.format("CONSUMER: %s , Starting to read messsages : %s", ThreadName(), date));
			try {

				while(StillProcessing()) {
					
					MQMessage msg = ReadMessagesFromQueue();
					String stringMsg = ConvertMessageToString(msg);
				//	System.out.println("Message size is " + stringMsg.length());
					
					messages++;					
					int split = (msg.messageFlags & MQConstants.MQMF_LAST_SEGMENT);
					if (split == MQConstants.MQMF_LAST_SEGMENT) {
						if (Log()) {
							if (Verbose()) {
								System.out.println("CONSUMER: " + ThreadName() + " , Message size is " + stringMsg.length() + " characters, has been split");
							}
						}
					} else {
						if (Log()) {
							if (Verbose()) {
								System.out.println("CONSUMER: " + ThreadName() + " , Message size is " + stringMsg.length() + " characters");							
							}
						}
					//	System.out.println("messages : " + messages);

						if (messages > 0) {
							if (messages % Statistics() == 0) {
								Date intermediate = new Date();
								date = dateFormat.format(intermediate);	
								long diff = intermediate.getTime() - intStartTime.getTime();
								intStartTime = intermediate;
								if (Log()) {
									System.out.println(String.format("CONSUMER: %s , Messages processed : %d in %d ms", ThreadName(), messages, diff));						
								}
							}
						}
					}
					
					int last = (msg.messageFlags & MQConstants.MQMF_LAST_MSG_IN_GROUP);
					if (last == MQConstants.MQMF_LAST_MSG_IN_GROUP) {
						QueueManager().commit();
						
					} else {
						if (messages % Sync() == 0) {
							QueueManager().commit();
						}
					}
					SetBrowseGMO(MQConstants.MQGMO_BROWSE_NEXT);

				}
				
			} catch (MQException e) {
				if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
						
					Date intermediate = new Date();
					date = dateFormat.format(intermediate);	
					long diff = intermediate.getTime() - startTime.getTime();
					if (Log()) {
						System.out.println(String.format("CONSUMER: %s , Messages processed : %d in %d", ThreadName(), messages, diff));						
					}
					if (Log()) {
					//	System.out.println("CONSUMER: " + ThreadName() + " , No more messages, all messages processed - Still processing : " + StillProcessing());
						System.out.println("CONSUMER: " + ThreadName() + " , No more messages, all messages processed");

					}
				} else {
					System.out.println(String.format("CONSUMER: %s , Error processing messages : %s Reason %d ", ThreadName(), e.getLocalizedMessage(), e.getReason() ));						
					
				}
			}

			// commit anything that is outstanding
			QueueManager().commit();

			
			
		} catch (MQException e) {
			System.out.println("CONSUMER: " + ThreadName() + " Unable to read messages from queue ... " + e.reasonCode + ": " + e.getLocalizedMessage());
			
		} 
		StopProcessor();
				
	}

	/*
	 * Open the queue for reading ...
	 */
	private void openQueueForReading() throws MQException, IOException {
		
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
						+ MQConstants.MQOO_INPUT_SHARED;
		
		openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF |
					  	MQConstants.MQOO_BROWSE |
					  	MQConstants.MQOO_FAIL_IF_QUIESCING;					
		//if (Simulation()) {
		//	openOptions += MQConstants.MQOO_BROWSE;
		//}
		
		if (Log()) {
			System.out.println("CONSUMER: " + ThreadName() + " , Opening Queue for input: " + this.queueName);
		}
		Queue(Controller().OpenQueue(QueueManager(), openOptions));
		
	}

	/*
	 * GetMessageOptions
	 */
	private MQGetMessageOptions GetGMO() {

		MQGetMessageOptions gmo = new MQGetMessageOptions();
		gmo.options = MQConstants.MQGMO_WAIT 
					| MQConstants.MQGMO_FAIL_IF_QUIESCING 
					| MQConstants.MQGMO_SYNCPOINT
					| MQConstants.MQGMO_COMPLETE_MSG
					| MQConstants.MQGMO_LOGICAL_ORDER;

		gmo.waitInterval = WaitInterval();

		return gmo;

	}
	
	private void SetBrowseGMO(int firstnext) {

		int options = MQConstants.MQGMO_NO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_ALL_MSGS_AVAILABLE
					| MQConstants.MQGMO_COMPLETE_MSG
					| MQConstants.MQGMO_LOGICAL_ORDER;

		if (Simulation()) {
			if (firstnext == MQConstants.MQGMO_BROWSE_FIRST) {
				options += MQConstants.MQGMO_BROWSE_FIRST;
				
			} else {
				if (firstnext == MQConstants.MQGMO_BROWSE_NEXT) {
					options += MQConstants.MQGMO_BROWSE_NEXT;					
				}
			}
		} else {
			options = MQConstants.MQGMO_NO_WAIT
					| MQConstants.MQGMO_FAIL_IF_QUIESCING
					| MQConstants.MQGMO_SYNCPOINT
					| MQConstants.MQGMO_COMPLETE_MSG
					| MQConstants.MQGMO_LOGICAL_ORDER;
		}
		
		Gmo().options = options;
	}
	
	/*
	 * Read messages from a queue
	 */
	private MQMessage ReadMessagesFromQueue() throws MQException, IOException {
		
		MQMessage newmsg = Controller().ReadFromQueue(Queue(), Gmo());
		return newmsg;
				
	}
	
	/*
	 * Convert message
	 */
	private String ConvertMessageToString(MQMessage msg) throws IOException {
		
		byte[] b = new byte[msg.getMessageLength()];
		msg.readFully(b);
		String mess = new String(b);
		return mess;
		
	}
	
	/*
	 * Stopping
	 */
	public void StopProcessor() throws MQException {
		
		if (Queue() != null) {
			CloseQueue();
		}

		if (QueueManager() != null) {
			CloseConnection();
		}
	}
	
	/*
	 * Disconnect
	 */
	private void CloseQueue() throws MQException {

		Queue().close();
        if (Log()) {
			System.out.println(String.format("CONSUMER: %s , Queue closed", ThreadName()));
        }

	}
	
	// Disconnect from queue manager
	private void CloseConnection() throws MQException {

		QueueManager().disconnect();
        if (Log()) {
			System.out.println(String.format("CONSUMER: %s , Disconnected", ThreadName()));
        }

	}
	
	
}
