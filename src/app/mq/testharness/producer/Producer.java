package app.mq.testharness.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import app.mq.testharness.mqcontroller.MQController;

public class Producer {

	private MQController controller = null;
	private MQQueueManager qmgr = null;
	private MQQueue queue = null;

	private String hostName;
	private String channelName;
	private int port;
	private String userId;
	private String password;
	private String queueName;
	private String msg;
	private String queueManager;
	private String cipher;
	private boolean mqcsp;
	private boolean usetls;
	private int per;
	private int function;
	
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

	public String Msg() {
		return msg;
	}
	public void Msg(String msg) {
		this.msg = msg;
	}

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
	
	public int Persistent() {
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
	public void MessageSize(int v) {
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

	private String inputfilename;
	public String InputFileName() {
		return inputfilename;
	}
	public void InputFileName(String v) {
		this.inputfilename = v;
	}
	
	private String version;
	public String Version() {
		return version;
	}
	public void Version(String v) {
		this.version = v;
	}

	private String encoding = "UTF-8";
	public String Encoding() {
		return encoding;
	}
	public void Encoding(String v) {
		this.encoding = v;
	}

	private int sleepinterval;
	public int SleepInterval() {
		return sleepinterval;
	}
	public void SleepInterval(int v) {
		this.sleepinterval = v;
	}
	
	/*
	 * Start processing ...
	 */
	public void Start() throws NoSuchAlgorithmException, MQException, IOException, InterruptedException {
	
		Process();
		
	}
	
	private void Process() throws MQException, IOException, InterruptedException, NoSuchAlgorithmException {
		
		//Connect to queue manager
		try {
			Controller(new MQController());
			Controller().QueueManagerName();
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
				System.out.println(String.format("PRODUCER: %s , Attempting to create a client connection to %s ", ThreadName(), QueueManagerName()));
			} else {
				System.out.println(String.format("PRODUCER: %s , Attempting to create a local binding connection to %s ", ThreadName(), QueueManagerName()));
			}
			Controller().Version(Version());
			
			MQQueueManager(Controller().ConnectToQueueManager());
			
			System.out.println(String.format("PRODUCER: %s , Connection established",ThreadName()));
			
		} catch (MQException e) {
			System.out.println("PRODUCER: Unable to connect to queue manager ... " + e.reasonCode + ": " + e.getLocalizedMessage());
			System.out.println("PRODUCER: See MQ logs for more details");
			StopProcessor();
			
		} catch (Exception e) {
			System.out.println("PRODUCER: Exception - Unable to connect to queue manager ... "  + e.getLocalizedMessage());
			System.out.println("PRODUCER: Exception - See MQ logs for more details");
			StopProcessor();
	}

		
		if (qmgr != null) {
			producer();
		}
	}
	
	/*
	 * Producer
	 */
	private void producer() throws MQException, IOException, InterruptedException, NoSuchAlgorithmException{
		
		/*
		 * Open the queue
		 */
		Queue(null);
		try {
			OpenQueueForWriting();
			
		} catch (MQException e1) {
			System.out.println(String.format("PRODUCER: %s , Unable to write messages to queue ... %s : %s", ThreadName(), e1.reasonCode, e1.getLocalizedMessage()));		
			StopProcessor();
			
		} catch (IOException e1) {
			System.out.println(String.format("PRODUCER: %s , IOException when reading message ... : %s", ThreadName(), e1.getLocalizedMessage()));
			StopProcessor();
		}
		
		/*
		 * Are we writing messages from a file ?
		 */
		StringBuilder sb = new StringBuilder();
		
		if (InputFileName() != null) {		
			System.out.println(String.format("PRODUCER: %s , Messages will be produced from file %s", ThreadName(), InputFileName()));

			try {
				File file = new File(InputFileName());
				
				FileInputStream fis = new FileInputStream(file);
				byte[] data = new byte[(int) file.length()];
				fis.read(data);
				fis.close();
				Msg(new String(data));
				
			} catch (FileNotFoundException e) {
				System.out.println(String.format("PRODUCER: %s , Input file %s, not found, continuing using dummy messages", ThreadName(), InputFileName()));
				InputFileName(null);
				
			} catch (Exception e) {
				System.out.println(String.format("PRODUCER: %s , Error processing %s file, continuing using dummy messages", ThreadName(), InputFileName()));
				InputFileName(null);
			}
			
			/*
			try {
				BufferedReader br = Files.newBufferedReader(Paths.get(InputFileName()), StandardCharsets.UTF_8);
				String line;
	            while ((line = br.readLine()) != null) {
	                sb.append(line);
	            }
	            br.close();
	            Msg(sb.toString());
	            
			} catch (Exception e) {
			}
			*/
		}
		
		try {
			MQPutMessageOptions pmo = setPMO();

			if (GroupMessages()) {
				GenerateGroupId();
				ResetGroupSeq();
			}
			
			Date startTime = new Date();
			Date runningDate = startTime;
			final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
			String date = dateFormat.format(startTime);	
			System.out.println(String.format("PRODUCER: %s , Starting to write messages : %s", ThreadName(), date));
			long i = 1;
			for (i = 1; i <= NumberOfMessages(); i++) {
				WriteMessageToQueue(pmo, i);
			
				if (i % Statistics() == 0) {				
					if (Log()) {
						Date intermediate = new Date();
						date = dateFormat.format(intermediate);	
						long diff = intermediate.getTime() - runningDate.getTime();	
						System.out.println(String.format("PRODUCER: %s , %d Messages processed in %d ms", ThreadName(), i, diff));
						runningDate = intermediate;
					}
				}
				if (i % Sync() == 0) {
					QueueManager().commit();
					//Thread.sleep(2000);
				}
			}
			i--;						// Ajust so the number is correct
			QueueManager().commit();

			Date endTime = new Date();
			date = dateFormat.format(endTime);	
			if (Log()) {
				System.out.println(String.format("PRODUCER: %s , End processing : %s ", ThreadName(), date));
			}
			long diff = endTime.getTime() - startTime.getTime();	
			long diffMS = diff / 1000;
			long diffSeconds = diff / 1000 % 60;
			long diffMinutes = diff / (60 * 1000) % 60;
			long diffHours = diff / (60 * 60 * 1000) % 24;
			
			String str = String.format("PRODUCER: %s , %d Messages processed, time taken : %d hours, %d mins, %d secs, %d ms", ThreadName(), i, diffHours, diffMinutes, diffSeconds, diff);
			System.out.println(str);
			
		} catch (MQException e) {
			System.out.println("PRODUCER: " + ThreadName() + " Unable to write message to queue ... " + e.reasonCode + ": " + e.getLocalizedMessage());
			
		}
		StopProcessor();
		
	}
	
	/*
	 * Open the queue for writing
	 */
	private void OpenQueueForWriting() throws MQException, IOException {
		
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
						+ MQConstants.MQOO_OUTPUT;

		if (Log()) {
			System.out.println("PRODUCER: " + ThreadName() + " , Opening Queue for output : " + QueueName());
		}
		Queue(Controller().OpenQueue(QueueManager(), openOptions));

	//	this.queue = qmgr.accessQueue(this.queueName, openOptions);
	}

	/*
	 * PutMessageOptions
	 */
	private MQPutMessageOptions setPMO() {
		
		MQPutMessageOptions pmo = new MQPutMessageOptions();
		pmo.options = MQConstants.MQPMO_FAIL_IF_QUIESCING 
					+ MQConstants.MQPMO_NEW_MSG_ID
					+ MQConstants.MQPMO_SYNCPOINT
					+ MQConstants.MQPMO_LOGICAL_ORDER;

		return pmo;
	}
	
	/*
	 * Write messages
	 */
	private void WriteMessageToQueue(MQPutMessageOptions pmo, long i) throws MQException, IOException, NoSuchAlgorithmException {
		
		WriteToQueue(pmo, i);
		
	}
	
	/*
	 * Write messages to a queue
	 */
	private void WriteToQueue(MQPutMessageOptions pmo, long i) throws MQException, IOException, NoSuchAlgorithmException {
		
		MQMessage newmsg = new MQMessage();
		newmsg.format 			= MQConstants.MQFMT_STRING;
		newmsg.messageId 		= MQConstants.MQMI_NONE;
		newmsg.correlationId 	= MQConstants.MQCI_NONE;
		newmsg.messageType      = MQConstants.MQMT_DATAGRAM;		
		
		/*
		 * Persistance ...
		 */
		if (Persistent() == MQConstants.MQPER_PERSISTENT) {
			newmsg.persistence 		= MQConstants.MQPER_PERSISTENT;
			
		} else if (Persistent() == MQConstants.MQPER_NOT_PERSISTENT) {
			newmsg.persistence 		= MQConstants.MQPER_NOT_PERSISTENT;

		} else {
			newmsg.persistence 		= MQConstants.MQPER_PERSISTENCE_AS_Q_DEF;

		}

		//X'000000000000000000000000000000000000000000000001' // 24
		/*
		 * https://en.wikipedia.org/wiki/EBCDIC_273
		 * 
		 * ebcdic value;
		 * 37, 500 ( 1148 - same as 500, with euro ), 1047 ( 1048 as 1047 )
		 *
		 * asci
		 * 819, 437
		 * 1148, 1141, 273 (Full Latin-1 charactset);  //ebcdic
		 * https://www.ibm.com/support/knowledgecenter/SSDP9S_11.3.0/com.ibm.swg.im.iis.fed.classic.nls.doc/topics/iiyfcnlscpcnv.html
		 * 
		 */
		newmsg.characterSet = CodePage();
		
		/*
		 * Create a dummy message ... if not using a file 
		 */
		//String message;
		if (InputFileName() == null) {
			int msgSize = 0;
			if (MessageSize() == 0) {
				Random rand = new Random();
				msgSize = rand.nextInt((2000 - 100) + 1) + 100;
			} else {
				msgSize = MessageSize();
			}

			//char[] characters = new char[DecimalOrBinary() * msgSize];	
			long fillint = i % 10;
			
			// ... fill character array and conver to a string
			String fillstr = Long.toString(fillint);
			char fill = fillstr.charAt(0);
			
			Msg(new String(new char[DecimalOrBinary() * msgSize]).replace('\0', fill));
		}
		
		boolean segments = false;
		
		/*
		 * Are we splitting messages (segments)?
		 */
		if (SplitMessages()) {
			if (Msg().length() > SplitMessageSize()) {
				if (Log()) {
					System.out.println("PRODUCER: " + ThreadName() + " , Message size is " + Msg().length() + " characters, message is being split");
				}
				segments = true;
			} else {
				if (MessageSize() == 0) { 						// if auto calculated size, then display the size
					if (Log()) {
						System.out.println("PRODUCER: " + ThreadName() + " , Message size is " + Msg().length() + " characters");
					}
				}
			}
		}
		if (SplitMessages()) {
			if (segments) {
				newmsg.messageFlags += MQConstants.MQMF_SEGMENTATION_ALLOWED;
			}	
		}
		
		if (GroupMessages()) {
			if (i % GroupMessageSize() == 0 ) {
			//	System.out.println("LAST");
				IncrementGroupSeq();			
				//newmsg.groupId = this.groupId;
				newmsg.messageSequenceNumber = this.groupSeq;
				newmsg.messageFlags += MQConstants.MQMF_LAST_MSG_IN_GROUP;
				//if (segments) {
				//	newmsg.messageFlags += MQConstants.MQMF_SEGMENTATION_ALLOWED;
				//}
				GenerateGroupId();
				ResetGroupSeq();
				
			} else {
				//System.out.println("FIRST");
				IncrementGroupSeq();
				//newmsg.groupId = this.groupId;
				//newmsg.messageSequenceNumber = this.groupSeq;
				newmsg.messageFlags += MQConstants.MQMF_MSG_IN_GROUP;
				//if (segments) {
				//	newmsg.messageFlags += MQConstants.MQMF_SEGMENTATION_ALLOWED;	
				//}
				
				if (i >= NumberOfMessages()) {
			//		newmsg.groupId = this.groupId;
			//		newmsg.messageSequenceNumber = this.groupSeq;
					newmsg.messageFlags += MQConstants.MQMF_MSG_IN_GROUP | MQConstants.MQMF_LAST_MSG_IN_GROUP;
					//if (segments) {
					//	newmsg.messageFlags += MQConstants.MQMF_SEGMENTATION_ALLOWED;
					//}
				}
			}
		}
		if (!Simulation()) {
			try {
				Thread.sleep(SleepInterval());
			} catch (Exception e) {
				
			}
			newmsg.write(Msg().getBytes(Encoding()));	
			Controller().WriteMessage(Queue(), newmsg, pmo);			
		}
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
			System.out.println(String.format("PRODUCER: %s , Queue closed", ThreadName()));
		}

	}
	
	// Disconnect from queue manager
	private void CloseConnection() throws MQException {

		QueueManager().disconnect();
		if (Log()) {
			System.out.println(String.format("PRODUCER: %s , Disconnected", ThreadName()));			
		}

	}
	
	
}
