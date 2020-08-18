package maersk.mq.testharness;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.Callable;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;

import maersk.mq.testharness.consumer.Consumer;
import maersk.mq.testharness.producer.Producer;

public class MessageProcessor implements Callable<Integer> {

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

	public String QueueManagerName() {
		return queueManager;
	}
	public void QueueManager(String queueManager) {
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
	
	public void StillProcessing(boolean v) {
		System.out.println("Setting Still Processing ...");
		this.stillProcessing = false;
	}
	public boolean StillProcessing() {
		return this.stillProcessing;
	}

	private int summerize;
	public int Summerize() {
		return summerize;
	}
	public void Summerize(int v) {
		this.summerize = v;
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

	private int sleepinterval;
	public int SleepInterval() {
		return sleepinterval;
	}
	public void SleepInterval(int v) {
		this.sleepinterval = v;
	}
	
	@Override
	public Integer call() throws Exception {
		int ret = 0;
		
		try {
			ThreadName(Thread.currentThread().getName());

			switch (Function()) {
				case Constants.PRODUCER:
					System.out.println("PRODUCER: " + ThreadName() + " , Thread name ");
			        break;
				case Constants.CONSUMER:
					System.out.println("CONSUMER: " + ThreadName() + " , Thread name ");
			        break;
				default:
					break;
			}
			
			processMessages();
			
			switch (Function()) {
				case Constants.PRODUCER:
					System.out.println("PRODUCER: " + ThreadName() + " , Thread complete " );
			        break;
				case Constants.CONSUMER:
					System.out.println("CONSUMER: " + ThreadName() + " , Thread complete " );
			        break;
				default:
					break;
			}
			
		} catch (MQException e) {
			System.out.println("ERROR: Error processor (1) " + e.getMessage());

		} catch (Exception e) {
			System.out.println("ERROR: Error processor (2) " + e.getMessage());

		}
		return ret;
		
	}
	
	/*
	 * Process what we want
	 */
	private void processMessages() throws MQException, IOException, InterruptedException, NoSuchAlgorithmException {
		
		switch (Function()) {
			case Constants.PRODUCER:
				Producer producer = new Producer();
				producer.ThreadName(ThreadName());
				producer.QueueManagerName(QueueManagerName());
				producer.QueueName(QueueName());
				producer.HostName(HostName());
				producer.Port(Port());
				producer.ChannelName(ChannelName());
				producer.Cipher(Cipher());
				producer.QueueName(QueueName());
				//producer.Msg(msg);
				producer.UserId(UserId());
				producer.Password(Password());
				producer.UseTLS(UseTLS());
				producer.MQCSP(MQCSP());
				producer.MessageSize(MessageSize());			// kb
				producer.DecimalOrBinary(DecimalOrBinary());	// default to 1024
				if (Persistent() == MQConstants.MQPER_PERSISTENT) {
					producer.Persistent(MQConstants.MQPER_PERSISTENT);
				} else {
					producer.Persistent(MQConstants.MQPER_NOT_PERSISTENT);					
				}
				producer.Function(Constants.PRODUCER);
				producer.NumberOfMessages(NumberOfMessages());
				producer.Simulation(Simulation());
				producer.CodePage(CodePage());
				producer.KeyStore(KeyStore());
				producer.StorePass(StorePass());
				producer.IBMCipher(IBMCipher());
				//
				producer.GroupMessages(GroupMessages());
				producer.GroupMessageSize(GroupMessageSize());
				producer.SplitMessages(SplitMessages());
				// 153600 - test message size
				producer.SplitMessageSize(SplitMessageSize());				// not used when SplitMessage is false 2,097,152
				producer.Statistics(Statistics());
				producer.Sync(Sync());
				producer.Log(Log());
				producer.Verbose(Verbose());
				producer.InputFileName(InputFileName());
				producer.Version(Version());
				producer.SleepInterval(SleepInterval());
				
				producer.Start();			
		        break;
	        
		case Constants.CONSUMER:
			Consumer consumer = new Consumer();
			consumer.ThreadName(ThreadName());
			consumer.QueueManagerName(QueueManagerName());
			consumer.QueueName(QueueName());
			consumer.HostName(HostName());
			consumer.Port(Port());
			consumer.ChannelName(ChannelName());
			consumer.Cipher(Cipher());
			consumer.QueueName(QueueName());
			//producer.Msg(msg);
			consumer.UserId(UserId());
			consumer.Password(Password());
			consumer.UseTLS(UseTLS());
			consumer.MQCSP(MQCSP());
			consumer.MesssageSize(MessageSize());			// kb
			consumer.DecimalOrBinary(DecimalOrBinary());	// default to 1024
			if (Persistent() == MQConstants.MQPER_PERSISTENT) {
				consumer.Persistent(MQConstants.MQPER_PERSISTENT);
			} else {
				consumer.Persistent(MQConstants.MQPER_NOT_PERSISTENT);					
			}
			consumer.Function(Constants.CONSUMER);
			consumer.NumberOfMessages(NumberOfMessages());
			consumer.Simulation(Simulation());
			consumer.CodePage(CodePage());
			consumer.KeyStore(KeyStore());
			consumer.StorePass(StorePass());
			consumer.IBMCipher(IBMCipher());
			//
			consumer.GroupMessages(GroupMessages());
			consumer.GroupMessageSize(GroupMessageSize());
			consumer.SplitMessages(SplitMessages());
			// 153600 - test message size
			consumer.SplitMessageSize(SplitMessageSize());				// not used when SplitMessage is false 2,097,152
			consumer.Statistics(Statistics());
			consumer.Sync(Sync());
			consumer.Log(Log());
			consumer.Verbose(Verbose());
			consumer.Version(Version());
			
			consumer.Start();			
	        break;
	        
		default:
			break;
		}
		
	}

	
}
