import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Timer;

public class NodeData extends Thread{
	
	/*
	 * I'm making a WorkerData class so that the Broker can have an array of Workers (or 
	 * an array or C&Cs), with each element (NodeData) containing all the relevant data 
	 * to get into contact with a given Worker/C&C. It is generated from the Worker's/C&C's
	 * volunteer packet. This also must contain a method for sending to the Node, as each
	 * connection between the Broker and a given node requires it's own set of timers/packet
	 * numbers/Go-Back-N windows.
	 */

	//Information derived from volunteer packet
	private final String nodeName;
	private final String nodeType; // worker or C&C
	private final int dstPort;
	private final DatagramSocket brokerSocket;
	private final InetSocketAddress dstAddress;
	private int nextSentPackNum;		//keeping track of sender window	(from perspective of Broker)
	private int nextExpectedPackNum;	//keeping track of receiver window	(from perspective of Broker)
	private String idNumber;			//Only for C&Cs. String, as this will be "02" instead of 2.
	public ArrayList<SNDContent> designatedJobs = new ArrayList<SNDContent>();
	
	//Information required to send packets to node
	// Public so that the Broker can access these Arrays directly
	public Timer[] goBackNWindow = new Timer[16];
	public SNDContent[] goBackNWindowContent = new SNDContent[16];
	int goBackNWindowSize = 0;

	//for Workers
	NodeData(DatagramSocket brokerSocket, DatagramPacket workerVolunteer, String nodeType)		
	{
		SNDContent newVolunteer = new SNDContent(workerVolunteer);
		nodeName = newVolunteer.getPacketContent();
		this.nodeType = nodeType;
		nextSentPackNum = 0;
		nextExpectedPackNum = 0;
		dstPort = workerVolunteer.getPort();
		dstAddress = new InetSocketAddress("localhost", dstPort);
		this.brokerSocket = brokerSocket;
	}
	
	
	//For C&Cs
	NodeData(DatagramSocket brokerSocket, DatagramPacket cAndCPacket, String nodeType, int idNumber)		
	{
		nodeName = "C&C" + idNumber;
		this.nodeType = nodeType;
		this.idNumber = (idNumber < 10) ? "0" + idNumber : "" + idNumber;
		nextSentPackNum = 0;
		nextExpectedPackNum = 0;
		dstPort = cAndCPacket.getPort();
		dstAddress = new InetSocketAddress("localhost", dstPort);
		this.brokerSocket = brokerSocket;
	}
	
	public String getNodeName() {
		return nodeName;
	}

	public int getDSTPort() {
		return dstPort;
	}

	public InetSocketAddress getDstAddress() {
		return dstAddress;
	}
	
	public String getNodeType() {
		return nodeType;
	}
	
	public String getIDNumber() {
		return idNumber;
	}

	public int getNextSentPackNum() {
		return nextSentPackNum;
	}
	
	public String getNextSentPackNumToString() {
		return "" + ((nextSentPackNum < 10) ? "0" + nextSentPackNum : nextSentPackNum);
	}

	public int getNextExpectedPackNum() {
		return nextExpectedPackNum;
	}
	
	public String getNextExpectedPackNumToString() {
		return "" + ((nextExpectedPackNum < 10) ? "0" + nextExpectedPackNum : nextExpectedPackNum);
	}
	
	public void incrementNextExpectedPackNum() 
	{
		nextExpectedPackNum = (nextExpectedPackNum + 1) % 16; // iterate nextSentPackNum
	}
	
	public void sendPacket(SNDContent PacketContentToSend) {
		// always one element on the goBackNWindow that is null.
		while (goBackNWindowSize >= 15) 
		{
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		
		// Reset packet number so it matches up with node's next expected packet number,
		// and then create Datagram packet
		PacketContentToSend.resetPacketNumber(nextSentPackNum);
		DatagramPacket packetToSend = PacketContentToSend.toDatagramPacket();
		packetToSend.setSocketAddress(dstAddress); // sets this node's dstAddress as the destination for this packet
		
		Timer packetTimer = new Timer(); // start new timer
		TimerFlowControl ARQ = new TimerFlowControl(brokerSocket, packetToSend);
		packetTimer.schedule(ARQ, 100, 5000); // delay of 2 seconds, repeat every 2 seconds
		goBackNWindow[nextSentPackNum] = packetTimer;
		goBackNWindowContent[nextSentPackNum] = new SNDContent(packetToSend); // make 2 arrays where any given i on
																					// both arrays are for the matching
																					// PacketContent and timer.
		nextSentPackNum = (nextSentPackNum + 1) % 16; // iterate nextSentPackNum
		goBackNWindowSize++;
	}
	
	public void jobFullyComplete(SNDContent relevantJob) {
		SNDContent jobMarkedForRemoval = null;
		for(SNDContent iterationJob : designatedJobs)
		{
			if(relevantJob.getOriginatingCAndC().equals(iterationJob.getOriginatingCAndC())
				&& relevantJob.getJobID().equals(iterationJob.getJobID()))
			{
				jobMarkedForRemoval = iterationJob;
				break;
			}
		}
		if(jobMarkedForRemoval != null) 
			designatedJobs.remove(jobMarkedForRemoval);
	}
}
