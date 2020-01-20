import java.net.DatagramPacket;

/*
 * 
 * This class is used for both error checking and categorising the packet, so that the receiver knows
 * exactly what to do with the packet, and that the sender is sending a packet in the correct format.
 * I do this by adding some information at the start of the packet, which clarifies if it is a packet 
 * containing valuable information (SND) or if it is an ACK. I then have to clarify what type of information
 * is contained in the packet (JOB for job listing, WVA/WVU for volunteering for work, CRI for a C&C requesting
 * it's designated ID number, or CMP for denoting that a specified job is done). These six characters, 
 * accompanied by two digits to denote their packet number, two digits to denote their originating C&C, 
 * and two digits to denote how many workers the C&C want to designate the job to, are appended to the start
 * of every packet being sent, and if a packet is received that is not in this format, then it is rejected 
 * by the C&C/Broker/Worker. All SND packets contain a string (either Job Listing or Worker Name), which we 
 * cannot error check for (as it is just a string).
 * 
 * (This was originally an abstract class, but I changed it to just a normal class as I didn't need multiple
 * classes to inherit it, I could do everything in just one class.)
 *  
 * String Types:
 *  Packet Types - SND and ACK (will introduce NCK later)
 *  Content Types - JOB, WVA/WVU, CRI, CMP, PAA
 *  Packet Number - ##
 *  Originating C&C Number - @@ (Up to 99 possible C&Cs)
 *  No. Workers given Job - %%	
 *  Job ID - &&&&
 * 
 * All possible combinations:
 *  SNDJOB##@@%%&&&&[JOBLISTING] - Send Job Description ((C&C to Broker) or (Broker to Worker))
 *	ACKJOB##@@%%&&&& - ACK Job Description ((Broker to C&C) or (Worker to Broker))
 *
 *	SNDWVA##00000000[WORKERNAME] - Send Worker Volunteer Available notification (Worker to Broker)
 *	ACKWVA##00000000 - ACK Worker Volunteer Available notification (Broker to Worker)
 *
 *	SNDWVU##00000000[WORKERNAME] - Send Worker Volunteer Unavailable notification (Worker to Broker)
 *	ACKWVU##00000000 - ACK Worker Volunteer Unavailable notification (Broker to Worker)
 *
 *	SNDCRI##00000000 - C&C requesting it's ID Number from broker
 *	ACKCRI##@@000000 - Broker responding with the ID Number to C&C
 *
 *	SNDCMP##@@00&&&&[JOBLISTING] - Send Job Complete (Can be (Worker to Broker) or (Broker to C&C))
 * 	ACKCMP##@@00&&&& - ACK Job Complete ((Broker to Worker) or (Broker to C&C))
 * 
 * 	ACKPAA##00000000 - A pacify ACK, used when a packet has been resent from one node to the other, 
 * 				   but the instruction has already been processed by the recipient (i.e. packet
 * 				   received, but initial ACK was lost.
 * 
 */

public class SNDContent {
	String fullDataPacketString;
	String packetType;					// SND
	String contentType;					// JOB, WVA/WVU, CMP, or NUL
	int packetNumber;					// ##
	String originatingCAndC;			// The ID number of the C&C originally requesting the job to be completed
	int numWorkersForJob;				// How many workers the C&C wants to do this job. -1 for all, 
										// 1+ for the number of workers, 0 means this is irrelevant for this packet.
	String jobID;						// ID number for the job, set by C&C
	String content;						// Job listing/Worker name string
	boolean validPacket;				// used for error checking
	int numTasksUntilJobComplete;		// how many more times this job has to be done before it is accepted as complete
										// i.e. how many workers are to do this job
	int distributionsLeft;				// how many more times the Broker has to distribute this job
	
	// for creating PacketContent from a received packet
	public SNDContent(DatagramPacket packet) 
	{
		validPacket = false;	//set as false at start
		byte[] data;
		data = packet.getData();
		fullDataPacketString = new String(data);
		
		//all elements have to be valid for the entire packet to be considered valid. These methods
		//also set the relevant variables within this PacketContent object at the same time.
		validPacket = setPacketType(fullDataPacketString) && setContentType(fullDataPacketString) 
					  && setPacketNumber(fullDataPacketString) && setOriginatingCAndC(fullDataPacketString)
					  && setNumWorkersForJob(fullDataPacketString)  && setJobID(fullDataPacketString)
					  && setPacketContent(fullDataPacketString);
		
		if(validPacket) this.fullDataPacketString = getPacketType() + getContentType() 
													+ getPacketNumberToString() + getOriginatingCAndC() 
													+ getNumWorkersForJobToString() + getJobID() 
													+ getPacketContent() + '\u0003';
		
		numTasksUntilJobComplete = numWorkersForJob;
		distributionsLeft = numWorkersForJob;
	}
	
	//for creating a PacketContent manually from a String in order to send it as packet
	public SNDContent(String inputString)		
	{	
		validPacket = false;	//set as false at start
		validPacket = setPacketType(inputString) && setContentType(inputString) 
					  && setPacketNumber(inputString) && setOriginatingCAndC(inputString)
					  && setNumWorkersForJob(inputString) && setJobID(inputString)
					  && setPacketContent(inputString);
		if(validPacket) this.fullDataPacketString = getPacketType() + getContentType() 
													+ getPacketNumberToString() + getOriginatingCAndC() 
													+ getNumWorkersForJobToString() + getJobID() 
													+ getPacketContent() + '\u0003';
		
		numTasksUntilJobComplete = numWorkersForJob;
		distributionsLeft = numWorkersForJob;
	}
	
	// for creating copy packets
//	public PacketContent(PacketContent packetCopied)
//	{
//		this.fullDataPacketString = packetCopied.fullDataPacketString;
//		this.packetType = packetCopied.packetType;	
//		this.contentType = packetCopied.contentType;
//		this.packetNumber = packetCopied.packetNumber;
//		this.originatingCAndC = packetCopied.originatingCAndC;
//		this.numWorkersForJob = packetCopied.numWorkersForJob;
//		this.jobID = packetCopied.jobID;
//		this.content = packetCopied.content;
//		this.validPacket = packetCopied.validPacket;
//		this.numTasksUntilJobComplete = packetCopied.numTasksUntilJobComplete;
//		this.distributionsLeft = packetCopied.distributionsLeft;
//	}
	
	public String toString() {
		return getPacketType() + getContentType() + getPacketNumberToString() + getOriginatingCAndC() 
			   + getNumWorkersForJobToString() + getJobID() + getPacketContent() + '\u0003';
	}
	
	/*
	 * toDatagramPacket() can be used (a) after instantiating a PacketContent object with the variables
	 * we want to send, and then turning it into a DatagramPacket in order to actually send it, or (b)
	 * after receiving a DatagramPacket and calling createACK() in order to turn it into an ACK packet.
	 * 
	 * '\u0003' is appended to the end of the fullDataPacketString. This is the character "End of Text",
	 * which makes sure that the receiver knows exactly when the String ends. I was getting some errors 
	 * without this, and it doesn't matter if there are multiple '\u0003's at the end of the packet, as 
	 * long as there is at least one.
	 */

	public DatagramPacket toDatagramPacket() {
		DatagramPacket packet= null;
		try 
		{
			this.fullDataPacketString = getPacketType() + getContentType() + getPacketNumberToString() 
										+ getOriginatingCAndC() + getNumWorkersForJobToString() + getJobID() 
										+ getPacketContent() + '\u0003';
			byte[] data = this.fullDataPacketString.getBytes();
			packet = new DatagramPacket(data, data.length);
		} catch(Exception e) { e.printStackTrace(); }
		return packet;
	}
	
	private boolean setPacketType(String packetString)	//boolean returns true if assignment is successful
	{
		String packetType = packetString.substring(0,3);	//gets first 3 letters of packet string
		if(packetType.equals("ACK") || packetType.equals("SND"))
		{
			this.packetType = packetType;
			return true;
		}
		return false;
	}
	
	//setContentType() establishes if a packet is of type JOB/CMP/WVU/WVA.
	
	private boolean setContentType(String packetString)
	{
		String contentType = packetString.substring(3, 6);
		if(contentType.equals("WVA") || contentType.contentEquals("WVU") 
				|| (contentType.equals("JOB") || contentType.equals("CMP")
				|| (contentType.equals("CRI"))))
		{
			this.contentType = contentType;
			return true;
		}
		
		return false;
	}
	
	//setPacketNumber establishes the packet number of the packet.
	
	private boolean setPacketNumber(String packetString)
	{
		String packetNumberString = packetString.substring(6, 8);
		this.packetNumber = Integer.parseInt(packetNumberString);
		if(packetNumber >= 0 && packetNumber <= 15)
			return true;	
		return false;
	}
	
	//setOriginatingCAndC establishes which C&C the job originated from.
	private boolean setOriginatingCAndC(String packetString)
	{
		String originatingCAndCString = packetString.substring(8, 10);
		this.originatingCAndC = originatingCAndCString;
		return true;
	}
	
	private boolean setNumWorkersForJob(String packetString)
	{
		String numWorkersString = packetString.substring(10, 12);
		this.numWorkersForJob = Integer.parseInt(numWorkersString);
		if(numWorkersForJob >= -1 && numWorkersForJob <= 99)
			return true;
		return false;
	}
	

	private boolean setJobID(String packetString)
	{
		String jobIDString = packetString.substring(12, 16);
		this.jobID = jobIDString;
		return true;
	}
	
	// setPacketContent() is for creating the string of information that is contained within a
	// packet, which will be either a job description or a worker name.
	
	private boolean setPacketContent(String packetString)
	{
		String content = packetString.substring(16, (packetString.indexOf('\u0003'))); //Cutting off at EndOfText char
		this.content = content;
		return true;
	}
	
	public void resetContentType(String newContentType)
	{
		this.contentType = newContentType;
	}
	
	public void resetPacketNumber(int newPacketNumber)
	{
		this.packetNumber = newPacketNumber;
		packetType = "SND"; // if we are resetting packet number, we know it will be a SND packet
//		String beforeNumWorkersString = fullDataPacketString.substring(0, 10);
//		String afterNumWorkersNumString = fullDataPacketString.substring(12);
//		fullDataPacketString = beforeNumWorkersString + getPacketNumberToString() + afterNumWorkersNumString + '\u0003';
	}
	
	public void resetNumWorkersForJob(int newNumWorkersForJob)
	{
		numWorkersForJob = newNumWorkersForJob;
//		String beforePackNumString = fullDataPacketString.substring(3, 6);
//		String afterPackNumString = fullDataPacketString.substring(8);
//		fullDataPacketString = "SND" + beforePackNumString + getNumWorkersForJobToString() + afterPackNumString + '\u0003';
	}
	
	public void resetDistributionsLeft(int set)
	{
		distributionsLeft = set;
	}

	// Within Broker and C&C, we need a way to edit the packet
	public void markOneTaskDone() 
	{
		numTasksUntilJobComplete--;
	}

	public void setCAndCID(String brokersIDForCAndC)
	{
		this.originatingCAndC = brokersIDForCAndC;
	}
	
	//following methods are just for the Worker/Broker/C&C to check the variables of this String content, if needed
	
	public String getPacketType()
	{
		return packetType;
	}
	
	public String getContentType()
	{
		return contentType;
	}
	
	public int getPacketNumber()
	{
		return packetNumber;
	}
	
	public String getPacketNumberToString()
	{
		String string  = "" + ((packetNumber < 10) ? "0" + packetNumber : packetNumber);
		return string;
		//return "" + ((packetNumber < 10) ? "0" + packetNumber : packetNumber);
	}
	
	public String getOriginatingCAndC()
	{
		String string = originatingCAndC;
		return string;
//		return originatingCAndC;
	}
	
	public int getNumWorkersForJob()
	{
		return numWorkersForJob;
	}
	
	public String getNumWorkersForJobToString()
	{
		return "" + ((numWorkersForJob < 10 && numWorkersForJob != -1) ? "0" + numWorkersForJob : numWorkersForJob);
	}
	
	public String getPacketContent()
	{
		return content;
	}
	
	public int getNumTasksUntilJobComplete()
	{
		return numTasksUntilJobComplete;
	}

	public int getDistributionsLeft()
	{
		return distributionsLeft;
	}
	
	public String getJobID()
	{
		return jobID;
	}
	
	public boolean isValid()
	{
		return validPacket;
	}
}
