import java.net.DatagramPacket;

public class ACKContent {
	String packetType;
	int packetNumber;
	boolean validACK;
	
	ACKContent(int packetNumber)
	{
		packetType = "ACK";
		this.packetNumber = packetNumber;
		validACK = true;
	}
	
	ACKContent(DatagramPacket packet) 
	{
		byte[] data;
		data = packet.getData();
		String packetString = new String(data);
		if(packetString.substring(0,3).equals("ACK"))
		{
			this.packetType = "ACK";
			this.packetNumber = Integer.parseInt(packetString.substring(3, 5));
			validACK = true;
			
		}
		else
			validACK = false;
	}
	
	public DatagramPacket toDatagramPacket() {
		DatagramPacket packet= null;
		try 
		{
			String content = packetType + ((packetNumber < 10) ? "0" + packetNumber : "" + packetNumber);
			byte[] data = content.getBytes();
			packet = new DatagramPacket(data, data.length);
		} catch(Exception e) { e.printStackTrace(); }
		return packet;
	}
	
	public boolean isValidACK()
	{
		return validACK;
	}
	
	public int getACKNumber()
	{
		return packetNumber;
	}
}
