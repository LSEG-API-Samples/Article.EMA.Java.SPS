///*|----------------------------------------------------------------------------------------------------
// *|            This source code is provided under the Apache 2.0 license      	--
// *|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
// *|                See the project's LICENSE.md for details.                  					--
// *|           Copyright Thomson Reuters 2016. All rights reserved.            		--
///*|----------------------------------------------------------------------------------------------------

package com.thomsonreuters.ema.examples;

import com.thomsonreuters.ema.access.Msg;
import com.thomsonreuters.ema.access.AckMsg;
import com.thomsonreuters.ema.access.GenericMsg;
import com.thomsonreuters.ema.access.RefreshMsg;
import com.thomsonreuters.ema.access.StatusMsg;
import com.thomsonreuters.ema.access.UpdateMsg;
import com.thomsonreuters.ema.access.Data;
import com.thomsonreuters.ema.access.DataType;
import com.thomsonreuters.ema.access.DataType.DataTypes;
import com.thomsonreuters.ema.access.EmaFactory;
import com.thomsonreuters.ema.access.EmaUtility;
import com.thomsonreuters.ema.access.FieldEntry;
import com.thomsonreuters.ema.access.FieldList;
import com.thomsonreuters.ema.access.Map;
import com.thomsonreuters.ema.access.MapEntry;
import com.thomsonreuters.ema.access.OmmConsumer;
import com.thomsonreuters.ema.access.OmmConsumerClient;
import com.thomsonreuters.ema.access.OmmConsumerEvent;
import com.thomsonreuters.ema.access.OmmException;
import com.thomsonreuters.ema.rdm.EmaRdm;
//Pimchaya
import com.thomsonreuters.ema.access.OmmBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Vector;
class AppClient implements OmmConsumerClient
{
	//Pimchaya
	 private Vector<String> SPSList;
	 private int level;
	 private boolean snapshot;
	 String spsRIC;
	 BufferedReader br;
	 OmmConsumer consumer;
	 public AppClient(OmmConsumer cons) {
		 consumer=cons;
		 SPSList = new Vector<String>();
		 level=0;
		 snapshot=true;
		 spsRIC=null;
		 br =  new BufferedReader(new InputStreamReader(System.in));
	 }
	 
	public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event)
	{
		System.out.println("Item Name: " + (refreshMsg.hasName() ? refreshMsg.name() : "<not set>"));
		System.out.println("Service Name: " + (refreshMsg.hasServiceName() ? refreshMsg.serviceName() : "<not set>"));
		
		System.out.println("Item State: " + refreshMsg.state());
		
		if (DataType.DataTypes.MAP == refreshMsg.payload().dataType()) {
			
			decode(refreshMsg.payload().map());
		}
		System.out.println();
		++level;
		
		
		if(requestNextlevel()){
			consumer.registerClient(EmaFactory.createReqMsg().domainType(11)
					.serviceName("API_ELEKTRON_EPD_RSSL").name(spsRIC).interestAfterRefresh(!snapshot), this);
			
		} else {
			System.out.println("=======Health Status of " + spsRIC + "=============="); 
		}
		if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType())
			decode(refreshMsg.payload().fieldList());
		System.out.println();
	}
	
	public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) 
	{
		System.out.println("UpdateMsg Item Name: " + (updateMsg.hasName() ? updateMsg.name() : "<not set>"));
		System.out.println("Service Name: " + (updateMsg.hasServiceName() ? updateMsg.serviceName() : "<not set>"));
		
		if (DataType.DataTypes.MAP == updateMsg.payload().dataType())
			decode(updateMsg.payload().map());
		
		System.out.println();
	}

	public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) 
	{
		System.out.println("Item Name: " + (statusMsg.hasName() ? statusMsg.name() : "<not set>"));
		System.out.println("Service Name: " + (statusMsg.hasServiceName() ? statusMsg.serviceName() : "<not set>"));

		if (statusMsg.hasState())
			System.out.println("Item State: " +statusMsg.state());
		
		System.out.println();
	}
	
	public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent){}
	public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent){}
	public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent){}
	
	void decode(Map map)
	{
		if (DataTypes.FIELD_LIST == map.summaryData().dataType())
		{
			System.out.println("Map Summary data:");
			decode(map.summaryData().fieldList());
			System.out.println();
		}
		SPSList.clear();
		for (MapEntry mapEntry : map)
		{
			if (DataTypes.BUFFER == mapEntry.key().dataType())
			{	
				
				System.out.println("Action: " + mapEntry.mapActionAsString() + " key value: " + EmaUtility.asHexString(mapEntry.key().buffer().buffer()));
				String keyStr=getKeyString(mapEntry.key().buffer());
				System.out.println("key="+keyStr);
				SPSList.add(keyStr);
			}
			
				

			if (DataTypes.FIELD_LIST == mapEntry.loadType())
			{
				System.out.println("Entry data:");
				decode(mapEntry.fieldList());
				System.out.println();
			}
		}
	}
	
	void decode(FieldList fieldList)
	{
		for (FieldEntry fieldEntry : fieldList)
		{
			System.out.print("Fid: " + fieldEntry.fieldId() + " Name = " + fieldEntry.name() + " DataType: " + DataType.asString(fieldEntry.load().dataType()) + " Value: ");

			if (Data.DataCode.BLANK == fieldEntry.code())
				System.out.println(" blank");
			else
				switch (fieldEntry.loadType())
				{
				case DataTypes.REAL :
					System.out.println(fieldEntry.real().asDouble());
					break;
				case DataTypes.DATE :
					System.out.println(fieldEntry.date().day() + " / " + fieldEntry.date().month() + " / " + fieldEntry.date().year());
					break;
				case DataTypes.TIME :
					System.out.println(fieldEntry.time().hour() + ":" + fieldEntry.time().minute() + ":" + fieldEntry.time().second() + ":" + fieldEntry.time().millisecond());
					break;
				case DataTypes.INT :
					System.out.println(fieldEntry.intValue());
					break;
				case DataTypes.UINT :
					System.out.println(fieldEntry.uintValue());
					break;
				case DataTypes.ASCII :
					System.out.println(fieldEntry.ascii());
					break;
				case DataTypes.ENUM :
					System.out.println(fieldEntry.enumValue());
					break;
				case DataTypes.ERROR :
					System.out.println("(" + fieldEntry.error().errorCodeAsString() + ")");
					break;
				default :
					System.out.println();
					break;
				}
		}
	}
	//Pimchaya
	String getKeyString(OmmBuffer ommBuf) {
		ByteBuffer buffer =  ommBuf.asHex();
		StringBuilder asString = new StringBuilder();
		int length =buffer.limit();
		for (int i = buffer.position(); i < length; i++)
		{
			
			byte b = buffer.get(i);
			asString.append((char)b);
		}
		return asString.toString();
	}
	public boolean requestNextlevel() {
    	//System.out.println("level="+level);
    	String key="";
    	if(level==1) {
    		key = "Provider SPS";
    		snapshot=true;
    	} else if(level==2) {
    		key = "SubProvider SPS";
    		snapshot=false;
    	}
    
    	if(level<3) {
    		System.out.println("List of " + key + ": " );
    		for(String SPS : SPSList) {
    			System.out.print(SPS + ",");
    		}
    		
    		System.out.println("\nEnter a " + key+ ": " );
    		try {
    			spsRIC = br.readLine();
    			
    		}catch(IOException ie) {
    			ie.printStackTrace();
    			System.exit(1);
    		}
    		return true;
    	} 
    	else return false;
    }
}

public class Consumer 
{
	public static void main(String[] args)
	{
		OmmConsumer consumer = null;
		
		try
		{
			
			
			consumer  = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig().host("192.168.27.48:14002").username("user"));
			AppClient appClient = new AppClient(consumer);
			consumer.registerClient(EmaFactory.createReqMsg().domainType(11)
															.serviceName("API_ELEKTRON_EPD_RSSL").name(".[SPSEMEA").interestAfterRefresh(false), appClient);
			
			Thread.sleep(300000);			// API calls onRefreshMsg(), onUpdateMsg() and onStatusMsg()
		}
		catch (InterruptedException | OmmException excp)
		{
			System.out.println(excp.getMessage());
		}
		finally 
		{
			if (consumer != null) consumer.uninitialize();
		}
	}
}


