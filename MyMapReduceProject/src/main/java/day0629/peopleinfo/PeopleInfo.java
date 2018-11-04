package day0629.peopleinfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PeopleInfo implements Writable{
	
	public void readFields(DataInput input) throws IOException {
		this.peopleID=input.readInt();
		this.gender=input.readUTF();
		this.height=input.readInt();
		
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(this.peopleID);
		output.writeUTF(this.gender);
		output.writeInt(this.height);
	}
	
	private int peopleID;
	private String gender;
	private int height;
	
	

	public int getPeopleID() {
		return peopleID;
	}

	public void setPeopleID(int peopleID) {
		this.peopleID = peopleID;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	

}
