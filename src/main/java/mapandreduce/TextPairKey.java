package mapandreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPairKey implements WritableComparable<TextPairKey> {
	
	private Text firstElement;
	private Text secondElement;
	
	public Text getFirstElement() {
		return firstElement;
	}

	public void setFirstElement(String firstElement) {
		this.firstElement = new Text(firstElement);
	}

	public Text getSecondElement() {
		return secondElement;
	}

	public void setSecondElement(String secondElement) {
		this.secondElement = new Text(secondElement);
	}	

	public TextPairKey(){
		this.firstElement = new Text();
		this.secondElement = new Text();
	}
	
	public TextPairKey(String s1, String s2){
		this.firstElement = new Text(s1);
		this.secondElement = new Text(s2);		
	}

	public void readFields(DataInput in) throws IOException {
		this.firstElement.readFields(in);
		this.secondElement.readFields(in);
		
	}

	public void write(DataOutput arg0) throws IOException {
		this.firstElement.write(arg0);
		this.secondElement.write(arg0);		
	}

	public int compareTo(TextPairKey o) {
		if(!this.firstElement.equals(o.firstElement))
			return -1;
		if(this.secondElement.equals(o.secondElement))
				return 0;
		else
			return -1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstElement == null) ? 0 : firstElement.hashCode());
		result = prime * result + ((secondElement == null) ? 0 : secondElement.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextPairKey other = (TextPairKey) obj;
		if (firstElement == null) {
			if (other.firstElement != null)
				return false;
		} else if (!firstElement.equals(other.firstElement))
			return false;
		if (secondElement == null) {
			if (other.secondElement != null)
				return false;
		} else if (!secondElement.equals(other.secondElement))
			return false;
		return true;
	}

}
