package mapandreduce;

import org.apache.hadoop.io.Text;

public class TextPairKey extends Text {
	
	private Text firstElement;
	private Text secondElement;
	
	public Text getFirstElement() {
		return firstElement;
	}

	public void setFirstElement(Text firstElement) {
		this.firstElement = firstElement;
	}

	public Text getSecondElement() {
		return secondElement;
	}

	public void setSecondElement(Text secondElement) {
		this.secondElement = secondElement;
	}	

	public TextPairKey(){
	}
	
	public TextPairKey(String s1, String s2){
		this.firstElement = new Text(s1);
		this.secondElement = new Text(s2);		
	}

}
