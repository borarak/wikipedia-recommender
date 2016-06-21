package wikiParser;

import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import config.ConfigProperties;

import java.util.*;
import java.io.*;

public class Parser extends DefaultHandler {
	
	@SuppressWarnings("unused")
	private static final long serialVersionUID = -5680024314669973300L;
	static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
	static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
	static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";
	
	private String logItem;
	private boolean hasLogItem;
	
	private String uname;
	private boolean hasUname;
	
	private long id;
	private boolean hasID;
	
	private String logTitle;
	private boolean hasLogTitle;
	
	private String tStamp;
	private boolean hasTimeStamp;
	
	private long editCount = 0;
	
	
	private CharArrayWriter logItemBuff = new CharArrayWriter();
	private CharArrayWriter unameBuff = new CharArrayWriter();
	private CharArrayWriter idBuff = new CharArrayWriter();
	private CharArrayWriter logTitleBuff = new CharArrayWriter();
	private CharArrayWriter tStampBuff = new CharArrayWriter();
	
	BufferedWriter bWriter;
	
	public void writeBuffer(String flatLine) throws IOException{
		bWriter.write(flatLine + "\n");
	}
	
	
	
	
	public void startDocument(){
		try {
			bWriter = new BufferedWriter(new FileWriter("/home/raktim/hadoop-shared/wikipedia/dumps/processed.txt", true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void endDocument(){
		try {
			bWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
		String startTag = localName;
		
		//Specify desired start tags and set flags on encountering them so that they can be processed later
		if (startTag.equalsIgnoreCase("logitem")) {
			this.hasLogItem = true;
			this.logItemBuff = new CharArrayWriter();
		}
		
		if (startTag.equalsIgnoreCase("username")) {
			this.hasUname = true;
			this.unameBuff = new CharArrayWriter();
		}
		
		if (startTag.equalsIgnoreCase("id")) {
			this.hasID = true;
			this.idBuff = new CharArrayWriter();
		}
		
		if (startTag.equalsIgnoreCase("logtitle")) {
			this.hasLogTitle = true;
			this.logTitleBuff = new CharArrayWriter();
		}
		
		if (startTag.equalsIgnoreCase("timestamp")) {
			this.hasTimeStamp = true;
			this.tStampBuff = new CharArrayWriter();
		}
	}
	
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if(this.hasLogItem){
			for (int j=start;j<start+length;j++) {				
				this.logItemBuff.append(ch[j]);
			}
		}
		if (this.hasUname) {
			for (int i = start; i <start+length; i++) {
				this.unameBuff.append(ch[i]);
			}
		}
		if(this.hasID){
			for (int j=start;j<start+length;j++) {				
				this.idBuff.append(ch[j]);
			}
		}
		
		if (this.hasLogTitle) {
			for (int i = start; i <start+length; i++) {
				this.logTitleBuff.append(ch[i]);
			}
		}
		
		if (this.hasTimeStamp) {
			for (int i = start; i <start+length; i++) {
				this.tStampBuff.append(ch[i]);
			}
		}
	}
	
	public void endElement(String uri, String localname, String qname) {
		String endTag = localname;
		if (endTag.equalsIgnoreCase("logItem")) {
			this.hasLogItem = false;
			//System.out.println(this.idBuff + "\t" + this.unameBuff + "\t" + this.logTitleBuff);
			try {
				this.writeBuffer(new StringBuilder(this.idBuff + "\t" + this.unameBuff + "\t" + this.tStampBuff + "\t" + this.logTitleBuff).toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			editCount++;
			if(editCount % 1000000 == 0){
				System.out.println(editCount + " entries processed");
			}
		}
		if (endTag.equalsIgnoreCase("username")) {
			this.hasUname = false;
		}
		if (endTag.equalsIgnoreCase("id")) {
			this.hasID = false;			
		}
		if (endTag.equalsIgnoreCase("logtitle")) {
			this.hasLogTitle = false;			
		}
		
		if (endTag.equalsIgnoreCase("timestamp")) {
			this.hasTimeStamp = false;
		}
	}
	
	static public void main(String[] args) throws Exception {
		String filename = null;
		boolean dtdValidate = false;
		boolean xsdValidate = true;
		
		
		String schemaSource = new ConfigProperties().getProperty("SCHEMA_SOURCE");
		Parser par = new Parser();		
		
		//Parse arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dtd")) {
				dtdValidate = true;
			} else if (args[i].equals("-xsd")) {
				xsdValidate = true;
			} else if (args[i].equals("-xsdss")) {
				if (i == args.length - 1) {
					usage();
				}
				xsdValidate = true;
				schemaSource = args[++i];
			} else if (args[i].equals("-usage")) {
				usage();
			} else if (args[i].equals("-help")) {
				usage();
			} 
		}
		if (filename == null) {
			filename = new ConfigProperties().getProperty("WIKIPEDIA_DUMP_FILE");
			//usage();
		}
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		
		// Validation part 1: set whether validation is on
		spf.setValidating(dtdValidate || xsdValidate);
		
		// Create a JAXP SAXParser
		SAXParser saxParser = spf.newSAXParser();
		
		// Validation part 2a: set the schema language if necessary
		if (xsdValidate) {
			try {
				saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
			} catch (SAXNotRecognizedException x) {
				// This can happen if the parser does not support JAXP 1.2
				System.err.println("Error: JAXP SAXParser property not recognized: " + JAXP_SCHEMA_LANGUAGE);
				System.err.println("Check to see if parser conforms to JAXP 1.2 spec.");
				System.exit(1);
			}
		}
		
		// Validation part 2b: Set the schema source, if any. See the JAXP
		// 1.2 maintenance update specification for more complex usages of
		// this feature.
		if (schemaSource != null) {
			saxParser.setProperty(JAXP_SCHEMA_SOURCE, new File(schemaSource));
		}
		
		// Get the encapsulated SAX XMLReader
		XMLReader xmlReader = saxParser.getXMLReader();
		
		// Set the ContentHandler of the XMLReader
		xmlReader.setContentHandler(par); // set new
		//xmlReader.setErrorHandler(new MyErrorHandler(System.err));
		
		// Tell the XMLReader to parse the XML document
		xmlReader.parse(convertToFileURL(filename));
		System.out.print("Finished parsing document");
		System.out.println("Total edits processed: " + par.editCount);
	}





	private static String convertToFileURL(String filename) {
		return "file:" + filename;
	}




	private static void usage() {
			System.err.println("Usage: Parser [-options] <file.xml>");
			System.err.println("-dtd = DTD validation");
			System.err.println("-xsd | -xsdss <file.xsd> = W3C XML Schema validation using xsi: hints");
			System.err.println("in instance document or schema source <file.xsd>");
			System.err.println("-xsdss <file> = W3C XML Schema validation using schema source <file>");
			System.err.println("-usage or -help = this message");
			System.exit(1);	
	}
	
	
	
	

}
