package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Writable; 
 

public class SyntacticNgram implements Writable {
        
    protected String word;					// The token\word in the syntactic ngram
    protected String pos_tag;				// Part of Speech tag
    protected String dep_label;				// Dependency Label
    protected int head_index;				// The word's head index in the syntactic ngram list
    
    
    /***************	 Constructor - receives a Stringed ngram, and parses it to a syntactic ngram object	 ***************/

    
    public SyntacticNgram(){}
    
    
    public SyntacticNgram(String ngram) throws ParseException {
        String[] ngramSplit = ngram.split("/");
    	for (int i = 0; i < ngramSplit.length; i++) {
    		System.out.println(ngramSplit[i]);
    	}
    	word = ngramSplit[0];
    	pos_tag = ngramSplit[1];
    	dep_label = ngramSplit[2];
    	head_index = Integer.parseInt(ngramSplit[3]);
    }
    
    
    public SyntacticNgram(SyntacticNgram other) {
    	word = other.getWord();
    	pos_tag = other.getPosTag();
    	dep_label = other.getDependancyLabel();
    	head_index = other.getHeadIndex();
    }
    
    
    
    /***************	 Read & Write fields methods of SyntacticNgram object	 ***************/
    
    
    public void readFields(DataInput in) throws IOException {
    	word = in.readUTF();
        pos_tag = in.readUTF();
        dep_label = in.readUTF();
        head_index = in.readInt();
    }
    
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeUTF(pos_tag);
        out.writeUTF(dep_label);
        out.writeInt(head_index);
    }
    
    
    
    /**********************************	 Getters  **********************************/

    
    public String getWord() { return word; }
    public String getPosTag() { return pos_tag; }
    public String getDependancyLabel() { return dep_label; }
    public int getHeadIndex() { return head_index; }
    
    
    
    @Override
    public String toString(){
    	return "Word: " + word + ", Part of Speech Tag: " + pos_tag + ", Dependancy Label: " + dep_label + ", Head index: " + head_index;
    }

}