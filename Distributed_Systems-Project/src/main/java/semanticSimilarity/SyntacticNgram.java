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
    
    protected boolean writtenToContext;		// If lexeme --> boolean if already written to context
    
    
    /***************	 Constructor - receives parsed ngram data, and constructs it to a syntactic ngram object	 ***************/

    
    public SyntacticNgram(){}
    
   
    
    public SyntacticNgram(String word, String pos_tag, String dep_label, String head_index) throws ParseException, IOException 
    {
    	this.word = Stemmer.stemWord(word.toLowerCase());
    	this.pos_tag = pos_tag.toLowerCase();
    	this.dep_label = dep_label.toLowerCase();
    	this.head_index = Integer.parseInt(head_index);
    	writtenToContext = false;
    }
    
    
    
    public SyntacticNgram(SyntacticNgram other) 
    {
    	word = other.getWord().toLowerCase();
    	pos_tag = other.getPosTag().toLowerCase();
    	dep_label = other.getDependancyLabel().toLowerCase();
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
    public boolean isWritten() { return writtenToContext; }
    
    
    
    /**********************************	 Setters  **********************************/
    
    
    public void setWritten() { this.writtenToContext = true; }
    public void setNotWritten() { this.writtenToContext = false; }
    
    
    
    /**********************************	 Deep copy Ngram  **********************************/
    
    
    public void copyNgram(SyntacticNgram otherNgram)
    {
    	this.word = otherNgram.getWord();
        this.pos_tag = otherNgram.getPosTag();
        this.dep_label = otherNgram.getDependancyLabel();
        this.head_index= otherNgram.getHeadIndex();
        this.writtenToContext = otherNgram.isWritten();
    }

    
    /**********************************	 To String  **********************************/

    
    @Override
    public String toString() { return word + "/" + pos_tag + "/" + dep_label + "/" + head_index; }

}