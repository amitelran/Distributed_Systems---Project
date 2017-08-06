package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable; 
 

public class SyntacticNgramLine implements Writable {
        
    protected Text headWord;															// The word which is the syntactic tree's root
    protected SyntacticNgram[] ngrams;													// Array of all syntactic ngrams in given line
    protected int total_count;															// Total counts as summed in the years
    

    public SyntacticNgramLine(){}
    
    
    
    /***************	 Constructor - receives line and parses into Syntactic Ngram line object	 ***************/
    
    // Line is of form: 	head_word <TAB> syntactic-ngrams <TAB> total_count <TAB> counts_by_year <TAB>
    // Syntactic-ngrams list elements are separated by space, and are of form:	 word/pos-tag/dep-label/head-index
    
    
    public SyntacticNgramLine(String line) throws ParseException, IOException 
    {
    	String[] lineSplit = line.split("\\t");						// Split line by tabs

        headWord = new Text(Stemmer.stemWord(lineSplit[0])); 		// Set headWord as the 'Stemmed' word
        
        String[] synNgramsSplit = lineSplit[1].split("\\s+");		// Split syntactic ngrams by whitespace (every split is of form "word/pos-tag/dep-label/head-index")
        ngrams = new SyntacticNgram[synNgramsSplit.length];
        for (int i = 0; i < synNgramsSplit.length; i++) 
        {
        	String[] ngramSplits = synNgramsSplit[i].split("/");
        	if ((ngramSplits.length == 4) && (ngramSplits[0] != null) && (ngramSplits[1] != null) && (ngramSplits[2] != null) && (ngramSplits[3] != null))
        	{							
        		SyntacticNgram synNgram = new SyntacticNgram(ngramSplits[0], ngramSplits[1], ngramSplits[2], ngramSplits[3]);
            	ngrams[i] = synNgram;;
        	}	
        }
        total_count = Integer.parseInt(lineSplit[2]);
    }
    
    
    /***************	 Copy Constructor	 ***************/    
    
    
	public SyntacticNgramLine(SyntacticNgramLine other)
	{
		
        headWord = other.getheadWord();
        
        SyntacticNgram[] otherNgrams = other.copyNgrams();
        ngrams = new SyntacticNgram[otherNgrams.length];
        for (int i = 0; i < otherNgrams.length; i++) 
        {
        	ngrams[i] = new SyntacticNgram(otherNgrams[i]);
        }
        
        total_count = other.total_count;
    }
    
    
    
    /***************	 Read & Write fields methods of Syntactic Ngram object	 ***************/
    
    
    public void readFields(DataInput in) throws IOException 
    {
        headWord = new Text(in.readUTF());
        
        int ngramsSize = in.readInt();					// First, read 'pushed' size of ngrams array
        ngrams = new SyntacticNgram[ngramsSize];
        
        for (int i = 0; i < ngramsSize; i++) {
        	SyntacticNgram syntNgram = new SyntacticNgram();
        	syntNgram.readFields(in);
        	ngrams[i] = syntNgram;
        }
        
        total_count = in.readInt();						
    }
    
    
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(headWord.toString());
        
        out.writeInt(ngrams.length); 				// 'Push' the size of the ngrams array in order to know how many elements to read
        for (SyntacticNgram syntNgram : ngrams) {
        	syntNgram.write(out);
        }
        
        out.writeInt(total_count);
    }
    
    
    
    /**********************************	 Getters  **********************************/

    
    public Text getheadWord() { return headWord; }
    public SyntacticNgram[] getNgrams() { return ngrams; }
    public int getTotalCount() { return total_count; }
    
    
    
    /**********************************	 Deep copy Ngrams  **********************************/

    
    public SyntacticNgram[] copyNgrams()
    {
    	SyntacticNgram[] copiedNgrams = new SyntacticNgram[this.ngrams.length];
    	for (int i = 0; i < this.ngrams.length; i++)
    	{
    		copiedNgrams[i] = new SyntacticNgram();
    		copiedNgrams[i].copyNgram(this.ngrams[i]);
    	}
    	return copiedNgrams;
    }

}