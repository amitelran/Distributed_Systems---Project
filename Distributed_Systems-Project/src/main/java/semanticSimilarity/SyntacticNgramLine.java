package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable; 
 

public class SyntacticNgramLine implements Writable {
        
    protected Text headWord;															// The word which is the syntactic tree's root
    protected SyntacticNgram[] ngrams;													// Array of all syntactic ngrams in given line
    protected int total_count;															// Total counts as summed in the years
    protected List<Pair<Integer, Integer>> counts_by_year = new LinkedList<Pair<Integer, Integer>>();				// Each element in the list is of form: <year, counts for year>
    

    public SyntacticNgramLine(){}
    
    
    
    /***************	 Constructor - receives line and parses into Syntactic Ngram line object	 ***************/
    
    // Line is of form: 	head_word <TAB> syntactic-ngrams <TAB> total_count <TAB> counts_by_year <TAB>
    // Syntactic-ngrams list elements are separated by space, and are of form:	 word/pos-tag/dep-label/head-index
    
    
    public SyntacticNgramLine(String line) throws ParseException {
    	
    	String[] lineSplit = line.split("\\t");						// Split line by tabs
    	for (int i = 0; i < lineSplit.length; i++) {
    		System.out.println(lineSplit[i]);
    	}
    	
        headWord = new Text(lineSplit[0]); 
        
        String[] synNgramsSplit = lineSplit[1].split("\\s+");		// Split syntactic ngrams by whitespace (every split is of form "word/pos-tag/dep-label/head-index")
        ngrams = new SyntacticNgram[synNgramsSplit.length];
        for (int i = 0; i < synNgramsSplit.length; i++) {
        	SyntacticNgram synNgram = new SyntacticNgram(synNgramsSplit[i]);
        	ngrams[i] = synNgram;
        }
        
        total_count = Integer.parseInt(lineSplit[2]);
        
        String[] countsByYearSplit = lineSplit[3].split("\\s+");
        for (int i = 0; i < countsByYearSplit.length; i++) {
        	String[] pairSplit = countsByYearSplit[i].split(",");
        	Pair<Integer, Integer> countByYearPair = new Pair<Integer, Integer>(Integer.parseInt(pairSplit[0]), Integer.parseInt(pairSplit[1]));
        	countByYearPair.printPair();
        	counts_by_year.add(countByYearPair);
        }

    }
    
    
    /***************	 Copy Constructor	 ***************/    
    
    
	public SyntacticNgramLine(SyntacticNgramLine other) {
		
        headWord = other.headWord;
        
        SyntacticNgram[] otherNgrams = other.getNgrams();
        ngrams = new SyntacticNgram[otherNgrams.length];
        for (int i = 0; i < otherNgrams.length; i++) {
        	ngrams[i] = otherNgrams[i];
        }
        
        total_count = other.total_count;
        
        /* Deep copy Pairs list */
        
        List<Pair<Integer, Integer>> new_counts_by_year = new LinkedList<Pair<Integer, Integer>>();	
        for (Pair<Integer, Integer> pair : other.counts_by_year) {
        	new_counts_by_year.add(new Pair<Integer, Integer>(pair));
        }
        counts_by_year = new_counts_by_year;
    }
    
    
    
    /***************	 Read & Write fields methods of Syntactic Ngram object	 ***************/
    
    
    public void readFields(DataInput in) throws IOException {
        headWord = new Text(in.readUTF());
        
        int ngramsSize = in.readInt();					// First, read 'pushed' size of ngrams array
        ngrams = new SyntacticNgram[ngramsSize];
        
        for (int i = 0; i < ngramsSize; i++) {
        	SyntacticNgram syntNgram = new SyntacticNgram();
        	syntNgram.readFields(in);
        	ngrams[i] = syntNgram;
        }
        
        total_count = in.readInt();						
        
        int countsByYearSize = in.readInt();			// First, read 'pushed' size of total counts list
        List<Pair<Integer, Integer>> countsByYearList = new LinkedList<Pair<Integer, Integer>>();
        for (int i = 0; i < countsByYearSize; i++) {
        	int year = in.readInt();
        	int count = in.readInt();
        	Pair<Integer, Integer> yearPair = new Pair<Integer, Integer>(year, count);
        	countsByYearList.add(yearPair);
        }
        counts_by_year = countsByYearList;
    }
    
    
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(headWord.toString());
        
        out.writeInt(ngrams.length); 				// 'Push' the size of the ngrams array in order to know how many elements to read
        for (SyntacticNgram syntNgram : ngrams) {
        	syntNgram.write(out);
        }
        
        out.writeInt(total_count);
        
        out.writeInt(counts_by_year.size()); 		// 'Push' the size of the linked list in order to know how many elements to read
        for (Pair<Integer, Integer> yearPair : counts_by_year) {
        	out.writeInt(yearPair.getFirst());
        	out.writeInt(yearPair.getSecond());
        }
    }
    
    
    
    /**********************************	 Getters  **********************************/

    
    public Text getheadWord() { return headWord; }
    public SyntacticNgram[] getNgrams() { return ngrams; }
    public int getTotalCount() { return total_count; }
    public List<Pair<Integer, Integer>> getCountsByYear() { return counts_by_year; }
    

}