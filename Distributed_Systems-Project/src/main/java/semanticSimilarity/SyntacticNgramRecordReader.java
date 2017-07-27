package semanticSimilarity;


import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
 

/**
 * RecordReader reads <key, value> pairs from an InputSplit.
 * 
 * RecordReader, typically, converts the byte-oriented view of the input, provided by the InputSplit, 
 * and presents a record-oriented view for the Mapper and Reducer tasks for processing. 
 * It thus assumes the responsibility of processing record boundaries and presenting the tasks with keys and values.
 * 
 * Reference to help understand : https://github.com/alexholmes/hadoop-book/blob/master/src/main/java/com/manning/hip/ch3/json/JsonInputFormat.java
 */

public class SyntacticNgramRecordReader extends RecordReader<String, SyntacticNgramLine> { 
 
	// LineRecordReader extracts keys and values from a given text file split, 
	// where the keys are the positions of the lines and the values are the content of the line
	
    LineRecordReader reader;
    String headWord;
    SyntacticNgramLine ngram;
        
    
    
    /***************************	 Constructor 	***************************/
    
    
    SyntacticNgramRecordReader() {
        reader = new LineRecordReader(); 
    }
    
    
    
    /** Called once at initialization
     * @param split: 
     * 		InputSplit represents the data to be processed by an individual Mapper.
     * 		Typically, it presents a byte-oriented view on the input and is the responsibility of RecordReader of
     * 		the job to process this and present a record-oriented view.
     * @param context:
     * 		The context for task attempts.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }
 
    
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while(reader.nextKeyValue()){
        	try{
        		ngram = new SyntacticNgramLine(reader.getCurrentValue().toString());
        		headWord = ngram.headWord;
        		return true;
        	}
        	catch(Exception e){
        		continue;
        	}
        }
        return false;
    }
    
     
    
    // Return key of mapping by reading a line and setting the head word as the key
    // The reader reads the current value/line (which is a Text object)
    
    @Override
    public String getCurrentKey() {
    	return headWord;
    }
    
    
    // Return value of mapping by reading a line and constructing a SyntacticNgramLine object
    // The reader reads the current value/line (which is a Text object), parses it to String, and creates a new parsed Syntactic ngram object.
   
    @Override
    public SyntacticNgramLine getCurrentValue() throws IOException, InterruptedException {
        return ngram;
    }
    
    
    // Get the progress within the split
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
    
    

    // Close reader
    @Override
    public void close() throws IOException {
        reader.close();        
    }
    
}

