package semanticSimilarity;

import java.io.IOException;
import java.io.PrintWriter;

public class Weka {
	
	
	
	/***************	 Create ARFF file for WEKA	 ***************/

	
	public static String create_ARFF_File(String path) 
	{
		try {
		    PrintWriter writer = new PrintWriter(path, "UTF-8");
		    create_ARFF_Header(writer);
		    writer.close();
		} 
		catch (IOException e) {
		   System.err.println("Error occured while creating Weka file\n");
		}
		return path;
	}
	
	
	
	/***************	 Append data to ARFF file	 ***************/

	
	public static void append_data_ARFF_File(String path, ) 
	{
		try {
		    PrintWriter writer = new PrintWriter(path, "UTF-8");
		    writer.close();
		} 
		catch (IOException e) {
		   System.err.println("Error occured while creating Weka file\n");
		}
		return path;
	}
	
	
	
    /***************	 Add ARFF header for created file  ***************/

	
	public static void create_ARFF_Header(PrintWriter writer)
	{
		writer.println("% Distributed Systems Programming: Project");
		writer.println("% Creators: Yoav Beeri, Amit Elran");
		writer.println();		

		writer.println("@RELATION biarcs");
		writer.println();

		writer.println("@ATTRIBUTE word1	STRING");
		writer.println("@ATTRIBUTE word2	STRING");
		writer.println("@ATTRIBUTE similar	{true,false}");
		
		/* Raw Frequency Measures */
		writer.println("@ATTRIBUTE raw_freq_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_cosine	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_dice	NUMERIC");
		
		/* Relative Frequency Measures */
		writer.println("@ATTRIBUTE rel_freq_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_cosine	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_dice	NUMERIC");
		
		/* PMI Measures */
		writer.println("@ATTRIBUTE pmi_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE pmi_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE pmi_cosine	NUMERIC");
		writer.println("@ATTRIBUTE pmi_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE pmi_dice	NUMERIC");
		
		/* T-test Measures */
		writer.println("@ATTRIBUTE t_test_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE t_test_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE t_test_cosine	NUMERIC");
		writer.println("@ATTRIBUTE t_test_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE t_test_dice	NUMERIC");
		
		writer.println();
		writer.println("@DATA");
	}

}
