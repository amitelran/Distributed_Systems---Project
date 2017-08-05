package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

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
	
	
	
	
	/***************	 Read S3 output file data, and append  to ARFF file	 ***************/

	
	public static void readFromS3file_to_ARFF_File(String path, AmazonS3 s3, String bucketName, String fileKey) 
	{
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(path, true));			/* append = true */
			try 
			{
				S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, fileKey));
				BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
				String line;
				while((line = reader.readLine()) != null) 
				{
					writer.println(line);
				}
			}
			finally {
			    writer.close();
			}
		} 
		catch (IOException e) {
		   System.err.println("Error occured in append_data_ARFF_File\n");
		}
		return;
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
