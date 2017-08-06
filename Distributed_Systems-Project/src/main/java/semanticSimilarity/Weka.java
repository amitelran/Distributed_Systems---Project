package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class Weka {
	
	
	
	/***************	 Perform WEKA evaluation	  ***************/

	
	public static void evaluateWEKA(String path, String outputPath) throws Exception 
	{
		BufferedReader buffReader = null;
		int folds = 10;
		try 
		{
			buffReader = new BufferedReader(new FileReader(path));		// Read the vectors similarities file
			Instances data = new Instances(buffReader);					// Instances will be the data lines from the file
			if (data.numInstances() < folds) 
			{
				folds = data.numInstances();							// Cannot have more folds than instances
			}
			data.setClassIndex(data.numAttributes() - 1);				// This means we are setting the 'similar' attribute (placed last in attributes)
			
			buffReader.close();			// Finished with buffered reader
			
			RandomForest randForest = new RandomForest();						// Set Random-Forest classifier (classifier doesn't matter)
			Evaluation eval = new Evaluation(data);								// We are setting  the data file to the Evaluation
			eval.crossValidateModel(randForest, data, folds, new Random(1));	// <classifier, data file, , 10-fold cross-validation, random number generator for randomization>
			
			System.out.println(eval.toSummaryString(("\nWeka Results\n===========\n"), true));
			System.out.println(eval.fMeasure(1) + " " + eval.precision(1) + " " + eval.recall(1));		// F1 measure, precision, recall stats
			writeResultsToFile("C:\\Users\\Amir\\Desktop\\yoav_amit_weka_results.txt", eval);
		}
		
		catch (FileNotFoundException e1) 
		{
			System.err.println("Error occured in evaluateWEKA\n");
			e1.printStackTrace();
		}
		return;
	}
	
	
	
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

	
	public static List<PairWritable> readFromS3file_to_ARFF_File(String path, AmazonS3 s3, String bucketName, String fileKey) 
	{
		List<PairWritable> pairList = new LinkedList<PairWritable>();		// List to store lexemes pair for latter use
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(path, true));			/* append = true */
			try 
			{
				List<String> arffLines = new LinkedList<String>();					// Generate new lines withot string attributes
				
				S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, fileKey));
				BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
				String line;
				
				
				/* Read line by line from S3 file, but put aside the lexemes pair (strings), and use them after the classification */
				
				while((line = reader.readLine()) != null) 
				{
					String[] splitLine = line.split(",");								// Split line to get out the lexemes
					pairList.add(new PairWritable(splitLine[0], splitLine[1])); 		// Store both lexemes from pair
					
					String newLine = "";
					for (int i = 2; i < (splitLine.length - 1); i++)					// Accumulate all elements to a single line, seperated by commas
					{
						newLine += splitLine[i] + ",";
					}
					newLine += splitLine[splitLine.length - 1];		// Add last element without a comma
					arffLines.add(newLine);
				}
				
				/* Write lines without the lexemes string within them */
				
				ListIterator<String> listIter = arffLines.listIterator();
				while (listIter.hasNext())
				{
					writer.println(listIter.next());
				}

			}
			finally {
			    writer.close();
			}
		} 
		catch (IOException e) {
		   System.err.println("Error occured in append_data_ARFF_File\n");
		}
		return pairList;
	}
	
	
	
    /***************	 Add ARFF header for created file  ***************/

	
	public static void create_ARFF_Header(PrintWriter writer)
	{
		writer.println("% Distributed Systems Programming: Project");
		writer.println("% Creators: Yoav Beeri, Amit Elran");
		writer.println();		

		writer.println("@RELATION biarcs");
		writer.println();

		//writer.println("@ATTRIBUTE word1	STRING");
		//writer.println("@ATTRIBUTE word2	STRING");
		
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
		
		writer.println("@ATTRIBUTE similar	{true,false}");
		
		writer.println();
		writer.println("@DATA");
	}
	
	
	
	/***************	 Write WEKA results to output file	 
	 * @throws Exception ***************/

	
	public static void writeResultsToFile(String outputPath, Evaluation eval) throws Exception 
	{
		try {
		    PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
		    writer.println("Yoav Beeri & Amit Elran - Distributed Systems Programming Project");
		    writer.println((eval.toSummaryString(("\nWeka Results\n========================\n"), true)));
		    writer.println("F1 Measure: " + eval.fMeasure(1));
		    writer.println("Precision: " + eval.precision(1));
		    writer.println("Recall: " + eval.recall(1));
		    writer.println();
		    writer.println("True-Positive rate: " + eval.truePositiveRate(1));
		    writer.println("False-Positive rate: " + eval.falsePositiveRate(1));
		    writer.println("True-Negative rate: " + eval.trueNegativeRate(1));
		    writer.println("False-Negative rate: " + eval.falseNegativeRate(1));
		    writer.println();
		    writer.println(eval.toMatrixString());
		    writer.println("========================");
		    writer.close();
		} 
		catch (IOException e) {
		   System.err.println("Error occured while writing Weka results to output file\n");
		}
		return;
	}

}
