package semanticSimilarity;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;


public class ExcpetionPrint {
	
    public static void printErrorACE(AmazonClientException ace)
    {
        System.out.println("Caught an AmazonClientException, which means the client encountered " +
                "a serious internal problem while trying to communicate with SQS, such as not " +
                "being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }
    
    
    public static void printErrorASE(AmazonServiceException ase)
    {
    	System.out.println("Caught an AmazonServiceException, which means your request made it " +
        		"to Amazon SQS, but was rejected with an error response for some reason.");
        	System.out.println("Error Message:    " + ase.getMessage());
        	System.out.println("HTTP Status Code: " + ase.getStatusCode());
        	System.out.println("AWS Error Code:   " + ase.getErrorCode());
        	System.out.println("Error Type:       " + ase.getErrorType());
        	System.out.println("Request ID:       " + ase.getRequestId());
    }
}
