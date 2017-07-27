package semanticSimilarity;


/*********** 	Generic Pair class for different uses	 ***********/

public class Pair<F, S> {

	protected F first; 			// First member of pair
	protected S second; 			// Second member of pair
	
	public Pair(){}

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }
    
    public Pair(Pair<F,S> other) {
    	this.first = other.getFirst();
    	this.second = other.getSecond();
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }
    
    public void printPair() {
    	System.out.println("First: " + this.first + " Second: " + this.second);
    }
    
    
}
