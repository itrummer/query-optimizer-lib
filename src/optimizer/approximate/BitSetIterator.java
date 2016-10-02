package optimizer.approximate;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class BitSetIterator implements Iterator<BitSet> {

private int k;
private int[] indices;
private List<Integer> elements;
private boolean hasNext = true;

public BitSetIterator(BitSet set, int k) throws IllegalArgumentException {
	this.k = k;
	if (k>0) {
	    this.indices = new int[k];
	    for(int i=0; i<k; i++)
	        indices[i] = k-1-i;
	    elements = new ArrayList<Integer>();
	    for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i+1)) {
	    	elements.add(i);
	    }		
	}
}

public boolean hasNext() {
    return hasNext;
}

private int inc(int[] indices, int maxIndex, int depth) throws IllegalStateException {
    if(depth == indices.length) {
        throw new IllegalStateException("The End");
    }
    if(indices[depth] < maxIndex) {
        indices[depth] = indices[depth]+1;
    } else {
        indices[depth] = inc(indices, maxIndex-1, depth+1)+1;
    }
    return indices[depth];
}

private boolean inc() {
    try {
        inc(indices, elements.size() - 1, 0);
        return true;
    } catch (IllegalStateException e) {
        return false;
    }
}

public BitSet next() {
    BitSet result = new BitSet();
    if (k > 0) {
        for(int i=indices.length-1; i>=0; i--) {
            result.set(elements.get(indices[i]));
        }
        hasNext = inc();
    } else {
    	hasNext = false;
    }
    return result;
}

public void remove() {
    throw new UnsupportedOperationException();
}
}

