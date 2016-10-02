package optimizer.approximate;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class BitSetIteratorTest {

	@Test
	public void test() {
		// test with small set numbers
		{
			BitSet set = new BitSet();
			set.set(1);
			set.set(3);
			set.set(4);
			BitSetIterator iter = new BitSetIterator(set, 1);
			// generate list of result BitSets
			List<BitSet> setsToCover = new ArrayList<BitSet>();
			for (int i : new int[] {1, 3, 4}) {
				BitSet resultSet = new BitSet();
				resultSet.set(i);
				setsToCover.add(resultSet);
			}
			// test whether iterator covers all sets
			int iterationCtr = 0;
			while (iter.hasNext()) {
				++iterationCtr;
				BitSet next = iter.next();
				Iterator<BitSet> coverIter = setsToCover.iterator();
				while (coverIter.hasNext()) {
					BitSet xorSet 		= (BitSet)next.clone();
					BitSet setToCover 	= coverIter.next();
					xorSet.xor(setToCover);
					if (xorSet.isEmpty()) {
						coverIter.remove();
					}
				}
			}
			assertEquals(iterationCtr, 3);
			assertTrue(setsToCover.isEmpty());			
		}
		// test with large number of sets
		{
			BitSet set = new BitSet();
			set.set(1);
			set.set(3);
			set.set(4);
			BitSetIterator iter = new BitSetIterator(set, 2);
			// generate list of result BitSets
			List<BitSet> setsToCover = new ArrayList<BitSet>();
			for (int i : new int[] {1, 3, 4}) {
				for (int j : new int[] {1, 3, 4}) {
					if (i != j) {
						BitSet resultSet = new BitSet();
						resultSet.set(i);
						resultSet.set(j);
						setsToCover.add(resultSet);							
					}
				}
			}
			// test whether iterator covers all sets
			int iterationCtr = 0;
			while (iter.hasNext()) {
				++iterationCtr;
				BitSet next = iter.next();
				Iterator<BitSet> coverIter = setsToCover.iterator();
				while (coverIter.hasNext()) {
					BitSet xorSet 		= (BitSet)next.clone();
					BitSet setToCover 	= coverIter.next();
					xorSet.xor(setToCover);
					if (xorSet.isEmpty()) {
						coverIter.remove();
					}
				}
			}
			assertEquals(iterationCtr, 3);
			assertTrue(setsToCover.isEmpty());
		}
	}

}
