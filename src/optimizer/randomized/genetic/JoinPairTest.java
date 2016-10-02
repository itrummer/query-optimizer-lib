package optimizer.randomized.genetic;

import static org.junit.Assert.*;

import org.junit.Test;

public class JoinPairTest {

	@Test
	public void test() {
		// Test equality check
		{
			// Each join pair equals itself
			JoinPair joinPair = new JoinPair(0, 10, 20);
			assertTrue(joinPair.equals(joinPair));
		}
		{
			// Two join pairs are not equal if their left operand differs
			JoinPair joinPair1 = new JoinPair(0, 10, 20);
			JoinPair joinPair2 = new JoinPair(1, 10, 20);
			assertFalse(joinPair1.equals(joinPair2));
			assertFalse(joinPair2.equals(joinPair1));
		}
		{
			// Two join pairs are not equal if their right operand differs
			JoinPair joinPair1 = new JoinPair(0, 10, 20);
			JoinPair joinPair2 = new JoinPair(0, 11, 20);
			assertFalse(joinPair1.equals(joinPair2));
			assertFalse(joinPair2.equals(joinPair1));
		}
		{
			// Two join pairs are not equal if their preferred operator differs
			JoinPair joinPair1 = new JoinPair(0, 10, 20);
			JoinPair joinPair2 = new JoinPair(0, 10, 21);
			assertFalse(joinPair1.equals(joinPair2));
			assertFalse(joinPair2.equals(joinPair1));
		}
		{
			// Otherwise, the join pairs are equivalent
			JoinPair joinPair1 = new JoinPair(0, 10, 20);
			JoinPair joinPair2 = new JoinPair(0, 10, 20);
			assertTrue(joinPair1.equals(joinPair2));
			assertTrue(joinPair2.equals(joinPair1));
		}
	}

}
