package util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static util.TestUtil.*;

public class MathUtilTest {

	@Test
	public void test() {
		// Test transformation to bit vector
		{
			boolean[] e = new boolean[] {true, false, true};
			assertEquals(Arrays.toString(e), Arrays.toString(MathUtil.toBitVector(5, 3)));
		}
		{
			boolean[] e = new boolean[] {true, false, true, true};
			assertEquals(Arrays.toString(e), Arrays.toString(MathUtil.toBitVector(13, 4)));
		}
		// Generation of BitSet from bit string
		{
			String bitstring = "1001";
			BitSet expected = new BitSet();
			expected.set(0);
			expected.set(3);
			assertEquals(expected, MathUtil.getBitSet(bitstring));
		}
		{
			String bitstring = "0100";
			BitSet expected = new BitSet();
			expected.set(2);
			assertEquals(expected, MathUtil.getBitSet(bitstring));
		}
		// Generation of Power set
		{
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("0"),
					MathUtil.getBitSet("1"),
			}));
			assertEquals(expectedSets, MathUtil.powerSet(0));
		}
		{
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("00"),
					MathUtil.getBitSet("01"),
					MathUtil.getBitSet("10"),
					MathUtil.getBitSet("11"),
			}));
			assertEquals(expectedSets, MathUtil.powerSet(1));
		}
		{
			Set<BitSet> expectedSets = new HashSet<BitSet>();
			expectedSets.addAll(Arrays.asList(new BitSet[] {
					MathUtil.getBitSet("000"),
					MathUtil.getBitSet("001"),
					MathUtil.getBitSet("010"),
					MathUtil.getBitSet("100"),
					MathUtil.getBitSet("101"),
					MathUtil.getBitSet("110"),
					MathUtil.getBitSet("101"),
					MathUtil.getBitSet("011"),
					MathUtil.getBitSet("111"),
			}));
			assertEquals(expectedSets, MathUtil.powerSet(2));
		}
		// test counting true entries
		{
			boolean[] v = new boolean[] {true, false, false, true, true};
			assertEquals(3, MathUtil.nrTrueValues(v));
		}
		// Test summing up vectors
		{
			double[] v = new double[] {1, 2.3, 4};
			assertEquals(7.3, MathUtil.aggSum(v), EPSILON);
		}
		// Test averaging vectors
		{
			double[] v = new double[] {1, 2.3, 3};
			assertEquals(2.1, MathUtil.aggMean(v), EPSILON);
		}
		// Test calculating the median
		{
			// For vectors with even number of entries
			{
				double[] v = new double[] {5, 1, 2, 3};
				assertEquals(2.5, MathUtil.aggMedian(v), EPSILON);
			}
			// For vectors with odd number of entries
			{
				double[] v = new double[] {5, 1, 2};
				assertEquals(2, MathUtil.aggMedian(v), EPSILON);
			}
			
		}
		// Test calculating variance
		{
			double[] v = new double[] {1, 2, 6};
			assertEquals((4+1+9)/3.0, MathUtil.aggVariance(v), EPSILON);
		}
		// Test calculating standard deviation
		{
			double[] v = new double[] {1, 2, 6};
			assertEquals(Math.sqrt((4+1+9)/3.0), MathUtil.aggStDev(v), EPSILON);
		}
		// test logarithm
		{
			assertEquals(MathUtil.logOfBase(4, 16), 2, EPSILON);
			assertEquals(MathUtil.logOfBase(10, 0.01), -2, EPSILON);
			assertEquals(Double.POSITIVE_INFINITY, 
					MathUtil.logOfBase(4, Double.POSITIVE_INFINITY),
					EPSILON);
		}
		// aggregate minimum
		{
			int[] i = new int[] {7, 2, 5, 7};
			int er = 2;
			int r = MathUtil.aggVectorMin(i);
			assertEquals(er, r);
		}
		// aggregate minimum
		{
			int[] i = new int[] {4, 7, 2, 5, 7};
			int er = 7;
			int r = MathUtil.aggVectorMax(i);
			assertEquals(er, r);
		}
		// vector ceil
		{
			double[] i = new double[] {2.2, 3};
			int[] er = new int[] {3, 3};
			int[] r = MathUtil.vectorCeil(i);
			assertArrayEquals(er, r);
		}
		// vector floor
		{
			double[] i = new double[] {2.2, 3};
			int[] er = new int[] {2, 3};
			int[] r = MathUtil.vectorFloor(i);
			assertArrayEquals(er, r);
		}
		// vector multiplication
		{
			int[] v1 = new int[] {2, 3};
			int[] v2 = MathUtil.vectorMult(v1, 2);
			assertEquals(v2[0], 4);
		}
		// vector division
		{
			int[] i1 = new int[] {2, 3};
			int[] r = MathUtil.vectorDiv(i1, 3);
			int[] er = new int[] {0, 1};
			assertArrayEquals(er, r);
		}
		// vector mod
		{
			int[] i1 = new int[] {4, 8, 4, 3};
			int[] er = new int[] {1, 2, 1, 0};
			int[] r = MathUtil.vectorMod(i1, 3);
			assertArrayEquals(er, r);
		}
		// power vectors
		{
			int[] input 			= new int[] {2, 3};
			double[] result 		= MathUtil.powerVector(10, input);
			double[] expectedResult = new double[] {100, 1000};
			assertArrayEquals(expectedResult, result, EPSILON);
		}
		// logarithm vectors
		{
			double[] input 			= new double[] {4, 16};
			double[] result 		= MathUtil.logVector(2, input);
			double[] expectedResult = new double[] {2, 4};
			assertArrayEquals(expectedResult, result, EPSILON);
		}
		// scalar product
		{
			long[] i1 	= new long[] {3, 5};
			int[] i2 	= new int[] {2, 1};
			long r 		= MathUtil.scalarProduct(i1, i2);
			long er 	= 11;
			assertEquals(er, r);
		}
		// vector addition
		{
			int[] v1 = new int[] {3, 5};
			int[] v2 = new int[] {2, 1};
			int[] v3 = MathUtil.vectorAdd(v1, v2);
			assertEquals(v3[0], 5);
			assertEquals(v3[1], 6);
		}
		// vector subtraction
		{
			int[] v1 = new int[] {3, 5};
			int[] v2 = new int[] {2, 1};
			int[] v3 = MathUtil.vectorSubtract(v1, v2);
			assertEquals(v3[0], 1);
			assertEquals(v3[1], 4);
		}
		// vector minimum
		{
			int[] i1 = new int[] {1, 3, 2};
			int[] i2 = new int[] {4, 2, 0};
			int[] er = new int[] {1, 2, 0};
			int[] r = MathUtil.vectorMin(i1, i2);
			assertArrayEquals(er, r);
		}
		// vectors concatenation
		{
			int[] v1 = new int[] {2, 3};
			int[] v2 = new int[] {4, 6, 7};
			int[] v3 = MathUtil.vectorConcatenate(v1, v2);
			assertEquals(v3[0], 2);
			assertEquals(v3[2], 4);
			assertEquals(v3[4], 7);
			assertEquals(v3.length, 5);
		}
	}

}
