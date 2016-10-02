package util;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class MathUtil {
	/**
	 * Transforms an integer into a bit vector of the specified length.
	 * 
	 * @param value		value to represent by bit vector
	 * @param length	length of bit vector to generate
	 * @return			bit vector of given length representing given value
	 */
	public static boolean[] toBitVector(int value, int length) {
		boolean[] bitVector = new boolean[length];
		for (int i=0; i<length; ++i) {
			if ((value & (1 << i)) != 0) {
				bitVector[i] = true;
			}
		}
		return bitVector;
	}
	/**
	 * Parses a binary string describing a set of integers and returns the corresponding BitSet.
	 * 
	 * @param bitstring	a String containing zeros and ones
	 * @return			a BitSet that contains the integers at whose position the input string has ones
	 */
	public static BitSet getBitSet(final String bitstring) {
        return BitSet.valueOf(new long[] { Long.parseLong(bitstring, 2) });
    }
	/**
	 * Generates the Power set of the set containing indices from zero to maxIndex.
	 * 
	 * @param maxIndex	the highest index contained in the Power set
	 * @return			all possible subsets of a set containing entries from zero to maxIndex
	 */
	public static Set<BitSet> powerSet(final int maxIndex) {
		Set<BitSet> result = new HashSet<BitSet>();
		result.add(new BitSet());
		for (int i=0; i<=maxIndex; ++i) {
			Set<BitSet> newSubsets = new HashSet<BitSet>();
			for (BitSet oldSubset : result) {
				BitSet newSubset = new BitSet();
				newSubset.or(oldSubset);
				newSubset.set(i);
				newSubsets.add(newSubset);
			}
			result.addAll(newSubsets);
		}
		return result;
	}
	
	// Calculates the number of entries that are set to true.
	public static int nrTrueValues(boolean[] v) {
		int nrTrueEntries = 0;
		int nrEntries = v.length;
		for (int i=0; i<nrEntries; ++i) {
			if (v[i]) {
				++nrTrueEntries;
			}
		}
		return nrTrueEntries;
	}
	// Sum vector components
	public static double aggSum(double[] values) {
		int nrValues = values.length;
		double sum = 0;
		for (int i=0; i<nrValues; ++i) {
			sum += values[i];
		}
		return sum;
	}
	// Calculate average value
	public static double aggMean(double[] values) {
		int nrValues = values.length;
		return aggSum(values)/nrValues;
	}
	// Calculate median value
	public static double aggMedian(double[] values) {
		int nrValues = values.length;
		Arrays.sort(values);
		if (nrValues % 2 == 0) {
			return (values[nrValues/2-1]+values[nrValues/2])/2.0; 
		} else {
			return values[nrValues/2];
		}
	}
	// Calculate variance
	public static double aggVariance(double[] values) {
		int nrValues = values.length;
		double mean = aggMean(values);
		double accDeviation = 0;
		for (double v : values) {
			accDeviation += (v-mean) * (v-mean);
		}
		return accDeviation / nrValues;
	}
	// Calculate standard deviation
	public static double aggStDev(double[] values) {
		return Math.sqrt(aggVariance(values));
	}
	// calculates logarithm for a custom base value
	public static double logOfBase(double base, double value) {
	    return Math.log(value)/Math.log(base);
	}
	// returns minimum value in vector for integer input
	public static int aggVectorMin(int[] inputVector) {
		int nrComponents = inputVector.length;
		int minValue = Integer.MAX_VALUE;
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			minValue = Math.min(minValue, inputVector[componentCtr]);
		}
		return minValue;
	}
	// returns minimum value in vector for double input
	public static double aggVectorMin(double[] inputVector) {
		int nrComponents = inputVector.length;
		double minValue = Double.POSITIVE_INFINITY;
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			minValue = Math.min(minValue, inputVector[componentCtr]);
		}
		return minValue;
	}
	// returns minimum value in vector for integer input
	public static int aggVectorMax(int[] inputVector) {
		int nrComponents = inputVector.length;
		int maxValue = Integer.MIN_VALUE;
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			maxValue = Math.max(maxValue, inputVector[componentCtr]);
		}
		return maxValue;
	}
	// returns minimum value in vector for double input
	public static double aggVectorMax(double[] inputVector) {
		int nrComponents = inputVector.length;
		double maxValue = Double.NEGATIVE_INFINITY;
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			maxValue = Math.max(maxValue, inputVector[componentCtr]);
		}
		return maxValue;
	}
	// returns vector of ceil values
	public static int[] vectorCeil(double[] inputVector) {
		int nrComponents = inputVector.length;
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = (int)Math.ceil(inputVector[componentCtr]);
		}
		return outputVector;
	}
	// returns vector of floor values
	public static int[] vectorFloor(double[] inputVector) {
		int nrComponents = inputVector.length;
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = (int)Math.floor(inputVector[componentCtr]);
		}
		return outputVector;
	}
	// returns new vector multiplying each component with a constant
	public static int[] vectorMult(int[] inputVector, int c) {
		int nrComponents = inputVector.length;
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = c * inputVector[componentCtr];
		}
		return outputVector;
	}
	// returns integer part of division result
	public static int[] vectorDiv(int[] inputVector, int c) {
		int nrComponents = inputVector.length;
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = inputVector[componentCtr]/c;
		}
		return outputVector;
	}
	// Returns integer remainder of division.
	public static int[] vectorMod(int[] inputVector, int c) {
		int nrComponents = inputVector.length;
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = inputVector[componentCtr] % c;
		}
		return outputVector;
	}
	// treats input vector as exponents and returns vector of powers using the specified base
	public static double[] powerVector(double base, int[] exponents) {
		int nrComponents = exponents.length;
		double[] outputVector = new double[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = Math.pow(base, exponents[componentCtr]);
		}
		return outputVector;
	}
	// returns vector of logarithms relative to the specified base
	public static double[] logVector(double base, double[] values) {
		int nrComponents = values.length;
		double[] outputVector = new double[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = MathUtil.logOfBase(base, values[componentCtr]);
		}
		return outputVector;
	}
	// scalar product between vectors
	public static long scalarProduct(long[] vector1, int[] vector2) {
		int nrComponents = vector1.length;
		assert(nrComponents == vector2.length);
		long result = 0;
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			result += vector1[componentCtr] * vector2[componentCtr];
		}
		return result;
	}
	// returns new vector adding second vector to first
	public static int[] vectorAdd(int[] baseVector, int[] addedVector) {
		int nrComponents = baseVector.length;
		assert(nrComponents == addedVector.length);
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = baseVector[componentCtr] + addedVector[componentCtr];
		}
		return outputVector;
	}
	// returns new vector adding second vector to first
	public static double[] vectorAdd(double[] firstVector, double[] secondVector) {
		int nrComponents = firstVector.length;
		assert(nrComponents == secondVector.length);
		double[] outputVector = new double[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = firstVector[componentCtr] + secondVector[componentCtr];
		}
		return outputVector;
	}
	// returns new vector subtracting second vector from first
	public static int[] vectorSubtract(int[] baseVector, int[] subtractedVector) {
		int nrComponents = baseVector.length;
		assert(nrComponents == subtractedVector.length);
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = baseVector[componentCtr] - subtractedVector[componentCtr];
		}
		return outputVector;
	}
	// returns component-wise minimum vector
	public static int[] vectorMin(int[] inputVector1, int[] inputVector2) {
		int nrComponents = inputVector1.length;
		assert(nrComponents == inputVector2.length);
		int[] outputVector = new int[nrComponents];
		for (int componentCtr=0; componentCtr<nrComponents; ++componentCtr) {
			outputVector[componentCtr] = Math.min(inputVector1[componentCtr], inputVector2[componentCtr]);
		}
		return outputVector;
	}
	// concatenates two vectors
	public static int[] vectorConcatenate(int[] inputVector1, int[] inputVector2) {
		int nrComponents1 = inputVector1.length;
		int nrComponents2 = inputVector2.length;
		int[] outputVector = new int[nrComponents1 + nrComponents2];
		for (int componentCtr=0; componentCtr<nrComponents1; ++componentCtr) {
			outputVector[componentCtr] = inputVector1[componentCtr];
		}
		for (int componentCtr=0; componentCtr<nrComponents2; ++componentCtr) {
			outputVector[nrComponents1 + componentCtr] = inputVector2[componentCtr];
		}
		return outputVector;
	}
	// multiply matrix by a scalar and return new matrix
	public static double[][] matrixMultiple(double[][] matrix, double scalar) {
		int nrRows = matrix.length;
		int nrColons = matrix[0].length;
		double[][] product = new double[nrRows][nrColons];
		for (int i=0; i<nrRows; ++i) {
			for (int j=0; j<nrColons; ++j) {
				product[i][j] = scalar * matrix[i][j];
			}
		}
		return product;
	}
}
