
package benchmark;

import util.MathUtil;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Collects and displays statistics about the benchmarked optimization algorithms. This class
 * maintains counters for features of type <code>long</code> and <code>double</code>. For the
 * same feature, different counters are stored for different query sizes, algorithms, and 
 * optimization time periods. This allows to compare different algorithms and to study the
 * evolution of feature average values over different query sizes and optimization time periods.
 * 
 * @author immanueltrummer
 *
 */
public class Statistics {
	/**
	 * Whether counters can be increased or not by calling the appropriate methods
	 */
	static boolean enabled = true;
	/**
	 * The number of benchmarked algorithms for which to create counters
	 */
	static int nrAlgorithms = -1;
	/**
	 * The number of different query sizes for which to create counters
	 */
	static int nrQuerySizes = -1;
	/**
	 * The number of optimization time periods for which to create counters
	 */
	static int nrPeriods = -1;
	/**
	 * The number of test queries per query size.
	 */
	static int nrQueriesPerSize = -1;
	/**
	 * Width of result table entries if they are not separated by tabs
	 */
	final static int nrSpaces = 30;
	/**
	 * Map from long feature names to corresponding counters per algorithm, period, 
	 * query size, and query.
	 */
	static Map<String, long[][][][]> longFeatures = new TreeMap<String, long[][][][]>();
	/**
	 * Map from double feature names to corresponding counters per algorithm, period, 
	 * query size, and query.
	 */
	static Map<String, double[][][][]> doubleFeatures = new TreeMap<String, double[][][][]>();
	/**
	 * Statistics can be updated after calling that method.
	 */
	public static void enable() {
		enabled = true;
	}
	/**
	 * Statistics cannot be updated after calling this method.
	 */
	public static void disable() {
		enabled = false;
	}
	/**
	 * Initializes statistic and must be called before any statistics are collected.
	 *  
	 * @param nrAlgorithms		the number of benchmarked optimization algorithms
	 * @param nrQuerySizes		the number of benchmarked query sizes
	 * @param nrPeriods			the number of time periods into which optimization is partitioned
	 * @param nrQueriesPerSize	the number of test queries with the same query size
	 */
	public static void init(int nrAlgorithms, int nrQuerySizes, int nrPeriods, int nrQueriesPerSize) {
		Statistics.nrAlgorithms = nrAlgorithms;
		Statistics.nrQuerySizes = nrQuerySizes;
		Statistics.nrPeriods = nrPeriods;
		Statistics.nrQueriesPerSize = nrQueriesPerSize;
		longFeatures.clear();
		doubleFeatures.clear();
	}
	/**
	 * Keeps track of how often each algorithm increased the counter for specific features.
	 * 
	 * @param featureName	the name of the feature
	 * @param algIndex		the index of the algorithm that increased the counter
	 * @param querySize		the query size for which the counter was increased
	 * @param period		the optimization time period during which the counter was increased
	 * @param queryIndex	query index in its query size class
	 */
	static void countFeatureAddition(String featureName, int algIndex, int querySize, 
			int period, int queryIndex) {
		String nrAddedFeatureName = "#Additions for \"" + featureName + "\"";
		if (longFeatures.get(nrAddedFeatureName) == null) {
			longFeatures.put(nrAddedFeatureName, 
					new long[nrQuerySizes][nrAlgorithms][nrPeriods][nrQueriesPerSize]);
		}
		long[][][][] featureValues = longFeatures.get(nrAddedFeatureName);
		featureValues[querySize][algIndex][period][queryIndex] += 1;
	}
	/**
	 * Increases the counter for a feature of type <code>long</code> if statistics are enabled.
	 * 
	 * @param featureName	the name of the feature
	 * @param algIndex		the index of the optimization algorithm
	 * @param querySize		the size of the query being optimized
	 * @param period		the optimization time period
	 * @param queryIndex	query index in its query size class
	 * @param added			the number by which to increase the feature counter
	 */
	public static void addToLongFeature(String featureName, int algIndex, int querySize, 
			int period, int queryIndex, long added) {
		// Count addition only if enabled
		if (enabled) {
			// Initialize feature if not already done
			if (longFeatures.get(featureName) == null) {
				longFeatures.put(featureName, 
						new long[nrQuerySizes][nrAlgorithms][nrPeriods][nrQueriesPerSize]);
			}
			// Update feature
			long[][][][] featureValues = longFeatures.get(featureName);
			featureValues[querySize][algIndex][period][queryIndex] += added;
			// Update number of additions
			countFeatureAddition(featureName, algIndex, querySize, period, queryIndex);
		}
	}
	/**
	 * Increases the counter for a feature of type <code>double</code> if statistics are enabled.
	 * 
	 * @param featureName	the name of the feature
	 * @param algIndex		the index of the optimization algorithm
	 * @param querySize		the size of the query being optimized
	 * @param period		the optimization time period
	 * @param queryIndex	query index in its query size class
	 * @param added			the value by which to increase the feature counter
	 */
	public static void addToDoubleFeature(String featureName, int algIndex, int querySize, 
			int period, int queryIndex, double added) {
		// Count addition only if enabled
		if (enabled) {
			// Initialize feature if not already done
			if (doubleFeatures.get(featureName) == null) {
				doubleFeatures.put(featureName, 
						new double[nrQuerySizes][nrAlgorithms][nrPeriods][nrQueriesPerSize]);
			}
			// Update feature
			double[][][][] featureValues = doubleFeatures.get(featureName);
			featureValues[querySize][algIndex][period][queryIndex] += added;
			// Update number of additions
			countFeatureAddition(featureName, algIndex, querySize, period, queryIndex);
		}
	}
	/**
	 * Displays a header row for a table showing benchmark results.
	 * 
	 * @param firstHeader	name of the first column (the headers are algorithm indices)
	 * @param useTabs		boolean flag indicating whether tabs are used to separate entries
	 */
	static void displayHeader(String firstHeader, boolean useTabs) {
		if (useTabs) {
			System.out.print(firstHeader + "\t");			
		} else {
			System.out.format("%-" + nrSpaces + "s", firstHeader);
		}
		for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
			if (useTabs) {
				System.out.print("Alg" + algCtr + "\t");				
			} else {
				System.out.format("%-" + nrSpaces + "s", "Alg" + algCtr);
			}
		}
		System.out.println();
	}
	/**
	 * Writes two header rows into the file: the first row contains the column names, the second
	 * row is a dummy row that makes sure that the LaTeX pgf plots package will not automatically
	 * remove plots with infinite value over the whole optimization time (this happens sometimes
	 * for difficult test cases and bad optimization algorithms).
	 * 
	 * @param firstHeader	name of first column
	 * @param writer		the writer to use for writing the header column
	 */
	static void writeHeaderToFile(String firstHeader, PrintWriter writer) {
		// Write column names
		writer.print(firstHeader + "\t");
		for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
			writer.print("Alg" + algCtr + "\t");
		}
		writer.println();
		// Write dummy row
		writer.print("-1\t");
		for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
			writer.print("1\t");
		}
		writer.println();
	}
	/**
	 * Prints a single long value either followed by a tab or preceded by blanks.
	 * 
	 * @param useTabs		whether tabs are used to separate long values
	 * @param featureValue	long value to display
	 */
	static void printLongValue(boolean useTabs, long featureValue) {
		if (useTabs) {
			System.out.print(featureValue + "\t");						
		} else {
			System.out.format("%" + nrSpaces + "d", featureValue);
		}
	}
	/**
	 * Prints a single double value either followed by a tab or preceded by blanks.
	 * 
	 * @param useTabs		whether tabs are used to separate double values
	 * @param featureValue	double value to display
	 */
	static void printDoubleValue(boolean useTabs, double featureValue) {
		if (useTabs) {
			System.out.format("%.2f\t", featureValue);						
		} else {
			System.out.format("%" + nrSpaces + ".2f", featureValue);
		}		
	}
	/**
	 * Calculate aggregate values based on raw values using the given aggregation function.
	 * 
	 * @param rawValues		raw feature values for different algorithms and queries
	 * @param function		aggregation function
	 * @return				aggregate feature values for different algorithms
	 */
	static double[][][] calculateAggregates(double[][][][] rawValues, AggregateFunction function) {
		double[][][] aggregates = new double[nrQuerySizes][nrAlgorithms][nrPeriods];
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
				for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
					// Calculate median value
					double[] curValues = rawValues[sizeCtr][algCtr][periodCtr];
					double aggregate = -1;
					switch (function) {
					case MEAN:
						aggregate = MathUtil.aggMean(curValues);
						break;
					case MEDIAN:
						aggregate = MathUtil.aggMedian(curValues);
						break;
					case NR_WINS:
						// Count the number of queries where this algorithm has the lowest 
						// value among all algorithms. This is a win for features like
						// approximation error.
						aggregate = 0;
						for (int queryCtr=0; queryCtr<nrQueriesPerSize; ++queryCtr) {
							double thisAlgVal = curValues[queryCtr];
							double minValue = Double.POSITIVE_INFINITY;
							for (int otherAlgCtr=0; otherAlgCtr<nrAlgorithms; ++otherAlgCtr) {
								double otherAlgVal = rawValues[sizeCtr][otherAlgCtr][periodCtr][queryCtr];
								minValue = Math.min(minValue, otherAlgVal);
							}
							if (thisAlgVal <= minValue) {
								aggregate += 1;
							}
						}
						break;
					}			
					aggregates[sizeCtr][algCtr][periodCtr] = aggregate;
				}
			}
		}
		return aggregates;
	}
	/**
	 * Display aggregate values for one specific feature. Two series of tables are displayed: 
	 * one shows how the aggregates evolve as a function of the period, the other shows how
	 * the aggregates evolve as a function of the query size.
	 * 
	 * @param featureName	name of the feature to display
	 * @param useTabs		whether to separate values by tabs or by spaces
	 * @param aggregates	aggregated feature values by query size, algorithm, and time period
	 * @param function		the function that was used for aggregation
	 */
	static void displayAggregates(String featureName, boolean useTabs, 
			double[][][] aggregates, AggregateFunction function) {
		// Display one table for each period
		System.out.println();
		System.out.println("Feature: " + featureName + 
				" by optimization period; " + function.toString());
		for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
			System.out.println("Period: " + periodCtr);
			displayHeader("Size", useTabs);
			for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
				System.out.print(sizeCtr + "\t");
				for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
					double aggregateValue = aggregates[sizeCtr][algCtr][periodCtr];
					printDoubleValue(useTabs, aggregateValue);
				}
				System.out.println();
			}				
		}
		// Display one table for each query size
		System.out.println();
		System.out.println("Feature: " + featureName + 
				" by query size; " + function.toString());
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			System.out.println("Size: " + sizeCtr);
			displayHeader("Period", useTabs);
			for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
				System.out.print(periodCtr + "\t");
				for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
					double aggregateValue = aggregates[sizeCtr][algCtr][periodCtr];
					printDoubleValue(useTabs, aggregateValue);
				}
				System.out.println();
			}				
		}
	}
	/**
	 * Transforms an array containing feature value of type long into an array containing
	 * the same values as doubles.
	 * 
	 * @param longValues	feature values as longs
	 * @return				feature values as doubles
	 */
	static double[][][][] doubleFeatureValues(long[][][][] longValues) {
		double[][][][] result = new double[nrQuerySizes][nrAlgorithms][nrPeriods][nrQueriesPerSize];
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
				for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
					for (int queryCtr=0; queryCtr<nrQueriesPerSize; ++queryCtr) {
						result[sizeCtr][algCtr][periodCtr][queryCtr] = 
								longValues[sizeCtr][algCtr][periodCtr][queryCtr];
					}
				}
			}
		}
		return result;
	}
	/**
	 * Outputs the raw values for some feature on the console.
	 * 
	 * @param featureName	the name of the feature to output
	 * @param featureValues	array containing feature values for different optimizers and test cases
	 */
	public static void displayRawValues(String featureName, double[][][][] featureValues) {
		System.out.println("Raw values for " + featureName);
		for (int sizeCtr=0; sizeCtr<nrQuerySizes; ++sizeCtr) {
			for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
				for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
					System.out.println("size " + sizeCtr + "; alg " + algCtr + 
							"; period: " + periodCtr + ":" + 
							Arrays.toString(featureValues[sizeCtr][algCtr][periodCtr]));
					for (int queryCtr=0; queryCtr<nrQueriesPerSize; ++queryCtr) {
						if (Double.isNaN(featureValues[sizeCtr][algCtr][periodCtr][queryCtr])) {
							System.out.println("Warning: NaN value for feature " + featureName);
							assert(false) : "sizeCtr: " + sizeCtr + "; algCtr: " + algCtr +
								"; periodCtr: " + periodCtr + "; queryCtr: " + queryCtr +
								"; featureName: " + featureName;
						}
					}
				}
			}
		}
	}
	/**
	 * Displays average values for each feature in different formats. Displays values for 
	 * each feature for which values were collected. Generates for each feature and query 
	 * size one table showing how feature values evolve over optimization time. Also 
	 * generates for each feature and optimization time period one table showing how
	 * feature values evolve as a function of the query size.
	 * 
	 * @param useTabs	boolean flag indicating whether table entries are separated by tabs.
	 */
	public static void display(boolean useTabs) {
		// Display aggregate values
		System.out.println();
		System.out.println("Long features:");
		// Iterate over long features
		for (Entry<String, long[][][][]> entry : longFeatures.entrySet()) {
			String featureName = entry.getKey();
			long[][][][] featureValues = entry.getValue();
			double[][][][] doubleFeatureValues = doubleFeatureValues(featureValues);
			displayRawValues(featureName, doubleFeatureValues);
			for (AggregateFunction function : AggregateFunction.values()) {
				double[][][] aggregates = calculateAggregates(doubleFeatureValues, function);
				displayAggregates(featureName, useTabs, aggregates, function);				
			}
		}
		System.out.println();
		System.out.println("Double features:");
		// Iterate over double features
		for (Entry<String, double[][][][]> entry : doubleFeatures.entrySet()) {
			String featureName = entry.getKey();
			double[][][][] featureValues = entry.getValue();
			displayRawValues(featureName, featureValues);
			for (AggregateFunction function : AggregateFunction.values()) {
				double[][][] aggregates = calculateAggregates(featureValues, function);
				displayAggregates(featureName, useTabs, aggregates, function);				
			}
		}
	}
	/**
	 * Writes the aggregate values for a given feature and query size into a tab separated file.
	 * The aggregation function to use is specified. 
	 * 
	 * @param featureName	the name of the feature whose aggregate values are written into the file 
	 * @param function		the aggregation function to use (e.g., average, median, ...)
	 * @param querySize		the query size index for which values are treated
	 * @param fileName		the name of the file to create
	 * @throws FileNotFoundException 
	 */
	public static void writeToFile(String featureName, AggregateFunction function, 
			int querySize, String fileName) throws FileNotFoundException {
		// Create file
		PrintWriter writer = new PrintWriter(fileName);
		// Output statistics into file
		write(featureName, function, querySize, writer);
		// Close file
		writer.close();
	}
	/**
	 * Writes the aggregate values for a given feature and query size to a given PrintWriter.
	 * 
	 * @param featureName	the name of the feature whose aggregate values are written into the file 
	 * @param function		the aggregation function to use (e.g., average, median, ...)
	 * @param querySize		the query size index for which values are treated
	 * @param fileName		the name of the file to create
	 */
	public static void write(String featureName, AggregateFunction function, 
			int querySize, PrintWriter writer) {	
		// Obtain feature values
		double[][][][] featureValues;
		if (longFeatures.containsKey(featureName)) {
			featureValues = doubleFeatureValues(longFeatures.get(featureName));
		} else {
			featureValues = doubleFeatures.get(featureName);
		}
		// Aggregate feature values
		double[][][] aggregates = calculateAggregates(featureValues, function);
		// Write out header row
		writeHeaderToFile("Period", writer);
		// Write out values
		for (int periodCtr=0; periodCtr<nrPeriods; ++periodCtr) {
			writer.print(periodCtr + "\t");
			for (int algCtr=0; algCtr<nrAlgorithms; ++algCtr) {
				double aggregateValue = aggregates[querySize][algCtr][periodCtr];
				writer.format("%.5f\t", aggregateValue);
			}
			writer.println();
		}
		// Flush writer
		writer.flush();
	}
}
