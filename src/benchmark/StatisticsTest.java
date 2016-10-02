package benchmark;

import static org.junit.Assert.*;
import static util.TestUtil.*;

import org.junit.Test;

public class StatisticsTest {

	@Test
	public void test() {
		// Test aggregation according to different aggregation functions
		{
			// Test mean aggregation
			{
				// Index semantic: query size, algorithm index, period, test case
				double[][][][] rawValues = new double[][][][] 
						{	// results for only query size
							{	// results for first algorithm
								{	// results for only period
									{
										1, 2, 6
									}
								},
								{	// results for second algorithm
									{	// results for only period
										6, 2, 4
									}
								}
							}
						};
				// Initialize statistics
				Statistics.init(2, 1, 1, 3);
				// Index semantic: query size, algorithm, period
				double[][][] aggregates = Statistics.calculateAggregates(
						rawValues, AggregateFunction.MEAN);
				// Make sure that result has the right dimensions
				assertEquals(1, aggregates.length);
				assertEquals(2, aggregates[0].length);
				assertEquals(1, aggregates[0][0].length);
				// Make sure that result contains the right values
				assertEquals(3, aggregates[0][0][0], EPSILON);
				assertEquals(4, aggregates[0][1][0], EPSILON);
			}
			// Test median aggregation
			{
				// Index semantic: query size, algorithm index, period, test case
				double[][][][] rawValues = new double[][][][] 
						{	// results for only query size
							{	// results for first algorithm
								{	// results for only period
									{
										1, 2, 6
									}
								},
								{	// results for second algorithm
									{	// results for only period
										6, 2, 4
									}
								}
							}
						};
				// Initialize statistics
				Statistics.init(2, 1, 1, 3);
				// Index semantic: query size, algorithm, period
				double[][][] aggregates = Statistics.calculateAggregates(
						rawValues, AggregateFunction.MEDIAN);
				// Make sure that result has the right dimensions
				assertEquals(1, aggregates.length);
				assertEquals(2, aggregates[0].length);
				assertEquals(1, aggregates[0][0].length);
				// Make sure that result contains the right values
				assertEquals(2, aggregates[0][0][0], EPSILON);
				assertEquals(4, aggregates[0][1][0], EPSILON);
			}
			{
				// Index semantic: query size, algorithm index, period, test case
				double[][][][] rawValues = new double[][][][] 
						{	// results for only query size
							{	// results for first algorithm
								{	// results for only period
									{
										1
									}
								},
								{	// results for second algorithm
									{	// results for only period
										6
									}
								}
							}
						};
				// Generate aggregate statistics
				double[][][] aggregates = Statistics.calculateAggregates(
						rawValues, AggregateFunction.MEDIAN);
				// Make sure that result contains the right values
				assertEquals(1, aggregates[0][0][0], EPSILON);
				assertEquals(6, aggregates[0][1][0], EPSILON);
			}
			// Test calculating the number of wins
			{
				// Index semantic: query size, algorithm index, period, test case
				double[][][][] rawValues = new double[1][2][1][3];
				// Values for first algorithm
				rawValues[0][0][0][0] = 1;
				rawValues[0][0][0][1] = 2;
				rawValues[0][0][0][2] = 6;
				// Values for second algorithm
				rawValues[0][1][0][0] = 6;
				rawValues[0][1][0][1] = 2;
				rawValues[0][1][0][2] = 4;
				// Initialize statistics
				Statistics.init(2, 1, 1, 3);
				// Index semantic: query size, algorithm, period
				double[][][] aggregates = Statistics.calculateAggregates(
						rawValues, AggregateFunction.NR_WINS);
				// Make sure that result has the right dimensions
				assertEquals(1, aggregates.length);
				assertEquals(2, aggregates[0].length);
				assertEquals(1, aggregates[0][0].length);
				// Make sure that result contains the right values
				assertEquals(2, aggregates[0][0][0], EPSILON);
				assertEquals(2, aggregates[0][1][0], EPSILON);
			}
			{
				// Index semantic: query size, algorithm index, period, test case
				double[][][][] rawValues = new double[1][2][1][3];
				// Values for first algorithm
				rawValues[0][0][0][0] = 3;
				rawValues[0][0][0][1] = 3;
				rawValues[0][0][0][2] = 2;
				// Values for second algorithm
				rawValues[0][1][0][0] = 3;
				rawValues[0][1][0][1] = 1;
				rawValues[0][1][0][2] = 1;
				// Initialize statistics
				Statistics.init(2, 1, 1, 3);
				// Index semantic: query size, algorithm, period
				double[][][] aggregates = Statistics.calculateAggregates(
						rawValues, AggregateFunction.NR_WINS);
				// Make sure that result has the right dimensions
				assertEquals(1, aggregates.length);
				assertEquals(2, aggregates[0].length);
				assertEquals(1, aggregates[0][0].length);
				// Make sure that result contains the right values
				assertEquals(1, aggregates[0][0][0], EPSILON);
				assertEquals(3, aggregates[0][1][0], EPSILON);
			}
		}
	}

}
