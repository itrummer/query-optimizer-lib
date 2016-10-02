package queries;

import static org.junit.Assert.*;
import static common.RandomNumbers.*;
import static util.TestUtil.*;

import org.junit.Test;

public class QueryFactoryTest {

	@Test
	public void test() {
		// Generation of chain queries
		{
			for (int i=0; i<50; ++i) {
				int nrTables = random.nextInt(10) + 2;
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CHAIN, nrTables, JoinType.MIN);
				assertEquals(nrTables, query.nrTables);
				// Verify that cardinality is within admissible range
				for (int table=0; table<nrTables; ++table) {
					double cardinality = query.tableCardinalities[table];
					assertTrue(cardinality >= 1 && cardinality <= 100000);
				}
				// Verify that selectivities are lower than one only for admissible index pairs
				for (int table1=0; table1<nrTables; ++table1) {
					for (int table2=0; table2<nrTables; ++table2) {
						if (!(table1 == table2+1 || table2 == table1+1)) {
							assertEquals(1, query.selectivities[table1][table2], EPSILON);
						}
					}
				}
			}
		}
		// Generation of star queries
		{
			for (int i=0; i<50; ++i) {
				int nrTables = random.nextInt(10) + 2;
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.STAR, nrTables, JoinType.MAX);
				assertEquals(nrTables, query.nrTables);
				// Verify that cardinality is within admissible range
				for (int table=0; table<nrTables; ++table) {
					double cardinality = query.tableCardinalities[table];
					assertTrue(cardinality >= 1 && cardinality <= 100000);
				}
				// Verify that selectivities are lower than one only for admissible index pairs
				for (int table1=0; table1<nrTables; ++table1) {
					for (int table2=0; table2<nrTables; ++table2) {
						if (!(table1 == 0 && table2 > table1 || table2 == 0 && table1 > table2)) {
							assertEquals(1, query.selectivities[table1][table2], EPSILON);
						}
					}
				}
			}
		}
		// Generation of cycle queries
		{
			for (int i=0; i<50; ++i) {
				int nrTables = random.nextInt(10) + 2;
				Query query = QueryFactory.produceSteinbrunn(JoinGraphType.CYCLE, nrTables, JoinType.MINMAX);
				assertEquals(nrTables, query.nrTables);
				// Verify that cardinality is within admissible range
				for (int table=0; table<nrTables; ++table) {
					double cardinality = query.tableCardinalities[table];
					assertTrue(cardinality >= 1 && cardinality <= 100000);
				}
				// Verify that selectivities are lower than one only for admissible index pairs
				for (int table1=0; table1<nrTables; ++table1) {
					for (int table2=0; table2<nrTables; ++table2) {
						if (!(table1 == table2+1 || table2 == table1+1 || 
								table1 == 0 && table2 == nrTables-1 ||
								table2 == 0 && table1 == nrTables - 1)) {
							assertEquals(1, query.selectivities[table1][table2], EPSILON);
						}
					}
				}
			}
		}
	}

}
