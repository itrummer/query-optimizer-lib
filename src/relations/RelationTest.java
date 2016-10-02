package relations;

import static util.TestUtil.*;
import static org.junit.Assert.*;
import queries.Query;
import util.TestUtil;

import org.junit.Test;


public class RelationTest {

	@Test
	public void test() {
		// create query
		double[] cardinalitites = new double[] {10, 100, 500};
		double[][] selectivities = TestUtil.defaultSelectivityMatrix(3);
		TestUtil.setSelectivity(selectivities, 0, 1, 0.5);
		TestUtil.setSelectivity(selectivities, 1, 2, 0.1);
		Query query = new Query(3, cardinalitites, selectivities);
		// create relations
		Relation rel100 = RelationFactory.createSingleTableRel(query, 0);
		Relation rel010 = RelationFactory.createSingleTableRel(query, 1);
		Relation rel001 = RelationFactory.createSingleTableRel(query, 2);
		Relation rel110 = RelationFactory.createJoinRel(query, rel100, rel010);
		Relation rel111 = RelationFactory.createJoinRel(query, rel110, rel001);
		// test relations
		assertEquals(rel100.cardinality, 10, EPSILON);
		assertEquals(rel010.cardinality, 100, EPSILON);
		assertEquals(rel110.cardinality, 500, EPSILON);
		assertTrue(rel110.tableSet.get(0));
		assertTrue(rel110.tableSet.get(1));
		assertFalse(rel110.tableSet.get(2));
		assertEquals(rel111.cardinality, 25000, EPSILON);
	}

}
