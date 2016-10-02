package util;

/**
 * Criteria for greedily selecting the next join to perform. Alternatives are
 * - MIN_SIZE: always select the join that leads to the intermediate result with minimal size
 * - MIN_SELECTIVITY: always select the join with the most selective join predicate
 * 
 * @author immanueltrummer
 *
 */
public enum GreedyCriterion {
	MIN_SIZE, MIN_SELECTIVITY
}
