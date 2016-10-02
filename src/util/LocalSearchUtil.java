package util;

import static common.RandomNumbers.*;
import static common.Constants.*;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import common.RandomNumbers;
import cost.CostModel;
import cost.MultiCostModel;
import plans.JoinPlan;
import plans.Plan;
import plans.PathNode;
import plans.ScanPlan;
import plans.operators.JoinOperator;
import plans.operators.ScanOperator;
import plans.spaces.PlanSpace;
import queries.Query;
import relations.Relation;
import relations.RelationFactory;

/**
 * Auxiliary functions used by randomized local search algorithms.
 * 
 * @author immanueltrummer
 *
 */
public class LocalSearchUtil {
	/**
	 * Counts the number of exhaustive hill climbing invocations.
	 */
	public static long nrExhaustiveClimbs = 0;
	/**
	 * Counts the number of steps taken during exhaustive hill climbing.
	 */
	public static long nrExhaustiveSteps = 0;
	/**
	 * Accumulated improvement measured by epsilon dominance on the climbing paths.
	 */
	public static double accEpsilonImprovement = 0;
	/**
	 * Selects random relation from relation list, deletes that relation
	 * in the list and returns selected relation.
	 * 
	 * @param rels	list of relations
	 * @return		a randomly selected relation that is also deleted from the list
	 */
	public static Relation popRandomRel(List<Relation> rels) {
		assert(rels.size()>0);
		int nrRels = rels.size();
		int selectedIndex = RandomNumbers.random.nextInt(nrRels);
		Relation selectedRel = rels.get(selectedIndex);
		rels.remove(selectedIndex);
		return selectedRel;
	}
	/**
	 * Selects random plan from the plan list, deletes that plan in the list,
	 * and returns the selected plan.
	 * 
	 * @param plans	list of query plans
	 * @return		a randomly selected plan that is deleted from the list
	 */
	public static Plan popRandomPlan(List<Plan> plans) {
		assert(plans.size()>0);
		int nrPlans = plans.size();
		int selectedIndex = RandomNumbers.random.nextInt(nrPlans);
		Plan selectedPlan = plans.get(selectedIndex);
		plans.remove(selectedIndex);
		return selectedPlan;
	}
	/**
	 * Extracts the indices of all tables joined by the given plan.
	 * 
	 * @param plan	a query plan for which we want to find out the joined tables
	 * @return		list of table indices representing joined tables
	 */
	public static Set<Integer> joinedTablesIndices(Plan plan) {
		if (plan instanceof ScanPlan) {
			int tableIndex = ((ScanPlan)plan).tableIndex;
			Set<Integer> resultSet = new TreeSet<Integer>();
			resultSet.add(tableIndex);
			return resultSet;
		} else {
			assert(plan instanceof JoinPlan);
			JoinPlan joinPlan = (JoinPlan)plan;
			Set<Integer> allIndices = new TreeSet<Integer>();
			allIndices.addAll(joinedTablesIndices(joinPlan.getLeftPlan()));
			allIndices.addAll(joinedTablesIndices(joinPlan.getRightPlan()));
			return allIndices;
		}
	}
	/**
	 * Generates randomly a left-deep query plan for the given query.
	 * Attention: this function does not initialize cost values.
	 * 
	 * @param query		query for which to generate a plan
	 * @param planSpace	determines which scan and join operators are available
	 * @return			a randomly generated left-deep plan
	 */
	public static Plan randomLeftDeepPlan(Query query, PlanSpace planSpace) {
		assert(query.nrTables>=1);
		// Create single table relations for query
		ArrayList<Relation> singleTableRels = new ArrayList<Relation>();
		int nrTables = query.nrTables;
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			Relation rel = RelationFactory.createSingleTableRel(query, tableCtr);
			singleTableRels.add(rel);
		}
		// Select random relation to start with
		Relation leftRel = popRandomRel(singleTableRels);
		ScanOperator leftScanOp = planSpace.randomScanOperator(leftRel);
		Plan leftPlan = new ScanPlan(leftRel, leftScanOp);
		Plan resultPlan = leftPlan;
		// Keep joining with other relations until all are used up
		while (singleTableRels.size()>0) {
			// create right scan plan
			Relation rightRel = popRandomRel(singleTableRels);
			ScanOperator rightScanOp = planSpace.randomScanOperator(rightRel);
			Plan rightPlan = new ScanPlan(rightRel, rightScanOp);
			// create result plan
			Relation resultRel = RelationFactory.createJoinRel(query, leftRel, rightRel);
			JoinOperator joinOp = planSpace.randomJoinOperator(leftPlan, rightPlan);
			resultPlan = new JoinPlan(leftRel, rightRel, resultRel, leftPlan, rightPlan, joinOp);
			// make result plan the new left plan too
			leftRel = resultRel;
			leftPlan = resultPlan;
		}
		if (SAFE_MODE) {
			TestUtil.validateOperators(resultPlan, planSpace);
		}
		return resultPlan;
	}
	/**
	 * Generates randomly a bushy plan for the given query joining the specified subset of tables.
	 * Attention: this function does not initialize cost values.
	 * 
	 * @param query			query for which to generate a plan
	 * @param planSpace		determines which scan and join operators are available
	 * @param tableIndices	which tables the plan should join (can be a subset of the query tables)
	 * @return				a randomly generated bushy plan
	 */
	public static Plan randomBushyPlan(Query query, PlanSpace planSpace, Set<Integer> tableIndices) {
		assert(query.nrTables>=1);
		// Create plans for scanning each single table
		ArrayList<Plan> partialPlans = new ArrayList<Plan>();
		for (int tableIndex : tableIndices) {
			Relation rel = RelationFactory.createSingleTableRel(query, tableIndex);
			ScanOperator scanOp = planSpace.randomScanOperator(rel);
			partialPlans.add(new ScanPlan(query, tableIndex, scanOp));
		}
		// Combine randomly selected plans with each other until we obtain a complete plan
		while (partialPlans.size() > 1) {
			Plan leftPlan = popRandomPlan(partialPlans);
			Plan rightPlan = popRandomPlan(partialPlans);
			JoinOperator joinOp = planSpace.randomJoinOperator(leftPlan, rightPlan);
			Plan newPlan = new JoinPlan(query, leftPlan, rightPlan, joinOp);
			partialPlans.add(newPlan);
		}
		Plan resultPlan = partialPlans.get(0);
		if (SAFE_MODE) {
			TestUtil.validateOperators(resultPlan, planSpace);
		}
		return resultPlan;
	}
	/**
	 * Generates randomly a bushy plan for the given query joining all query tables.
	 * Attention: this function does not initialize cost values.
	 * 
	 * @param query		query for which to generate a plan
	 * @param planSpace	determines which scan and join operators are available
	 * @return			a randomly generated bushy plan joining all query tables
	 */
	public static Plan randomBushyPlan(Query query, PlanSpace planSpace) {
		int nrTables = query.nrTables;
		Set<Integer> tableIndices = new TreeSet<Integer>();
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			tableIndices.add(tableCtr);
		}
		return randomBushyPlan(query, planSpace, tableIndices);
	}
	/**
	 * Obtains set of mutated sub-trees of the original one with their cost already calculated.
	 * We use the mutations proposed by Ioannidis and Kang and described by Steinbrunn, 97.
	 * 
	 * @param query			query for which to generate a plan
	 * @param originalPlan	query plan to apply mutations to
	 * @param planSpace		determines applicable operators
	 * @param costModel		used to calculate cost of mutated plans
	 * @return				a list of mutated plans with their cost already calculated
	 */
	public static List<Plan> mutatedPlans(
			Query query, Plan originalPlan, PlanSpace planSpace, MultiCostModel costModel) {
		Relation resultRel = originalPlan.resultRel;
		// Try mutating plan
		List<Plan> mutatedPlans = new LinkedList<Plan>();
		// Distinguish scan and join plans
		if (originalPlan instanceof ScanPlan) {
			// Operator mutations
			for (ScanOperator scanOperator : planSpace.scanOperatorsShuffled(resultRel)) {
				Plan newPlan = new ScanPlan(resultRel, scanOperator);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
		} else {
			assert(originalPlan instanceof JoinPlan);
			JoinPlan originalJoinPlan = (JoinPlan)originalPlan;
			// extract first child level in query tree
			Plan leftPlan = originalJoinPlan.getLeftPlan();
			Plan rightPlan = originalJoinPlan.getRightPlan();
			JoinOperator joinOp = originalJoinPlan.getJoinOperator();
			// extract second child level in query tree
			boolean leftPlanIsJoin, rightPlanIsJoin;
			Plan leftLeftPlan, leftRightPlan, rightLeftPlan, rightRightPlan;
			if (leftPlan instanceof JoinPlan) {
				leftPlanIsJoin = true;
				JoinPlan leftJoinPlan = (JoinPlan)leftPlan;
				leftLeftPlan = leftJoinPlan.getLeftPlan();
				leftRightPlan = leftJoinPlan.getRightPlan();
			} else {
				leftPlanIsJoin = false;
				leftLeftPlan = null;
				leftRightPlan = null;
			}
			if (rightPlan instanceof JoinPlan) {
				rightPlanIsJoin = true;
				JoinPlan rightJoinPlan = (JoinPlan)rightPlan;
				rightLeftPlan = rightJoinPlan.getLeftPlan();
				rightRightPlan = rightJoinPlan.getRightPlan();
			} else {
				rightPlanIsJoin = false;
				rightLeftPlan = null;
				rightRightPlan = null;
			}
			// Operator mutations
			for (JoinOperator joinOperator : planSpace.joinOperatorsShuffled(leftPlan, rightPlan)) {
				Plan newPlan = new JoinPlan(query, leftPlan, rightPlan, joinOperator);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
			// Commutativity
			{
				Plan newPlan = new JoinPlan(query, rightPlan, leftPlan, joinOp);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
			// Associativity
			if (leftPlanIsJoin) {
				Plan A = leftLeftPlan;
				Plan B = leftRightPlan;
				Plan C = rightPlan;
				Plan newLeftPlan = A;
				for (JoinOperator newRightJoinOp : planSpace.joinOperatorsShuffled(B, C)) {
					Plan newRightPlan = new JoinPlan(query, B, C, newRightJoinOp);
					costModel.updateRoot(newRightPlan);
					for (JoinOperator newJoinOp : planSpace.joinOperatorsShuffled(
							newLeftPlan, newRightPlan)) {
						Plan newPlan = new JoinPlan(query, newLeftPlan, newRightPlan, newJoinOp);
						costModel.updateRoot(newPlan);
						mutatedPlans.add(newPlan);											
					}
				}
			}
			// Left join exchange
			if (leftPlanIsJoin) {
				Plan A = leftLeftPlan;
				Plan B = leftRightPlan;
				Plan C = rightPlan;
				for (JoinOperator newLeftJoinOp : planSpace.joinOperatorsShuffled(A, C)) {
					Plan newLeftPlan = new JoinPlan(query, A, C, newLeftJoinOp);
					costModel.updateRoot(newLeftPlan);
					for (JoinOperator newJoinOp : planSpace.joinOperatorsShuffled(
							newLeftPlan, B)) {
						Plan newPlan = new JoinPlan(query, newLeftPlan, B, newJoinOp);
						costModel.updateRoot(newPlan);
						mutatedPlans.add(newPlan);											
					}
				}
			}
			// Right join exchange
			if (rightPlanIsJoin) {
				Plan A = leftPlan;
				Plan B = rightLeftPlan;
				Plan C = rightRightPlan;
				for (JoinOperator newRightJoinOp : planSpace.joinOperatorsShuffled(A, C)) {
					Plan newRightPlan = new JoinPlan(query, A, C, newRightJoinOp);
					costModel.updateRoot(newRightPlan);
					for (JoinOperator newJoinOp : planSpace.joinOperatorsShuffled(B, newRightPlan)) {
						Plan newPlan = new JoinPlan(query, B, newRightPlan, newJoinOp);
						costModel.updateRoot(newPlan);
						mutatedPlans.add(newPlan);											
					}
				}
			}
		}
		if (SAFE_MODE) {
			TestUtil.validatePlans(mutatedPlans, planSpace, costModel, true);
		}
		return mutatedPlans;
	}
	/**
	 * Returns a list of mutated plans that mutate only the join operator while standard operators
	 * are used. This mutation function is used by algorithms that select operators separately.
	 * We use the mutations for bushy plan spaces described by Steinbrunn et all., VLDB '97.
	 * 
	 * @param query			the query being optimized
	 * @param originalPlan	the plan that is mutated at the root
	 * @param planSpace		determines default join operators to use
	 * @param costModel		used to re-calculate the cost of mutated plans
	 * @return				list of plans that differ from the original by the join order at the root
	 */
	public static List<Plan> mutatedJoinOrders(
			Query query, Plan originalPlan, PlanSpace planSpace, MultiCostModel costModel) {
		// Always use standard join operator
		JoinOperator joinOp = planSpace.defaultJoinOperator;
		// Try mutating plan
		List<Plan> mutatedPlans = new LinkedList<Plan>();
		// Join order can only be mutated for join plans but not for scan plans
		if (originalPlan instanceof JoinPlan) {
			JoinPlan originalJoinPlan = (JoinPlan)originalPlan;
			// extract first child level in query tree
			Plan leftPlan = originalJoinPlan.getLeftPlan();
			Plan rightPlan = originalJoinPlan.getRightPlan();
			// extract second child level in query tree
			boolean leftPlanIsJoin, rightPlanIsJoin;
			Plan leftLeftPlan, leftRightPlan, rightLeftPlan, rightRightPlan;
			if (leftPlan instanceof JoinPlan) {
				leftPlanIsJoin = true;
				JoinPlan leftJoinPlan = (JoinPlan)leftPlan;
				leftLeftPlan = leftJoinPlan.getLeftPlan();
				leftRightPlan = leftJoinPlan.getRightPlan();
			} else {
				leftPlanIsJoin = false;
				leftLeftPlan = null;
				leftRightPlan = null;
			}
			if (rightPlan instanceof JoinPlan) {
				rightPlanIsJoin = true;
				JoinPlan rightJoinPlan = (JoinPlan)rightPlan;
				rightLeftPlan = rightJoinPlan.getLeftPlan();
				rightRightPlan = rightJoinPlan.getRightPlan();
			} else {
				rightPlanIsJoin = false;
				rightLeftPlan = null;
				rightRightPlan = null;
			}
			// Commutativity
			{
				Plan newPlan = new JoinPlan(query, rightPlan, leftPlan, joinOp);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
			// Associativity
			if (leftPlanIsJoin) {
				Plan A = leftLeftPlan;
				Plan B = leftRightPlan;
				Plan C = rightPlan;
				Plan newLeftPlan = A;
				Plan newRightPlan = new JoinPlan(query, B, C, joinOp);
				costModel.updateRoot(newRightPlan);
				Plan newPlan = new JoinPlan(query, newLeftPlan, newRightPlan, joinOp);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
			// Left join exchange
			if (leftPlanIsJoin) {
				Plan A = leftLeftPlan;
				Plan B = leftRightPlan;
				Plan C = rightPlan;
				Plan newLeftPlan = new JoinPlan(query, A, C, joinOp);
				costModel.updateRoot(newLeftPlan);
				Plan newPlan = new JoinPlan(query, newLeftPlan, B, joinOp);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
			// Right join exchange
			if (rightPlanIsJoin) {
				Plan A = leftPlan;
				Plan B = rightLeftPlan;
				Plan C = rightRightPlan;
				Plan newRightPlan = new JoinPlan(query, A, C, joinOp);
				costModel.updateRoot(newRightPlan);
				Plan newPlan = new JoinPlan(query, B, newRightPlan, joinOp);
				costModel.updateRoot(newPlan);
				mutatedPlans.add(newPlan);
			}
		}
		if (SAFE_MODE) {
			TestUtil.validatePlans(mutatedPlans, planSpace, costModel, true);
		}
		/*
		System.out.println("Mutations for " + originalPlan.orderToString());
		for (Plan mutatedPlan : mutatedPlans) {
			System.out.println(mutatedPlan.orderToString());
		}
		*/
		return mutatedPlans;
	}
	/**
	 * Returns a mutated plan that has lower cost in the input cost metrics for the relevant metrics
	 * or a null pointer if no improvements are possible. The plan to improve might be partial and 
	 * in that case we make sure that the improved plan produces output that is suitable for next 
	 * join operator.
	 * 
	 * @param query				query for which to generate a plan
	 * @param rootPlan			the plan to improve
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @param planSpace			determines considered scan and join operators
	 * @param costModel			used to compare plans
	 * @param nextJoinOperator	result generated by improved plan must be acceptable as
	 * 							input for that join operator; null for no constraints.
	 * @return					an improved plan or null if no improvement possible
	 */
	public static Plan improvedPlan(Query query, Plan rootPlan, boolean[] consideredMetric, 
			PlanSpace planSpace, MultiCostModel costModel, JoinOperator nextJoinOperator) {
		double[] originalCost = rootPlan.cost;
		// Try mutations of the root
		List<Plan> mutatedRoot = mutatedPlans(query, rootPlan, planSpace, costModel);
		for (Plan mutatedPlan : mutatedRoot) {
			if (PruningUtil.ParetoDominates(mutatedPlan.cost, originalCost, consideredMetric)) {
				boolean compatible;
				if (nextJoinOperator == null) {
					compatible = true;
				} else if (mutatedPlan instanceof ScanPlan) {
					ScanPlan scanPlan = (ScanPlan)mutatedPlan;
					ScanOperator scanOperator = scanPlan.scanOperator;
					compatible = planSpace.scanOutputCompatible(scanOperator, nextJoinOperator);
				} else {
					assert(mutatedPlan instanceof JoinPlan);
					JoinPlan joinPlan = (JoinPlan)mutatedPlan;
					JoinOperator joinOperator = joinPlan.getJoinOperator();
					compatible = planSpace.joinOutputComptatible(joinOperator, nextJoinOperator);
				}
				if (SAFE_MODE) {
					TestUtil.validatePlan(mutatedPlan, planSpace, costModel, true);
				}
				if (compatible) {
					return mutatedPlan;					
				}
			}
		}
		// Contains plan nodes where mutations need to be tried out
		if (rootPlan instanceof JoinPlan) {
			JoinPlan originalJoinPlan = (JoinPlan)rootPlan;
			Deque<PathNode> untreatedNodes = new LinkedList<PathNode>();
			PathNode rootNode = new PathNode(rootPlan, null, false);
			untreatedNodes.push(new PathNode(originalJoinPlan.getLeftPlan(), rootNode, true));
			untreatedNodes.push(new PathNode(originalJoinPlan.getRightPlan(), rootNode, false));
			while (!untreatedNodes.isEmpty()) {
				PathNode mutatedNode = untreatedNodes.pop();
				Plan plan = mutatedNode.plan;
				PathNode parentNode = mutatedNode.parent;
				JoinPlan parentPlan = (JoinPlan)parentNode.plan;
				boolean isLeftChild = mutatedNode.isLeftChild;
				// Add child nodes if there are any
				if (plan instanceof JoinPlan) {
					JoinPlan joinPlan = (JoinPlan)plan;
					untreatedNodes.push(new PathNode(joinPlan.getLeftPlan(), mutatedNode, true));
					untreatedNodes.push(new PathNode(joinPlan.getRightPlan(), mutatedNode, false));
				}
				// We will need to adapt child plan pointers of parent - store original values
				// to restore them afterwards.
				Plan originalLeftPlan = parentPlan.getLeftPlan();
				Plan originalRightPlan = parentPlan.getRightPlan();
				// Mutate nodes and compare cost of mutated plans
				List<Plan> mutatedPlans = mutatedPlans(query, plan, planSpace, costModel);
				for (Plan mutatedPlan : mutatedPlans) {
					// Adapt parent to point to mutated plan
					if (isLeftChild) {
						parentPlan.setLeftPlan(mutatedPlan);
					} else {
						parentPlan.setRightPlan(mutatedPlan);
					}
					// Make sure that mutation did not lead to inconsistent plan
					// (new output properties might be unsuitable as parent join input)
					if (planSpace.joinOperatorApplicable(
							parentPlan.getJoinOperator(), parentPlan.getLeftPlan(), parentPlan.getRightPlan())) {
						// Update cost values bottom-up
						PathNode nodeToUpdate = parentNode;
						while (nodeToUpdate != null) {
							costModel.updateRoot(nodeToUpdate.plan);
							nodeToUpdate = nodeToUpdate.parent;
						}
						// Check whether cost has improved
						if (PruningUtil.ParetoDominates(rootPlan.cost, originalCost, consideredMetric)) {
							if (SAFE_MODE) {
								TestUtil.validatePlan(rootPlan, planSpace, costModel, true);
							}
							return rootPlan;	// Return changed plan
						}
					}
				}
				// Restore original child nodes
				parentPlan.setLeftPlan(originalLeftPlan);
				parentPlan.setRightPlan(originalRightPlan);
			}
		}
		// If we arrive here then no improvement was possible
		return null;
	}
	/**
	 * 
	 */
	public static Plan fastImprovedPlan(Query query, Plan rootPlan, boolean[] consideredMetric, 
			PlanSpace planSpace, MultiCostModel costModel, JoinOperator nextJoinOperator) {
		double[] originalCost = rootPlan.cost;
		// Try mutations of the root
		List<Plan> mutatedRoot = mutatedPlans(query, rootPlan, planSpace, costModel);
		for (Plan mutatedPlan : mutatedRoot) {
			if (PruningUtil.ParetoDominates(mutatedPlan.cost, originalCost, consideredMetric)) {
				boolean compatible;
				if (nextJoinOperator == null) {
					compatible = true;
				} else if (mutatedPlan instanceof ScanPlan) {
					ScanPlan scanPlan = (ScanPlan)mutatedPlan;
					ScanOperator scanOperator = scanPlan.scanOperator;
					compatible = planSpace.scanOutputCompatible(scanOperator, nextJoinOperator);
				} else {
					assert(mutatedPlan instanceof JoinPlan);
					JoinPlan joinPlan = (JoinPlan)mutatedPlan;
					JoinOperator joinOperator = joinPlan.getJoinOperator();
					compatible = planSpace.joinOutputComptatible(joinOperator, nextJoinOperator);
				}
				if (SAFE_MODE) {
					TestUtil.validatePlan(mutatedPlan, planSpace, costModel, true);
				}
				if (compatible) {
					return mutatedPlan;					
				}
			}
		}
		// Contains plan nodes where mutations need to be tried out
		if (rootPlan instanceof JoinPlan) {
			JoinPlan originalJoinPlan = (JoinPlan)rootPlan;
			Deque<PathNode> untreatedNodes = new LinkedList<PathNode>();
			PathNode rootNode = new PathNode(rootPlan, null, false);
			untreatedNodes.push(new PathNode(originalJoinPlan.getLeftPlan(), rootNode, true));
			untreatedNodes.push(new PathNode(originalJoinPlan.getRightPlan(), rootNode, false));
			while (!untreatedNodes.isEmpty()) {
				PathNode mutatedNode = untreatedNodes.pop();
				Plan plan = mutatedNode.plan;
				PathNode parentNode = mutatedNode.parent;
				JoinPlan parentPlan = (JoinPlan)parentNode.plan;
				boolean isLeftChild = mutatedNode.isLeftChild;
				// Add child nodes if there are any
				if (plan instanceof JoinPlan) {
					JoinPlan joinPlan = (JoinPlan)plan;
					untreatedNodes.push(new PathNode(joinPlan.getLeftPlan(), mutatedNode, true));
					untreatedNodes.push(new PathNode(joinPlan.getRightPlan(), mutatedNode, false));
				}
				// We will need to adapt child plan pointers of parent - store original values
				// to restore them afterwards.
				Plan originalLeftPlan = parentPlan.getLeftPlan();
				Plan originalRightPlan = parentPlan.getRightPlan();
				// Mutate nodes and compare cost of mutated plans
				List<Plan> mutatedPlans = mutatedPlans(query, plan, planSpace, costModel);
				for (Plan mutatedPlan : mutatedPlans) {
					// Adapt parent to point to mutated plan
					if (isLeftChild) {
						parentPlan.setLeftPlan(mutatedPlan);
					} else {
						parentPlan.setRightPlan(mutatedPlan);
					}
					// Make sure that mutation did not lead to inconsistent plan
					// (new output properties might be unsuitable as parent join input)
					if (planSpace.joinOperatorApplicable(
							parentPlan.getJoinOperator(), parentPlan.getLeftPlan(), parentPlan.getRightPlan())) {
						// Due to the principle of optimality, the entire mutated plan can only dominate
						// the old plan if the new sub-plan dominates the old sub-plan.
						if (PruningUtil.ParetoDominates(mutatedPlan.cost, plan.cost, consideredMetric)) {
							// Update cost values bottom-up
							PathNode nodeToUpdate = parentNode;
							while (nodeToUpdate != null) {
								costModel.updateRoot(nodeToUpdate.plan);
								nodeToUpdate = nodeToUpdate.parent;
							}
							// Check whether cost has improved
							if (PruningUtil.ParetoDominates(rootPlan.cost, originalCost, consideredMetric)) {
								if (SAFE_MODE) {
									TestUtil.validatePlan(rootPlan, planSpace, costModel, true);
								}
								return rootPlan;	// Return changed plan
							}							
						}
					}
				}
				// Restore original child nodes
				parentPlan.setLeftPlan(originalLeftPlan);
				parentPlan.setRightPlan(originalRightPlan);
			}
		}
		// If we arrive here then no improvement was possible
		return null;
	}
	/**
	 * Returns a plan whose cost vector dominates the one of the original. Only the join order is
	 * changed comparing the original plan and the mutation. The mutations use the standard join
	 * operator specified in the plan space. Returns a null pointer if no improvement of the join
	 * order is possible. This function assumes that the input plan also uses only the default join
	 * and scan operators.
	 * 
	 * @param query				query for which to generate a plan
	 * @param rootPlan			the plan whose join order to improve
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @param planSpace			defines the standard join operator to use for mutated plans
	 * @param costModel			used to compare plans
	 * @return					an improved plan or null if no improvement possible
	 */
	public static Plan improvedJoinOrder(Query query, Plan rootPlan, boolean[] consideredMetric, 
			PlanSpace planSpace, MultiCostModel costModel) {
		double[] originalCost = rootPlan.cost;
		// Try mutations of the root
		List<Plan> mutatedRoot = mutatedJoinOrders(query, rootPlan, planSpace, costModel);
		for (Plan mutatedPlan : mutatedRoot) {
			if (PruningUtil.ParetoDominates(mutatedPlan.cost, originalCost, consideredMetric)) {
				if (SAFE_MODE) {
					TestUtil.validatePlan(mutatedPlan, planSpace, costModel, true);
				}
				return mutatedPlan;
			}
		}
		// Contains plan nodes where mutations need to be tried out
		if (rootPlan instanceof JoinPlan) {
			JoinPlan originalJoinPlan = (JoinPlan)rootPlan;
			Deque<PathNode> untreatedNodes = new LinkedList<PathNode>();
			PathNode rootNode = new PathNode(rootPlan, null, false);
			untreatedNodes.push(new PathNode(originalJoinPlan.getLeftPlan(), rootNode, true));
			untreatedNodes.push(new PathNode(originalJoinPlan.getRightPlan(), rootNode, false));
			while (!untreatedNodes.isEmpty()) {
				PathNode mutatedNode = untreatedNodes.pop();
				Plan plan = mutatedNode.plan;
				PathNode parentNode = mutatedNode.parent;
				JoinPlan parentPlan = (JoinPlan)parentNode.plan;
				boolean isLeftChild = mutatedNode.isLeftChild;
				// Add child nodes if there are any
				if (plan instanceof JoinPlan) {
					JoinPlan joinPlan = (JoinPlan)plan;
					untreatedNodes.push(new PathNode(joinPlan.getLeftPlan(), mutatedNode, true));
					untreatedNodes.push(new PathNode(joinPlan.getRightPlan(), mutatedNode, false));
				}
				// We will need to adapt child plan pointers of parent - store original values
				// to restore them afterwards.
				Plan originalLeftPlan = parentPlan.getLeftPlan();
				Plan originalRightPlan = parentPlan.getRightPlan();
				// Mutate nodes and compare cost of mutated plans
				List<Plan> mutatedPlans = mutatedJoinOrders(query, plan, planSpace, costModel);
				for (Plan mutatedPlan : mutatedPlans) {
					// Adapt parent to point to mutated plan
					if (isLeftChild) {
						parentPlan.setLeftPlan(mutatedPlan);
					} else {
						parentPlan.setRightPlan(mutatedPlan);
					}
					// As we only use the standard operators, we do not need to check compatibility
					// of mutated plan output and next join operator.
					// Update cost values bottom-up
					PathNode nodeToUpdate = parentNode;
					while (nodeToUpdate != null) {
						costModel.updateRoot(nodeToUpdate.plan);
						nodeToUpdate = nodeToUpdate.parent;
					}
					// Check whether cost has improved
					if (PruningUtil.ParetoDominates(rootPlan.cost, originalCost, consideredMetric)) {
						if (SAFE_MODE) {
							TestUtil.validatePlan(rootPlan, planSpace, costModel, true);
						}
						return rootPlan;	// Return changed plan
					}
				}
				// Restore original child nodes
				parentPlan.setLeftPlan(originalLeftPlan);
				parentPlan.setRightPlan(originalRightPlan);
			}
		}
		// If we arrive here then no improvement was possible
		return null;
	}
	/**
	 * Perform hill climbing starting from given plan and return locally optimal plan.
	 * This hill climbing variant systematically tries all possible moves from current node.
	 * It therefore finds any possible improvement but might incur overheads if none possible.
	 * 
	 * @param query				query for which to generate a plan
	 * @param plan				the plan to improve
	 * @param planSpace			determines considered scan and join operators
	 * @param costModel			used to compare plans
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @param nextJoinOperator	result generated by improved plan must be acceptable as
	 * 							input for that join operator; null for no constraints.
	 * @return					next local Pareto optimum in plan search space
	 */
	public static Plan exhaustivePlanClimbing(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric, JoinOperator nextJoinOperator) {
		++nrExhaustiveClimbs;
		Plan improvedPlan = improvedPlan(query, plan, consideredMetric, planSpace, costModel, nextJoinOperator);
		while (improvedPlan != null) {
			++nrExhaustiveSteps;
			accEpsilonImprovement += ParetoUtil.epsilonError(plan.cost, improvedPlan.cost, consideredMetric);
			plan = improvedPlan;
			improvedPlan = improvedPlan(query, plan, consideredMetric, planSpace, costModel, nextJoinOperator);
		}
		return plan;
	}
	/**
	 * Perform hill climbing in the space of join orders using standard scan and join operators.
	 * This hill climbing variant systematically tries all possible moves from current node.
	 * Returns locally optimal join order.
	 * 
	 * @param query				the query for which to optimize the join order
	 * @param joinOrder			a join order to start the climb from
	 * @param planSpace			determines the used default scan and join operators
	 * @param costModel			calculates cost of query plans according to multiple cost metrics
	 * @param consideredMetric	Boolean flags which cost metrics are considered during climb
	 * @return					a locally optimal join order that was reached by hill climbing
	 */
	public static Plan exhaustiveJoinOrderClimbing(Query query, Plan joinOrder, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric) {
		++nrExhaustiveClimbs;
		Plan improvedJoinOrder = improvedJoinOrder(query, joinOrder, consideredMetric, planSpace, costModel);
		while (improvedJoinOrder != null) {
			++nrExhaustiveSteps;
			accEpsilonImprovement += ParetoUtil.epsilonError(
					joinOrder.cost, improvedJoinOrder.cost, consideredMetric);
			joinOrder = improvedJoinOrder;
			improvedJoinOrder = improvedJoinOrder(query, joinOrder, consideredMetric, planSpace, costModel);
		}
		return joinOrder;
	}
	/**
	 * Calculate maximum over all cost metrics by which the tested plan is worse than the reference plan.
	 * 
	 * @param testCosts			cost vector of test plan
	 * @param refCosts			reference cost vector
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @return					Maximal difference between test cost and reference cost
	 */
	static double maxCostDelta(double[] testCosts, double[] refCosts, boolean[] consideredMetric) {
		assert(testCosts.length == NR_COST_METRICS);
		assert(refCosts.length == NR_COST_METRICS);
		assert(consideredMetric.length == NR_COST_METRICS);
		double maxDelta = 0;
		for (int metricCtr=0; metricCtr<NR_COST_METRICS; ++metricCtr) {
			if (consideredMetric[metricCtr]) {
				double testCost = testCosts[metricCtr];
				double refCost = refCosts[metricCtr];
				double delta = Math.max(testCost - refCost, 0);
				maxDelta = Math.max(maxDelta, delta);
			}
		}
		return maxDelta;
	}
	/**
	 * Decides whether to accept one move in the plan space by comparing cost values before and
	 * after the move.
	 * 
	 * @param costBefore		cost vector before move
	 * @param costAfter			cost vector after move
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @param allowWorsening	If <code>true</code> then moves towards non-dominating plans are 
	 * 							accepted with non zero probability; the probability depends then on
	 * 							the <code>temperature</code> parameter.
	 * 							If <code>false</code> then only moves towards dominating plans are
	 * 							accepted.
	 * @param temperature		Used for simulated annealing: the higher the temperature, the more
	 * 							likely a move to a non-dominating plan. Only matters if flag
	 * 							<code>allowWorsening</code> is set to true.
	 * @return					Boolean indicating if the plan space move is accepted
	 */
	public static boolean acceptMove(double[] costBefore, double[] costAfter, 
			boolean[] consideredMetric, boolean allowWorsening, double temperature) {
		if (allowWorsening) {
			if (PruningUtil.ParetoDominates(costAfter, costBefore, consideredMetric)) {
				return true;
			} else {
				// The new plan does not dominate the old plan. We still return true
				// with a certain probability.
				double maxCostDelta = maxCostDelta(costAfter, costBefore, consideredMetric);
				double probability = Math.exp(-maxCostDelta/temperature);
				return random.nextDouble() <= probability;
			}
		} else {
			return PruningUtil.ParetoDominates(costAfter, costBefore, consideredMetric);
		}
	}
	/**
	 * Perform local search starting from given plan. This variant only tries a specified number
	 * of random moves to see whether they yield an improvement. If the corresponding parameters
	 * are set then moves leading to worse plans are accepted with a small probability that
	 * depends on the given temperature and the maximal cost difference over all considered metrics.
	 * Checks for timeouts by comparing with given start millis.
	 * 
	 * @param query				query for which to search plans
	 * @param plan				starting point in plan space
	 * @param planSpace			determines considered scan and join operators
	 * @param costModel			used to compare plans
	 * @param consideredMetric	Boolean flags indicating considered cost metrics
	 * @param nrTries			how many moves to try before assuming a local optimum
	 * @param allowWorsening	whether we move to non-dominating plans
	 * @param temperature		decides probability of moving to non-dominating plans if enabled
	 * @param startMillis		start time in milliseconds to check for timeouts
	 * @param keepEachNth		return each n-th plan encountered on the path
	 * @return					a list of plans on the path to the next local optimum
	 */
	public static List<Plan> localSearch(
			Query query, Plan plan, PlanSpace planSpace, MultiCostModel costModel, 
			boolean[] consideredMetric, int nrTries, boolean allowWorsening, double temperature, 
			long startMillis, int keepEachNth) {
		List<Plan> newPlans = new LinkedList<Plan>();
		Plan improvedPlan = plan;
		boolean improved;
		long nrSteps = 0;
		do {
			improved = false;
			for (int tryCtr=0; tryCtr<nrTries; ++tryCtr) {
				++nrSteps;
				Plan randomMove = randomMove(query, improvedPlan, planSpace, costModel);
				if (nrSteps % keepEachNth == 0) {
					newPlans.add(randomMove.deepMutableCopy());
				}
				if (SAFE_MODE) {
					TestUtil.validatePlan(randomMove, planSpace, costModel, true);
				}
				if (acceptMove(improvedPlan.getCostValuesCopy(), randomMove.getCostValuesCopy()	, consideredMetric, 
						allowWorsening, temperature)) {
					improvedPlan = randomMove;
					improved = true;
					break;
				}
			}
			// Leave loop if timeout occurred
			if (System.currentTimeMillis() - startMillis > TIMEOUT_MILLIS) {
				break;
			}
		} while (improved);
		newPlans.add(improvedPlan.deepMutableCopy());
		return newPlans;
	}
	/**
	 * Obtains all plan nodes in the given plan.
	 * @param plan			the query plan whose nodes we collect
	 * @param parent		the parent node of the current plan
	 * @param isLeftChild	whether the current plan is left sub-plan of its parent plan
	 * @return				a flat list containing all query plan nodes
	 */
	public static List<PathNode> planNodes(Plan plan, PathNode parent, boolean isLeftChild) {
		List<PathNode> nodeList = new LinkedList<PathNode>();
		PathNode currentNode = new PathNode(plan, parent, isLeftChild);
		nodeList.add(currentNode);
		if (plan instanceof JoinPlan) {
			JoinPlan joinPlan = (JoinPlan)plan;
			nodeList.addAll(planNodes(joinPlan.getLeftPlan(), currentNode, true));
			nodeList.addAll(planNodes(joinPlan.getRightPlan(), currentNode, false));
		}
		return nodeList;
	}
	/**
	 * Performs random move in plan space and returns resulting plan.
	 * 
	 * @param query		the query for which we consider plans
	 * @param inputPlan	original plan from which we move
	 * @param planSpace	the plan space in which moves are executed; determines applicable operators
	 * @param costModel	used to calculate cost of new plan after random move
	 * @return			a random neighbor plan of input plan
	 */
	public static Plan randomMove(Query query, Plan inputPlan, 
			PlanSpace planSpace, MultiCostModel costModel) {
		// Applying moves will change the plan - therefore we make a deep copy.
		Plan plan = inputPlan.deepMutableCopy();
		List<PathNode> planNodes = planNodes(plan, null, false);
		// Randomly select plan node to mutate
		int nrPlanNodes = planNodes.size();
		int nodeIndex = random.nextInt(nrPlanNodes);
		PathNode selectedNode = planNodes.get(nodeIndex);
		Plan selectedPlan = selectedNode.plan;
		PathNode parentNode = selectedNode.parent;
		JoinPlan parentPlan = parentNode != null ? (JoinPlan)parentNode.plan : null;
		// Mutate and randomly select one mutation that keeps the parent node consistent!
		List<Plan> localMutations = mutatedPlans(query, selectedPlan, planSpace, costModel);
		if (SAFE_MODE) {
			TestUtil.validatePlans(localMutations, planSpace, costModel, true);
		}
		// Only keep mutations that are consistent with parent if any
		if (parentNode != null) {
			JoinOperator parentOperator = parentPlan.getJoinOperator();
			Iterator<Plan> localMutationIter = localMutations.iterator();
			while (localMutationIter.hasNext()) {
				Plan localMutation = localMutationIter.next();
				// Check if local mutation keeps parent join operator choice consistent
				Plan leftPlan = selectedNode.isLeftChild ? localMutation : parentPlan.getLeftPlan();
				Plan rightPlan = !selectedNode.isLeftChild ? localMutation : parentPlan.getRightPlan();
				// Only keep local mutations where parent remains consistent
				if (!planSpace.joinOperatorApplicable(parentOperator, leftPlan, rightPlan)) {
					localMutationIter.remove();
				}
			}			
		}
		int nrMutations = localMutations.size();
		int selectedMutationIndex = random.nextInt(nrMutations);
		Plan selectedMutation = localMutations.get(selectedMutationIndex);
		// Include mutation into plan and update
		// Check whether mutation occurred at the root
		if (parentNode == null) {
			return selectedMutation;
		} else {
			// Mutation occurred inside tree - need to update tree and cost values.
			if (selectedNode.isLeftChild) {
				parentPlan.setLeftPlan(selectedMutation);
			} else {
				parentPlan.setRightPlan(selectedMutation);
			}
			// Need to update all cost values on path from selected mutation to plan root.
			PathNode nodeToUpdate = parentNode;
			while (nodeToUpdate != null) {
				costModel.updateRoot(nodeToUpdate.plan);
				nodeToUpdate = nodeToUpdate.parent;
			}
			// Plan variable points now to mutated plan tree
			if (SAFE_MODE) {
				TestUtil.validatePlan(plan, planSpace, costModel, true);
			}
			return plan;
		}
	}
	/**
	 * Estimate standard deviation of cost distribution. Generates 20 sample plans, calculates
	 * their cost according to different metrics and estimates the standard deviation for those
	 * values.
	 * 
	 * @param query				the query for which we consider plans
	 * @param consideredMetrics	Boolean flags indicating considered cost metrics
	 * @param planSpace			determines set of applicable operators
	 * @param costModel			used to calculate different cost metric on query plans
	 * @return					estimated standard deviation of plan cost distribution
	 */
	public static double estimateCostStDev(
			Query query, boolean[] consideredMetrics, PlanSpace planSpace, CostModel costModel) {
		int nrConsideredMetrics = MathUtil.nrTrueValues(consideredMetrics);
		int nrSamples = 20;
		double[] costSamples = new double[nrSamples];
		for (int sampleCtr=0; sampleCtr<nrSamples; ++sampleCtr) {
			Plan samplePlan = LocalSearchUtil.randomBushyPlan(query, planSpace);
			costModel.updateAll(samplePlan);
			double accumulatedCost = 0;
			for (int metricCtr=0; metricCtr<NR_COST_METRICS; ++metricCtr) {
				if (consideredMetrics[metricCtr]) {
					accumulatedCost += samplePlan.getCostValue(metricCtr);
				}
			}
			costSamples[sampleCtr] = accumulatedCost/nrConsideredMetrics;
		}
		return MathUtil.aggStDev(costSamples);
	}
	
	/**
	 * Keeps one Pareto-optimal plan for each output data format. If a plan with the same
	 * output data properties as the new plan is already in the plan list then the new plan
	 * is only inserted if it strictly dominates the old plan.
	 * 
	 * @param plans
	 * @param newPlan
	 * @param consideredMetric
	 */
	public static void keepBest(List<Plan> plans, Plan newPlan, boolean[] consideredMetric) {
		double[] newCost = newPlan.cost;
		Iterator<Plan> oldIter = plans.iterator();
		while (oldIter.hasNext()) {
			Plan oldPlan = oldIter.next();
			if (PruningUtil.sameOutputProperties(newPlan, oldPlan)) {
				if (PruningUtil.ParetoDominates(newCost, oldPlan.cost, consideredMetric)) {
					oldIter.remove();
					plans.add(newPlan);
					return;
				}
			}
		}
	}
	/**
	 * Returns for each output data format one Pareto-optimal plan.
	 * 
	 * @param plan
	 * @param planSpace
	 * @param costModel
	 * @return
	 */
	public static List<Plan> ParetoClimbStep(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric) {
		List<Plan> resultPlans = new LinkedList<Plan>();
		resultPlans.add(plan);
		if (plan instanceof JoinPlan) {
			JoinPlan joinPlan = (JoinPlan)plan;
			Plan originalLeftPlan = joinPlan.getLeftPlan();
			Plan originalRightPlan = joinPlan.getRightPlan();
			Relation leftRel = originalLeftPlan.resultRel;
			Relation rightRel = originalRightPlan.resultRel;
			Relation resultRel = plan.resultRel;
			JoinOperator originalJoinOperator = joinPlan.getJoinOperator();
			List<Plan> leftPlans = ParetoClimbStep(query, originalLeftPlan, 
					planSpace, costModel, consideredMetric);
			List<Plan> rightPlans = ParetoClimbStep(query, originalRightPlan, 
					planSpace, costModel, consideredMetric);
			for (Plan leftPlan : leftPlans) {
				for (Plan rightPlan : rightPlans) {
					// Operator change
					for (JoinOperator joinOperator : 
						planSpace.joinOperatorsShuffled(leftPlan, rightPlan)) {
						Plan newPlan = new JoinPlan(leftRel, rightRel, 
								resultRel, leftPlan, rightPlan, joinOperator);
						costModel.updateRoot(newPlan);
						keepBest(resultPlans, newPlan, consideredMetric);
					}
					// Commutativity
					{
						Plan newPlan = new JoinPlan(rightRel, leftRel,
								resultRel, rightPlan, leftPlan, originalJoinOperator);
						costModel.updateRoot(newPlan);
						keepBest(resultPlans, newPlan, consideredMetric);
					}
					// extract second child level in query tree
					boolean leftPlanIsJoin, rightPlanIsJoin;
					Plan leftLeftPlan, leftRightPlan, rightLeftPlan, rightRightPlan;
					if (leftPlan instanceof JoinPlan) {
						leftPlanIsJoin = true;
						JoinPlan leftJoinPlan = (JoinPlan)leftPlan;
						leftLeftPlan = leftJoinPlan.getLeftPlan();
						leftRightPlan = leftJoinPlan.getRightPlan();
					} else {
						leftPlanIsJoin = false;
						leftLeftPlan = null;
						leftRightPlan = null;
					}
					if (rightPlan instanceof JoinPlan) {
						rightPlanIsJoin = true;
						JoinPlan rightJoinPlan = (JoinPlan)rightPlan;
						rightLeftPlan = rightJoinPlan.getLeftPlan();
						rightRightPlan = rightJoinPlan.getRightPlan();
					} else {
						rightPlanIsJoin = false;
						rightLeftPlan = null;
						rightRightPlan = null;
					}
					// Associativity
					if (leftPlanIsJoin) {
						Plan A = leftLeftPlan;
						Plan B = leftRightPlan;
						Plan C = rightPlan;
						Plan newLeftPlan = A;
						JoinOperator rightJoinOp = planSpace.randomJoinOperator(B, C);
						Plan newRightPlan = new JoinPlan(query, B, C, rightJoinOp);
						costModel.updateRoot(newRightPlan);
						JoinOperator joinOp = planSpace.randomJoinOperator(newLeftPlan, newRightPlan);
						Plan newPlan = new JoinPlan(query, newLeftPlan, newRightPlan, joinOp);
						costModel.updateRoot(newPlan);
						keepBest(resultPlans, newPlan, consideredMetric);
					}
					// Left join exchange
					if (leftPlanIsJoin) {
						Plan A = leftLeftPlan;
						Plan B = leftRightPlan;
						Plan C = rightPlan;
						JoinOperator leftJoinOp = planSpace.randomJoinOperator(A, C);
						Plan newLeftPlan = new JoinPlan(query, A, C, leftJoinOp);
						costModel.updateRoot(newLeftPlan);
						JoinOperator joinOp = planSpace.randomJoinOperator(newLeftPlan, B);
						Plan newPlan = new JoinPlan(query, newLeftPlan, B, joinOp);
						costModel.updateRoot(newPlan);
						keepBest(resultPlans, newPlan, consideredMetric);
					}
					// Right join exchange
					if (rightPlanIsJoin) {
						Plan A = leftPlan;
						Plan B = rightLeftPlan;
						Plan C = rightRightPlan;
						JoinOperator rightJoinOp = planSpace.randomJoinOperator(A, C);
						Plan newRightPlan = new JoinPlan(query, A, C, rightJoinOp);
						costModel.updateRoot(newRightPlan);
						JoinOperator joinOp = planSpace.randomJoinOperator(B, newRightPlan);
						Plan newPlan = new JoinPlan(query, B, newRightPlan, joinOp);
						costModel.updateRoot(newPlan);
						keepBest(resultPlans, newPlan, consideredMetric);
					}
				}
			}
		}
		return resultPlans;
	}
	/**
	 * Multi-objective variant of hill climbing. Executes Pareto climb steps until no
	 * further improvement according to all cost metrics is possible and returns the
	 * resulting plan. The resulting plan is a local Pareto optimum. In contrast to the
	 * other functions with a similar functionality, this function is roughly one order
	 * of magnitude faster since it improves the query plan simultaneously in several
	 * sub trees without generating complete intermediate query plans. 
	 * 
	 * @param query
	 * @param plan
	 * @param planSpace
	 * @param costModel
	 * @param consideredMetric
	 * @return
	 */
	public static Plan ParetoClimb(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric) {
		++nrExhaustiveClimbs;
		// Improve via fast Pareto climbing
		boolean climbed = true;
		while (climbed) {
			++nrExhaustiveSteps;
			climbed = false;
			List<Plan> improvedPlans = LocalSearchUtil.ParetoClimbStep(
					query, plan, planSpace, costModel, consideredMetric);
			for (Plan improvedPlan : improvedPlans) {
				if (PruningUtil.ParetoDominates(improvedPlan.cost, plan.cost, consideredMetric)) {
					plan = improvedPlan;
					climbed = true;
				}
			}
		}
		return plan;
	}
	/**
	 * Improve the given plan via local mutations until a Pareto-optimum is reached.
	 * A Pareto-optimum is reached once n random mutations with n the number of query
	 * tables do not yield any plan improvement.
	 * 
	 * @param query				the query being optimized
	 * @param plan				a plan that should be improved via randomized hill climbing
	 * @param planSpace			determines the set of applicable operators
	 * @param costModel			multi-objective query plan cost model
	 * @param consideredMetric	which of the cost metrics should be considered during climbing
	 * @return					a locally Pareto-optimal plan
	 */
	public static Plan randomizedParetoClimb(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric) {
		int nrTables = query.nrTables;
		boolean improving = true;
		// Iterate while the plan is still improving, i.e. a local Pareto-optimum was not reached
		while (improving) {
			improving = false;
			// The number of tables corresponds approximately to the number of join edges
			// for chain, star, and cycle queries.
			int remainingTries = nrTables;
			// Iterate until we found a better plan or until all tries are used up
			while (!improving && remainingTries > 0) {
				// Select a random neighbor of the current plan
				Plan neighbor = randomMove(query, plan, planSpace, costModel);
				// Check whether the neighbor has lower cost than the current plan
				if (PruningUtil.ParetoDominates(neighbor.cost, plan.cost, consideredMetric)) {
					// If the neighbor dominates the current plan then the neighbor becomes the
					// new current plan and we have not found a local Pareto-opimum yet.
					plan = neighbor;
					improving = true;
				}
				remainingTries--;
			}
		}
		return plan;
	}
	
	public static Plan constrainedParetoClimb(Query query, Plan plan, PlanSpace planSpace, 
			MultiCostModel costModel, boolean[] consideredMetric, JoinOperator nextJoinOperator) {
		++nrExhaustiveClimbs;
		// Improve via fast Pareto climbing
		boolean climbed = true;
		while (climbed) {
			++nrExhaustiveSteps;
			climbed = false;
			List<Plan> improvedPlans = LocalSearchUtil.ParetoClimbStep(
					query, plan, planSpace, costModel, consideredMetric);
			for (Plan improvedPlan : improvedPlans) {
				JoinPlan joinPlan = (JoinPlan)improvedPlan;
				JoinOperator joinOperator = joinPlan.getJoinOperator();
				if (planSpace.joinOutputComptatible(joinOperator, nextJoinOperator) &&
						PruningUtil.ParetoDominates(
								improvedPlan.cost, plan.cost, consideredMetric)) {
						plan = improvedPlan;
						climbed = true;
				}
			}
		}
		return plan;
	}
}
