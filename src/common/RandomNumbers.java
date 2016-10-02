package common;

import java.util.Random;

public class RandomNumbers {
	// using only one random generator in the entire code is useful since we can fix the seed for testing
	public static Random random = new Random();
}
