package de.hs.osnabrueck.tenbeitel.mr.association.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;

public class AprioriUtils {
	private static ItemSetWritable itemSetWritable = new ItemSetWritable();

	public static Set<ItemSetWritable> generateCandidates(Set<ItemSetWritable> itemSets, int actualItemSetLength) {
		Set<ItemSetWritable> kItemSet = new HashSet<ItemSetWritable>();
		if (actualItemSetLength > 2) {
			for (ItemSetWritable firstItemSet : itemSets) {
				ItemSetWritable actualItemSet = new ItemSetWritable(firstItemSet);
				for (ItemSetWritable itemSet : itemSets) {
					if (!actualItemSet.equals(itemSet)) {
						String[] firstArray = actualItemSet.getStringItemSet();
						String[] secondArray = itemSet.getStringItemSet();

						String[] subsetFirstArray = Arrays.copyOfRange(firstArray, 0, firstArray.length - 1);
						String[] subsetSecondArray = Arrays.copyOfRange(secondArray, 0, secondArray.length - 1);

						if (Arrays.deepEquals(subsetFirstArray, subsetSecondArray)) {
							String[] candidate = Arrays.copyOf(firstArray, firstArray.length + 1);
							candidate[firstArray.length + 1] = secondArray[secondArray.length];

							Arrays.sort(candidate);

							if (checkCandidateSubset(itemSets, generateCandidateSubset(candidate))) {
								itemSetWritable = new ItemSetWritable(candidate);
								kItemSet.add(itemSetWritable);
							}

						}
					}
				}
			}
		} else {
			for (ItemSetWritable firstItemSet : itemSets) {
				ItemSetWritable actualItemSet = new ItemSetWritable(firstItemSet);
				for (ItemSetWritable itemSet : itemSets) {
					System.out.println("actual");
					System.out.println(actualItemSet);
					System.out.println("itemSet");
					System.out.println(itemSet.toString());

					if (!actualItemSet.equals(itemSet)) {
						itemSetWritable = new ItemSetWritable(
								new String[] { actualItemSet.getStringItemSet()[0], itemSet.getStringItemSet()[0] });
						System.out.println("New itemset");
						System.out.println(itemSetWritable.toString());
						kItemSet.add(itemSetWritable);
					}
				}
			}
		}
		return kItemSet;
	}

	private static String[][] generateCandidateSubset(String[] candidate) {
		String[][] comb = new String[candidate.length][candidate.length - 1];

		for (int i = 0; i < candidate.length; i++) {
			for (int j = 0; j < candidate.length - 1; j++) {
				if (i != j) {
					comb[i][j] = candidate[j];
				} else {
					comb[i][j] = candidate[candidate.length - 1];
				}
			}
		}

		return comb;

	}

	private static boolean checkCandidateSubset(Set<ItemSetWritable> itemSet, String[][] subsets) {
		for (int i = 0; i < subsets.length; i++) {
			itemSetWritable = new ItemSetWritable(subsets[i]);
			if (!itemSet.contains(itemSetWritable)) {
				return false;
			}
		}
		return true;
	}

	public static boolean arrayContainsSubset(String[] array, String[] subset) {
		List<String> arrayList = Arrays.asList(array);
		List<String> subsetList = Arrays.asList(subset);
		return arrayList.containsAll(subsetList);
	}

	public static boolean arrayContainsSubset(List<String> array, String[] subset) {
		List<String> subsetList = Arrays.asList(subset);
		return array.containsAll(subsetList);
	}
}
