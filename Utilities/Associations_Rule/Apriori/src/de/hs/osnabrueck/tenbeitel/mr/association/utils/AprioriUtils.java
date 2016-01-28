package de.hs.osnabrueck.tenbeitel.mr.association.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSet;
import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;

public class AprioriUtils {

	public static Set<ItemSet> generateCandidates(Set<ItemSet> itemSets, int actualItemSetLength) {
		Set<ItemSet> kItemSet = new HashSet<ItemSet>();
		if (actualItemSetLength > 2) {
			for (ItemSet firstItemSet : itemSets) {
				for (ItemSet itemSet : itemSets) {
					if (!firstItemSet.equals(itemSet)) {
						String[] firstArray = firstItemSet.get();
						String[] secondArray = itemSet.get();

						String[] subsetFirstArray = Arrays.copyOfRange(firstArray, 0, firstArray.length - 1);
						String[] subsetSecondArray = Arrays.copyOfRange(secondArray, 0, secondArray.length - 1);

						if (Arrays.deepEquals(subsetFirstArray, subsetSecondArray)) {
							String[] candidate = Arrays.copyOf(firstArray, firstArray.length + 1);
							candidate[firstArray.length + 1] = secondArray[secondArray.length];

							Arrays.sort(candidate);

							if (checkCandidateSubset(itemSets, generateCandidateSubset(candidate))) {
								itemSet = new ItemSet(candidate);
								kItemSet.add(itemSet);
							}

						}
					}
				}
			}
		} else {
			for (ItemSet firstItemSet : itemSets) {
				for (ItemSet itemSet : itemSets) {
					System.out.println(Arrays.toString(firstItemSet.get()));
					System.out.println(Arrays.toString(itemSet.get()));
					if (!Arrays.deepEquals(firstItemSet.get(), itemSet.get())) {
						itemSet = new ItemSet(new String[] { firstItemSet.get()[0], itemSet.get()[0] });
						System.out.println("New itemset");
						System.out.println(itemSet.toString());
						kItemSet.add(itemSet);
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

	private static boolean checkCandidateSubset(Iterable<ItemSet> itemSet, String[][] subsets) {
		Set<ItemSet> set = Sets.newHashSet(itemSet);

		for (int i = 0; i < subsets.length; i++) {
			ItemSet item = new ItemSet(subsets[i]);
			if (!set.contains(item)) {
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
