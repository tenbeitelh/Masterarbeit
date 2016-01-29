package de.hs.osnabrueck.tenbeitel.mr.association.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSet;

public class AprioriUtils {

	public static Set<String[]> generateCandidates(Set<String[]> itemSets, int actualItemSetLength) {
		List<String[]> itemSetsList = new ArrayList<String[]>(itemSets);
		Set<String[]> kItemSet = new HashSet<String[]>();
		if (actualItemSetLength > 2) {
			for (int i = 0; i < itemSetsList.size(); i++) {
				for (int j = 0; j < itemSetsList.size(); j++) {

					if (!itemSetsList.get(i).equals(itemSetsList.get(j))) {
						String[] firstArray = itemSetsList.get(i);
						String[] secondArray = itemSetsList.get(j);

						String[] subsetFirstArray = Arrays.copyOfRange(firstArray, 0, firstArray.length - 1);
						String[] subsetSecondArray = Arrays.copyOfRange(secondArray, 0, secondArray.length - 1);

						if (Arrays.deepEquals(subsetFirstArray, subsetSecondArray)) {
							String[] candidate = Arrays.copyOf(firstArray, firstArray.length + 1);
							candidate[firstArray.length + 1] = secondArray[secondArray.length];

							Arrays.sort(candidate);

							if (checkCandidateSubset(itemSetsList, generateCandidateSubset(candidate))) {
								kItemSet.add(candidate);
							}

						}
					}
				}
			}
		} else {
			for (int i = 0; i < itemSetsList.size(); i++) {
				for (int j = 0; j < itemSetsList.size(); j++) {
					System.out.println(Arrays.toString(itemSetsList.get(i)));
					System.out.println(Arrays.toString(itemSetsList.get(j)));
					if (!Arrays.deepEquals(itemSetsList.get(i), itemSetsList.get(j))) {
						String[] newItemSet = new String[] { itemSetsList.get(i)[0], itemSetsList.get(j)[0] };
						System.out.println("New itemset");
						System.out.println(Arrays.toString(newItemSet));
						kItemSet.add(newItemSet);
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

	private static boolean checkCandidateSubset(List<String[]> itemSet, String[][] subsets) {
		for (int i = 0; i < subsets.length; i++) {

			if (!itemSet.contains(subsets[i])) {
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
