package com.sp;

import java.util.*;

public class ImperativeStyle {

    public static void main(String[] args) {

        var names = List.of("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil");
        var fnames = nameGraterThanFilter(names, 4);
        System.out.println("NAMES : %s".formatted(fnames));
    }

    private static List<String> nameGraterThanFilter(List<String> names, int i) {
        var filterNameList = new ArrayList<String>();
        for (var name : names) {
            if (name.length() <= i || filterNameList.contains(name.toUpperCase()))
                continue;
            filterNameList.add(name.toUpperCase());
        }
        Collections.sort(filterNameList, Comparator.reverseOrder());
        return filterNameList;
    }
}
