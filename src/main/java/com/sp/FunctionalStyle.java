package com.sp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionalStyle {

    public static void main(String[] args) {

        var names = List.of("aman", "aniket", "sonhan", "nikhil", "amle", "nikhil");
        var fnames = nameGraterThanFilter(names, 4);
        System.out.println("NAMES : %s".formatted(fnames));
    }

    private static List<String> nameGraterThanFilter(List<String> names, int i) {
        return names.parallelStream()
                .filter(s -> s.length() > i)
                .distinct()
                .map(String::toUpperCase)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }


}
