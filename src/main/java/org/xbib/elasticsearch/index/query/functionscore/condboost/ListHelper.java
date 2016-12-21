package org.xbib.elasticsearch.index.query.functionscore.condboost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by miroslaw.piatkowski on 12/21/2016.
 */
class ListHelper {
    static <T extends Comparable<? super T>> boolean equalLists(Collection<T> one, Collection<T> two) {
        if (one == null && two == null) {
            return true; // -------------->
        }

        if ((one == null && two != null)
                || one != null && two == null
                || one.size() != two.size()) {
            return false;
        }

        //to avoid messing the order of the lists we will use a copy
        List<T> oneList = new ArrayList<>(one);
        List<T> twoList = new ArrayList<>(two);

        Collections.sort(oneList);
        Collections.sort(twoList);
        return oneList.equals(twoList);

    }
}
