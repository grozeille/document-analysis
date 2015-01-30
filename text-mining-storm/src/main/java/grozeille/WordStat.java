package grozeille;

import org.apache.commons.lang.mutable.MutableInt;

/**
 * Created by mathias on 20/01/2015.
 */
public class WordStat {
    private final MutableInt documents = new MutableInt();
    private final MutableInt count = new MutableInt();

    public MutableInt getDocuments() {
        return documents;
    }

    public MutableInt getCount() {
        return count;
    }
}
