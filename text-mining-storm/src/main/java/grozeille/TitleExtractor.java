package grozeille;

import java.io.InputStream;
import java.util.List;

/**
 * Created by Mathias on 11/01/2015.
 */
public interface TitleExtractor {
    List<String> findTitles(InputStream stream) throws Exception;
}
