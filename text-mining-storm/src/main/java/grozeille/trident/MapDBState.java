package grozeille.trident;

import org.apache.commons.lang.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import storm.trident.state.map.IBackingMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Mathias on 18/01/2015.
 */
public class MapDBState<T> implements IBackingMap<T> {

    private DB db;
    private Map dbMap;


    public void MapDBState(String path){
        db = DBMaker.newFileDB(new File(path))
                .closeOnJvmShutdown()
                .cacheLRUEnable()
                .make();
        dbMap = db.getTreeMap("state");
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> result = new ArrayList<>();
        
        
        for(List<Object> k : keys){
            String key = StringUtils.join(k, "-");
            result.add((T)dbMap.get(key));
        }

        return result;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

        for(int cpt = 0; cpt < keys.size(); cpt++){
            String key = StringUtils.join(keys.get(cpt), "-");
            Object o = vals.get(cpt);

            dbMap.put(key, o);
        }
    }
}
