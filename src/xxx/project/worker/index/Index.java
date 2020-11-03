package xxx.project.worker.index;

import xxx.project.util.SpatioTextualObject;
import xxx.project.util.WarehouseQuery;

import java.io.IOException;
import java.util.Map;

public interface Index {
    void addObject(SpatioTextualObject object);
    void readMaterializedCubesFromFile(String cubeFilePath) throws IOException, ClassNotFoundException;
    Map<String, Integer> processCountQuery(WarehouseQuery query);
    Map<String, Map<String, Integer>> processWordCountQuery(WarehouseQuery query);
}
