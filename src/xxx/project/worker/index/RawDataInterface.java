package xxx.project.worker.index;

import xxx.project.util.SpatioTextualObject;
import xxx.project.util.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface RawDataInterface {
    void addObject(SpatioTextualObject object);
    Optional<List<SpatioTextualObject>> getObjects();
    Optional<List<SpatioTextualObject>> getObjectsByRange(Tuple2<Double, Double> cornerFrom,
                                                          Tuple2<Double, Double> cornerTo);
    Optional<List<SpatioTextualObject>> getObjectsByTerms(Set<String> terms);
    Optional<List<SpatioTextualObject>> getObjectsByTime(String startTime, String endTime);
    Optional<List<SpatioTextualObject>> getObjectsByRangeTerms(Tuple2<Double, Double> cornerFrom,
                                                               Tuple2<Double, Double> cornerTo,
                                                               Set<String> terms);
    Optional<List<SpatioTextualObject>> getObjectsByTimeRange(String startTime,
                                                              String endTime,
                                                              Tuple2<Double, Double> cornerFrom,
                                                              Tuple2<Double, Double> cornerTo);
    Optional<List<SpatioTextualObject>> getObjectsByTimeTerms(String startTime,
                                                              String endTime,
                                                              Set<String> terms);
    Optional<List<SpatioTextualObject>> getObjectsByTimeRangeTerms(String startTime,
                                                                   String endTime,
                                                                   Tuple2<Double, Double> cornerFrom,
                                                                   Tuple2<Double, Double> cornerTo,
                                                                   Set<String> terms);
    void removeDataAtHour(String hour);
    int getNumObjects();
    List<String> getHours();
}
