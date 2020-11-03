package xxx.project.util;

import java.util.*;
import java.util.stream.Collectors;

public class MultiTimeWindowIndex {
    class Window {
        public int level;
        public int fromBaseIndex;
        public int toBaseIndex;
        public Map<Integer, Map<String, AggrValues>> queryId2Result = new HashMap<>();

        public Window(int level, int fromBase, int toBase) {
            this.level = level;
            this.fromBaseIndex = fromBase;
            this.toBaseIndex = toBase;
        }

        public Window(Window oldWin, Window newWin, Map<Integer, WarehouseQuery> queryMap,
                      int baseIntervalLength,
                      int maxQueryIntervalLength) {
            this.level = oldWin.level + 1;
            this.fromBaseIndex = oldWin.fromBaseIndex;
            this.toBaseIndex = newWin.toBaseIndex;

            queryId2Result = new HashMap<>(newWin.queryId2Result);
            for (Map.Entry<Integer, Map<String, AggrValues>> entry : oldWin.queryId2Result.entrySet()) {
                int queryId = entry.getKey();
                WarehouseQuery query = queryMap.get(queryId);
                int queryInterval = query.getIntervalLength() / baseIntervalLength;

                if ((fromBaseIndex + queryInterval) % maxQueryIntervalLength > toBaseIndex) {
                    Map<String, AggrValues> oldValuesMap = entry.getValue();
                    Map<String, AggrValues> newValuesMap = queryId2Result.get(queryId);
                    for(Map.Entry<String, AggrValues> oldEntry : oldValuesMap.entrySet()) {
                        String key = oldEntry.getKey();
                        AggrValues oldValues = oldEntry.getValue();

                        AggrValues newValues = newValuesMap.get(key);
                        if (newValues == null) {
                            newValues = new AggrValues(query.getMeasure(), query.getkOfTopK());
                            newValuesMap.put(key, newValues);
                        }
                        newValues.merge(oldValues);
                    }
                }
            }
        }

        public void insertQuery(int queryId) {
            queryId2Result.put(queryId, new HashMap<>());
        }

        public void updateResult(SpatioTextualObject object, Map<Integer, WarehouseQuery> queryMap) {
            for (Map.Entry<Integer, Map<String, AggrValues>> entry : queryId2Result.entrySet()) {
                int queryId = entry.getKey();
                Map<String, AggrValues> result = entry.getValue();
                WarehouseQuery query = queryMap.get(queryId);
                String objGroup = object.getGroupBy(query.getGroupBy());
                AggrValues values = result.get(objGroup);
                if (values == null) {
                    values = new AggrValues(query.getMeasure(), query.getkOfTopK());
                    result.put(objGroup, values);
                }
                values.update(object);
            }
        }
        
        public void removeQueries(Set<Integer> inactiveQueries) {
            for (int queryId : inactiveQueries)
                queryId2Result.remove(queryId);
        }
    }

    class MTIndex {
        public int baseLength;
        public int maxQueryIntervalLength;
        public int maxLevel;
        public Map<Integer, Deque<Window>> level2WindowDeque = new HashMap<>();
        public Map<Integer, WarehouseQuery> queryMap = new HashMap<>();
        public Set<Integer> inactiveQueryIds = new HashSet<>();

        private long lastUpdateTime = System.currentTimeMillis();
        private int INACTIVE_LIMIT = 50000;
        private int headIndex;
        private int tailIndex;

        public MTIndex(int baseLength, int maxQueryIntervalLength) {
            this.baseLength = baseLength;
            this.maxQueryIntervalLength = maxQueryIntervalLength;

            maxLevel = 0;
            int base = baseLength, maxLen = maxQueryIntervalLength;
            while (base * 2 <= maxLen) {
                ++maxLevel;
                base *= 2;
            }
            headIndex = 0;
            tailIndex = maxQueryIntervalLength - 1;

            int numWindows = maxQueryIntervalLength;
            int winLen = 1;
            for (int i = 0; i <= maxLevel; ++i) {
                level2WindowDeque.put(i, new LinkedList<>());
                Deque<Window> deque = level2WindowDeque.get(i);
                for (int j = 0; j < numWindows; ++j) {
                    deque.add(new Window(i, j * winLen, j * winLen + winLen - 1));
                }
                numWindows /= 2;
                winLen *= 2;
            }
        }

        public void insertQuery(WarehouseQuery query) {
            queryMap.put(query.getQueryId(), query);
            for (int i = 0; i <= maxLevel; ++i) {
                Deque<Window> deque = level2WindowDeque.get(i);
                Window window = deque.getLast();
                if (window.toBaseIndex == tailIndex)
                    window.insertQuery(query.getQueryId());
            }
        }
        
        public void deleteQuery(WarehouseQuery query) {
            int id = query.getQueryId();
            if (!queryMap.containsKey(id))
                return;

            inactiveQueryIds.add(id);
            if (inactiveQueryIds.size() >= INACTIVE_LIMIT)
                deleteInactiveQueries();

            for (int queryId : inactiveQueryIds)
                queryMap.remove(queryId);
            inactiveQueryIds.clear();
        }
        
        private void deleteInactiveQueries() {
            for (int i = 0; i <= maxLevel; ++i) {
                Deque<Window> windows = level2WindowDeque.get(i);
                for (Window window : windows)
                    window.removeQueries(inactiveQueryIds);
            }
        }

        public void processObject(SpatioTextualObject object) {
            long curTime = System.currentTimeMillis();
            double elaspedMins = (curTime - lastUpdateTime) / (1000 * 60.0);
            if (elaspedMins > baseLength) {
                for (int i = 0; i <= maxLevel; ++i) {
                    Deque<Window> deque = level2WindowDeque.get(i);
                    Window window = deque.getFirst();
                    if (window.fromBaseIndex == headIndex) {
                        deque.removeFirst();
                        headIndex = (headIndex + 1) % maxQueryIntervalLength;
                        tailIndex = (tailIndex + 1) % maxQueryIntervalLength;
                    }
                }

                Window newBaseWindow = new Window(0, (tailIndex+1) % maxQueryIntervalLength,
                        (tailIndex+1) % maxQueryIntervalLength);
                int lowerLevelToIndex = level2WindowDeque.get(0).getLast().toBaseIndex;
                level2WindowDeque.get(0).addLast(newBaseWindow);

                for (int i = 1; i <= maxLevel; ++i) {
                    Deque<Window> deque = level2WindowDeque.get(i);
                    if (deque.getLast().toBaseIndex != lowerLevelToIndex)
                        break;

                    Window lastWindow = level2WindowDeque.get(i - 1).removeLast();
                    Window secondLastWindow = level2WindowDeque.get(i - 1).removeLast();
                    Window newWindow = new Window(secondLastWindow, lastWindow, queryMap,
                            baseLength, maxQueryIntervalLength);
                    deque.addLast(newWindow);

                    level2WindowDeque.get(i - 1).addLast(secondLastWindow);
                    level2WindowDeque.get(i - 1).addLast(lastWindow);
                }
            } else {
                for (int i = 0; i <= maxLevel; ++i) {
                    Deque<Window> deque = level2WindowDeque.get(i);
                    Window window = deque.getLast();
                    if (window.toBaseIndex == maxQueryIntervalLength - 1)
                        window.updateResult(object, queryMap);
                }
            }
            lastUpdateTime = System.currentTimeMillis();
        }

        public String getQueryResult(int queryId) {
            if (!queryMap.containsKey(queryId))
                return "";

            List<Window> resultWindows = new ArrayList<>();
            Set<Integer> indexSet = new HashSet<>();
            for (int i = maxLevel; i >= 0; --i) {
                List<Window> wins = level2WindowDeque.get(i).stream().
                        filter(w -> !indexSet.contains(w.fromBaseIndex) && !indexSet.contains(w.toBaseIndex)).
                        collect(Collectors.toList());
                resultWindows.addAll(wins);
                for (Window win : wins) {
                    for (int j = win.fromBaseIndex; j != win.toBaseIndex; j = (j + 1) % maxQueryIntervalLength)
                        indexSet.add(j);
                    indexSet.add(win.toBaseIndex);
                }
                if (indexSet.size() == maxQueryIntervalLength)
                    break;
            }

            Map<String, AggrValues> resultMap = new HashMap<>();
            WarehouseQuery query = queryMap.get(queryId);
            for (Window window : resultWindows) {
                if (!window.queryId2Result.containsKey(queryId))
                    continue;
                Map<String, AggrValues> winMap = window.queryId2Result.get(queryId);
                for(Map.Entry<String, AggrValues> winEntry : winMap.entrySet()) {
                    String key = winEntry.getKey();
                    AggrValues oldValues = winEntry.getValue();

                    AggrValues newValues = resultMap.get(key);
                    if (newValues == null) {
                        newValues = new AggrValues(query.getMeasure(), query.getkOfTopK());
                        resultMap.put(key, newValues);
                    }
                    newValues.merge(oldValues);
                }
            }

            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, AggrValues> entry : resultMap.entrySet()) {
                result.append(entry.getKey() + ": "  + entry.getValue().getResult(query.getkOfTopK()) + "\n");
            }

            return result.toString();
        }
    }

    private MTIndex baseWinSize1Index = new MTIndex(1, 10);
    private MTIndex baseWinSize5Index = new MTIndex(5, 20);
    private MTIndex baseWinSize10Index = new MTIndex(10, 30);

    public void insertQuery(WarehouseQuery query) {
        int queryIntervalLen = query.getIntervalLength();
        if (queryIntervalLen < 5)
            baseWinSize1Index.insertQuery(query);
        else if (queryIntervalLen < 10)
            baseWinSize5Index.insertQuery(query);
        else
            baseWinSize10Index.insertQuery(query);
    }

    public void deleteQuery(WarehouseQuery query) {
        baseWinSize1Index.deleteQuery(query);
        baseWinSize5Index.deleteQuery(query);
        baseWinSize10Index.deleteQuery(query);
    }

    public void processObject(SpatioTextualObject object) {
        baseWinSize1Index.processObject(object);
        baseWinSize5Index.processObject(object);
        baseWinSize10Index.processObject(object);
    }

    public String getQueryResult(int queryId) {
        String result = "";
        result = baseWinSize1Index.getQueryResult(queryId);
        if (result.length() > 0)
            return result;
        
        result = baseWinSize5Index.getQueryResult(queryId);
        if (result.length() > 0)
            return result;

        result = baseWinSize10Index.getQueryResult(queryId);
        return result;
    }
}
