package pgdp.streams;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataScience {
  static PenguinData penguinData;

  public static Stream<Penguin> getDataByTrackId(Stream<PenguinData> stream) {
    Map<String, List<PenguinData>> newMap = stream  //assigning new Map
            .collect(Collectors.groupingBy(PenguinData::getTrackID)); //grouping the stream by trackID
    return newMap.entrySet().stream().map(entry ->{ //assign new List with entrySet
      String trackID = entry.getKey();  //get key(trackID) of the entry set
      List<Geo> newGeo = entry.getValue().stream().map(PenguinData::getGeom).collect(Collectors.toList());  //get value(List<Geo>) of the entry set(); getGeom for each; collect in a list
      return new Penguin(newGeo, trackID);  //returning new Penguin with mapped geo and trackID for each entry
    });
  }

  public static void outputPenguinStream() {
  long counter = getDataByTrackId(CSVReading.processInputFile()).count();
    System.out.println(counter);
    getDataByTrackId(CSVReading.processInputFile())
            .sorted(Comparator.comparing(x -> x.getTrackID()))
            .forEach(x -> {
              System.out.println(x.toStringUsingStreams());
            });
  }

  public static void outputLocationRangePerTrackid(Stream<PenguinData> stream) {
    Map<String, List<PenguinData>> newMap = stream  //assigning new Map
        .collect(Collectors.groupingBy(PenguinData::getTrackID)); //grouping the stream by trackID
    newMap.entrySet().stream()
            .sorted(Comparator.comparing(x -> x.getKey()))  // sorting by key / TrackID
            .forEach(entry ->{  //assign new List with entrySet
        System.out.println(entry.getKey()); //print out trackID
        StringBuilder sb = new StringBuilder();
          sb.append("Min Longitude: "); //appending Min, Max and Avg for longitude and latitude
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLongitude()).min().getAsDouble());
          sb.append(" Max Longitude: ");
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLongitude()).max().getAsDouble());
          sb.append(" Avg Longitude: ");
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLongitude()).average().getAsDouble());
          sb.append(" Min Latitude: ");
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLatitude()).min().getAsDouble());
          sb.append(" Max Latitude: ");
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLatitude()).max().getAsDouble());
          sb.append(" Avg Latitude: ");
          sb.append(entry.getValue().stream().mapToDouble(x -> x.geom.getLatitude()).average().getAsDouble());
      System.out.println(sb); //print out final SB
    });
  }

  public static void main(String[] args) {
    List<Geo> newGeo = new LinkedList<>();
    newGeo.add(new Geo(10, 3));
    newGeo.add(new Geo(6, 7));
    newGeo.add(new Geo(4, 8));
    newGeo.add(new Geo(12, 14));
    Penguin newPenguin = new Penguin(newGeo, "Pingu");
    System.out.print(newPenguin.toStringUsingStreams());
  }
}