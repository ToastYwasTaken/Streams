package pgdp.streams;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Penguin {
  private List<Geo> locations;
  private String trackID;
  PenguinData penguinData;
  Geo geom;

  public Penguin(List<Geo> locations, String trackID) {
    this.locations = locations;
    this.trackID = trackID;
  }

  @Override
  public String toString() {
    return "Penguin{" +
        "locations=" + locations +
        ", trackID='" + trackID + '\'' +
        '}';
  }

  public List<Geo> getLocations() {
    return locations;
  }

  public String getTrackID() {
    return trackID;
  }

  public String toStringUsingStreams() {//
    String str;
    str = "Penguin{locations=[".concat(getLocations().stream() //add String; new stream on locations
            .sorted((p1, p2) -> {   //sorting by latitude, then longitude
              int temp = Double.compare((p1.getLatitude()),(p2.getLatitude())); //sort by latitude
              if(temp == 0){
                temp = Double.compare((p1.getLongitude()), (p2.getLongitude()));  //sort by longitude
              }
              return -temp;  //returns -1 / 0 / 1; returns -temp because "absteigende Reihenfolge"
            })
                    .map(p -> p.toString()).collect(Collectors.joining(", "))) //mapping to String; collecting the objects and divide them by ", "
            .concat("], trackID='").concat(getTrackID()).concat("'}");  // get trackID and add other missing Strings
    return str;
  }
}
