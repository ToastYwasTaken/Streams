package pgdp.streams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CollectionCertStoreParameters;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    public static Stream<Customer> processInputFileCustomer()  {
        try {
            return Files.lines(Path.of(baseDataDirectory.toString().concat("/customer.tbl")))    //Customer Path
                    .map(x -> {
                String[] stringarr = x.split("\\|");
                return new Customer(Integer.parseInt(stringarr[0]), stringarr[1].toCharArray(),
                        Integer.parseInt(stringarr[3]), stringarr[4].toCharArray(),
                        Float.parseFloat(stringarr[5]), stringarr[6], stringarr[7].toCharArray());
            });
            //10|Customer#000000010|6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2|5|15-741-346-9870|2753.54|HOUSEHOLD|es regular deposits haggle. fur|
        } catch (IOException e){    //handling empty files
            System.out.println("The Stream<Customer> is empty");
        }
        return Stream.of();
    }

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            return Files.lines(Path.of(baseDataDirectory.toString().concat("/lineitem.tbl")))   //LineItem Path
                .map(x -> {
            String[] stringarr = x.split("\\|");
            return new LineItem(Integer.parseInt(stringarr[0]), Integer.parseInt(stringarr[1]),
                    Integer.parseInt(stringarr[2]), Integer.parseInt(stringarr[3]),
                    Integer.parseInt(stringarr[4])*100, Float.parseFloat(stringarr[5]),
                    Float.parseFloat(stringarr[6]), Float.parseFloat(stringarr[7]),
                    stringarr[8].charAt(0), stringarr[9].charAt(0),
                    LocalDate.parse(stringarr[10]), LocalDate.parse(stringarr[11]),
                    LocalDate.parse(stringarr[12]), stringarr[13].toCharArray(),
                    stringarr[14].toCharArray(), stringarr[15].toCharArray());
        });
        } catch (IOException e){    //handling empty files
            System.out.println("The Stream<InputFile> is empty");
        }
        return Stream.of();
        //3|20|10|2|49|45080.98|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|

    }

    public static Stream<Order> processInputFileOrders() {
            try {
                return Files.lines(Path.of(baseDataDirectory.toString().concat("/orders.tbl"))) //Orders Path
                    .map(x -> {
                String[] stringarr = x.split("\\|");
                        //System.out.println(Arrays.toString(stringarr));
                return new Order(Integer.parseInt(stringarr[0]), Integer.parseInt(stringarr[1]),
                        stringarr[2].charAt(0), Float.parseFloat(stringarr[3]),
                        LocalDate.parse(stringarr[4]), stringarr[5].toCharArray(),
                        stringarr[6].toCharArray(), Integer.parseInt(stringarr[7]), stringarr[8].toCharArray());
                });
            } catch (IOException e){    //handling empty files
                System.out.println("The Stream<Order> is empty");
            }
            return Stream.of();
    }

    public static long getAverageQuantityPerMarketSegment(String marketsegment){
        if(processInputFileOrders().count() == 0 || processInputFileCustomer().count() == 0 || processInputFileLineItem().count() == 0){
            System.out.println("One of the Streams is empty");
            return -1;
        }
        Map<Integer, String> marketSegmentMapCustKey = processInputFileCustomer()
                .collect(Collectors.toMap(customer -> customer.custKey,
                        customer -> customer.mktsegment));
        Map<Integer, String> marketSegmentMapOrderKey = processInputFileOrders()
                .collect(Collectors.toMap(order -> order.orderKey,
                        customer -> marketSegmentMapCustKey.get(customer.custKey)));

        if(marketSegmentMapOrderKey.isEmpty() || marketSegmentMapCustKey.isEmpty()){
            System.out.println("MarketSegmentMapOrderKey or marketSegmentMapCustKey is empty");
        }

        long count = 0; //counting only if marketsegment is in there
        long count2 = 0;    //counting all elements
        Iterator<LineItem> fileLineIterator = processInputFileLineItem().iterator();
        while(fileLineIterator.hasNext()){
            LineItem current = fileLineIterator.next();
            if(marketSegmentMapOrderKey.get(current.orderKey).equals(marketsegment)){
                count++;
                if(!fileLineIterator.hasNext()){
                    System.out.println("UnexpectedExceptionError: next Element of fileLineIterator is null");
                    break;
                }
                count2 += current.quantity;
            }
        }

        //System.out.println("Count1: " + count + " Count2: " + count2);
        if (count == 0 || count2 == 0){
            System.out.println("Count is 0");
            count = -1;
            return count;
        }
        return count2 / count;  //dividing for average
    }

    public Database() {

    }

    public static void main(String[] args) {
//        processInputFileOrders();
//    String newmktsgmt = "AUTOMOBILE";
//    getAverageQuantityPerMarketSegment(newmktsgmt);
    }
}
