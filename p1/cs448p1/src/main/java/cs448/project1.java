package cs448;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentMap;


public class project1 {

    /* Data structure for Main-memory back-end */
    HashMap<String,Dictionary> mm_map = new HashMap<String,Dictionary>();

    /* Data structure for MapDB persistent storage*/
    String dbfile = "data.db";
    DB db = DBMaker.fileDB(dbfile).make();

    // use this for MapDB storage
    ConcurrentMap mapdb = db.hashMap("map").make();

    void load_mainmemory(String file_path) throws IOException {
        /** Put your code here **/
        // open the file
        FileReader fileReader = new FileReader(file_path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        // read the first header line
        String firstLine = bufferedReader.readLine();
        String[] headers = firstLine.split("\t");

        // read the remaining line
        String line;
        String returnLine = "";
        while((line = bufferedReader.readLine()) != null) { // read each line in the file
            String[] fields = line.split("\t");
            Dictionary<String, String> valueDic = new Hashtable<String, String>();
            int i = 0;
            for(String field : fields){ //put all attribute of an element into the dictionary
                if(i == 0) {    // skip the key
                    i++;
                    continue;
                }
                valueDic.put(headers[i], field);
                i++;
            }
            mm_map.put(fields[0], valueDic);    // put item into the hashmap
        }
    }

    void load_mapdb(String file_path) throws IOException{
        /** Put your code here **/
    }

    String select_file(String file_path, String key, String[] column_names) throws IOException{
        /** Put your code here **/
        // open the file
        FileReader fileReader = new FileReader(file_path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        // read the first header line
        String firstLine = bufferedReader.readLine();
        String[] headers = firstLine.split("\t");

        // read the remaining line
        String line;
        String returnLine = "";
        while((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("\t");
            if(fields[0].equals(key)) { // select the key
                returnLine = line;  //selected line
            }
        }

        // return formatting
        if(!returnLine.isEmpty()) { //key is found
            // build return string
            String returnStr = "";
            for(String headerRet : column_names){   //select output columns
                int index = -1;
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].equals(headerRet)) {
                        index = i;
                        break;
                    }
                }
                if(index==-1) { // requested column does not exist
                    return "";
                }
                returnStr += headerRet + "=" + returnLine.split("\t")[index] + "\t";
            }
            return returnStr.trim();
        } else {    //key not found
            return "";
        }
    }

    String select_mainmemory(String key, String[] column_names){
        /** Put your code here **/
        try{
            Dictionary returnDic = mm_map.get(key);
            String returnStr = "";
            for(String headerRet : column_names){
                returnStr += headerRet + "=" + returnDic.get(headerRet) + "\t";
            }
            return returnStr.trim();
        } catch (NullPointerException e) {  // key does not exist
            e.printStackTrace();
            System.out.println("NULL POINTER!");
            return "";
        }
    }
    String select_mapdb(String key, String[] column_names){
        /** Put your code here **/
        return "";
    }

    int fastestLoad(){
        // 1: Main-memory Load
        // 2: MapDB Load
        /** Put your code here **/
        return -1;
    }

    int fastestSelect(){
        // 0: File Select
        // 1: Main-memory Select
        // 2: MapDB Select
        /** Put your code here **/
        return -1;
    }
}
