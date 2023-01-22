package cs448;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;
import java.util.Dictionary;
import java.util.HashMap;
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
    }

    void load_mapdb(String file_path) throws IOException{
        /** Put your code here **/
    }

    String select_file(String file_path, String key, String[] column_names) throws IOException{
        /** Put your code here **/

        return "";
    }

    String select_mainmemory(String key, String[] column_names){
        /** Put your code here **/
        return "";
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
