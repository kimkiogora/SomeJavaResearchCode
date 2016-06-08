/**
 * Test memory map
*/
import com.kiogora.memorymap.MemoryMap;
class TestMemoryMap{
    public static void main(String...args){
      try {
        String cache_data = MemoryMap.getMemoryContents(logging, props.getQueueSaveFile());
        System.out.println(cache_data);
      } catch(IOException e){
            logging.error(Utilities.getLogPreString()
                    + "Unable to load saved queues...");
      }
    }
}
