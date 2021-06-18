import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class test {
    public static void main(String[] args) {
        // 1. 使用 Iterator 遍历 HashMap EntrySet
        Map< Integer, String > coursesMap = new HashMap< Integer, String >();
        coursesMap.put(1, "C");
        coursesMap.put(2, "C++");
        coursesMap.put(3, "Java");
        coursesMap.put(4, "Spring Framework");
        coursesMap.put(5, "Hibernate ORM framework");

        for (Map.Entry < Integer, String > entry: coursesMap.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
    }
}
