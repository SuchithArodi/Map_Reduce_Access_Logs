import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

public abstract class MapReduce {
    private TreeMap<String, LinkedList<String>> temp = new TreeMap();
    private TreeMap<String, LinkedList<String>> results = new TreeMap();

    public abstract void mapper(String var1);

    public abstract void reducer(String var1, LinkedList<String> var2);

    public final void emit_intermediate(String string, String string2) {
        if (this.temp.containsKey(string)) {
            LinkedList<String> linkedList = this.temp.get(string);
            linkedList.add(string2);
        } else {
            LinkedList<String> linkedList = new LinkedList<String>();
            linkedList.add(string2);
            this.temp.put(string, linkedList);
        }
    }

    public final void emit(String string, String string2) {
        if (this.results.containsKey(string)) {
            LinkedList<String> linkedList = this.results.get(string);
            linkedList.add(string2);
        } else {
            LinkedList<String> linkedList = new LinkedList<String>();
            linkedList.add(string2);
            this.results.put(string, linkedList);
        }
    }

    public final void dumpIntermediate() {
        System.out.println("Dumping Intermediate Emits\n");
        Set<String> set = this.temp.keySet();
        for (String string : set) {
            System.out.println("Key :" + string);
            LinkedList<String> linkedList = this.temp.get(string);
            Iterator<String> iterator = linkedList.iterator();
            System.out.print("\t");
            while (iterator.hasNext()) {
                System.out.print(iterator.next() + " ");
            }
            System.out.println();
        }
    }

    public final TreeMap<String, LinkedList<String>> getResults() {
        return this.results;
    }

    public final void execute(String string) {
        this.mapper(string);
        Set<String> set = this.temp.keySet();
        for (String string2 : set) {
            this.reducer(string2, this.temp.get(string2));
        }
    }
}
