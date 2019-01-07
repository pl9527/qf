import org.apache.hadoop.io.LongWritable;

public class RowSequence {

    private LongWritable result = new LongWritable();

    public RowSequence() {
        result.set(0);
    }

    public LongWritable evaluate() {
        result.set(result.get() + 1);
        return result;
    }

}
