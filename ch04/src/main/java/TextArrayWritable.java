import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/3/13
 * Time: 7:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
}
