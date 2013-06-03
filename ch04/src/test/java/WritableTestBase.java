import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/3/13
 * Time: 5:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class WritableTestBase {
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static byte[] desrialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }

    public static String serializeToString (Writable src) throws IOException {
        return StringUtils.byteToHexString(serialize(src));
    }

    public static String writeTo(Writable src, Writable dest) throws IOException {
        byte[] data = desrialize(dest, serialize(src));
        return StringUtils.byteToHexString(data);
    }
}
