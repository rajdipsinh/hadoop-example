import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/3/13
 * Time: 10:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConfigurationPrinter extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = getConf();
        for (Map.Entry<String, String> entry : configuration) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ConfigurationPrinter(), args));
    }
}
