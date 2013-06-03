import org.apache.hadoop.io.Text;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * Created with IntelliJ IDEA.
 * Date: 6/3/13
 * Time: 5:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class TextTest extends WritableTestBase {
    @Test
    public void test() {
        Text t = new Text("hadoop");
        MatcherAssert.assertThat(t.getLength(), Is.is(6));
        MatcherAssert.assertThat(t.getBytes().length, Is.is(6));

        MatcherAssert.assertThat(t.charAt(2), Is.is((int)'d'));
        MatcherAssert.assertThat("Out of bounds", t.charAt(100), Is.is(-1));
    }

    @Test
    public void find () {
        Text t = new Text("hadoop");
        MatcherAssert.assertThat("Find a substring", t.find("do"), Is.is(2));
        MatcherAssert.assertThat("Find first 'o'", t.find("o"), Is.is(3));
    }

    @Test
    public void mutability() {
        Text t = new Text("hadoop");
        t.set("pig");
        MatcherAssert.assertThat(t.getLength(), Is.is(3));
        MatcherAssert.assertThat(t.getBytes().length, Is.is(3));
    }

    @Test
    public void byteArrayNotShortened() {
        Text t = new Text("hadoop");
        t.set(new Text("pig"));
        MatcherAssert.assertThat(t.getLength(), Is.is(3));
    }

    @Test
    public void toStringMethod() {
        MatcherAssert.assertThat(new Text("hadoop").toString(), Is.is("hadoop"));
    }

    @Test
    public void comparision() {
        MatcherAssert.assertThat("\ud800\udc00".compareTo("\ue000"), Matchers.lessThan(0));
        MatcherAssert.assertThat(new Text("\ud800\udc00").compareTo(new Text("\ue000")), Matchers.greaterThan(0));
    }

    @Test
    public void withSupplementaryCharacters() throws UnsupportedEncodingException {
        String s = "\u0041\u00DF\u6771\uD801\uDC00";
        MatcherAssert.assertThat(s.length(), Is.is(5));
        MatcherAssert.assertThat(s.getBytes("UTF-8").length, Is.is(10));

        MatcherAssert.assertThat(s.indexOf('\u0041'), Is.is(0));
    }

}
