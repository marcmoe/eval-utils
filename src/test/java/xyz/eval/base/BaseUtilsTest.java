package xyz.eval.base;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;

import org.elasticsearch.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

/**
 *
 * @author Y. Petrick
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class BaseUtilsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void shouldCreateNewInstancesFromSupplier() {
        final List<MyClass> myRef = Lists.newArrayList(new MyClass("a"), new MyClass("b"));
        final Supplier<List<MyClass>> supplier = BaseUtils.supplierFor(myRef, MAPPER.getTypeFactory().constructCollectionType(List.class, MyClass.class));

        Assert.assertNotEquals(myRef, supplier.get());
    }

    public static class MyClass {

        @JsonProperty("s")
        private String s;

        @JsonCreator
        public MyClass(@JsonProperty("s") final String s) {
            this.s = s;
        }

        public String getS() {
            return s;
        }

        public void setS(final String s) {
            this.s = s;
        }

    }
}
