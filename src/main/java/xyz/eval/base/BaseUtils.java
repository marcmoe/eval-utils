package xyz.eval.base;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;

/**
 *
 * @author Y. Petrick
 */
public class BaseUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Create a supplier which will provide new instance cloned from src. New
     * instances of T will have no referential access on src and will provide
     * same values.
     *
     * @param src
     * @param type
     * @return
     */
    public static <T> Supplier<T> supplierFor(final T src, final JavaType type) {

        return new Supplier<T>() {

            @Override
            public T get() {
                return MAPPER.convertValue(src, type);
            }
        };
    }
}
