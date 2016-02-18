package xyz.eval.i18n;

/**
 * Utility class to map values for new other nodes introduced with ADAM-583 to contants.
 *
 * @author Y. Petrick
 */
public final class OtherNodes {
    public static final String EMPTY = "#empty";
    public static final String MINCOUNT = "#mincount";

    private OtherNodes() {
    }

    /**
     * Convenience method to lookup translation for possible values of other nodes
     * 
     * @param value a <code>String</code> value to translate
     * @param languageTag representing a {@link java.util.Locale}
     * @param enumType implementation of {@link net.meetrics.adam.xlsexport.i18.Translatable} to use for lookup 
     * 
     * @return a <code>String</code> translation for the given value
     * @throws IllegalArgumentException if no translation was found
     */
    public static <T extends Enum<T> & Translatable<T>> String translate(final String value, final String languageTag, final Class<T> enumType) {
        for(final T type : enumType.getEnumConstants()){
            if (type.key().equals(value)) {
                return type.translate(languageTag);
            }
        }
        throw new IllegalArgumentException(String.format("no translation for value %s using languageTag %s mapped by %s ", value, languageTag, enumType.getName()));
    }

}
