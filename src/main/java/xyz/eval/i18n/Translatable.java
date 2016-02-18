package xyz.eval.i18n;

/**
 * Common Interface for Enums which provide translation for constants.
 *
 * @author Y. Petrick
 */
public interface Translatable<E extends Enum<E> & Translatable<E>> {

    /**
     * Translate constant to the given locale
     *
     * @param languageTag representing the {@link java.util.Locale}
     * @return a <code>String</code> translation for given locale
     */
    String translate(String languageTag);

    String key();

}
