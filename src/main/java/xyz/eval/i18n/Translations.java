package xyz.eval.i18n;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 * @author Y. Petrick
 */
public enum Translations implements Translatable<Translations> {
    EMPTY(OtherNodes.EMPTY), MINCOUNT(OtherNodes.MINCOUNT);

    private final String key;

    Translations(final String key) {
        this.key = key;
    }

    @Override
    public String translate(final String languageTag) {
        return ResourceBundle.getBundle(Translations.class.getName(), Locale.forLanguageTag(languageTag)).getString(key);
    }

    @Override
    public String key() {
        return key;
    }

}
