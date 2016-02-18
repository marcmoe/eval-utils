package xyz.eval.i18n;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 * @author Y. Petrick
 */
public enum Abbreviations implements Translatable<Abbreviations> {
    EMPTY(OtherNodes.EMPTY), MINCOUNT(OtherNodes.MINCOUNT);

    private final String key;

    Abbreviations(final String key) {
        this.key = key;
    }

    @Override
    public String translate(final String languageTag) {
        return ResourceBundle.getBundle(Abbreviations.class.getName(), Locale.forLanguageTag(languageTag)).getString(key);
    }

    @Override
    public String key() {
        return key;
    }

}
