package liquibase.sqlgenerator.ext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import liquibase.logging.LogFactory;

public class TruncHandler implements SqlMappingHandler {
    protected static Pattern PATTERN = Pattern.compile("TRUNC\\(([^\\)]*)\\)");
    
    @Override
    public boolean matches(final StringBuffer sql) {
	return PATTERN.matcher(sql.toString()).find();
    }
    
    @Override
    public void translate(final StringBuffer queryBuffer) {	    
	final Matcher matcher = PATTERN.matcher(queryBuffer.toString());
	while (matcher.find()) {
	    String translation = matcher.group(1);
	    info("Found a match. Translated to " + translation);
	    if (translation.equalsIgnoreCase("SYSDATE")) {
		translation = "NOW()";
	    }
	    queryBuffer.replace(matcher.start(), matcher.end(), translation);
	}
    }

    protected void info(final String message) {
	LogFactory.getLogger().info(message);
    }

    protected void debug(final String message) {
	LogFactory.getLogger().debug(message);
    }
}
