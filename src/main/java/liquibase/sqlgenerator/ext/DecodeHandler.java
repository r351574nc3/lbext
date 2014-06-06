package liquibase.sqlgenerator.ext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DecodeHandler implements SqlMappingHandler {
    protected static Pattern PATTERN = Pattern.compile("DECODE\\(([^\\)]*)\\)");
    
    @Override
    public boolean matches(final StringBuffer sql) {
	return PATTERN.matcher(sql.toString()).find();
    }
    
    @Override
    public void translate(final StringBuffer queryBuffer) {	    
	final Matcher matcher = PATTERN.matcher(queryBuffer.toString());
	while (matcher.find()) {
	    String[] group = matcher.group(1).split(",");
	    String elseValue = group[group.length - 1];
	    String translation = String.format("IF(%s=%s, %s, %s)", group[0], group[1], group[2], elseValue);
	    
	    queryBuffer.replace(matcher.start(), matcher.end(), translation);
	}
    }
}
