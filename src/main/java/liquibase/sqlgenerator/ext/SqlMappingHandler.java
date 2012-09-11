package liquibase.sqlgenerator.ext;

public interface SqlMappingHandler {
    boolean matches(final StringBuffer sql);
    
    void translate(final StringBuffer queryBuffer);
}
