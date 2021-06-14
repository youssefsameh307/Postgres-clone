public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    SQLTerm(String _strColumnName,   String _strOperator,    Object _objValue){
        this._strColumnName=_strColumnName;
    this._strOperator = _strOperator;
    this._objValue = _objValue;
    }
    SQLTerm(){
        
    }
}
