using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AzureWebApp.Libs
{
    public class Mandatory : Attribute
    {
    }

    public interface IValidatable
    {
        bool IsValid();
    }

    public class Validatable : IValidatable
    {
        public bool IsValid()
        {
            bool valid = true;

            foreach (PropertyInfo prop in GetType().GetProperties())
            {
                Attribute mandatory = prop.GetCustomAttribute(typeof(Mandatory), false);
                if (mandatory != default(Attribute))
                {
                    object value = prop.GetValue(this);

                    if (value == default(object))
                    {
                        valid = false;
                    }
                }
            }

            return valid;
        }
    }
    public interface IExpression
    {
        bool useParentheses { get; set; }
        string ToSQL();
    }

    public interface IOrderByExpression : IExpression
    {
        bool asc { get; set; }
    }

    public interface IStringExpression : IExpression
    {
        string alias { get; set; }
    }

    public class LikeExpression: Validatable, IExpression
    {
        public bool useParentheses { get; set; } = false;
        public bool parameterization { get; set; } = false;
        [Mandatory]
        public string expression { get; set; }
        [Mandatory]
        public string like { get; set; }
        public string ToSQL()
        {
            if (parameterization)
            {
                return useParentheses ? string.Format("({0} LIKE {1})", expression, like) : string.Format("{0} LIKE {1}", expression, like);
            }
            else
            {
                return useParentheses ? string.Format("({0} LIKE '{1}')", expression, like) : string.Format("{0} LIKE '{1}'", expression, like);
            }
        }
    }

    public class StringExpression : Validatable, IStringExpression
    {
        public bool useParentheses { get; set; } = false;
        public bool distinct { get; set; } = false;
        [Mandatory]
        public string expression { get; set; }
        public string alias { get; set; }

        public string ToSQL()
        {
            if (string.IsNullOrEmpty(alias))
            {
                if (distinct)
                {
                    return useParentheses ? string.Format("DISTINCT ({0})", expression) : "DISTINCT " + expression;
                }
                else
                {
                    return useParentheses ? string.Format("({0})", expression) : expression;
                }
                //return useParentheses ? string.Format("({0})", expression) : expression;                
            }
            else
            {                
                if (distinct)
                {
                    return useParentheses ? string.Format("DISTINCT ({0}) AS {1}", expression, alias) : string.Format("DISTINCT {0} AS {1}", expression, alias);

                }
                else
                {
                    return useParentheses ? string.Format("({0}) AS {1}", expression, alias) : string.Format("{0} AS {1}", expression, alias);

                }
            }
        }
    }

    public class OrderByExpression : Validatable, IOrderByExpression
    {
        public bool useParentheses { get; set; } = false;
        [Mandatory]
        public string column { get; set; }
        public bool asc { get; set; }

        public string ToSQL()
        {            
            return useParentheses ? string.Format("({0})", column) + (asc ? " ASC" : " DESC") : column + (asc ? " ASC" : " DESC");                        
        }

        public override string ToString()
        {
            return ToSQL();
        }        
    }
    
    public interface IArrayExpression<T> : IExpression
    {
        bool useQuotesOnValue { get; set; }
        T[] arrayValue { get; set; }
        void Append(T item);
    } 

    public class ValueItem : Tuple<string, bool>
    {
        string value { get; } = string.Empty;
        bool useQuote { get; } = false;
        public ValueItem(string item1, bool item2) 
            : base(item1, item2)
        {
            value = item1;
            useQuote = item2;
        }

        public override string ToString()
        {
            if (string.IsNullOrEmpty(value))
            {
                return useQuote ? "''" : "";
            }
            else
            {
                return useQuote ? string.Format("'{0}'", value.Replace("'", "''")) : value.Replace("'", "''");
            }            
        }
    }

    public class ArrayExpression<T> : Validatable, IArrayExpression<T>
    {
        public bool useParentheses { get; set; } = false;
        public bool useQuotesOnValue { get; set; } = false;
        [Mandatory]
        public T[] arrayValue { get; set; } 

        public void Append(T item)
        {
            if (arrayValue == default(T[]))
            {
                arrayValue = new T[] { };
            }
            arrayValue = arrayValue.Append(item).ToArray();
        }

        public string ToSQL()
        {
            string arraySQLString = "";

            foreach (T element in arrayValue)
            {
                if (useQuotesOnValue)
                {
                    arraySQLString += useParentheses ? string.Format("('{0}'), ", element) : string.Format("'{0}', ", element);
                }
                else
                {
                    arraySQLString += useParentheses ? string.Format("({0}), ", element) : string.Format("{0}, ", element);
                }
            }

            if(!string.IsNullOrEmpty(arraySQLString))
            {
                arraySQLString = arraySQLString.Remove(arraySQLString.Length - 2); //Remove trailing ", "
            }

            return arraySQLString;
        }
    }

    public interface ILogicalExpression : IExpression
    {
        ILogicalExpression Or(IExpression _expression);
        ILogicalExpression And(IExpression _expression);
        ILogicalExpression Not();
        ILogicalExpression Between(IExpression _expression);
        ILogicalExpression Exists(IExpression _expression);
        ILogicalExpression In(IExpression _expression);
        ILogicalExpression NotIn(IExpression _expression);
    }

    public class LogicalExpression : Validatable, ILogicalExpression
    {
        public bool useParentheses { get; set; } = false;

        [Mandatory]
        public string expression { get; set; }

        public ILogicalExpression Or(IExpression _expression)
        {
            if (string.IsNullOrEmpty(expression))
            {
                expression = useParentheses ? string.Format("({0})", _expression.ToSQL()) : _expression.ToSQL();
            }
            else
            {
                if (useParentheses)
                {
                    expression = string.Format("({0} OR {1})", expression, _expression.ToSQL());
                }
                else
                {
                    expression += " OR " + _expression.ToSQL();
                }
            }
            return this;
        }

        public ILogicalExpression And(IExpression _expression)
        {
            if (string.IsNullOrEmpty(expression))
            {
                expression = useParentheses ? string.Format("({0})", _expression.ToSQL()) : _expression.ToSQL();
            }
            else
            {
                if (useParentheses)
                {
                    expression = string.Format("({0} AND {1})", expression, _expression.ToSQL());
                }
                else
                {
                    expression += " AND " + _expression.ToSQL();
                }
            }
            return this;
        }

        public ILogicalExpression Not()
        {
            if (useParentheses)
            {
                expression = string.Format("(NOT ({0}))", expression);
            }
            else
            {
                expression = " NOT " + expression;
            }            
            return this;
        }

        public ILogicalExpression Between(IExpression _expression)
        {            
            expression += useParentheses ? string.Format("(BETWEEN {0})", _expression.ToSQL()) : " BETWEEN " + _expression.ToSQL();            
            return this;
        }

        public ILogicalExpression Exists(IExpression _expression)
        {
            expression = useParentheses ? string.Format("(EXISTS ({0}))", _expression.ToSQL()) : " EXISTS " + _expression.ToSQL();
            return this;
        }

        public ILogicalExpression In(IExpression _expression)
        {
            expression += useParentheses ? string.Format("(IN ({0}))", _expression.ToSQL()) : " IN " + _expression.ToSQL();
            return this;
        }

        public ILogicalExpression NotIn(IExpression _expression)
        {
            expression += useParentheses ? string.Format("(NOT IN ({0}))", _expression.ToSQL()) : string.Format(" NOT IN {0} ", _expression.ToSQL());
            return this;
        }

        public string ToSQL()
        {
            return expression;
        }        
    }

    public class EqualExpression<T> : Validatable, IExpression
    {
        public EqualExpression()
        {
            useQuotesOnValue = !typeof(T).IsNumericType();
        }

        public bool useQuotesOnValue { get; set; }
        public bool useParentheses { get; set; } = true;

        [Mandatory]
        public KeyValuePair<string, T> key { get; set; }

        [Mandatory]

        public virtual string ToSQL()
        {
            string valueStr = useQuotesOnValue ? string.Format("'{0}'", key.Value) : string.Format("{0}", key.Value);
            
            return useParentheses ? string.Format("({0} = {1})", key.Key, valueStr) : string.Format("{0} = {1}", key.Key, valueStr);
        }
    }

    public class RSComparision<T> : Validatable, IExpression
    {
        public RSComparision()
        {
            useQuotesOnValue = !typeof(T).IsNumericType();
        }

        public bool useQuotesOnValue { get; set; }
        public bool useParentheses { get; set; } = true;

        [Mandatory]
        public KeyValuePair<string, T> key { get; set; }

        [Mandatory]
        public RSSearchComparator? opr { get; set; }

        public virtual string ToSQL()
        {
            string valueStr = useQuotesOnValue ? string.Format("'{0}'", key.Value) : string.Format("{0}", key.Value);
            if (opr != null)
            {
                switch (opr)
                {
                    case RSSearchComparator.Eq: return useParentheses ? string.Format("({0} = {1})", key.Key, valueStr) : string.Format("{0} = {1}", key.Key, valueStr);
                    case RSSearchComparator.Ne: return useParentheses ? string.Format("({0} <> {1})", key.Key, valueStr) : string.Format("{0} <> {1}", key.Key, valueStr);
                    case RSSearchComparator.Ge: return useParentheses ? string.Format("({0} >= {1})", key.Key, valueStr) : string.Format("{0} >= {1}", key.Key, valueStr);
                    case RSSearchComparator.Le: return useParentheses ? string.Format("({0} <= {1})", key.Key, valueStr) : string.Format("{0} <= {1}", key.Key, valueStr);
                    case RSSearchComparator.Gt: return useParentheses ? string.Format("({0} > {1})", key.Key, valueStr) : string.Format("{0} > {1}", key.Key, valueStr);
                    case RSSearchComparator.Lt: return useParentheses ? string.Format("({0} < {1})", key.Key, valueStr) : string.Format("{0} < {1}", key.Key, valueStr);
                    default: throw new Exception("Incorrect operator was used on the expression");
                }
            }
            else
            {
                return useParentheses ? string.Format("({0} = {1})", key.Key, valueStr) : string.Format("{0} = {1}", key.Key, valueStr);
            }
        }
    }


    public interface IQuery
    {
        string ToSQL();
    }

    /*
     * SELECT select_list [ INTO new_table ] 1
     * [ FROM table_source ] [ WHERE search_condition ]
     * [ GROUP BY group_by_expression ]
     * [ HAVING search_condition ]
     * [ ORDER BY order_expression [ ASC | DESC ] ]
     */
    public interface ISelectQuery : IQuery
    {
        IList<KeyValuePair<string, IExpression>> join { get; set; }
        IList<KeyValuePair<string, IExpression>> include { get; set; }
        IExpression where { get; set; }
        IExpression groupBy { get; set; }
        IExpression having { get; set; }
        IExpression orderBy { get; set; }
        int skip { get; set; }
        int take { get; set; }
        string sqlStatement { get; set; }
        string invalidMessage { get; set; }
        string invalidParams { get; set; }
    }

    public interface IInsertQuery : IQuery
    {
        IExpression values { get; set; }
        IExpression columns { get; set; }
    }

    public interface IUpdateQuery : IQuery
    {
        IList<KeyValuePair<string, IExpression>> set { get; set; }
        IExpression where { get; set; }
    }

    public interface IDeleteQuery : IQuery
    {
        IExpression where { get; set; }
    }

    public class SelectQuery : Validatable, ISelectQuery
    {
        [Mandatory]
        public IExpression select { get; set; }
        [Mandatory]
        public string from { get; set; }
        public IList<KeyValuePair<string, IExpression>> join { get; set; }
        public IList<KeyValuePair<string, IExpression>> include { get; set; }
        public IExpression where { get; set; }
        public IExpression groupBy { get; set; }
        public IExpression having { get; set; }
        public IExpression orderBy { get; set; }
        public int skip { get; set; } = 0;
        public int take { get; set; } = 0;
        public string sqlStatement { get; set; }
        public string invalidMessage { get; set; }
        public string invalidParams { get; set; }
        public string ToSQL()
        {
            string composedSQL = "";
            try
            {
                // Generate sql according to sqlStatement
                composedSQL = sqlStatement;
                if (string.IsNullOrEmpty(composedSQL))
                {
                    // Generate sql according to select, from, where, group ...
                    composedSQL = "SELECT " + select.ToSQL();
                    composedSQL += " FROM " + from;

                    if (join != default(IList<KeyValuePair<string, IExpression>>))
                    {
                        foreach (var joinItem in join)
                        {
                            composedSQL += string.Format(" INNER JOIN {0} ON {1}", joinItem.Key, joinItem.Value.ToSQL());
                        }
                    }
                    if (include != default(IList<KeyValuePair<string, IExpression>>))
                    {
                        foreach (var includeItem in include)
                        {
                            composedSQL += string.Format(" LEFT JOIN {0} ON {1}", includeItem.Key, includeItem.Value.ToSQL());
                        }
                    }

                    if (where != default(IExpression))
                    {
                        composedSQL += " WHERE " + where.ToSQL();
                    }
                    if (groupBy != default(IExpression))
                    {
                        composedSQL += " GROUP BY " + groupBy.ToSQL();
                    }
                    if (having != default(IExpression))
                    {
                        composedSQL += " HAVING " + having.ToSQL();
                    }
                    if (orderBy != default(IExpression))
                    {
                        composedSQL += " ORDER BY " + orderBy.ToSQL();

                        if (skip >= 0 && take > 0)
                        {
                            //only possible to use together with ORDER BY
                            composedSQL += string.Format(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY ", skip, take);
                        }
                    }
                }
            }
            catch
            {
                //do nothing with ex for now
                composedSQL = "";
            }
            return composedSQL;
        }
    }

    public class FullTextSearchExpression : Validatable, IExpression
    {
        public bool useParentheses { get; set; } = false;
        [Mandatory]
        public string value { get; set; }
        public string columns { private get; set; }

        public string ToSQL()
        {
            return useParentheses ? string.Format("(CONTAINS (({0}), '\"{1}*\"'))", columns, value) : string.Format("CONTAINS (({0}), '\"{1}*\"')", columns, value);
        }
    }

    public class DeleteQuery : Validatable, IDeleteQuery
    {
        [Mandatory]
        public string from { get; set; }
        public IExpression where { get; set; }
        public string ToSQL()
        {
            string composedSQL = "";
            try
            {
                composedSQL = "DELETE FROM " + from;
                if (where != default(IExpression))
                {
                    composedSQL += " WHERE " + where.ToSQL();
                }
            }
            catch
            {
                composedSQL = "";
            }
            return composedSQL;
        }
    }

    public class UpdateQuery : Validatable, IUpdateQuery
    {
        [Mandatory]
        public IList<KeyValuePair<string, IExpression>> set { get; set; }
        [Mandatory]
        public string from { get; set; }
        public IExpression where { get; set; }
        public string ToSQL()
        {
            string composedSQL = "";
            try
            {
                composedSQL = "UPDATE " + from;
                if (set != default(IList<KeyValuePair<string, IExpression>>))
                {
                    composedSQL += " SET ";
                    foreach (var setItem in set)
                    {
                        composedSQL += string.Format(" {0} = ISNULL({1}, NULL),", setItem.Key, setItem.Value.ToSQL());
                    }
                    composedSQL = composedSQL.Remove(composedSQL.LastIndexOf(','));
                }

                if (where != default(IExpression))
                {
                    composedSQL += " WHERE " + where.ToSQL();
                }
            }
            catch
            {
                composedSQL = "";
            }
            return composedSQL;
        }
    }

    public class InsertQuery : Validatable, IInsertQuery
    {
        [Mandatory]
        public IExpression values { get; set; }
        [Mandatory]
        public string from { get; set; }
        public IExpression columns { get; set; }

        public string ToSQL()
        {
            string composedSQL = "";
            try
            {
                composedSQL += "INSERT INTO ";
                composedSQL += " " + from + " ";
                if (columns != default(IExpression))
                {
                    composedSQL += "(" + columns.ToSQL() + ")";
                }

                if (values != default(IExpression))
                {
                    composedSQL += " VALUES (" + values.ToSQL() + ")";
                }
            }
            catch 
            {
                composedSQL = "";
            }
            return composedSQL;
        }
    }
}
