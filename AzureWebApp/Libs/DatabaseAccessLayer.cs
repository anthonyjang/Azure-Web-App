using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebApp.Libs
{
    #region Extensions
    public static class ParamType
    {
        /// <summary>        
        /// Extension to find if a certain type is numeric 
        /// </summary>
        public static bool IsNumericType(this Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
    }

    /// <summary>        
    /// DAL class to MS SQL - Mapping data provider to DTO
    /// </summary>
    public static class DataReader
    {
        /// <summary>        
        /// Extension to check if selected column has been read.         
        /// </summary>
        public static bool HasColumn(this IDataRecord dr, string columnName)
        {
            for (int i = 0; i < dr.FieldCount; i++)
            {
                if (dr.GetName(i).Equals(columnName, StringComparison.CurrentCultureIgnoreCase))
                    return true;
            }
            return false;
        }
    }
    #endregion

    #region Interfaces for expressions
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

    public interface IArrayExpression<T> : IExpression
    {
        bool useQuotesOnValue { get; set; }
        T[] arrayValue { get; set; }
    }
    #endregion

    public class StringExpression : IStringExpression
    {
        public bool useParentheses { get; set; } = false;
        public bool distinct { get; set; } = false;
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

    public class OrderByExpression : IOrderByExpression
    {
        public bool useParentheses { get; set; } = false;
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

    public class ArrayExpression<T> : IArrayExpression<T>
    {
        public bool useParentheses { get; set; } = false;
        public bool useQuotesOnValue { get; set; } = false;
        public T[] arrayValue { get; set; }

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

            if (!string.IsNullOrEmpty(arraySQLString))
            {
                arraySQLString = arraySQLString.Remove(arraySQLString.Length - 2); //Remove trailing ", "
            }

            return arraySQLString;
        }
    }

    public class LogicalExpression : IExpression
    {
        public bool useParentheses { get; set; } = false;

        public string expression { get; set; }

        public string ToSQL()
        {
            return expression;
        }
    }

    public class EqualExpression<T> : IExpression
    {
        public EqualExpression()
        {
            useQuotesOnValue = !typeof(T).IsNumericType();
        }

        public bool useQuotesOnValue { get; set; }
        public bool useParentheses { get; set; } = true;

        public KeyValuePair<string, T> key { get; set; }

        public virtual string ToSQL()
        {
            string valueStr = useQuotesOnValue ? string.Format("'{0}'", key.Value) : string.Format("{0}", key.Value);

            return useParentheses ? string.Format("({0} = {1})", key.Key, valueStr) : string.Format("{0} = {1}", key.Key, valueStr);
        }
    }

    public interface IQuery
    {
        string ToSQL();
    }

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

    public class SelectQuery : ISelectQuery
    {
        public IExpression select { get; set; }
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

    public enum DBTypeParam
    {
        BigInt = 0, Binary = 1, Bit = 2, Char = 3, DateTime = 4, Decimal = 5, Float = 6, Image = 7, Int = 8, Money = 9, NChar = 10, NText = 11,
        NVarChar = 12, Real = 13, UniqueIdentifier = 14, SmallDateTime = 15, SmallInt = 16, SmallMoney = 17, Text = 18, Timestamp = 19, TinyInt = 20,
        VarBinary = 21, VarChar = 22, Variant = 23, Xml = 25, Udt = 29, Structured = 30, Date = 31, Time = 32, DateTime2 = 33, DateTimeOffset = 34
    }

    public class QueryParameter
    {
        public string name { get; set; }
        public object value { get; set; }
        public ParameterDirection direction { get; set; } = ParameterDirection.Input;
        public DBTypeParam type { get; set; }
        public int size { get; set; }

        public SqlParameter ToSqlParam()
        {
            return new SqlParameter(name, value) { Direction = direction, Size = size, SqlDbType = (SqlDbType)type };
        }

        public void FillFromSqlParam(SqlParameter src)
        {
            value = src.Value;
        }
    }

    /// <summary>        
    /// Class to run query or execute TSQL on MS SQL
    /// </summary>
    public class QueryExecutor : IDisposable
    {
        private SqlConnection _connection = default(SqlConnection);
        private SqlTransaction _dbTransaction = default(SqlTransaction);
        private string _connectionString;

        public QueryExecutor(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>        
        /// Query and fill DTO asynchronous with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="querySQL">TSQL string to query DB</param>
        /// <param name="mapper">mapping function between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>   
        /// <param name="commandBehavior">Use CommandBehavior.SequentialAccess to read large data in good performance.
        /// Note that you always should read fields in the order they are returned by index (using constants).
        /// </param>
        public async Task<IList<T>> QueryAsync<T>(string querySQL, Func<IDataReader, T> mapper, object param = null, CommandType commandType = CommandType.Text, CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            SqlCommand cmd = null;
            List<T> list = new List<T>();
            SqlConnection connection = default(SqlConnection);
            try
            {
                if (_dbTransaction != default(SqlTransaction))
                {
                    cmd = new SqlCommand(querySQL, _connection) { CommandType = commandType };
                    cmd.Transaction = _dbTransaction;
                }
                else
                {
                    connection = new SqlConnection(_connectionString);
                    await connection.OpenAsync();
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {
                    SetParam(param, cmd);

                    using (var reader = await cmd.ExecuteReaderAsync(commandBehavior))
                    {
                        while (await reader.ReadAsync())
                        {
                            list.Add(mapper(reader));
                        }
                        reader.Close();
                    }
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    connection.Close();
                }
            }

            return list;
        }

        /// <summary>        
        /// Query and fill DTO asynchronous with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="queryObject">Query object that can be translated to SQL</param>
        /// <param name="mapper">mapping function between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public async Task<IList<T>> QueryAsync<T>(IQuery queryObject, Func<IDataReader, T> mapper, object param = null, CommandType commandType = CommandType.Text) where T : class
        {
            return await QueryAsync<T>(queryObject.ToSQL(), mapper, param, commandType);
        }

        private void SetParam(object param, SqlCommand cmd)
        {
            cmd.Parameters.Clear();

            if (param != default(object))
            {
                if (param.GetType() == typeof(QueryParameter[]))
                {
                    foreach (var item in (QueryParameter[])param)
                    {
                        cmd.Parameters.Add(item.ToSqlParam());
                    }
                }
                else
                {
                    foreach (PropertyInfo prop in param.GetType().GetProperties())
                    {
                        object value = prop.GetValue(param);
                        cmd.Parameters.AddWithValue(prop.Name, value);
                    }
                }
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).

                    if (_connection != null)
                    {
                        _connection.Dispose();
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~QueryExecutor() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
