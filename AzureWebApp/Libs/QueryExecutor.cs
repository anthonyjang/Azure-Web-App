using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;

namespace AzureWebApp.Libs
{
    /// <summary>        
    /// DAL class to MS SQL - Mapping data provider to DTO
    /// </summary>
    public static class DataReader
    {
        /// <summary>        
        /// Extension to read only selected columns from the reader. 
        /// If you want to load selected columns, pass selected columns on querySQL or queryObject
        /// </summary>
        public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> generator)
        {
            while (reader.Read())
            {
                yield return generator(reader);
            }
        }

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

    /// <summary>        
    /// Class to run query or execute TSQL on MS SQL
    /// </summary>
    public class QueryExecutor : IDisposable
    {        
        private SqlConnection _connection = default(SqlConnection);     
        private SqlTransaction _dbTransaction = default(SqlTransaction);
        private string _connectionString;
        private readonly ILogger _logger = default(ILogger);
        private long _loginUserID = 0;
        private string _loginIpAddress;
        private Stopwatch stopWatch = new Stopwatch();

        public QueryExecutor(string connectionString, ILogger logger)
        {
            _logger = logger;
            _connectionString = connectionString;            
        }

        public long GetLoginUserID()
        {
            return _loginUserID;
        }

        public void SetLoginUserID(long loginUserID)
        {
            if ((loginUserID > 0) && (loginUserID != _loginUserID))
            {
                _loginUserID = loginUserID;
            }
        }

        public void SetLoginIpAddress(string loginIpAddress)
        {
            if (_loginIpAddress != loginIpAddress)
            {
                _loginIpAddress = loginIpAddress;
            }
        }

        public bool CreateTransaction(IsolationLevel iso)
        {
            _connection = new SqlConnection(_connectionString);
            OpenConnection(_connection);
            try
            {                
                _dbTransaction = _connection.BeginTransaction(iso);
            }
            catch (Exception ex)
            {
                LogError("CreateTransaction - Exception in BeginTransaction - exception message- {0} ", ex.Message);
                throw;
            }            

            return true;
        }

        public void CommitTransaction()
        {
            if (_dbTransaction != default(SqlTransaction))
            {
                _dbTransaction.Commit();
                _connection.Close();

                _dbTransaction.Dispose();
                _dbTransaction = default(SqlTransaction);                                
            }
        }

        public void RollbackTransaction()
        {
            if (_dbTransaction != default(SqlTransaction))
            {
                _dbTransaction.Rollback();
                _connection.Close();                
                _dbTransaction = default(SqlTransaction);
            }
        }

        public void DisposeTransaction()
        {
            if (_dbTransaction != default(SqlTransaction))
            {
                _connection.Close();
                _dbTransaction.Dispose();                
                _dbTransaction = default(SqlTransaction);                                
            }
        }

        private void OpenConnection(SqlConnection connection)
        {
            try
            {
                stopWatch.Start();

                if (!_connectionString.Contains("User ID", StringComparison.InvariantCultureIgnoreCase))
                {
                    // No user id in the connection string so we try to use Azure credential 
                    AzureServiceTokenProvider azureServiceTokenProvider = null;

                    // Check the Azure Environment
                    string msi_endpoint = System.Environment.GetEnvironmentVariable("MSI_ENDPOINT");
                    LogInformation("MSI_ENDPOINT is: {0}", msi_endpoint);

                    if (!String.IsNullOrEmpty(msi_endpoint))
                    {
                        // Azure environment using User Managed Identity 
                        azureServiceTokenProvider = new AzureServiceTokenProvider();
                    }
                    else
                    {
                        // On-Prem  Environment 
                        azureServiceTokenProvider = new AzureServiceTokenProvider(string.Empty);
                        LogInformation("AzureProviderConnectionString is: {0}", string.Empty);
                    }

                    connection.AccessToken = azureServiceTokenProvider.GetAccessTokenAsync("https://database.windows.net/").Result;
                }
                
                connection.Open();

                stopWatch.Stop();
                LogInformation("OpenConnection took {0} ms", stopWatch.ElapsedMilliseconds);
                stopWatch.Reset();

                //Set session context
                if (_loginUserID > 0)
                {
                    SqlCommand cmd = new SqlCommand(String.Format("EXECUTE rls.Set_Session_Context @UserID = {0}, @IpAddress = '{1}'", _loginUserID, _loginIpAddress), connection);
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                LogError("OpenConnection - Exception in openning connection exception message- {0} ", ex.Message);
                throw;
            }
        }

        private async Task<bool> OpenConnectionAsync(SqlConnection connection)
        {
            try
            {
                stopWatch.Start();

                if (!_connectionString.Contains("User ID", StringComparison.InvariantCultureIgnoreCase))
                {
                    // No user id in the connection string so we try to use Azure credential 
                    AzureServiceTokenProvider azureServiceTokenProvider = null;

                    // Check the Azure Environment
                    string msi_endpoint = System.Environment.GetEnvironmentVariable("MSI_ENDPOINT");
                    LogInformation("MSI_ENDPOINT is: {0}", msi_endpoint);

                    if (!String.IsNullOrEmpty(msi_endpoint))
                    {
                        // Azure environment using User Managed Identity 
                        azureServiceTokenProvider = new AzureServiceTokenProvider();
                    }
                    else
                    {
                        // On-Prem  Environment 
                        azureServiceTokenProvider = new AzureServiceTokenProvider(string.Empty);
                        LogInformation("AzureProviderConnectionString is: {0}", string.Empty);
                    }

                    connection.AccessToken = await azureServiceTokenProvider.GetAccessTokenAsync("https://database.windows.net/");
                }
                await connection.OpenAsync();

                stopWatch.Stop();
                LogInformation("OpenConnectionAsync took {0} ms", stopWatch.ElapsedMilliseconds);
                stopWatch.Reset();

                //Set session context
                if (_loginUserID > 0)
                {
                    SqlCommand cmd = new SqlCommand(String.Format("EXECUTE rls.Set_Session_Context @UserID = {0}, @IpAddress = '{1}'", _loginUserID, _loginIpAddress), connection);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                LogError("OpenConnectionAsync - Exception in openning connection exception message- {0} ", ex.Message);
                throw;
            }

            return true;
        }

        private void CloseConnection(SqlConnection connection)
        {
            try
            {
                if (connection != default(SqlConnection))
                {
                    stopWatch.Start();

                    connection.Close();

                    stopWatch.Stop();
                    LogInformation("OpenConnectionAsync took {0} ms", stopWatch.ElapsedMilliseconds);
                    stopWatch.Reset();
                }
            }
            catch (Exception ex)
            {
                LogError("CloseConnection - Exception in closing connection exception message- {0} ", ex.Message);
                throw;
            }
        }

        /// <summary>        
        /// Query and fill DTO based on matching name between property on DTO and data reader
        /// </summary>
        /// <param name="querySQL">TSQL string to query DB</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public IList<T> Query<T>(string querySQL, object param = null, CommandType commandType = CommandType.Text) where T : class
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

                    OpenConnection(connection);
                    
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {
                    SetParam(param, cmd);
                    using (var reader = cmd.ExecuteReader())
                    {
                        T obj = default(T);
                        while (reader.Read())
                        {
                            obj = Activator.CreateInstance<T>();
                            foreach (PropertyInfo prop in obj.GetType().GetProperties())
                            {
                                if (reader.HasColumn(prop.Name) && !object.Equals(reader[prop.Name], DBNull.Value))
                                {
                                    prop.SetValue(obj, reader[prop.Name], null);
                                }
                            }

                            list.Add(obj);
                        }
                        reader.Close();
                    }
                }
            }
            finally
            {                
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return list;
        }

        /// <summary>        
        /// Query and fill DTO with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="querySQL">TSQL string to query DB</param>
        /// <param name="mapper">mapping function between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public IList<T> Query<T>(string querySQL, Func<IDataReader, T> mapper, object param = null, CommandType commandType = CommandType.Text)
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
                    OpenConnection(connection);
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                SetParam(param, cmd);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        list.Add(mapper(reader));
                    }
                    reader.Close();
                }
            }
            finally
            {
                cmd.Dispose();

                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return list;
        }

        /// <summary>        
        /// Query and fill DTO asynchronous based on matching name between property on DTO and data reader
        /// </summary>
        /// <param name="querySQL">TSQL string to query DB</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public async Task<IList<T>> QueryAsync<T>(string querySQL, object param = null, CommandType commandType = CommandType.Text) where T : class
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
                    await OpenConnectionAsync(connection);
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {
                    SetParam(param, cmd);

                    stopWatch.Start();
                    
                    using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.Default))
                    {
                        stopWatch.Stop();
                        LogInformation("QueryAsync query for Type {0} took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                        stopWatch.Start();

                        T obj = default(T);
                        while (await reader.ReadAsync())
                        {
                            obj = Activator.CreateInstance<T>();                            
                            foreach (PropertyInfo prop in obj.GetType().GetProperties())
                            {                                    
                                if (reader.HasColumn(prop.Name) && !object.Equals(reader[prop.Name], DBNull.Value))
                                {
                                    prop.SetValue(obj, reader[prop.Name], null);
                                }                                    
                            }
                            list.Add(obj);
                        }
                        reader.Close();

                        stopWatch.Stop();

                        LogInformation("QueryAsync fetch for Type {0} took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                    }
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return list;
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
                    await OpenConnectionAsync(connection);
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {
                    SetParam(param, cmd);

                    stopWatch.Start();

                    using (var reader = await cmd.ExecuteReaderAsync(commandBehavior))
                    {
                        stopWatch.Stop();
                        LogInformation("QueryAsync query for Type {0} with Mapper took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                        stopWatch.Start();

                        while (await reader.ReadAsync())
                        {
                            list.Add(mapper(reader));
                        }
                        reader.Close();

                        stopWatch.Stop();

                        LogInformation("QueryAsync fetch for Type {0} with Mapper took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                    }
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return list;
        }

        /// <summary>        
        /// Query and fill DTO asynchronous with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="querySQL">TSQL string to query DB</param>
        /// <param name="mapper">mapping action between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>   
        /// <param name="commandBehavior">Use CommandBehavior.SequentialAccess to read large data in good performance.
        /// Note that you always should read fields in the order they are returned by index (using constants).
        /// </param>
        public async Task<IList<T>> QueryAsync<T>(string querySQL, Action<IDataReader, IList<T>> mapper, object param = null, CommandType commandType = CommandType.Text, CommandBehavior commandBehavior = CommandBehavior.Default)
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
                    await OpenConnectionAsync(connection);
                    cmd = new SqlCommand(querySQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {
                    SetParam(param, cmd);

                    stopWatch.Start();

                    using (var reader = await cmd.ExecuteReaderAsync(commandBehavior))
                    {
                        stopWatch.Stop();
                        LogInformation("QueryAsync query for Type {0} with Mapper having action took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                        stopWatch.Start();

                        while (await reader.ReadAsync())
                        {
                            mapper(reader, list);
                        }
                        reader.Close();

                        stopWatch.Stop();

                        LogInformation("QueryAsync fetch for Type {0} with Mapper having action took {1} ms for query {2}", typeof(T).Name, stopWatch.ElapsedMilliseconds, querySQL);
                        stopWatch.Reset();
                    }
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return list;
        }

        /// <summary>        
        /// Query and fill DTO based on matching name between property on DTO and data reader
        /// </summary>
        /// <param name="queryObject">Query object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public IList<T> Query<T>(IQuery queryObject, object param = null, CommandType commandType = CommandType.Text) where T : class
        {
            return Query<T>(queryObject.ToSQL(), param, commandType);
        }

        /// <summary>        
        /// Query and fill DTO with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="queryObject">Query object that can be translated to SQL</param>
        /// <param name="mapper">mapping function between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public IList<T> Query<T>(IQuery queryObject, Func<IDataReader, T> mapper, object param = null, CommandType commandType = CommandType.Text) where T : class
        {
            return Query<T>(queryObject.ToSQL(), mapper, param, commandType);
        }        

        /// <summary>        
        /// Query and fill DTO asynchronous based on matching name between property on DTO and data reader
        /// </summary>
        /// <param name="queryObject">Query object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public async Task<IList<T>> QueryAsync<T>(IQuery queryObject, object param = null, CommandType commandType = CommandType.Text) where T : class
        {
            return await QueryAsync<T>(queryObject.ToSQL(), param, commandType);
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

        /// <summary>        
        /// Query and fill DTO asynchronous with mapping rule passed as mapper parameter 
        /// </summary>
        /// <param name="queryObject">Query object that can be translated to SQL</param>
        /// <param name="mapper">mapping action between data provider to DTO</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (Text by default)</param>
        public async Task<IList<T>> QueryAsync<T>(IQuery queryObject, Action<IDataReader, IList<T>> mapper, object param = null, CommandType commandType = CommandType.Text) where T : class
        {
            return await QueryAsync<T>(queryObject.ToSQL(), mapper, param, commandType);
        }

        /// <summary>        
        /// Execute an TSQL and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="executeSQL">TSQL to execute</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public int Execute(string executeSQL, object param = null, CommandType commandType = CommandType.StoredProcedure)
        {
            SqlCommand cmd = null;
            int affectedRecordCount = 0;
            SqlConnection connection = default(SqlConnection);

            try
            {
                if (_dbTransaction != default(SqlTransaction))
                {
                    cmd = new SqlCommand(executeSQL, _connection) { CommandType = commandType };
                    cmd.Transaction = _dbTransaction;
                }
                else
                {
                    connection = new SqlConnection(_connectionString);                    
                    OpenConnection(connection);
                    cmd = new SqlCommand(executeSQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {

                    SetParam(param, cmd);
                    stopWatch.Start();

                    affectedRecordCount = cmd.ExecuteNonQuery();

                    stopWatch.Stop();
                    LogInformation("Execute {0} took {1} ms", executeSQL, stopWatch.ElapsedMilliseconds);
                    stopWatch.Reset();
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }


            return affectedRecordCount;
        }

        /// <summary>        
        /// Execute an TSQL and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="executeSQL">TSQL to execute</param>
        /// <param name="param">the list of parameters for parametrized SQL. Please, define direction to read value from output parameters</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public int Execute(string executeSQL, QueryParameter[] param, CommandType commandType = CommandType.StoredProcedure)
        {
            SqlCommand cmd = null;
            int affectedRecordCount = 0;
            SqlConnection connection = default(SqlConnection);

            try
            {
                if (_dbTransaction != default(SqlTransaction))
                {
                    cmd = new SqlCommand(executeSQL, _connection) { CommandType = commandType };
                    cmd.Transaction = _dbTransaction;
                }
                else
                {
                    connection = new SqlConnection(_connectionString);                    
                    OpenConnection(connection);
                    cmd = new SqlCommand(executeSQL, connection) { CommandType = commandType };
                }

                using (cmd)
                {

                    SetParam(param, cmd);
                    stopWatch.Start();

                    affectedRecordCount = cmd.ExecuteNonQuery();

                    stopWatch.Stop();
                    LogInformation("Execute {0} to return took {1} ms", executeSQL, stopWatch.ElapsedMilliseconds);
                    stopWatch.Reset();

                    if (param != null)
                    {
                        foreach (var paramItem in param)
                        {
                            if ((paramItem.direction == ParameterDirection.Output) || (paramItem.direction == ParameterDirection.InputOutput))
                            {
                                paramItem.FillFromSqlParam(cmd.Parameters[paramItem.name]);
                            }
                        }
                    }
                }
            }
            finally
            {                
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return affectedRecordCount;
        }

        /// <summary>        
        /// Execute an TSQL asynchronous and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="executeSQL">TSQL to execute</param>
        /// <param name="param">the list of parameters for parametrized SQL (null by default)</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public async Task<int> ExecuteAsync(string executeSQL, object param = null, CommandType commandType = CommandType.StoredProcedure)
        {
            SqlCommand cmd = null;
            int affectedRecordCount = 0;
            SqlConnection connection = default(SqlConnection);

            try
            {
                if (_dbTransaction != default(SqlTransaction))
                {
                    cmd = new SqlCommand(executeSQL, _connection) { CommandType = commandType };
                    cmd.Transaction = _dbTransaction;
                }
                else
                {
                    connection = new SqlConnection(_connectionString);
                    await OpenConnectionAsync(connection);
                    cmd = new SqlCommand(executeSQL, connection) { CommandType = commandType };
                }
                using (cmd)
                {
                    SetParam(param, cmd);

                    stopWatch.Start();

                    affectedRecordCount = await cmd.ExecuteNonQueryAsync();

                    stopWatch.Stop();
                    LogInformation("ExecuteAsync {0} took {1} ms", executeSQL, stopWatch.ElapsedMilliseconds);
                    stopWatch.Reset();
                }
            }
            finally
            {                
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return affectedRecordCount;
        }

        /// <summary>        
        /// Execute an TSQL asynchronous and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="executeSQL">TSQL to execute</param>
        /// <param name="param">the list of parameters for parametrized SQL. Please, define direction to read value from output parameters</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public async Task<int> ExecuteAsync(string executeSQL, QueryParameter[] param, CommandType commandType = CommandType.StoredProcedure, bool transaction = true)
        {

            SqlCommand cmd = null;
            int affectedRecordCount = 0;
            SqlConnection connection = default(SqlConnection);

            try
            {
                if (_dbTransaction != default(SqlTransaction))
                {
                    cmd = new SqlCommand(executeSQL, _connection) { CommandType = commandType };
                    cmd.Transaction = _dbTransaction;
                }
                else
                {
                    connection = new SqlConnection(_connectionString);
                    await OpenConnectionAsync(connection);
                    cmd = new SqlCommand(executeSQL, connection) { CommandType = commandType };
                }

                if (transaction == false)
                {
                    cmd.Transaction = null;
                }

                using (cmd)
                {
                    SetParam(param, cmd);

                    stopWatch.Start();

                    affectedRecordCount = await cmd.ExecuteNonQueryAsync();

                    stopWatch.Stop();
                    LogInformation("ExecuteAsync {0} to return took {1} ms", executeSQL, stopWatch.ElapsedMilliseconds);
                    stopWatch.Reset();

                    if (param != null)
                    {
                        foreach (var paramItem in param)
                        {
                            if ((paramItem.direction == ParameterDirection.Output) || (paramItem.direction == ParameterDirection.InputOutput))
                            {
                                paramItem.FillFromSqlParam(cmd.Parameters[paramItem.name]);
                            }
                        }
                    }
                }
            }
            finally
            {
                if (connection != default(SqlConnection))
                {
                    CloseConnection(connection);
                }
            }

            return affectedRecordCount;
        }

        /// <summary>        
        /// Execute an TSQL and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="execObject">IQuery object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public int Execute(IQuery execObject, object param = null, CommandType commandType = CommandType.StoredProcedure)
        {
            return Execute(execObject.ToSQL(), param, commandType);
        }

        /// <summary>        
        /// Execute an TSQL asynchronous and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="execObject">IQuery object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public async Task<int> ExecuteAsync(IQuery execObject, object param = null, CommandType commandType = CommandType.StoredProcedure)
        {
            return await ExecuteAsync(execObject.ToSQL(), param, commandType);
        }

        /// <summary>        
        /// Execute an TSQL and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="execObject">IQuery object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL. Please, define direction to read value from output parameters</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public int Execute(IQuery execObject, QueryParameter[] param, CommandType commandType = CommandType.StoredProcedure)
        {
            return Execute(execObject.ToSQL(), param, commandType);
        }

        /// <summary>        
        /// Execute an TSQL asynchronous and return the affected records count. 
        /// To run ordinary TSQL statement, pass commandType parameter CommandType.Text
        /// </summary>
        /// <param name="execObject">IQuery object that can be translated to SQL</param>
        /// <param name="param">the list of parameters for parametrized SQL. Please, define direction to read value from output parameters</param>
        /// <param name="commandType">command type (StoredProcedure by default)</param>
        public async Task<int> ExecuteAsync(IQuery execObject, QueryParameter[] param, CommandType commandType = CommandType.StoredProcedure)
        {
            return await ExecuteAsync(execObject.ToSQL(), param, commandType);
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

        private void LogInformation(string message, params object[] args)
        {
            if (_logger != default(ILogger))
            {
                _logger.LogInformation(message, args);
            }
        }

        private void LogError(string message, params object[] args)
        {
            if (_logger != default(ILogger))
            {
                _logger.LogError(message, args);
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
