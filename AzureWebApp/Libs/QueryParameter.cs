using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace AzureWebApp.Libs
{
    /*
     * Note that this is identical to SqlDbType enumeration
     */
    public enum DBTypeParam
    {
        BigInt = 0, Binary = 1, Bit = 2, Char = 3, DateTime = 4, Decimal = 5, Float = 6, Image = 7, Int = 8, Money = 9, NChar = 10, NText = 11,
        NVarChar = 12, Real = 13, UniqueIdentifier = 14, SmallDateTime = 15, SmallInt = 16, SmallMoney = 17, Text = 18, Timestamp = 19, TinyInt = 20,
        VarBinary = 21, VarChar = 22, Variant = 23, Xml = 25, Udt = 29, Structured = 30, Date = 31, Time = 32, DateTime2 = 33, DateTimeOffset = 34
    }

    public static class ParamArray
    {
        /// <summary>        
        /// Extension to find paramater among parameter array
        /// </summary>
        public static T Find<T>(this Array paramArray, Func<T, bool> compare)
        {
            foreach(var item in paramArray)
            {
                if (compare((T)item)) {
                    return (T)item;
                }
            }
            return default(T);
        }        
    }

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
}
