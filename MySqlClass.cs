using MySql.Data.MySqlClient;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System;

namespace PUSHKA
{
    namespace EasySQL
    {
        public class SqlDataBase
        {
            public enum LogState
            {
                Enabled,
                Disabled
            }
            public static LogState logState = LogState.Disabled;


            string connectionString;
            private MySqlConnection connection;

            public SqlDataBase(string DataSource, string Database, string User, string Pass)
            {   
                connectionString = $"server={DataSource};port=3306;Database={Database};CharacterSet=utf8mb4;user={User};password={Pass};POOLING=FALSE;";
                connection = new MySqlConnection(connectionString);
                connection.Open();
            }

            public bool RunQuery(string query)
            {
                if (connection.State == ConnectionState.Broken || connection.State == ConnectionState.Closed)
                {
                    Close();

                    connection = new MySqlConnection(connectionString);
                    connection.Open();
                }

                try
                {
                    MySqlCommand cmd = new MySqlCommand(CheckChars(query), connection);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    
                    if(logState == LogState.Enabled)
                    {
                        Console.WriteLine(CheckChars(query));
                    }
                    return true;
                }
                catch (MySqlException ex)
                {
                    if (logState == LogState.Enabled)
                    {
                        Console.WriteLine($"RUN ERROR: {CheckChars(query)} {ex.Message}");
                    }
                    return false;
                    throw;
                }
            }
            public bool SelectQuery(string query, out DataTable dataTable)
            {
                dataTable = new DataTable();

                if (connection.State == ConnectionState.Broken || connection.State == ConnectionState.Closed)
                {
                    Close();

                    connection = new MySqlConnection(connectionString);
                    connection.Open();
                }

                try
                {
                    MySqlCommand cmd = new MySqlCommand(CheckChars(query), connection);
                    var adapter = new MySqlDataAdapter(cmd);
                    adapter.Fill(dataTable);

                    adapter.Dispose();
                    cmd.Dispose();

                    if (logState == LogState.Enabled)
                    {
                        Console.WriteLine($"SELECTED {dataTable.Rows.Count} ROWS --- {CheckChars(query)}");
                    }

                    return true;
                }
                catch (MySqlException ex)
                {
                    if (logState == LogState.Enabled)
                    {
                        Console.WriteLine($"SELECT ERROR: {CheckChars(query)}; {ex.Message}");
                    }
                    return false;
                    throw;
                }
            }

            public bool SelectQuery<T>(string query, out List<T> data)
        where T : new()
            {
                DataTable dataTable = new DataTable();
                data = new List<T>();

                if (connection.State == ConnectionState.Broken || connection.State == ConnectionState.Closed)
                {
                    Close();

                    connection = new MySqlConnection(connectionString);
                    connection.Open();
                }

                try
                {
                    MySqlCommand cmd = new MySqlCommand(CheckChars(query), connection);
                    var adapter = new MySqlDataAdapter(cmd);
                    adapter.Fill(dataTable);

                    adapter.Dispose();
                    cmd.Dispose();

                    foreach (DataRow row in dataTable.Rows)
                    {
                        data.Add(ToObject<T>(row));
                    }

                    if (logState == LogState.Enabled)
                    {
                        Console.WriteLine($"SELECTED {dataTable.Rows.Count} ROWS --- {CheckChars(query)}");
                        Console.WriteLine($"   FINDED {data.Count} ROWS");
                    }

                    return true;
                }
                catch (MySqlException ex)
                {
                    if (logState == LogState.Enabled)
                    {
                        Console.WriteLine($"SELECT ERROR: {CheckChars(query)}; {ex.Message}");
                    }
                    return false;
                    throw;
                }
            }

            private T ToObject<T>(DataRow dataRow)
        where T : new()
            {
                T item = new T();

                foreach (DataColumn column in dataRow.Table.Columns)
                {
                    PropertyInfo property = GetProperty(typeof(T), column.ColumnName);

                    if (property != null && dataRow[column] != DBNull.Value && dataRow[column].ToString() != "NULL")
                    {
                        property.SetValue(item, ChangeType(dataRow[column], property.PropertyType), null);
                    }
                }

                return item;
            }
            private PropertyInfo GetProperty(Type type, string attributeName)
            {
                PropertyInfo property = type.GetProperty(attributeName);

                if (property != null)
                {
                    return property;
                }

                return type.GetProperties()
                     .Where(p => p.IsDefined(typeof(DisplayNameAttribute), false) && p.GetCustomAttributes(typeof(DisplayNameAttribute), false).Cast<DisplayNameAttribute>().Single().DisplayName == attributeName)
                     .FirstOrDefault();
            }
            private object ChangeType(object value, Type type)
            {
                if (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    if (value == null)
                    {
                        return null;
                    }

                    return Convert.ChangeType(value, Nullable.GetUnderlyingType(type));
                }

                return Convert.ChangeType(value, type);
            }



            public void Insert(DataRow Row)
            {
                List<string> ColumnNames = new List<string>();
                List<string> ColumnValues = new List<string>();

                for (int i = 0; i < Row.Table.Columns.Count; i++)
                {
                    if (string.IsNullOrEmpty(Row[i].ToString()) && Row.Table.Columns[i].AutoIncrement)
                    {
                        ColumnNames.Add($"{Row.Table.Columns[i].ColumnName}");
                        ColumnValues.Add($"'{Row[i]}'");
                    }
                }

                RunQuery($"INSERT INTO {Row.Table.TableName} ({string.Join(", ", ColumnNames)}) VALUES ({string.Join(", ", ColumnValues)})");

            }
            public void Update(DataRow Row, int ColumnIndex)
            {
                List<string> NonChanged = new List<string>();
                for (int i = 0; i < Row.Table.Columns.Count; i++)
                {
                    if (i != ColumnIndex && !string.IsNullOrEmpty(Row[i].ToString()))
                    {
                        NonChanged.Add($"{Row.Table.Columns[i].ColumnName} = '{Row[i]}'");
                    }
                }

                RunQuery($"update {Row.Table.TableName} SET {Row.Table.Columns[ColumnIndex].ColumnName} = '{Row[ColumnIndex]}' WHERE ({string.Join(" and ", NonChanged)})");
            }
            public void Remove(DataRow Row)
            {
                List<string> NonChanged = new List<string>();
                for (int i = 0; i < Row.Table.Columns.Count; i++)
                {
                    if (!string.IsNullOrEmpty(Row[i].ToString()))
                    {
                        NonChanged.Add($"{Row.Table.Columns[i].ColumnName} = '{Row[i]}'");
                    }
                }

                RunQuery($"DELETE FROM {Row.Table.TableName} WHERE ({string.Join(" and ", NonChanged)})");
            }



            public void Close()
            {
                if (connection != null && connection.State == ConnectionState.Open)
                    connection.Close();
                if (connection != null)
                    connection.Dispose();

                MySqlConnection.ClearPool(connection);
                connection = null;
                MySqlConnection.ClearAllPools();
            }

            private string CheckChars(string query)
            {
                string ErrorChar = "\\;";
                foreach (var item in ErrorChar)
                    query = query.Replace(item.ToString(), "");

                return query + ";";
            }
        }
    }
}
