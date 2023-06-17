using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace Csharp_SQLDatabaseConnection
{
    public static class SqlDatabaseConnection
    {
        //Connect to the Database
        public static SqlConnection DBConnect(this SqlConnection sqlConnection, string connectionstring)
        {
            try
            {

                if (connectionstring==null) 
                {
                sqlConnection.Close();
                }
            else
                {
                    sqlConnection = new SqlConnection(connectionstring);
                    sqlConnection.Open();
                    return sqlConnection;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Error in the connection establishment "+ex.Message);
            }
            return null;

        }

        //Close to the Database Connection
        public static void DataBaseClose(this SqlConnection sqlConnection)
        {
            try
            {
                sqlConnection.Close();
            }
            catch(Exception ex)
            {
                Console.WriteLine("Error in CLosing DB Connection "+ex.Message);
            }
        }


        //SQL Query Execution
        public static DataTable ExecuteQuery(this SqlConnection sqlConnection, string querystring)
        {
            DataSet dataset; 
            try
            {
                //Checking the state of the connection
                if (sqlConnection == null || ((sqlConnection != null && (sqlConnection.State == ConnectionState.Closed || sqlConnection.State == ConnectionState.Broken)))) sqlConnection.Open();
                SqlDataAdapter dataAdapter = new SqlDataAdapter();
                dataAdapter.SelectCommand = new SqlCommand(querystring, sqlConnection);
                dataAdapter.SelectCommand.CommandType = CommandType.Text;

                dataset = new DataSet();
                dataAdapter.Fill(dataset, "table");
                sqlConnection.Close();
                return dataset.Tables["table"];
            }
            catch (Exception ex)
            {
                dataset = null;
                sqlConnection.Close();
                Console.WriteLine("Error in Executing DB Query " + ex.Message);
                return null;
            }
            finally
            {
                sqlConnection.Close();
                dataset = null;
            }
        }

        public static DataTable ExecuteProcedureWithParametersDataTable(this SqlConnection sqlConnection, string procedureName, Hashtable parameters)
        {
            DataSet dataset;
            try
            {
                
                SqlDataAdapter dataAdapter = new SqlDataAdapter();
                dataAdapter.SelectCommand = new SqlCommand(procedureName, sqlConnection);
                dataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;

                if(parameters!=null)
                    foreach(DictionaryEntry de in parameters)
                    {
                        SqlParameter sp = new SqlParameter(de.Key.ToString(), de.Value.ToString());
                        dataAdapter.SelectCommand.Parameters.Add(sp);
                    }


                dataset = new DataSet();
                dataAdapter.Fill(dataset, "table");
                sqlConnection.Close();
                return dataset.Tables["table"];
            }
            catch (Exception ex)
            {
                dataset = null;
                sqlConnection.Close();
                Console.WriteLine("Error in Executing Stored Procedure with Parameters DataTable" + ex.Message);
                return null;
            }
            finally
            {
                sqlConnection.Close();
                dataset = null;
            }
        }

    }
}
