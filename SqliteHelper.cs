/***shilin(2010.4.15)***/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.Data;
using System.Collections;

namespace Util
{
    public class SqliteHelper
    {
        private SqliteHelper()
        {
            //   
            // TODO: 在此处添加构造函数逻辑   
            //   
        }
        #region 静态私有方法
        /// <SUMMARY></SUMMARY>   
        /// 附加参数   
        ///    
        /// <PARAM name="command" />   
        /// <PARAM name="commandParameters" />   
        private static void AttachParameters(SQLiteCommand command, SQLiteParameter[] commandParameters)
        {
            command.Parameters.Clear();
            foreach (SQLiteParameter p in commandParameters)
            {
                if (p.Direction == ParameterDirection.InputOutput && p.Value == null)
                    p.Value = DBNull.Value;
                command.Parameters.Add(p);
            }
        }
        /// <SUMMARY></SUMMARY>   
        /// 分配参数值   
        ///    
        /// <PARAM name="commandParameters" />   
        /// <PARAM name="parameterValues" />   
        private static void AssignParameterValues(SQLiteParameter[] commandParameters, object[] parameterValues)
        {
            if (commandParameters == null || parameterValues == null)
                return;
            if (commandParameters.Length != parameterValues.Length)
                throw new ArgumentException("Parameter count does not match Parameter Value count.");
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                commandParameters[i].Value = parameterValues[i];
            }
        }
        /// <SUMMARY></SUMMARY>   
        /// 预备执行command命令   
        ///    
        /// <PARAM name="command" />   
        /// <PARAM name="connection" />   
        /// <PARAM name="transaction" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <PARAM name="commandParameters" />   
        private static void PrepareCommand(SQLiteCommand command,
            SQLiteConnection connection, SQLiteTransaction transaction,
            CommandType commandType, string commandText, SQLiteParameter[] commandParameters
            )
        {
            if (commandType == CommandType.StoredProcedure)
            {
                throw new ArgumentException("SQLite 暂时不支持存储过程");
            }
            if (connection.State != ConnectionState.Open)
                connection.Open();
            command.Connection = connection;
            command.CommandText = commandText;
            if (transaction != null)
                command.Transaction = transaction;
            command.CommandType = commandType;
            if (commandParameters != null)
                AttachParameters(command, commandParameters);
            return;
        }
        #endregion

        #region ExecuteNonQuery 执行SQL命令，返回影响行数
        /// <SUMMARY></SUMMARY>   
        /// 执行SQL命名   
        ///    
        /// <PARAM name="connectionString" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <RETURNS></RETURNS>   
        public static int ExecuteNonQuery(string connectionString, string commandText)
        {
            return ExecuteNonQuery(connectionString, commandText, (SQLiteParameter[])null);
        }
        /// <SUMMARY></SUMMARY>   
        /// 不支持存储过程，但可以参数化查询   
        ///    
        /// <PARAM name="connectionString" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <PARAM name="commandParameters" />   
        /// <RETURNS></RETURNS>   
        public static int ExecuteNonQuery(string connectionString, string commandText, params SQLiteParameter[] commandParameters)
        {
            using (SQLiteConnection conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                return ExecuteNonQuery(conn, CommandType.Text, commandText, commandParameters);
            }
        }
        private static int ExecuteNonQuery(SQLiteConnection connection, CommandType commandType, string commandText)
        {
            return ExecuteNonQuery(connection, commandType, commandText, (SQLiteParameter[])null);
        }
        private static int ExecuteNonQuery(SQLiteConnection connection, CommandType commandType, string commandText, params SQLiteParameter[] commandParameters)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            PrepareCommand(cmd, connection, (SQLiteTransaction)null, commandType, commandText, commandParameters);
            int retval = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            connection.Close();
            return retval;
        }

        /// <summary>
        /// 在事务中执行NonQuery
        /// </summary>
        /// <param name="tran"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="commandParameters"></param>
        /// <returns></returns>
        public static int ExecuteNonQuery(SQLiteConnection conn, SQLiteTransaction tran, string commandText, params SQLiteParameter[] commandParameters)
        {
            return ExecuteNonQuery(conn, tran, CommandType.Text, commandText, commandParameters);
        }

        /// <summary>
        /// 在事务中执行NonQuery
        /// </summary>
        /// <param name="tran"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="commandParameters"></param>
        /// <returns></returns>
        private static int ExecuteNonQuery(SQLiteConnection conn, SQLiteTransaction tran, CommandType commandType, string commandText, params SQLiteParameter[] commandParameters)
        {
            using (SQLiteCommand cmd = new SQLiteCommand())
            {
                PrepareCommand(cmd, conn, tran, commandType, commandText, commandParameters);

                return cmd.ExecuteNonQuery();
            }
        }

        #endregion

        #region ExecuteDataSet 执行SQL查询，并将返回数据填充到DataSet
        private static DataSet ExecuteDataset(SQLiteConnection connection, CommandType commandType, string commandText, params SQLiteParameter[] commandParameters)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            PrepareCommand(cmd, connection, (SQLiteTransaction)null, commandType, commandText, commandParameters);
            SQLiteDataAdapter da = new SQLiteDataAdapter(cmd);
            DataSet ds = new DataSet();
            da.Fill(ds);
            cmd.Parameters.Clear();
            connection.Close();
            return ds;
        }
        private static DataSet ExecuteDataset(SQLiteConnection connection, CommandType commandType, string commandText)
        {
            return ExecuteDataset(connection, commandType, commandText, (SQLiteParameter[])null);
        }
        public static DataSet ExecuteDataset(string connectionString, string commandText, params SQLiteParameter[] commandParameters)
        {

            using (SQLiteConnection cn = new SQLiteConnection(connectionString))
            {
                cn.Open();

                return ExecuteDataset(cn, CommandType.Text, commandText, commandParameters);
            }
        }
        public static DataSet ExecuteDataset(string connectionString, string commandText)
        {
            return ExecuteDataset(connectionString, commandText, (SQLiteParameter[])null);
        }
        #endregion

        #region ExecuteReader 执行SQL查询,返回DbDataReader
        private static SQLiteDataReader ExecuteReader(SQLiteConnection connection, SQLiteTransaction transaction, CommandType commandType, string commandText, SQLiteParameter[] commandParameters, DbConnectionOwnership connectionOwnership)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);
            SQLiteDataReader dr;
            if (connectionOwnership == DbConnectionOwnership.External)
                dr = cmd.ExecuteReader();
            else
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            cmd.Parameters.Clear();
            return dr;
        }
        /// <SUMMARY></SUMMARY>   
        ///读取数据后将自动关闭连接   
        ///    
        /// <PARAM name="connectionString" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <PARAM name="commandParameters" />   
        /// <RETURNS></RETURNS>   
        public static SQLiteDataReader ExecuteReader(string connectionString, string commandText, params SQLiteParameter[] commandParameters)
        {
            SQLiteConnection cn = new SQLiteConnection(connectionString);
            cn.Open();
            try
            {
                return ExecuteReader(cn, null, CommandType.Text, commandText, commandParameters, DbConnectionOwnership.Internal);
            }
            catch
            {
                cn.Close();
                throw;
            }
        }
        /// <SUMMARY></SUMMARY>   
        /// 读取数据后将自动关闭连接   
        ///    
        /// <PARAM name="connectionString" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <RETURNS></RETURNS>   
        public static SQLiteDataReader ExecuteReader(string connectionString, string commandText)
        {
            return ExecuteReader(connectionString, commandText, (SQLiteParameter[])null);
        }
        /// <SUMMARY></SUMMARY>   
        /// 读取数据以后需要自行关闭连接   
        ///    
        /// <PARAM name="connection" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <RETURNS></RETURNS>   
        private static SQLiteDataReader ExecuteReader(SQLiteConnection connection, CommandType commandType, string commandText)
        {
            return ExecuteReader(connection, commandType, commandText, (SQLiteParameter[])null);
        }
        /// <SUMMARY></SUMMARY>   
        /// 读取数据以后需要自行关闭连接   
        ///    
        /// <PARAM name="connection" />   
        /// <PARAM name="commandType" />   
        /// <PARAM name="commandText" />   
        /// <PARAM name="commandParameters" />   
        /// <RETURNS></RETURNS>   
        private static SQLiteDataReader ExecuteReader(SQLiteConnection connection, CommandType commandType, string commandText, params SQLiteParameter[] commandParameters)
        {
            return ExecuteReader(connection, (SQLiteTransaction)null, commandType, commandText, commandParameters, DbConnectionOwnership.External);
        }
        #endregion

        #region 执行多条SQL语句
        public static void ExecuteSqlTran(string connectionString, Hashtable SQLStringList)
        {
            using (SQLiteConnection cn = new SQLiteConnection(connectionString))
            {
                cn.Open();
                using (SQLiteTransaction tran = cn.BeginTransaction())
                {
                    SQLiteCommand cmd = new SQLiteCommand();
                    try
                    {
                        foreach (DictionaryEntry de in SQLStringList)
                        {
                            string sql = de.Key.ToString();
                            SQLiteParameter[] cmdParams = (SQLiteParameter[])de.Value;
                            if (sql.Trim().Length > 1)
                            {
                                PrepareCommand(cmd, cn, tran, CommandType.Text, sql, cmdParams);
                                cmd.ExecuteNonQuery();
                                cmd.Parameters.Clear();
                            }
                        }
                        tran.Commit();
                    }
                    catch
                    {
                        tran.Rollback();
                    }
                    finally
                    {
                        cn.Close();
                    }
                }
            }
        }

        public static void ExecuteSqlTran(string connectionString, ArrayList SQLStringList)
        {
            using (SQLiteConnection cn = new SQLiteConnection(connectionString))
            {
                cn.Open();
                using (SQLiteTransaction tran = cn.BeginTransaction())
                {
                    SQLiteCommand cmd = new SQLiteCommand();
                    try
                    {
                        for (int n = 0; n < SQLStringList.Count; n++)
                        {
                            string sql = SQLStringList[n].ToString();
                            if (sql.Trim().Length > 1)
                            {
                                PrepareCommand(cmd, cn, tran, CommandType.Text, sql, null);
                                cmd.ExecuteNonQuery();
                                cmd.Parameters.Clear();
                            }
                        }
                        tran.Commit();
                    }
                    catch
                    {
                        tran.Rollback();
                    }
                    finally
                    {
                        cn.Close();
                    }
                }
            }
        }



        #endregion


    }




    /// <SUMMARY></SUMMARY>   
    /// DbConnectionOwnership DataReader以后是否自动关闭连接   
    ///    
    public enum DbConnectionOwnership
    {
        /// <SUMMARY></SUMMARY>   
        /// 自动关闭   
        ///    
        Internal,
        /// <SUMMARY></SUMMARY>   
        /// 手动关闭   
        ///    
        External
    }


}

