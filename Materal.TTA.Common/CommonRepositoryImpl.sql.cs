using System.Data;

namespace Materal.TTA.Common;

public partial class CommonRepositoryImpl<TEntity>
{
    /// <summary>
    /// 获取连接字符串
    /// </summary>
    /// <returns></returns>
    /// <exception cref="TTAException"></exception>
    protected abstract string GetConnectionString();
    /// <summary>
    /// 获取连接
    /// </summary>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    protected abstract IDbConnection GetConnection(string connectionString);
    /// <summary>
    /// 获取连接
    /// </summary>
    /// <returns></returns>
    protected virtual IDbConnection GetConnection()
    {
        string connectionString = GetConnectionString();
        return GetConnection(connectionString);
    }
    #region 直接执行SQL
    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <returns></returns>
    protected virtual void ExecuteSql(string tSql)
        => ExecuteSql(tSql, null, null);
    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <returns></returns>
    protected virtual void ExecuteSql(string tSql, ICollection<IDataParameter> sqlParameters)
        => ExecuteSql(tSql, sqlParameters, null);
    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    protected virtual void ExecuteSql(string tSql, Action<IDataReader> onHandler)
        => ExecuteSql(tSql, null, onHandler);
    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    /// <exception cref="TTAException"></exception>
    protected virtual void ExecuteSql(string tSql, ICollection<IDataParameter>? sqlParameters, Action<IDataReader>? onHandler)
    {
        using IDbConnection sqlConnection = GetConnection();
        try
        {
            sqlConnection.Open();
            IDbCommand sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = tSql;
            if (sqlParameters is not null && sqlParameters.Count > 0)
            {
                foreach (IDataParameter sqlParameter in sqlParameters)
                {
                    sqlCommand.Parameters.Add(sqlParameter);
                }
            }
            IDataReader sqlDataReader = sqlCommand.ExecuteReader();
            while (sqlDataReader.Read())
            {
                onHandler?.Invoke(sqlDataReader);
            }
            sqlDataReader.Close();
        }
        finally
        {
            sqlConnection.Close();
        }
    }
    #endregion
    #region 非查询
    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <returns></returns>
    /// <exception cref="TTAException"></exception>
    protected virtual int ExecuteNonQuery(string tSql, ICollection<IDataParameter> sqlParameters)
    {
        using IDbConnection sqlConnection = GetConnection();
        try
        {
            sqlConnection.Open();
            IDbCommand sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = tSql;
            if (sqlParameters is not null && sqlParameters.Count > 0)
            {
                foreach (IDataParameter sqlParameter in sqlParameters)
                {
                    sqlCommand.Parameters.Add(sqlParameter);
                }
            }
            int result = sqlCommand.ExecuteNonQuery();
            return result;
        }
        finally
        {
            sqlConnection.Close();
        }
    }
    #endregion
    #region 查询
    #region 非泛型

    /// <summary>
    /// 执行查询SQL语句（非泛型）
    /// </summary>
    /// <param name="tSql"></param>
    /// <returns></returns>
    protected virtual List<object[]> ExecuteQuerySql(string tSql)
        => ExecuteQuerySql(tSql, null, null);
    /// <summary>
    /// 执行查询SQL语句（非泛型）
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <returns></returns>
    protected virtual List<object[]> ExecuteQuerySql(string tSql, ICollection<IDataParameter>? sqlParameters)
        => ExecuteQuerySql(tSql, sqlParameters, null);
    /// <summary>
    /// 执行查询SQL语句（非泛型）
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    protected virtual List<object[]> ExecuteQuerySql(string tSql, Func<IDataReader, object[]> onHandler)
        => ExecuteQuerySql(tSql, null, onHandler);
    /// <summary>
    /// 执行查询SQL语句（非泛型）
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <param name="onHandler">每行数据的处理回调，返回结果将存入列表</param>
    /// <returns></returns>
    protected virtual List<object[]> ExecuteQuerySql(string tSql, ICollection<IDataParameter>? sqlParameters, Func<IDataReader, object[]>? onHandler)
    {
        List<object[]> result = [];
        ExecuteSql(tSql, sqlParameters, dr =>
        {
            if (onHandler is null)
            {
                object[] row = new object[dr.FieldCount];
                dr.GetValues(row);
                result.Add(row);
            }
            else
            {
                result.Add(onHandler.Invoke(dr));
            }
        });
        return result;
    }
    #endregion
    #region 泛型
    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <returns></returns>
    protected virtual List<TModel> ExcuteQuerySql<TModel>(string tSql)
        where TModel : new()
        => ExcuteQuerySql<TModel>(tSql, null, null);
    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <returns></returns>
    protected virtual List<TModel> ExcuteQuerySql<TModel>(string tSql, ICollection<IDataParameter> sqlParameters)
        where TModel : new()
        => ExcuteQuerySql<TModel>(tSql, sqlParameters, null);
    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    protected virtual List<TModel> ExcuteQuerySql<TModel>(string tSql, Func<IDataReader, TModel> onHandler)
        where TModel : new()
        => ExcuteQuerySql<TModel>(tSql, null, onHandler);
    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="tSql"></param>
    /// <param name="sqlParameters"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    protected virtual List<TModel> ExcuteQuerySql<TModel>(string tSql, ICollection<IDataParameter>? sqlParameters, Func<IDataReader, TModel>? onHandler)
        where TModel : new()
    {
        List<TModel> result = [];
        ExecuteSql(tSql, sqlParameters, dr =>
        {
            if (onHandler is null)
            {
                result.Add(BindData<TModel>(dr));
            }
            else
            {
                result.Add(onHandler.Invoke(dr));
            }
        });
        return result;
    }
    #endregion
    /// <summary>
    /// 绑定数据
    /// </summary>
    /// <param name="sqlDataReader"></param>
    /// <returns></returns>
    protected virtual TModel BindData<TModel>(IDataReader sqlDataReader)
        where TModel : new()
    {
        PropertyInfo[] propertyInfos = typeof(TModel).GetProperties();
        TModel result = new();
        MethodInfo getFieldValuemethodInfo = sqlDataReader.GetType().GetMethod("GetFieldValue") ?? throw new TTAException("获取字段值方法获取失败");
        for (int i = 0; i < sqlDataReader.FieldCount; i++)
        {
            string name = sqlDataReader.GetName(i);
            PropertyInfo? propertyInfo = propertyInfos.FirstOrDefault(m => m.Name == name);
            if (propertyInfo is null) continue;
            object? value;
            if (!sqlDataReader.IsDBNull(i))
            {
                Type propertyType = propertyInfo.PropertyType;
                if (propertyType.IsGenericType && propertyType.GenericTypeArguments.Length == 1 && propertyType == typeof(Nullable<>).MakeGenericType(propertyType.GenericTypeArguments[0]))
                {
                    propertyType = propertyType.GenericTypeArguments[0];
                }
                value = getFieldValuemethodInfo.MakeGenericMethod(propertyType).Invoke(sqlDataReader, [i]);
            }
            else
            {
                value = null;
            }
            propertyInfo.SetValue(result, value);
        }
        return result;
    }
    /// <summary>
    /// 绑定列表
    /// </summary>
    /// <typeparam name="TModel"></typeparam>
    /// <param name="sqlDataReader"></param>
    /// <param name="onHandler"></param>
    /// <returns></returns>
    protected virtual List<TModel> BindList<TModel>(IDataReader sqlDataReader, Func<IDataReader, TModel>? onHandler = null)
        where TModel : new()
    {
        List<TModel> result = [];
        while (sqlDataReader.Read())
        {
            if (onHandler is null)
            {
                result.Add(BindData<TModel>(sqlDataReader));
            }
            else
            {
                result.Add(onHandler.Invoke(sqlDataReader));
            }
        }
        sqlDataReader.Close();
        return result;
    }
    #endregion
}
