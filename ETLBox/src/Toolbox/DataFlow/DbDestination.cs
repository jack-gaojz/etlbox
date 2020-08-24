using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A database destination represents a table where data from the flow is inserted.
    /// Inserts are done in batches (using Bulk insert or an equivalent).
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Data type for input, preferably representing the destination table.</typeparam>
    public class DbDestination<TInput> : DataFlowBatchDestination<TInput>
    {
        #region Public properties

        public override string TaskName => $"Write data into table {DestinationTableDefinition?.Name ?? TableName}";

        /// <summary>
        /// If you don't want ETLBox to dynamically read the destination table definition from the database,
        /// you can provide your own table definition.
        /// </summary>
        public TableDefinition DestinationTableDefinition { get; set; }

        /// <summary>
        /// Name of the target table that receives the data from the data flow.
        /// </summary>
        public string TableName { get; set; }

        public IConnectionManager BulkInsertConnectionManager { get; protected set; }

        #endregion

        #region Connection manager

        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IConnectionManager DbConnectionManager
        {
            get
            {
                if (ConnectionManager == null)
                    return (IConnectionManager)ControlFlow.ControlFlow.DefaultDbConnection;
                else
                    return (IConnectionManager)ConnectionManager;
            }
        }

        public string QB => DbConnectionManager.QB;
        public string QE => DbConnectionManager.QE;
        public ConnectionManagerType ConnectionType => this.DbConnectionManager.ConnectionManagerType;

        #endregion

        #region Constructors

        public DbDestination()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
        }

        public DbDestination(string tableName) : this()
        {
            TableName = tableName;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        public DbDestination(string tableName, int batchSize) : this(tableName)
        {
            BatchSize= batchSize;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize) : this(connectionManager, tableName)
        {
            BatchSize = batchSize;
        }

        #endregion

        #region Implementation

        TypeInfo TypeInfo { get; set; }
        bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        TableData<TInput> TableData { get; set; }

        private void LoadTableDefinitionFromTableName()
        {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.FromTableName(this.DbConnectionManager, TableName);
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }

        protected override void PrepareWrite()
        {
            if (!HasDestinationTableDefinition)
                LoadTableDefinitionFromTableName();
            BulkInsertConnectionManager = this.DbConnectionManager.CloneIfAllowed();
            BulkInsertConnectionManager.IsInBulkInsert = true;
            BulkInsertConnectionManager.PrepareBulkInsert(DestinationTableDefinition.Name);
            TableData = new TableData<TInput>(DestinationTableDefinition, BatchSize);
        }

        protected override void TryBulkInsertData(TInput[] data)
        {
            TryAddDynamicColumnsToTableDef(data);
            try
            {
                TableData.ClearData();
                ConvertAndAddRows(data);
                var sql = new SqlTask($"Execute Bulk insert")
                {
                    DisableLogging = true,
                    ConnectionManager = BulkInsertConnectionManager
                };
                sql.CopyLogTaskProperties(this);
                sql
                .BulkInsert(TableData, DestinationTableDefinition.Name);
                BulkInsertConnectionManager.CheckLicenseOrThrow(ProgressCount);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput[]>(data));
            }
        }

        protected override void FinishWrite()
        {
            TableData?.Close();
            if (BulkInsertConnectionManager != null)
            {
                BulkInsertConnectionManager.IsInBulkInsert = false;
                BulkInsertConnectionManager.CleanUpBulkInsert(DestinationTableDefinition?.Name);
                BulkInsertConnectionManager.CloseIfAllowed();
            }
        }

        private void TryAddDynamicColumnsToTableDef(TInput[] data)
        {
            if (TypeInfo.IsDynamic && data.Length > 0)
            {
                for (int i = 0; i < data.Length; i++)
                {
                    if (data[i] == null) continue;
                    foreach (var column in (IDictionary<string, object>)data[i])
                    {
                        int newPropIndex = TableData.DynamicColumnNames.Count;
                        if (!TableData.DynamicColumnNames.ContainsKey(column.Key))
                            TableData.DynamicColumnNames.Add(column.Key, newPropIndex);
                    }
                }
            }
        }

        private void ConvertAndAddRows(TInput[] data)
        {
            foreach (var CurrentRow in data)
            {
                if (CurrentRow == null) continue;
                object[] rowResult;
                if (TypeInfo.IsArray)
                {
                    rowResult = CurrentRow as object[];
                }
                else if (TypeInfo.IsDynamic)
                {
                    IDictionary<string, object> propertyValues = (IDictionary<string, object>)CurrentRow;
                    rowResult = new object[TableData.DynamicColumnNames.Count];
                    foreach (var prop in propertyValues)
                    {
                        int columnIndex = TableData.DynamicColumnNames[prop.Key];
                        rowResult[columnIndex] = prop.Value;
                    }
                }
                else
                {
                    rowResult = new object[TypeInfo.PropertyLength];
                    int index = 0;
                    foreach (PropertyInfo propInfo in TypeInfo.Properties)
                    {
                        rowResult[index] = propInfo.GetValue(CurrentRow);
                        index++;
                    }
                }
                TableData.Rows.Add(rowResult);
            }
        }

        #endregion
    }

    /// <summary>
    /// A database destination represents a table where data from the flow is inserted.
    /// Inserts are done in batches (using Bulk insert or an equivalent).
    /// The DbDestination uses the dynamic ExpandoObject as input type.
    /// If you need other data types, use the generic DbDestination instead.
    /// </summary>
    /// <see cref="DbDestination{TInput}"/>
    public class DbDestination : DbDestination<ExpandoObject>
    {
        public DbDestination() : base() { }

        public DbDestination(string tableName) : base(tableName) { }

        public DbDestination(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName) { }

        public DbDestination(string tableName, int batchSize) : base(tableName, batchSize) { }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize) : base(connectionManager, tableName, batchSize) { }
    }
}
