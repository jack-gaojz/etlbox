using ETLBox.ControlFlow;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A destination in memory - it will store all you data in a list
    /// </summary>
    /// <see cref="MemoryDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    public class MemoryDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        public override string TaskName => $"Write data into memory";
        public IList<TInput> Data { get; set; } = new List<TInput>();

        #endregion

        #region Constructors

        public MemoryDestination()
        {

        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteRecord, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                MaxDegreeOfParallelism = 1
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
        }

        #endregion

        #region Implementation

        protected void WriteRecord(TInput row)
        {
            NLogStartOnce();
            if (Data == null) Data = new List<TInput>();
            if (row == null) return;
            Data.Add(row);
            LogProgress();
        }

        #endregion
    }

    /// <summary>
    /// A destination in memory - it will store all you data in a list.
    /// The MemoryDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="MemoryDestination{TInput}"/>
    public class MemoryDestination : MemoryDestination<ExpandoObject>
    {
        public MemoryDestination() : base() { }
    }
}
