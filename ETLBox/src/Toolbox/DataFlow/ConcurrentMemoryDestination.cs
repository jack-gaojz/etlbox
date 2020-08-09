using ETLBox.ControlFlow;
using System;
using System.Collections.Concurrent;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A destination in memory which can be accessed concurrently - it will store all you data in a blocking collection.
    /// </summary>
    /// <see cref="MemoryDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    public class ConcurrentMemoryDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        public override string TaskName => $"Write data into memory";
        public BlockingCollection<TInput> Data { get; set; } = new BlockingCollection<TInput>();

        #endregion

        #region Constructors

        public ConcurrentMemoryDestination()
        {

        }

        #endregion

        #region Implement abstract methods

        internal override void InitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteRecord, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                MaxDegreeOfParallelism = 1
            });
        }

        protected override void CleanUpOnSuccess()
        {
            Data?.CompleteAdding();
            NLogFinish();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
            Data?.CompleteAdding();
        }

        #endregion

        #region Implementation

        protected void WriteRecord(TInput row)
        {
            NLogStartOnce();
            if (Data == null) Data = new BlockingCollection<TInput>();
            if (row == null) return;
            Data.Add(row);
            LogProgress();
        }

        #endregion
    }

    /// <summary>
    /// A destination in memory - it will store all you data in a blocking collection.
    /// The MemoryDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="MemoryDestination{TInput}"/>
    public class ConcurrentMemoryDestination : ConcurrentMemoryDestination<ExpandoObject>
    {
        public ConcurrentMemoryDestination() : base() { }
    }
}
