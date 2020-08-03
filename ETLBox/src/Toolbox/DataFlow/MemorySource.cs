using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Reads data from a memory source. While reading the data from the list, data is also asnychronously posted into the targets.
    /// Data is read a as string from the source and dynamically converted into the corresponding data format.
    /// </summary>
    public class MemorySource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        public override string TaskName => $"Read data from memory";
        public IEnumerable<TOutput> Data { get; set; }
        public IList<TOutput> DataAsList
        {
            get
            {
                return Data as IList<TOutput>;
            }
            set
            {
                Data = value;
            }
        }

        #region Constructors

        public MemorySource()
        {
            Data = new List<TOutput>();
        }

        public MemorySource(IEnumerable<TOutput> data)
        {
            Data = data;
        }

        #endregion

        #region Execution

        protected override void OnExecutionDoSynchronousWork()
        {
            NLogStart();
        }

        protected override void OnExecutionDoAsyncWork()
        {
            ReadRecordAndSendIntoBuffer();
            NLogFinish();
        }


        private void ReadRecordAndSendIntoBuffer()
        {
            foreach (TOutput record in Data)
            {
                if (!Buffer.SendAsync(record).Result)
                    throw Exception;
                LogProgress();
            }
            Buffer.Complete();
        }

        #endregion

    }

    /// <summary>
    /// Reads data from a memory source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// MemorySource as a nongeneric type always return a dynamic object as output. If you need typed output, use
    /// the MemorySource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="MemorySource{TOutput}"/>
    public class MemorySource : MemorySource<ExpandoObject>
    {
        public MemorySource() : base() { }
        public MemorySource(IList<ExpandoObject> data) : base(data) { }
    }
}
