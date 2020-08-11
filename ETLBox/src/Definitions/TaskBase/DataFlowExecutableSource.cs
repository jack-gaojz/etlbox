using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowExecutableSource<TOutput> : DataFlowSource<TOutput>, IDataFlowExecutableSource<TOutput>
    {
        #region Buffer and completion
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();
        protected override Task BufferCompletion => Buffer.Completion;
        public override void InitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            Completion = new Task(
               () =>
               {
                   try
                   {
                       OnExecutionDoAsyncWork();
                       CompleteBuffer();
                       ErrorSource?.CompleteBuffer();
                       CleanUpOnSuccess();
                   }
                   catch (Exception e)
                   {
                       FaultBuffer(e);
                       ErrorSource?.FaultBuffer(e);
                       CleanUpOnFaulted(e);
                       throw e;
                   }
               }
               , TaskCreationOptions.LongRunning);
        }

        protected override void CompleteBuffer() => SourceBlock.Complete();
        protected override void FaultBuffer(Exception e) => SourceBlock.Fault(e);

        #endregion

        #region Execution and IDataFlowExecutableSource
        public virtual void Execute() //remove virtual
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.RunSynchronously();
        }

        public Task ExecuteAsync()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.Start();
            return Completion;
        }

        protected virtual void OnExecutionDoSynchronousWork() { } //abstract

        protected virtual void OnExecutionDoAsyncWork() { } //abstract

        #endregion
    }
}
