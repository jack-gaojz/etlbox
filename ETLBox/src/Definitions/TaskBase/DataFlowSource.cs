using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, IDataFlowExecutableSource<TOutput>, IDataFlowSource<TOutput>
    {
        #region Public properties
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;

        #endregion

        #region Buffer and completion
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();
        protected override Task BufferCompletion => Buffer.Completion;
        internal override void InitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            Completion = new Task(OnExecutionDoAsyncWork, TaskCreationOptions.LongRunning);
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

        #region Linking

        internal override void LinkBuffers(DataFlowTask successor, LinkPredicates linkPredicates)
        {
            var s = successor as IDataFlowDestination<TOutput>;
            var linker = new BufferLinker<TOutput>(linkPredicates);
            linker.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target)
            => InternalLinkTo<TOutput>(target);

        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
           => InternalLinkTo<TOutput>(target, rowsToKeep);

        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target)
             => InternalLinkTo<TConvert>(target);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
            => InternalLinkTo<TConvert>(target, rowsToKeep);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);

        #endregion
    }
}
