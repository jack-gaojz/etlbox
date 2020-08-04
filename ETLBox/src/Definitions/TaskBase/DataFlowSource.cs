using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, ITask, IDataFlowExecutableSource<TOutput>
    {
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();
        protected override Task BufferCompletion => Buffer.Completion;
        protected override void InitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            Completion = new Task(OnExecutionDoAsyncWork, TaskCreationOptions.LongRunning);
        }

        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

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

        protected override void CompleteBuffer()
        {
            SourceBlock.Complete();
        }

        protected override void FaultBuffer(Exception e)
        {
            SourceBlock.Fault(e);
        }

        protected override void LinkBuffers(DataFlowTask successor, LinkPredicate linkPredicate)
        {
            var s = successor as IDataFlowDestination<TOutput>;
            var lp = new Linker<TOutput>(linkPredicate?.Predicate, linkPredicate?.VoidPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        //public IDataFlowLinkSource<TOutput> LinkTo(DataFlowTask target)
        //    => InternalLinkTo<TOutput>(target);

        //public IDataFlowLinkSource<TOutput> LinkTo(DataFlowTask target, Predicate<TOutput> predicate)
        //    =>  InternalLinkTo<TOutput>(target, predicate);

        //public IDataFlowLinkSource<TOutput> LinkTo(DataFlowTask target, Predicate<TOutput> predicate, Predicate<TOutput> voidPredicate)
        //    => InternalLinkTo<TOutput>(target, predicate, voidPredicate);

        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(DataFlowTask target)
        // => InternalLinkTo<TConvert>(target);
        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(DataFlowTask target, Predicate<TOutput> predicate)
        //    => InternalLinkTo<TConvert>(target, predicate);

        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(DataFlowTask target, Predicate<TOutput> predicate, Predicate<TOutput> voidPredicate)
        //    => InternalLinkTo<TConvert> (target, predicate, voidPredicate);

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

        //TODO
        //LinkError
        public void LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, SourceBlock.Completion);

    }
}
