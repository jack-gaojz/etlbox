using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, IDataFlowSource<TOutput>, ITask
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

        protected override void FaultBufferExplicitly(Exception e)
        {
            SourceBlock.Fault(e);
        }

        //protected override void LinkBuffers(DataFlowTask successor, LinkPredicate linkPredicate)
        //{
        //    var s = successor as IDataFlowLinkTarget<TOutput>;
        //    if (linkPredicate.Predicate != null)
        //    {
        //        this.SourceBlock.LinkTo<TOutput>(s.TargetBlock, linkPredicate.GetPredicate<TOutput>());
        //        if (linkPredicate.VoidPredicate != null)
        //            this.SourceBlock.LinkTo<TOutput>(DataflowBlock.NullTarget<TOutput>(), linkPredicate.GetVoidPredicate<TOutput>());
        //    }
        //    else
        //        this.SourceBlock.LinkTo<TOutput>(s.TargetBlock);
        //}

        protected override void LinkBuffers(DataFlowTask successor, LinkPredicate linkPredicate)
        {
            var s = successor as IDataFlowLinkTarget<TOutput>;
            var lp = new Linker<TOutput>(linkPredicate?.Predicate, linkPredicate?.VoidPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }



        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, SourceBlock.Completion);

    }
}
