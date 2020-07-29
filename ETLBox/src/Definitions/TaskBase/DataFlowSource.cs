using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, IDataFlowLinkSource<TOutput>, ITask
    {
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();

        protected override void InitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public abstract void Execute();

        public virtual void ExecuteSyncPart() { }
        public virtual void ExecuteAsyncPart() { }
        public Task ExecuteAsync()
        {
            ExecuteSyncPart();
            return Task.Factory.StartNew(ExecuteAsyncPart);
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
        {
            this.Successors.Add(target);
            target.Predecessors.Add(this);
            return target as IDataFlowLinkSource<TOutput>;
        }

        internal override void LinkBuffers()
        {
            foreach (var succesor in Successors)
            {
                var s = succesor as IDataFlowLinkTarget<TOutput>;
                this.SourceBlock.LinkTo<TOutput>(s.TargetBlock);
                s.AddPredecessorCompletion(SourceBlock.Completion);
                //succesor.LinkBuffers();
                var x = succesor as DataFlowTask;
                x.LinkBuffers();
            }
        }

        //public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

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
