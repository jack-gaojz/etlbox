using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, IDataFlowDestination<TInput>
    {
        #region Public properties

        public ITargetBlock<TInput> TargetBlock => TargetAction;

        public void Wait() => Completion.Wait();

        #endregion

        protected virtual ActionBlock<TInput> TargetAction { get; set; }
        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler(); //remove

        protected override Task BufferCompletion => TargetBlock.Completion;

        protected override void CompleteBuffer() => TargetBlock.Complete();

        protected override void FaultBuffer(Exception e) => TargetBlock.Fault(e);

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
    }
}
